"""
DAG: forum_log_aggregation

Ежедневная агрегация логов форума → итоговый CSV.

Расписание: @daily (обрабатывает данные за предыдущий день).
Ручной запуск: передать date_from / date_to через Airflow Params.

Структура:
  check_data_availability → extract_raw_logs → validate_data
      → transform_aggregates → export_to_csv → alert_on_anomaly
"""

import os
from datetime import datetime, timedelta

import pandas as pd
import pandera as pa
from pandera import Column, Check, DataFrameSchema

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─── Constants ───────────────────────────────────────────────────────────────

CONN_ID = "forum_db"
TMP_DIR = "/tmp/forum_agg"
OUTPUT_DIR = "/opt/airflow/output"


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _resolve_dates(context):
    """Возвращает (date_from, date_to) как строки YYYY-MM-DD.

    Если переданы params.date_from / date_to — использует их.
    Иначе — logical_date (== вчера для @daily).
    """
    p = context["params"]
    if p.get("date_from") and p.get("date_to"):
        return str(p["date_from"]), str(p["date_to"])
    ds = context["ds"]  # logical_date как YYYY-MM-DD
    return ds, ds


# ─── Sensor: check_data_availability ────────────────────────────────────────

class DataAvailabilitySensor(BaseSensorOperator):
    """Ожидает наличия данных в user_logs за указанный период."""

    def __init__(self, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def poke(self, context) -> bool:
        date_from, date_to = _resolve_dates(context)
        hook = PostgresHook(postgres_conn_id=self.conn_id)
        row = hook.get_first(
            """
            SELECT COUNT(*)
            FROM user_logs
            WHERE created_at >= %s::timestamptz
              AND created_at < (%s::date + INTERVAL '1 day')::timestamptz
            """,
            parameters=(date_from, date_to),
        )
        count = row[0] if row else 0
        self.log.info("Found %d records for %s .. %s", count, date_from, date_to)
        return count > 0


# ─── Task: extract_raw_logs ─────────────────────────────────────────────────

def _extract_raw_logs(**context):
    """Извлекает сырые логи за период (плюс 1 предыдущий день для расчёта динамики и D1 retention)."""
    date_from, date_to = _resolve_dates(context)

    # Захватываем date_from - 1 день, чтобы рассчитать topics_growth_pct и d1_retention_pct
    ext_from = (
        datetime.strptime(date_from, "%Y-%m-%d").date() - timedelta(days=1)
    ).isoformat()

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    sql = """
        SELECT id, user_id, session_or_ip, action_type, description,
               target_object_id, response_status, response_code, created_at
        FROM user_logs
        WHERE created_at >= %s::timestamptz
          AND created_at < (%s::date + INTERVAL '1 day')::timestamptz
        ORDER BY created_at
    """
    df = hook.get_pandas_df(sql, parameters=(ext_from, date_to))

    os.makedirs(TMP_DIR, exist_ok=True)
    path = os.path.join(TMP_DIR, "raw_logs.parquet")
    df.to_parquet(path, index=False)

    print(
        f"✅ Extracted {len(df)} rows for {ext_from} .. {date_to} (target: {date_from} .. {date_to})"
    )


# ─── Task: validate_data ────────────────────────────────────────────────────

def _validate_data(**context):
    """Pandera-валидация извлечённых данных."""
    df = pd.read_parquet(os.path.join(TMP_DIR, "raw_logs.parquet"))

    schema = DataFrameSchema({
        "id": Column(str, nullable=False),
        "action_type": Column(
            str,
            Check.isin([
                "first_visit",
                "register",
                "login",
                "logout",
                "create_topic",
                "view_topic",
                "delete_topic",
                "create_post",
            ]),
            nullable=False,
        ),
        "description": Column(str, nullable=False),
        "response_status": Column(
            str, Check.isin(["success", "error"]), nullable=False
        ),
        "response_code": Column(
            "Int64", Check.in_range(100, 599), nullable=False, coerce=True
        ),
        "created_at": Column("datetime64[ns, UTC]", nullable=False, coerce=True),
    })
    schema.validate(df, lazy=True)

    # Проверка последовательности и бизнес-правил (п. 5 ТЗ)
    ok_topics = df[
        (df["action_type"] == "create_topic")
        & (df["response_status"] == "success")
    ]
    assert (
        ok_topics["user_id"].notna().all()
    ), "Found successful create_topic without user_id"

    print("✅ Data validation passed")


# ─── Task: transform_aggregates ─────────────────────────────────────────────

def _transform_aggregates(**context):
    """Вычисляет ежедневные метрики из логов."""
    date_from, date_to = _resolve_dates(context)
    d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    d_to = datetime.strptime(date_to, "%Y-%m-%d").date()

    df = pd.read_parquet(os.path.join(TMP_DIR, "raw_logs.parquet"))
    df["day"] = df["created_at"].dt.date

    # Агрегируем только целевые дни [d_from .. d_to], используя prev_day из df
    target_days = sorted([d for d in df["day"].unique() if d_from <= d <= d_to])
    rows = []

    for day in target_days:
        day_df = df[df["day"] == day]
        success = day_df[day_df["response_status"] == "success"]

        # ── new_accounts ─────────────────────────────────────────────────
        new_accounts = int((success["action_type"] == "register").sum())

        # ── total_messages & anon_messages_pct ───────────────────────────
        msg = success[success["action_type"] == "create_post"]
        total_messages = len(msg)
        anon_messages = int(msg["user_id"].isna().sum())
        anon_messages_pct = (
            round(anon_messages / total_messages * 100, 2)
            if total_messages
            else 0.0
        )

        # ── topics_growth_pct ────────────────────────────────────────────
        topics_today = int((success["action_type"] == "create_topic").sum())

        prev_day = day - timedelta(days=1)
        prev_df = df[
            (df["day"] == prev_day) & (df["response_status"] == "success")
        ]
        topics_yesterday = (
            int((prev_df["action_type"] == "create_topic").sum())
            if len(prev_df)
            else 0
        )

        if topics_yesterday > 0:
            topics_growth_pct = round(
                (topics_today - topics_yesterday) / topics_yesterday * 100, 2
            )
        else:
            topics_growth_pct = None

        # ── avg_session_duration_sec ─────────────────────────────────────
        logins = success[success["action_type"] == "login"]
        logouts = success[success["action_type"] == "logout"]

        durations = []
        for uid in logins["user_id"].dropna().unique():
            u_logins = (
                logins[logins["user_id"] == uid]
                .sort_values("created_at")["created_at"]
                .tolist()
            )
            u_logouts = (
                logouts[logouts["user_id"] == uid]
                .sort_values("created_at")["created_at"]
                .tolist()
            )
            for li_ts, lo_ts in zip(u_logins, u_logouts):
                dur = (lo_ts - li_ts).total_seconds()
                if dur > 0:
                    durations.append(dur)

        avg_session = (
            round(sum(durations) / len(durations), 2) if durations else None
        )

        # ── d1_retention_pct ─────────────────────────────────────────────
        reg_yesterday = (
            df[
                (df["day"] == prev_day)
                & (df["action_type"] == "register")
                & (df["response_status"] == "success")
            ]["user_id"]
            .dropna()
            .unique()
        )

        if len(reg_yesterday) > 0:
            active_today = set(day_df["user_id"].dropna().unique())
            returned = sum(1 for uid in reg_yesterday if uid in active_today)
            d1_retention_pct = round(returned / len(reg_yesterday) * 100, 2)
        else:
            d1_retention_pct = None

        rows.append({
            "day": str(day),
            "new_accounts": new_accounts,
            "anon_messages_pct": anon_messages_pct,
            "total_messages": total_messages,
            "topics_growth_pct": topics_growth_pct,
            "avg_session_duration_sec": avg_session,
            "d1_retention_pct": d1_retention_pct,
        })

    result = pd.DataFrame(rows)
    result.to_parquet(os.path.join(TMP_DIR, "aggregates.parquet"), index=False)
    print(f"✅ Aggregated {len(result)} day(s)")
    print(result.to_string(index=False))


# ─── Task: export_to_csv ────────────────────────────────────────────────────

def _export_to_csv(**context):
    """Сохраняет агрегаты в CSV."""
    date_from, date_to = _resolve_dates(context)

    result = pd.read_parquet(os.path.join(TMP_DIR, "aggregates.parquet"))

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    filename = f"aggregation_{date_from}_to_{date_to}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    result.to_csv(path, index=False)

    print(f"✅ Exported to {path}")


# ─── Task: alert_on_anomaly ─────────────────────────────────────────────────

def _alert_on_anomaly(**context):
    """Логирует предупреждения при аномальных значениях метрик."""
    result = pd.read_parquet(os.path.join(TMP_DIR, "aggregates.parquet"))

    warnings_found = False

    for _, row in result.iterrows():
        day = row["day"]

        if row["anon_messages_pct"] > 80:
            print(f"⚠️  [{day}] anon_messages_pct = {row['anon_messages_pct']}% (> 80%)")
            warnings_found = True

        if row["total_messages"] == 0:
            print(f"⚠️  [{day}] total_messages = 0")
            warnings_found = True

        if row["new_accounts"] == 0:
            print(f"⚠️  [{day}] new_accounts = 0")
            warnings_found = True

        if row["avg_session_duration_sec"] is not None and row["avg_session_duration_sec"] < 10:
            print(f"⚠️  [{day}] avg_session_duration = {row['avg_session_duration_sec']}s (< 10s)")
            warnings_found = True

        if row["topics_growth_pct"] is not None and abs(row["topics_growth_pct"]) > 200:
            print(f"⚠️  [{day}] topics_growth_pct = {row['topics_growth_pct']}% (|x| > 200%)")
            warnings_found = True

        if row["d1_retention_pct"] is not None and row["d1_retention_pct"] < 5:
            print(f"⚠️  [{day}] d1_retention_pct = {row['d1_retention_pct']}% (< 5%)")
            warnings_found = True

    if not warnings_found:
        print("✅ No anomalies detected")


# ─── DAG definition ─────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=3),
}

with DAG(
    dag_id="forum_log_aggregation",
    default_args=default_args,
    description="Ежедневная агрегация логов форума → CSV",
    start_date=datetime(2026, 10, 1),
    schedule="@daily",
    catchup=False,
    tags=["forum", "aggregation"],
    params={
        "date_from": "",      # YYYY-MM-DD или пусто (= logical_date)
        "date_to": "",
    },
) as dag:

    check = DataAvailabilitySensor(
        task_id="check_data_availability",
        conn_id=CONN_ID,
        mode="poke",
        timeout=600,
        poke_interval=30,
    )

    extract   = PythonOperator(task_id="extract_raw_logs",      python_callable=_extract_raw_logs)
    validate  = PythonOperator(task_id="validate_data",          python_callable=_validate_data)
    transform = PythonOperator(task_id="transform_aggregates",   python_callable=_transform_aggregates)
    export    = PythonOperator(task_id="export_to_csv",          python_callable=_export_to_csv)
    alert     = PythonOperator(task_id="alert_on_anomaly",       python_callable=_alert_on_anomaly)

    check >> extract >> validate >> transform >> export >> alert
