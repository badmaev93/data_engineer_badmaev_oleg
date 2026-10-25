import pandas as pd
import os
from datetime import datetime, timedelta
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .helpers import _resolve_dates, TMP_DIR, CONN_ID

def _extract_raw_logs(**context):
    """Выгружает сырые логи из БД за ds и ds-1."""
    date_from, date_to = _resolve_dates(context)

    dt_from = datetime.strptime(date_from, "%Y-%m-%d")
    ext_from = (dt_from - timedelta(days=1)).strftime("%Y-%m-%d")

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    sql = """
        SELECT id, user_id, session_or_ip, action_type, description, target_object_id, response_status, response_code, created_at
        FROM user_logs
        WHERE created_at >= %(ext_from)s::timestamptz AND created_at < (%(date_to)s::date + INTERVAL '1 day')::timestamptz
    """
    df = pd.read_sql(sql, engine, params={"ext_from": ext_from, "date_to": date_to})

    os.makedirs(TMP_DIR, exist_ok=True)
    out_path = os.path.join(TMP_DIR, "raw_logs.parquet")

    if df.empty:
        # Пишем пустой DataFrame, но с нужными колонками, чтобы не упало дальше
        cols = ["id", "user_id", "session_or_ip", "action_type", "description", "target_object_id", "response_status", "response_code", "created_at"]
        df = pd.DataFrame(columns=cols)

    df.to_parquet(out_path, index=False)
    print(f"Извлечено {len(df)} строк в {out_path}")
