import pandas as pd
import os
from airflow.providers.postgres.hooks.postgres import PostgresHook
from .helpers import _resolve_dates, TMP_DIR, CONN_ID

def _extract_raw_logs(**context):
    """Выгружает сырые логи из БД за ds и ds-1."""
    ds, prev_ds = _resolve_dates(context)

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    engine = hook.get_sqlalchemy_engine()

    sql = """
        SELECT id, user_id, action_type, response_status, response_code, created_at
        FROM user_logs
        WHERE DATE(created_at) IN (%(ds)s, %(prev_ds)s)
    """
    df = pd.read_sql(sql, engine, params={"ds": ds, "prev_ds": prev_ds})

    os.makedirs(TMP_DIR, exist_ok=True)
    out_path = os.path.join(TMP_DIR, "raw_logs.parquet")

    if df.empty:
        # Пишем пустой DataFrame, но с нужными колонками, чтобы не упало дальше
        cols = ["id", "user_id", "action_type", "response_status", "response_code", "created_at"]
        df = pd.DataFrame(columns=cols)

    df.to_parquet(out_path, index=False)
    print(f"Extracted {len(df)} rows to {out_path}")
