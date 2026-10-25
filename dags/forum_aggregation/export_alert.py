import pandas as pd
import os

from .helpers import _resolve_dates, TMP_DIR

OUTPUT_DIR = "/app/output"

def _export_to_csv(**context):
    """Сохраняет агрегаты в CSV."""
    date_from, date_to = _resolve_dates(context)

    result = pd.read_parquet(os.path.join(TMP_DIR, "aggregates.parquet"))

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    # 🛡️ Sentinel: [MEDIUM] Sanitize output filename to prevent path traversal
    # Ensure filename only contains base name and not directory traversal dots
    safe_date_from = os.path.basename(date_from)
    safe_date_to = os.path.basename(date_to)
    filename = f"aggregation_{safe_date_from}_to_{safe_date_to}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    result.to_csv(path, index=False)

    print(f"Exported to {path}")

def _alert_on_anomaly(**context):
    """Логирует предупреждения при аномальных значениях метрик."""
    result = pd.read_parquet(os.path.join(TMP_DIR, "aggregates.parquet"))

    warnings_found = False

    for row in result.itertuples(index=False):
        day = row.day

        if row.anon_messages_pct > 80:
            print(f"[{day}] anon_messages_pct = {row.anon_messages_pct}% (> 80%)")
            warnings_found = True

        if row.total_messages == 0:
            print(f"[{day}] total_messages = 0")
            warnings_found = True

        if row.new_accounts == 0:
            print(f"[{day}] new_accounts = 0")
            warnings_found = True

        if pd.notna(row.avg_session_duration_sec) and row.avg_session_duration_sec < 10:
            print(f"[{day}] avg_session_duration = {row.avg_session_duration_sec}s (< 10s)")
            warnings_found = True

        if pd.notna(row.topics_growth_pct) and abs(row.topics_growth_pct) > 200:
            print(f"[{day}] topics_growth_pct = {row.topics_growth_pct}% (|x| > 200%)")
            warnings_found = True

        if pd.notna(row.d1_retention_pct) and row.d1_retention_pct < 5:
            print(f"[{day}] d1_retention_pct = {row.d1_retention_pct}% (< 5%)")
            warnings_found = True

    if not warnings_found:
        print("No anomalies detected.")
