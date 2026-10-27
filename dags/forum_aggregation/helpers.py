import json
import os
from datetime import datetime, timedelta

TMP_DIR = "/tmp/forum_agg"
CONN_ID = "forum_db"
OUTPUT_DIR = "/opt/airflow/output"

def _resolve_dates(context):
    params = context.get("params", {})
    date_from = params.get("date_from")
    date_to = params.get("date_to")

    if date_from and date_to:
        date_from_str = str(date_from)
        date_to_str = str(date_to)
        # 🔒 Security enhancement: Validate user input format
        datetime.strptime(date_from_str, "%Y-%m-%d")
        datetime.strptime(date_to_str, "%Y-%m-%d")
        return date_from_str, date_to_str

    ds = context.get("ds")
    if not ds:
        ds = datetime.now().strftime("%Y-%m-%d")

    return ds, ds
