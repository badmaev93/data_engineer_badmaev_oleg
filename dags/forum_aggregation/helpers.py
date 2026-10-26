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
        return str(date_from), str(date_to)

    ds = context.get("ds")
    if not ds:
        ds = datetime.now().strftime("%Y-%m-%d")

    return ds, ds
