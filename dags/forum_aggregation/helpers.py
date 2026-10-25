import json
import os
from datetime import datetime, timedelta

TMP_DIR = "/tmp/forum_agg"
CONN_ID = "forum_db"

def _resolve_dates(context):
    ds = context.get("ds")
    if not ds:
        ds = datetime.now().strftime("%Y-%m-%d")
    dt = datetime.strptime(ds, "%Y-%m-%d")
    prev_dt = dt - timedelta(days=1)
    return ds, prev_dt.strftime("%Y-%m-%d")
