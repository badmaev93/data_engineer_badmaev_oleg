import logging
import pandas as pd
import os
from datetime import datetime, timedelta

from .helpers import _resolve_dates, TMP_DIR
from .metrics import build_daily_metrics

def _transform_aggregates(**context):
    """Вычисляет ежедневные метрики из логов."""
    date_from, date_to = _resolve_dates(context)
    d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    d_to = datetime.strptime(date_to, "%Y-%m-%d").date()

    df = pd.read_parquet(os.path.join(TMP_DIR, "raw_logs.parquet"))
    df["day"] = df["created_at"].dt.date

    target_days = sorted([d for d in df["day"].unique() if d_from <= d <= d_to])
    rows = []

    for day in target_days:
        day_df = df[df["day"] == day]
        prev_df = df[df["day"] == day - timedelta(days=1)]
        rows.append(build_daily_metrics(day, day_df, prev_df))

    result = pd.DataFrame(rows)
    result.to_parquet(os.path.join(TMP_DIR, "aggregates.parquet"), index=False)
    logging.getLogger(__name__).info("Агрегировано %d дней", len(result))
