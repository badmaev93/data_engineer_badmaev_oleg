import logging
import pandas as pd
import os

from .helpers import _resolve_dates, TMP_DIR, OUTPUT_DIR

def _export_to_csv(**context):
    """Сохраняет агрегаты в CSV."""
    date_from, date_to = _resolve_dates(context)

    result = pd.read_parquet(os.path.join(TMP_DIR, "aggregates.parquet"))

    os.makedirs(OUTPUT_DIR, exist_ok=True)
    safe_date_from = os.path.basename(date_from)
    safe_date_to = os.path.basename(date_to)
    filename = f"aggregation_{safe_date_from}_to_{safe_date_to}.csv"
    path = os.path.join(OUTPUT_DIR, filename)
    result.to_csv(path, index=False)

    logging.getLogger(__name__).info("Экспортировано в %s", path)

def _alert_on_anomaly(**context):
    """Логирует предупреждения при аномальных значениях метрик."""
    result = pd.read_parquet(os.path.join(TMP_DIR, "aggregates.parquet"))

    warnings_found = False
    logger = logging.getLogger(__name__)

    for row in result.itertuples(index=False):
        day = row.day

        if row.anon_messages_pct > 80:
            logger.info("[%s] anon_messages_pct = %s%% (> 80%%)", day, row.anon_messages_pct)
            warnings_found = True

        if row.total_messages == 0:
            logger.info("[%s] total_messages = 0", day)
            warnings_found = True

        if row.new_accounts == 0:
            logger.info("[%s] new_accounts = 0", day)
            warnings_found = True

        if pd.notna(row.avg_session_duration_sec) and row.avg_session_duration_sec < 10:
            logger.info("[%s] avg_session_duration = %ss (< 10s)", day, row.avg_session_duration_sec)
            warnings_found = True

        if pd.notna(row.topics_growth_pct) and abs(row.topics_growth_pct) > 200:
            logger.info("[%s] topics_growth_pct = %s%% (|x| > 200%%)", day, row.topics_growth_pct)
            warnings_found = True

        if pd.notna(row.d1_retention_pct) and row.d1_retention_pct < 5:
            logger.info("[%s] d1_retention_pct = %s%% (< 5%%)", day, row.d1_retention_pct)
            warnings_found = True

    if not warnings_found:
        logger.info("Аномалии не обнаружены.")