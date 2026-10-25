"""
DAG: forum_log_aggregation

Считает ежедневные метрики (агрегаты) форума на основе user_logs.
Idempotent: можно перезапускать за любой день (ds).

Структура:
  check_data_availability (sensor) →
  extract_raw_logs → validate_data → transform_aggregates →
  [export_to_csv, alert_on_anomaly]
"""
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

from forum_aggregation.sensor import DataAvailabilitySensor
from forum_aggregation.extract import _extract_raw_logs
from forum_aggregation.validate import _validate_data
from forum_aggregation.transform import _transform_aggregates
from forum_aggregation.export_alert import _export_to_csv, _alert_on_anomaly
from forum_aggregation.helpers import CONN_ID

default_args = {
    "owner": "airflow",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="forum_log_aggregation",
    default_args=default_args,
    description="Агрегация логов форума в ежедневные витрины",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    params={"date_from": None, "date_to": None},
    catchup=False,
    tags=["forum", "aggregation"],
) as dag:

    # 1. Сенсор: ждём, пока в БД появятся данные за расчетный день ds
    t1_sensor = DataAvailabilitySensor(
        task_id="check_data_availability",
        conn_id=CONN_ID,
        poke_interval=60,
        timeout=600,
        mode="poke",
    )

    t2_extract = PythonOperator(task_id="extract_raw_logs", python_callable=_extract_raw_logs)
    t3_validate = PythonOperator(task_id="validate_data", python_callable=_validate_data)
    t4_transform = PythonOperator(task_id="transform_aggregates", python_callable=_transform_aggregates)

    t5_export = PythonOperator(task_id="export_to_csv", python_callable=_export_to_csv)
    t6_alert = PythonOperator(task_id="alert_on_anomaly", python_callable=_alert_on_anomaly)

    t1_sensor >> t2_extract >> t3_validate >> t4_transform >> t5_export >> t6_alert
