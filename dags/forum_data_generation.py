from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator

from forum_generation.generate_users import _generate_users
from forum_generation.generate_logs import _generate_logs
from forum_generation.validate_data import _validate_generated_data
from forum_generation.load_postgres import _load_to_postgres

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="forum_data_generation",
    default_args=default_args,
    description="Генерация синтетических данных форума за один месяц",
    start_date=datetime(2026, 10, 1),
    schedule=None,
    catchup=False,
    tags=["forum", "generation"],
    params={
        "year": 2026,
        "month": 10,
        "n_users": 250,
    },
) as dag:
    t1 = PythonOperator(task_id="generate_users", python_callable=_generate_users)
    t2 = PythonOperator(task_id="generate_logs", python_callable=_generate_logs)
    t3 = PythonOperator(task_id="validate_generated_data", python_callable=_validate_generated_data)
    t4 = PythonOperator(task_id="load_to_postgres", python_callable=_load_to_postgres)

    t1 >> t2 >> t3 >> t4
