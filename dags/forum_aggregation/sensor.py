from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime
from .helpers import _resolve_dates

class DataAvailabilitySensor(BaseSensorOperator):
    """
    Проверяет, есть ли записи в user_logs за расчетный день (ds).
    """
    def __init__(self, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def poke(self, context) -> bool:
        date_from, date_to = _resolve_dates(context)

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        sql = "SELECT 1 FROM user_logs WHERE created_at >= %s::timestamptz AND created_at < (%s::date + INTERVAL '1 day')::timestamptz LIMIT 1"
        records = hook.get_records(sql, parameters=(date_from, date_to))
        if records and len(records) > 0:
            self.log.info(f"Данные с {date_from} по {date_to} найдены.")
            return True
        else:
            self.log.info(f"Данных с {date_from} по {date_to} пока нет.")
            return False
