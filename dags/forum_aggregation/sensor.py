from airflow.sensors.base import BaseSensorOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime

class DataAvailabilitySensor(BaseSensorOperator):
    """
    Проверяет, есть ли записи в user_logs за расчетный день (ds).
    """
    def __init__(self, conn_id: str, **kwargs):
        super().__init__(**kwargs)
        self.conn_id = conn_id

    def poke(self, context) -> bool:
        ds = context.get("ds")
        if not ds:
            ds = datetime.now().strftime("%Y-%m-%d")

        hook = PostgresHook(postgres_conn_id=self.conn_id)
        sql = "SELECT 1 FROM user_logs WHERE DATE(created_at) = %s LIMIT 1"
        records = hook.get_records(sql, parameters=(ds,))
        if records and len(records) > 0:
            self.log.info(f"Данные за {ds} найдены.")
            return True
        else:
            self.log.info(f"Данных за {ds} пока нет.")
            return False
