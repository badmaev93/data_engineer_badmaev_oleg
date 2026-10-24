"""
Shared pytest configuration and Airflow mock setup.
Allows running DAG code directly on the host without full Apache Airflow installation.
"""

import sys
from types import ModuleType

# Простой mock-модуль без наследования от MagicMock во избежание рекурсии
class _FakeObject:
    def __init__(self, *args, **kwargs):
        pass
    def __getattr__(self, name):
        return _FakeObject()
    def __call__(self, *args, **kwargs):
        return _FakeObject()
    def __enter__(self):
        return self
    def __exit__(self, *args):
        pass
    def __rshift__(self, other):
        return other
    def __lshift__(self, other):
        return other

modules_to_mock = [
    "airflow",
    "airflow.models",
    "airflow.operators",
    "airflow.operators.python",
    "airflow.sensors",
    "airflow.sensors.base",
    "airflow.providers",
    "airflow.providers.postgres",
    "airflow.providers.postgres.hooks",
    "airflow.providers.postgres.hooks.postgres",
]

for mod in modules_to_mock:
    if mod not in sys.modules:
        m = ModuleType(mod)
        sys.modules[mod] = m

# Классы Airflow
class FakeDAG(_FakeObject):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

class FakePythonOperator(_FakeObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.python_callable = kwargs.get("python_callable")
        self.op_kwargs = kwargs.get("op_kwargs", {})

class FakeBaseSensorOperator(_FakeObject):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.log = _FakeObject()

class FakePostgresHook(_FakeObject):
    pass

sys.modules["airflow"].DAG = FakeDAG
sys.modules["airflow.operators.python"].PythonOperator = FakePythonOperator
sys.modules["airflow.sensors.base"].BaseSensorOperator = FakeBaseSensorOperator
sys.modules["airflow.providers.postgres.hooks.postgres"].PostgresHook = FakePostgresHook
