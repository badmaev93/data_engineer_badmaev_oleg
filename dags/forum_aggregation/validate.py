import pandas as pd
import os
from pandera import DataFrameSchema, Column, Check

from .helpers import _resolve_dates, TMP_DIR

ACTION_TYPES = [
    "first_visit", "register", "login", "logout",
    "create_topic", "view_topic", "delete_topic", "create_post"
]

def _validate_data(**context):
    """Строгая валидация сырых данных через pandera (Fail-fast)."""
    in_path = os.path.join(TMP_DIR, "raw_logs.parquet")
    df = pd.read_parquet(in_path)

    if df.empty:
        print("Data is empty, nothing to validate.")
        return

    schema = DataFrameSchema({
        "id": Column(str, nullable=False),
        "user_id": Column(str, nullable=True),
        "action_type": Column(str, Check.isin(ACTION_TYPES), nullable=False),
        "response_status": Column(str, Check.isin(["success", "error"]), nullable=False),
        "response_code": Column(int, Check.in_range(100, 599), nullable=False),
        # coerce=True для безопасного парсинга дат из разных форматов/типов БД
        "description": Column(str, nullable=False),
        "created_at": Column("datetime64[ns, UTC]", coerce=True, nullable=False),
    })

    schema.validate(df, lazy=True)

    # Проверка последовательности и бизнес-правил (п. 5 ТЗ)
    ok_topics = df[
        (df["action_type"] == "create_topic")
        & (df["response_status"] == "success")
    ]
    assert (
        ok_topics["user_id"].notna().all()
    ), "Found successful create_topic without user_id"

    print("Data validation passed")
