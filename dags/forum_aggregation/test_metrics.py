import pandas as pd
from datetime import datetime
from dags.forum_aggregation.metrics import calc_avg_session_duration

def test_calc_avg_session_duration_basic():
    rows = [
        {"user_id": 1, "action_type": "login", "created_at": datetime(2023, 1, 1, 10, 0, 0)},
        {"user_id": 1, "action_type": "logout", "created_at": datetime(2023, 1, 1, 10, 30, 0)},  # 1800s
        {"user_id": 2, "action_type": "login", "created_at": datetime(2023, 1, 1, 11, 0, 0)},
        {"user_id": 2, "action_type": "logout", "created_at": datetime(2023, 1, 1, 12, 0, 0)},   # 3600s
        {"user_id": 1, "action_type": "login", "created_at": datetime(2023, 1, 1, 13, 0, 0)},
        {"user_id": 1, "action_type": "logout", "created_at": datetime(2023, 1, 1, 13, 15, 0)},  # 900s
    ]
    df = pd.DataFrame(rows)

    # Expected average: (1800 + 3600 + 900) / 3 = 6300 / 3 = 2100.0
    res = calc_avg_session_duration(df)
    assert res == 2100.0, f"Expected 2100.0, got {res}"

def test_calc_avg_session_duration_empty():
    rows = []
    df = pd.DataFrame(rows, columns=["user_id", "action_type", "created_at"])
    res = calc_avg_session_duration(df)
    assert res is None, f"Expected None, got {res}"

def test_calc_avg_session_duration_no_logins_or_logouts():
    rows = [
        {"user_id": 1, "action_type": "create_post", "created_at": datetime(2023, 1, 1, 10, 0, 0)},
    ]
    df = pd.DataFrame(rows)
    res = calc_avg_session_duration(df)
    assert res is None, f"Expected None, got {res}"

def test_calc_avg_session_duration_unmatched():
    rows = [
        {"user_id": 1, "action_type": "login", "created_at": datetime(2023, 1, 1, 10, 0, 0)},
        {"user_id": 1, "action_type": "logout", "created_at": datetime(2023, 1, 1, 10, 30, 0)},  # 1800s
        {"user_id": 2, "action_type": "login", "created_at": datetime(2023, 1, 1, 11, 0, 0)},
        # Missing logout for user 2
    ]
    df = pd.DataFrame(rows)
    # Merged dataset should only match user 1's full session
    res = calc_avg_session_duration(df)
    assert res == 1800.0, f"Expected 1800.0, got {res}"
