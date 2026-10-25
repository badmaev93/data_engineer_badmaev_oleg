import logging
import pandas as pd
from pandera import Column, Check, DataFrameSchema
from dags.forum_generation.helpers import ACTION_TYPES, _load

logger = logging.getLogger(__name__)

def _validate_generated_data(**context):
    """Валидация сгенерированных данных через pandera + бизнес-правила."""
    logs_data = _load("logs.json")
    users_data = _load("registered_users.json")
    topics_data = _load("topics.json")
    posts_data = _load("posts.json")

    df = pd.DataFrame(logs_data)

    schema = DataFrameSchema({
        "id":              Column(str, nullable=False),
        "action_type":     Column(str, Check.isin(ACTION_TYPES)),
        "description":     Column(str, nullable=False),
        "response_status": Column(str, Check.isin(["success", "error"])),
        "response_code":   Column("Int64", Check.in_range(100, 599)),
        "created_at":      Column(str, nullable=False),
    })
    schema.validate(df, lazy=True)

    logout_uids = set(df[df["action_type"] == "logout"]["user_id"].dropna())
    login_uids = set(df[df["action_type"] == "login"]["user_id"].dropna())
    assert not (logout_uids - login_uids), "Logout without login"

    ok_topics = df[(df["action_type"] == "create_topic") & (df["response_status"] == "success")]
    assert ok_topics["user_id"].notna().all(), "Success create_topic null user_id"

    err_topics = df[(df["action_type"] == "create_topic") & (df["response_status"] == "error")]
    assert err_topics["user_id"].isna().all(), "Error create_topic not null user_id"

    df["day"] = pd.to_datetime(df["created_at"]).dt.date
    daily = df.groupby(["day", "action_type"]).size().unstack(fill_value=0)
    for action in ACTION_TYPES:
        if action in daily.columns:
            assert int(daily[action].min()) >= 5, f"{action} < 5/day"

    err_daily = err_topics.groupby(pd.to_datetime(err_topics["created_at"]).dt.date).size()
    assert err_daily.min() >= 2, "Error create_topic < 2/day"

    post_logs = df[(df["action_type"] == "create_post") & (df["response_status"] == "success")]
    n_auth = int(post_logs["user_id"].notna().sum())
    n_anon = int(post_logs["user_id"].isna().sum())
    total = n_auth + n_anon
    ratio = n_anon / total if total > 0 else 0
    assert 0.3 <= ratio <= 0.7, "Anon post ratio not [30%-70%]"

    df_auth = df[df["user_id"].notna()].copy()
    df_auth["ts"] = pd.to_datetime(df_auth["created_at"])
    df_auth["day"] = df_auth["ts"].dt.date
    df_auth = df_auth.sort_values("ts")

    for (day, uid), u_actions in df_auth.groupby(["day", "user_id"]):
        logins = u_actions[u_actions["action_type"] == "login"]["ts"].tolist()
        logouts = u_actions[u_actions["action_type"] == "logout"]["ts"].tolist()
        assert len(logins) > 0 and len(logouts) > 0 and len(logins) == len(logouts), "Mismatched logins/logouts"
        for li, lo in zip(logins, logouts):
            assert li < lo, "Login not before logout"
            outside = u_actions[(u_actions["action_type"].isin(["create_topic", "create_post", "delete_topic", "view_topic"])) & ((u_actions["ts"] < li) | (u_actions["ts"] > lo))]
            assert len(outside) == 0, "Actions outside session"

    topic_map = {t["id"]: pd.to_datetime(t["created_at"]) for t in topics_data}
    for p in posts_data:
        tc = topic_map.get(p["topic_id"])
        if tc is not None:
            assert pd.to_datetime(p["created_at"]) >= tc, "Post before topic"

    logger.info("Валидация успешно завершена")
