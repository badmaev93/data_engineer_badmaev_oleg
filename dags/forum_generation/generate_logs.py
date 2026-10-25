import logging
from calendar import monthrange
from datetime import datetime
from dags.forum_generation.helpers import _load, _dump
from dags.forum_generation.actions import (
    gen_first_visits, gen_registrations, gen_returning_logins,
    gen_topics, gen_posts, gen_views_and_deletes, gen_logouts
)

logger = logging.getLogger(__name__)

def _generate_logs(**context):
    """Главная функция генерации логов и активностей."""
    params = context.get("params", {})
    year = params.get("year", 2026)
    month = params.get("month", 10)
    _, days_in_month = monthrange(year, month)

    users_pool = _load("users_pool.json")
    forum = _load("forum.json")
    user_map = {u["id"]: u for u in users_pool}

    logs, topics, posts = [], [], []
    registered, yesterday_registered = [], []
    pool_idx = 0
    available_topics, topic_created_map = [], {}

    logger.info(f"Генерация логов для {year}-{month:02d} (дней: {days_in_month})")

    for day_num in range(1, days_in_month + 1):
        base_dt = datetime(year, month, day_num)
        session_bounds = {}

        logs.extend(gen_first_visits(base_dt))

        reg_logs, registered, today_reg, pool_idx = gen_registrations(base_dt, users_pool, pool_idx, days_in_month - day_num, session_bounds)
        logs.extend(reg_logs)

        logs.extend(gen_returning_logins(base_dt, registered, yesterday_registered, session_bounds))

        t_logs, day_topics = gen_topics(base_dt, forum["id"], session_bounds, user_map, available_topics, topic_created_map)
        logs.extend(t_logs)
        topics.extend(day_topics)

        p_logs, day_posts = gen_posts(base_dt, available_topics, topic_created_map, session_bounds, user_map)
        logs.extend(p_logs)
        posts.extend(day_posts)

        logs.extend(gen_views_and_deletes(base_dt, available_topics, topic_created_map, session_bounds))
        logs.extend(gen_logouts(session_bounds, user_map))

        yesterday_registered = today_reg

    logs.sort(key=lambda x: x["created_at"])
    _dump("registered_users.json", [u for u in users_pool if u.get("joined_at")])
    _dump("topics.json", topics)
    _dump("posts.json", posts)
    _dump("logs.json", logs)

    logger.info(f"Сгенерировано логов: {len(logs)}, тем: {len(topics)}, сообщений: {len(posts)}")
