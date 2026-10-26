import logging
from calendar import monthrange
from datetime import datetime
from .helpers import _load, _dump
from .session_actions import gen_first_visits, gen_registrations, gen_returning_logins, gen_logouts
from .content_actions import gen_topics, gen_posts, gen_views_and_deletes

logger = logging.getLogger(__name__)

def _generate_logs(**context):
    """Координирует посуточную генерацию логов форума за выбранный месяц."""
    params = context.get("params", {})
    year = params.get("year", 2026)
    month = params.get("month", 10)
    _, days_in_month = monthrange(year, month)

    users_pool = _load("users_pool.json")
    forum = _load("forum.json")
    user_map = {u["id"]: u for u in users_pool}

    logs, topics, posts = [], [], []
    registered, yesterday_reg, available_topics = [], [], []
    topic_created_map = {}
    pool_idx = 0

    for day_num in range(1, days_in_month + 1):
        base_dt = datetime(year, month, day_num)
        days_left = days_in_month - day_num
        session_bounds = {}

        logs.extend(gen_first_visits(base_dt))
        today_reg, reg_logs, pool_idx = gen_registrations(base_dt, users_pool, pool_idx, days_left, session_bounds)
        registered.extend(today_reg)
        logs.extend(reg_logs)
        logs.extend(gen_returning_logins(base_dt, registered, yesterday_reg, session_bounds))

        new_topics, topic_logs = gen_topics(base_dt, forum["id"], session_bounds, user_map, available_topics, topic_created_map)
        topics.extend(new_topics)
        logs.extend(topic_logs)

        new_posts, post_logs = gen_posts(base_dt, available_topics, topic_created_map, session_bounds, user_map)
        posts.extend(new_posts)
        logs.extend(post_logs)

        logs.extend(gen_views_and_deletes(base_dt, available_topics, topic_created_map, session_bounds))
        logs.extend(gen_logouts(session_bounds, user_map))
        yesterday_reg = today_reg

    logs.sort(key=lambda x: x["created_at"])
    _dump("registered_users.json", [u for u in users_pool if u.get("joined_at")])
    _dump("topics.json", topics)
    _dump("posts.json", posts)
    _dump("logs.json", logs)
    logger.info("Сгенерировано: %d пользователей, %d тем, %d сообщений, %d логов", len(registered), len(topics), len(posts), len(logs))
