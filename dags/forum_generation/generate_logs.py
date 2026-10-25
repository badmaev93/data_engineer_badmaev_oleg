import random
import uuid
from calendar import monthrange
from datetime import datetime, timedelta
from dags.forum_generation.helpers import fake, _dump, _load, _log_entry

def _gen_first_visits(logs, base):
    """Генерация первых визитов (анонимы)."""
    for _ in range(random.randint(5, 15)):
        ts = base + timedelta(seconds=random.randint(0, 86399))
        logs.append(_log_entry(None, "first_visit", "Первый заход на форум", ts))

def _gen_registrations(logs, base, users_pool, pool_idx, days_in_month, day_num, registered, today_registered, session_bounds):
    """Генерация регистраций и авто-логинов."""
    remaining_days = days_in_month - day_num
    remaining_users = len(users_pool) - pool_idx
    max_today = remaining_users - remaining_days * 5
    n_reg = max(5, min(random.randint(5, 8), max_today))

    for _ in range(n_reg):
        if pool_idx >= len(users_pool):
            break
        u = users_pool[pool_idx]
        pool_idx += 1
        reg_ts = base + timedelta(seconds=random.randint(60, 25200))
        u["joined_at"] = reg_ts.isoformat()
        registered.append(u)
        today_registered.append(u)
        logs.append(_log_entry(u["id"], "register", f"Регистрация пользователя {u['username']}", reg_ts, code=201))

        login_ts = reg_ts + timedelta(seconds=random.randint(1, 30))
        session_dur = random.randint(7200, 28800)
        logout_ts = min(login_ts + timedelta(seconds=session_dur), base + timedelta(hours=23, minutes=55))
        session_bounds[u["id"]] = (login_ts, logout_ts)
        logs.append(_log_entry(u["id"], "login", f"Авто-логин {u['username']}", login_ts))

    return pool_idx

def _gen_logins(logs, base, registered, yesterday_registered, session_bounds):
    """Генерация логинов (в т.ч. D1 retention)."""
    d1_returns = []
    if yesterday_registered:
        n_d1 = max(1, min(len(yesterday_registered), random.randint(2, max(2, len(yesterday_registered) - 1))))
        d1_returns = random.sample(yesterday_registered, n_d1)

    other_candidates = [u for u in registered if u["id"] not in session_bounds and u not in d1_returns]
    random.shuffle(other_candidates)

    existing_logins = d1_returns + other_candidates[:random.randint(3, 7)]
    for u in existing_logins:
        login_ts = base + timedelta(seconds=random.randint(3600, 50400))
        session_dur = random.randint(7200, 32400)
        logout_ts = min(login_ts + timedelta(seconds=session_dur), base + timedelta(hours=23, minutes=55))
        session_bounds[u["id"]] = (login_ts, logout_ts)
        logs.append(_log_entry(u["id"], "login", f"Вход {u['username']}", login_ts))

def _gen_create_topic_success(logs, topics, forum, user_map, active_uids, session_bounds, available_topics, topic_created_map):
    """Успешное создание тем."""
    n_ct = random.randint(7, 12)
    for _ in range(min(n_ct, len(active_uids))):
        uid = random.choice(active_uids)
        u = user_map[uid]
        tid = str(uuid.uuid4())
        li_ts, lo_ts = session_bounds[uid]
        span = int((lo_ts - li_ts).total_seconds())
        sec_offset = random.randint(30, max(31, int(span * 0.35)))
        ts = li_ts + timedelta(seconds=sec_offset)

        topic = {
            "id": tid, "forum_id": forum["id"], "author_id": uid,
            "slug": fake.slug()[:200], "title": fake.sentence(nb_words=6)[:255],
            "status": "open", "created_at": ts.isoformat(), "last_post_at": ts.isoformat(),
        }
        topics.append(topic)
        available_topics.append(tid)
        topic_created_map[tid] = ts
        logs.append(_log_entry(uid, "create_topic", f"Создана тема", ts, target=tid, code=201))

def _gen_create_topic_error(logs, base):
    """Ошибки создания тем."""
    for _ in range(random.randint(2, 5)):
        ts = base + timedelta(seconds=random.randint(28800, 72000))
        logs.append(_log_entry(None, "create_topic", "Ошибка: без логина", ts, status="error", code=401))

def _gen_create_posts(logs, posts, base, user_map, active_uids, session_bounds, available_topics, topic_created_map):
    """Генерация ответов в темах (auth/anon)."""
    n_post = random.randint(14, 24)
    n_auth_posts = n_post // 2 + random.randint(0, 1)
    for i in range(n_post):
        if not available_topics:
            break
        tid = random.choice(available_topics)
        t_created = topic_created_map[tid]
        pid = str(uuid.uuid4())

        if i < n_auth_posts and active_uids:
            eligible_users = [uid for uid in active_uids if session_bounds[uid][1] > max(session_bounds[uid][0], t_created) + timedelta(seconds=30)]
            if eligible_users:
                uid = random.choice(eligible_users)
                u = user_map[uid]
                li_ts, lo_ts = session_bounds[uid]
                start_sec = max(int((li_ts - base).total_seconds()), int((t_created - base).total_seconds())) + 10
                end_sec = int((lo_ts - base).total_seconds()) - 10
                ts = base + timedelta(seconds=random.randint(start_sec, max(start_sec + 1, end_sec)))

                posts.append({"id": pid, "topic_id": tid, "author_id": uid, "author_guest_name": None, "body": fake.paragraph(), "created_at": ts.isoformat()})
                logs.append(_log_entry(uid, "create_post", f"Сообщение {u['username']}", ts, target=pid, code=201))
                continue

        guest = fake.first_name()
        start_sec = max(0, int((t_created - base).total_seconds()) + 10)
        ts = base + timedelta(seconds=random.randint(start_sec, 86300))
        posts.append({"id": pid, "topic_id": tid, "author_id": None, "author_guest_name": guest, "body": fake.paragraph(), "created_at": ts.isoformat()})
        logs.append(_log_entry(None, "create_post", f"Сообщение {guest}", ts, target=pid, code=201))

def _gen_view_topics(logs, base, active_uids, session_bounds, available_topics, topic_created_map):
    """Генерация просмотров тем."""
    for _ in range(random.randint(8, 20)):
        if not available_topics:
            break
        tid = random.choice(available_topics)
        t_created = topic_created_map[tid]

        if random.random() > 0.5 and active_uids:
            eligible_users = [uid for uid in active_uids if session_bounds[uid][1] > max(session_bounds[uid][0], t_created) + timedelta(seconds=20)]
            if eligible_users:
                uid = random.choice(eligible_users)
                li_ts, lo_ts = session_bounds[uid]
                start_sec = max(int((li_ts - base).total_seconds()), int((t_created - base).total_seconds())) + 5
                end_sec = int((lo_ts - base).total_seconds()) - 5
                ts = base + timedelta(seconds=random.randint(start_sec, max(start_sec + 1, end_sec)))
                logs.append(_log_entry(uid, "view_topic", f"Просмотр темы", ts, target=tid))
                continue

        start_sec = max(0, int((t_created - base).total_seconds()) + 5)
        ts = base + timedelta(seconds=random.randint(start_sec, 86300))
        logs.append(_log_entry(None, "view_topic", f"Просмотр темы", ts, target=tid))

def _gen_delete_topics(logs, base, active_uids, session_bounds, available_topics, topic_created_map):
    """Удаление тем."""
    candidate_topics = [tid for tid in available_topics if any(session_bounds[u][1] > topic_created_map[tid] + timedelta(seconds=30) for u in active_uids)]
    n_del = random.randint(5, min(7, len(candidate_topics))) if len(candidate_topics) >= 5 else len(candidate_topics)
    topics_to_delete = random.sample(candidate_topics, n_del) if candidate_topics else []

    for tid in topics_to_delete:
        t_created = topic_created_map[tid]
        eligible_users = [uid for uid in active_uids if session_bounds[uid][1] > max(session_bounds[uid][0], t_created) + timedelta(seconds=30)]
        if eligible_users:
            uid = random.choice(eligible_users)
            li_ts, lo_ts = session_bounds[uid]
            start_sec = max(int((li_ts - base).total_seconds()), int((t_created - base).total_seconds())) + 15
            end_sec = int((lo_ts - base).total_seconds()) - 5
            ts = base + timedelta(seconds=random.randint(start_sec, max(start_sec + 1, end_sec)))
        else:
            uid = random.choice(active_uids)
            ts = session_bounds[uid][1] - timedelta(seconds=random.randint(10, 60))

        available_topics.remove(tid)
        logs.append(_log_entry(uid, "delete_topic", f"Удаление темы", ts, target=tid))

def _gen_logouts(logs, user_map, session_bounds):
    """Логаут всех активных."""
    for uid, (li_ts, lo_ts) in session_bounds.items():
        logs.append(_log_entry(uid, "logout", f"Выход", lo_ts))

def _generate_logs(**context):
    """Главная функция генерации логов и активностей."""
    params = context["params"]
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

    for day_num in range(1, days_in_month + 1):
        base = datetime(year, month, day_num)
        session_bounds = {}
        today_registered = []

        _gen_first_visits(logs, base)
        pool_idx = _gen_registrations(logs, base, users_pool, pool_idx, days_in_month, day_num, registered, today_registered, session_bounds)
        _gen_logins(logs, base, registered, yesterday_registered, session_bounds)
        active_uids = list(session_bounds.keys())

        _gen_create_topic_success(logs, topics, forum, user_map, active_uids, session_bounds, available_topics, topic_created_map)
        _gen_create_topic_error(logs, base)
        _gen_create_posts(logs, posts, base, user_map, active_uids, session_bounds, available_topics, topic_created_map)
        _gen_view_topics(logs, base, active_uids, session_bounds, available_topics, topic_created_map)
        _gen_delete_topics(logs, base, active_uids, session_bounds, available_topics, topic_created_map)
        _gen_logouts(logs, user_map, session_bounds)

        yesterday_registered = today_registered

    logs.sort(key=lambda x: x["created_at"])
    _dump("registered_users.json", [u for u in users_pool if u["joined_at"]])
    _dump("topics.json", topics)
    _dump("posts.json", posts)
    _dump("logs.json", logs)
