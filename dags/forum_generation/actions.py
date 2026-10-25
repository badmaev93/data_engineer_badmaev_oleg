import random
import uuid
from datetime import datetime, timedelta
from dags.forum_generation.helpers import fake, _log_entry

def _pick_session_ts(session_bounds, active_uids, min_ts=None, offset_start=10, offset_end=5):
    """Выбор случайного пользователя и времени в пределах сессии."""
    if not active_uids:
        return None, None
    eligible = [u for u in active_uids if session_bounds[u][1] > (min_ts or session_bounds[u][0]) + timedelta(seconds=offset_start)]
    if not eligible:
        return None, None

    uid = random.choice(eligible)
    li_ts, lo_ts = session_bounds[uid]
    start_ts = (max(li_ts, min_ts) if min_ts else li_ts) + timedelta(seconds=offset_start)
    end_ts = lo_ts - timedelta(seconds=offset_end)

    if start_ts >= end_ts:
        return uid, start_ts

    start_sec = int(start_ts.timestamp())
    end_sec = int(end_ts.timestamp())
    return uid, datetime.fromtimestamp(random.randint(start_sec, end_sec))

def gen_first_visits(base_dt):
    """Первые визиты от анонимов (5-15 в день)."""
    logs = []
    for _ in range(random.randint(5, 15)):
        ts = base_dt + timedelta(seconds=random.randint(0, 86399))
        logs.append(_log_entry(None, "first_visit", "Первый заход на форум", ts))
    return logs

def gen_registrations(base_dt, users_pool, pool_idx, days_left, session_bounds):
    """Регистрация 5-8 пользователей и авто-логин."""
    logs, registered, today_registered = [], [], []
    rem_users = len(users_pool) - pool_idx
    n_reg = max(5, min(random.randint(5, 8), rem_users - days_left * 5))

    for _ in range(n_reg):
        if pool_idx >= len(users_pool): break
        u = users_pool[pool_idx]
        pool_idx += 1
        reg_ts = base_dt + timedelta(seconds=random.randint(60, 25200))
        u["joined_at"] = reg_ts.isoformat()
        registered.append(u)
        today_registered.append(u)
        logs.append(_log_entry(u["id"], "register", f"Регистрация {u['username']}", reg_ts, code=201))

        login_ts = reg_ts + timedelta(seconds=random.randint(1, 30))
        logout_ts = min(login_ts + timedelta(seconds=random.randint(7200, 28800)), base_dt + timedelta(hours=23, minutes=55))
        session_bounds[u["id"]] = (login_ts, logout_ts)
        logs.append(_log_entry(u["id"], "login", f"Авто-логин {u['username']}", login_ts))

    return logs, registered, today_registered, pool_idx

def gen_returning_logins(base_dt, registered, yesterday_registered, session_bounds):
    """Логины: 40-70% вчерашних + случайные из старых."""
    logs = []
    d1_returns = []
    if yesterday_registered:
        n_d1 = max(1, min(len(yesterday_registered), random.randint(int(len(yesterday_registered)*0.4), int(len(yesterday_registered)*0.7))))
        d1_returns = random.sample(yesterday_registered, n_d1)

    other_cands = [u for u in registered if u["id"] not in session_bounds and u not in d1_returns]
    random.shuffle(other_cands)

    for u in d1_returns + other_cands[:random.randint(3, 7)]:
        login_ts = base_dt + timedelta(seconds=random.randint(3600, 50400))
        logout_ts = min(login_ts + timedelta(seconds=random.randint(7200, 32400)), base_dt + timedelta(hours=23, minutes=55))
        session_bounds[u["id"]] = (login_ts, logout_ts)
        logs.append(_log_entry(u["id"], "login", f"Вход {u['username']}", login_ts))
    return logs

def gen_topics(base_dt, forum_id, session_bounds, user_map, available_topics, topic_created_map):
    """Создание 7-12 тем и 2-5 ошибок."""
    logs, topics = [], []
    active_uids = list(session_bounds.keys())

    for _ in range(random.randint(7, 12)):
        uid = random.choice(active_uids)
        li_ts, lo_ts = session_bounds[uid]
        ts = li_ts + timedelta(seconds=random.randint(30, max(31, int((lo_ts - li_ts).total_seconds() * 0.35))))
        tid = str(uuid.uuid4())

        topics.append({
            "id": tid, "forum_id": forum_id, "author_id": uid,
            "slug": fake.slug()[:200], "title": fake.sentence(nb_words=6)[:255],
            "status": "open", "created_at": ts.isoformat(), "last_post_at": ts.isoformat()
        })
        available_topics.append(tid)
        topic_created_map[tid] = ts
        logs.append(_log_entry(uid, "create_topic", "Создана тема", ts, target=tid, code=201))

    for _ in range(random.randint(2, 5)):
        ts = base_dt + timedelta(seconds=random.randint(28800, 72000))
        logs.append(_log_entry(None, "create_topic", "Ошибка: без логина", ts, status="error", code=401))

    return logs, topics

def gen_posts(base_dt, available_topics, topic_created_map, session_bounds, user_map):
    """Ответы в темах: 14-24 постов (~50/50 auth/anon)."""
    logs, posts = [], []
    n_post = random.randint(14, 24)
    n_auth = n_post // 2 + random.randint(0, 1)
    active_uids = list(session_bounds.keys())

    for i in range(n_post):
        if not available_topics: break
        tid = random.choice(available_topics)
        t_created = topic_created_map[tid]
        pid = str(uuid.uuid4())

        if i < n_auth:
            uid, ts = _pick_session_ts(session_bounds, active_uids, min_ts=t_created)
            if uid:
                posts.append({"id": pid, "topic_id": tid, "author_id": uid, "author_guest_name": None, "body": fake.paragraph(), "created_at": ts.isoformat()})
                logs.append(_log_entry(uid, "create_post", f"Сообщение {user_map[uid]['username']}", ts, target=pid, code=201))
                continue

        guest = fake.first_name()
        ts = base_dt + timedelta(seconds=random.randint(max(0, int((t_created - base_dt).total_seconds()) + 10), 86300))
        posts.append({"id": pid, "topic_id": tid, "author_id": None, "author_guest_name": guest, "body": fake.paragraph(), "created_at": ts.isoformat()})
        logs.append(_log_entry(None, "create_post", f"Сообщение {guest}", ts, target=pid, code=201))

    return logs, posts

def gen_views_and_deletes(base_dt, available_topics, topic_created_map, session_bounds):
    """Просмотры (8-20) и удаление тем (5-7)."""
    logs = []
    active_uids = list(session_bounds.keys())

    for _ in range(random.randint(8, 20)):
        if not available_topics: break
        tid = random.choice(available_topics)
        t_created = topic_created_map[tid]
        if random.random() > 0.5:
            uid, ts = _pick_session_ts(session_bounds, active_uids, min_ts=t_created)
            if uid:
                logs.append(_log_entry(uid, "view_topic", "Просмотр темы", ts, target=tid))
                continue
        ts = base_dt + timedelta(seconds=random.randint(max(0, int((t_created - base_dt).total_seconds()) + 5), 86300))
        logs.append(_log_entry(None, "view_topic", "Просмотр темы", ts, target=tid))

    for tid in random.sample(available_topics, min(random.randint(5, 7), len(available_topics))):
        uid, ts = _pick_session_ts(session_bounds, active_uids, min_ts=topic_created_map[tid], offset_start=15)
        if not uid:
            uid = random.choice(active_uids)
            ts = session_bounds[uid][1] - timedelta(seconds=random.randint(10, 60))
        available_topics.remove(tid)
        logs.append(_log_entry(uid, "delete_topic", "Удаление темы", ts, target=tid))

    return logs

def gen_logouts(session_bounds, user_map):
    """Выход всех залогиненных пользователей."""
    return [_log_entry(uid, "logout", "Выход", lo_ts) for uid, (li_ts, lo_ts) in session_bounds.items()]
