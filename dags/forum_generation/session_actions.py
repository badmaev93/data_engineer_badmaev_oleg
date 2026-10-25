import random
from datetime import datetime, timedelta
from .helpers import fake, _log_entry

def pick_session_ts(session_bounds, active_uids, min_ts=None, offset_start=10, offset_end=5):
    """Выбирает пользователя и случайный момент внутри его сессии после min_ts."""
    eligible = []
    for uid in active_uids:
        li_ts, lo_ts = session_bounds[uid]
        start_bound = max(li_ts, min_ts) if min_ts else li_ts
        if lo_ts > start_bound + timedelta(seconds=offset_start + offset_end + 5):
            eligible.append((uid, start_bound, lo_ts))
    if not eligible:
        return None, None
    uid, start_bound, lo_ts = random.choice(eligible)
    span = int((lo_ts - start_bound).total_seconds()) - offset_end
    sec = random.randint(offset_start, max(offset_start + 1, span))
    return uid, start_bound + timedelta(seconds=sec)

def gen_first_visits(base_dt: datetime) -> list[dict]:
    """Генерирует 5–15 первых посещений форума анонимами."""
    logs = []
    for _ in range(random.randint(5, 15)):
        ts = base_dt + timedelta(seconds=random.randint(0, 86399))
        logs.append(_log_entry(None, "first_visit", "Первый заход на форум", ts))
    return logs

def gen_registrations(base_dt, users_pool, pool_idx, days_left, session_bounds):
    """Регистрирует 5–8 новых пользователей утром и открывает им сессию."""
    remaining_users = len(users_pool) - pool_idx
    n_reg = max(5, min(random.randint(5, 8), remaining_users - days_left * 5))
    today_reg, logs = [], []

    for _ in range(n_reg):
        if pool_idx >= len(users_pool):
            break
        u = users_pool[pool_idx]
        pool_idx += 1
        reg_ts = base_dt + timedelta(seconds=random.randint(60, 25200))
        u["joined_at"] = reg_ts.isoformat()
        today_reg.append(u)
        logs.append(_log_entry(u["id"], "register", f"Регистрация {u['username']}", reg_ts, code=201))

        login_ts = reg_ts + timedelta(seconds=random.randint(1, 30))
        logout_ts = min(login_ts + timedelta(seconds=random.randint(7200, 28800)), base_dt + timedelta(hours=23, minutes=55))
        session_bounds[u["id"]] = (login_ts, logout_ts)
        logs.append(_log_entry(u["id"], "login", f"Вход {u['username']} после регистрации", login_ts))

    return today_reg, logs, pool_idx

def gen_returning_logins(base_dt, registered, yesterday_registered, session_bounds) -> list[dict]:
    """Генерирует входы вернувшихся пользователей (включая вчерашнюю когорту D1)."""
    d1_returns = []
    if yesterday_registered:
        n_d1 = max(1, min(len(yesterday_registered), random.randint(2, max(2, len(yesterday_registered) - 1))))
        d1_returns = random.sample(yesterday_registered, n_d1)

    others = [u for u in registered if u["id"] not in session_bounds and u not in d1_returns]
    random.shuffle(others)
    logs = []
    for u in d1_returns + others[:random.randint(3, 7)]:
        login_ts = base_dt + timedelta(seconds=random.randint(3600, 50400))
        logout_ts = min(login_ts + timedelta(seconds=random.randint(7200, 32400)), base_dt + timedelta(hours=23, minutes=55))
        session_bounds[u["id"]] = (login_ts, logout_ts)
        logs.append(_log_entry(u["id"], "login", f"Вход пользователя {u['username']}", login_ts))
    return logs

def gen_logouts(session_bounds, user_map) -> list[dict]:
    """Завершает сессии всех активных за день пользователей."""
    return [
        _log_entry(uid, "logout", f"Выход пользователя {user_map[uid]['username']}", lo_ts)
        for uid, (li_ts, lo_ts) in session_bounds.items()
    ]
