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

def gen_first_visits(base_dt: datetime, n_pool: int = 250) -> list[dict]:
    """Генерирует первые посещения форума анонимами (масштабируется от n_pool)."""
    min_visits = max(5, n_pool // 50)
    max_visits = max(15, n_pool // 15)
    logs = []
    for _ in range(random.randint(min_visits, max_visits)):
        ts = base_dt + timedelta(seconds=random.randint(0, 86399))
        logs.append(_log_entry(None, "first_visit", "Первый заход на форум", ts))
    return logs

def gen_registrations(base_dt, users_pool, pool_idx, days_left, session_bounds):
    """Регистрирует новых пользователей утром и открывает им сессию (масштабируется от пула)."""
    remaining_users = len(users_pool) - pool_idx
    # Равномерно распределяем пул по оставшимся дням с разбросом рандомайзера
    avg_daily = max(5, remaining_users // (days_left + 1))
    min_reg = max(5, int(avg_daily * 0.7))
    max_reg = max(8, int(avg_daily * 1.3))
    n_reg = max(5, min(random.randint(min_reg, max_reg), remaining_users - days_left * 5))

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
        min_d1 = max(1, int(len(yesterday_registered) * 0.25))
        max_d1 = max(min_d1, int(len(yesterday_registered) * 0.75))
        n_d1 = min(len(yesterday_registered), random.randint(min_d1, max_d1))
        d1_returns = random.sample(yesterday_registered, n_d1)

    others = [u for u in registered if u["id"] not in session_bounds and u not in d1_returns]
    random.shuffle(others)

    min_others = max(3, int(len(others) * 0.1))
    max_others = max(7, int(len(others) * 0.25))
    n_others = min(len(others), random.randint(min_others, max_others))

    logs = []
    for u in d1_returns + others[:n_others]:
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