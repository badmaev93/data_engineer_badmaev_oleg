"""
DAG: forum_data_generation

Генерирует синтетические данные форума за один календарный месяц
и загружает их в PostgreSQL.

Структура:
  generate_users → generate_logs → validate_generated_data → load_to_postgres
"""

import json
import os
import random
import uuid
from calendar import monthrange
from datetime import datetime, timedelta

import pandas as pd
from pandera import Column, Check, DataFrameSchema
from faker import Faker

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# ─── Constants ───────────────────────────────────────────────────────────────

TMP_DIR = "/tmp/forum_data"
CONN_ID = "forum_db"

ACTION_TYPES = [
    "first_visit", "register", "login", "logout",
    "create_topic", "view_topic", "delete_topic", "create_post",
]

fake = Faker("ru_RU")


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _dump(filename, data):
    os.makedirs(TMP_DIR, exist_ok=True)
    with open(os.path.join(TMP_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)


def _load(filename):
    with open(os.path.join(TMP_DIR, filename), "r", encoding="utf-8") as f:
        return json.load(f)


def _log_entry(user_id, action, desc, ts, target=None, status="success", code=200):
    return {
        "id": str(uuid.uuid4()),
        "user_id": user_id,
        "session_or_ip": fake.ipv4(),
        "action_type": action,
        "description": desc,
        "target_object_id": target,
        "response_status": status,
        "response_code": code,
        "created_at": ts.isoformat(),
    }


# ─── Task 1: generate_users ─────────────────────────────────────────────────

def _generate_users(**context):
    """Создаёт пул фейковых пользователей и один форум."""
    params = context["params"]
    year = params.get("year", 2026)
    month = params.get("month", 10)
    _, days_in_month = monthrange(year, month)

    # Гарантируем достаточно пользователей: 8/день * кол-во дней + запас
    n_users = params.get("n_users", 250)
    if n_users > 0:
        n_users = max(n_users, days_in_month * 8 + 10)

    fake.unique.clear()
    users = []
    for _ in range(n_users):
        users.append({
            "id": str(uuid.uuid4()),
            "username": fake.unique.user_name()[:40],
            "email": fake.unique.email(),
            "display_name": fake.name()[:80],
            "status": "active",
            "joined_at": None,
        })

    forum = {
        "id": str(uuid.uuid4()),
        "slug": "general",
        "name": "Основной раздел",
        "description": "Главный форум для обсуждений",
        "position": 0,
    }

    _dump("users_pool.json", users)
    _dump("forum.json", forum)
    print(f"Generated pool of {len(users)} users and 1 forum")


# ─── Task 2: generate_logs ──────────────────────────────────────────────────

def _generate_logs(**context):
    """Генерирует лог-записи за каждый день месяца с рандомайзером."""
    params = context["params"]
    year = params.get("year", 2026)
    month = params.get("month", 10)
    _, days_in_month = monthrange(year, month)

    users_pool = _load("users_pool.json")
    forum = _load("forum.json")
    user_map = {u["id"]: u for u in users_pool}

    logs = []
    topics = []
    posts = []

    registered = []           # все зарегистрированные пользователи
    yesterday_registered = [] # пользователи, зарегистрированные в предыдущий день (для D1 retention)
    pool_idx = 0              # указатель на следующего в пуле
    available_topics = []     # id существующих тем
    topic_created_map = {}    # topic_id -> datetime создания темы

    for day_num in range(1, days_in_month + 1):
        base = datetime(year, month, day_num)
        session_bounds = {}   # user_id -> (login_ts, logout_ts)
        today_registered = []

        # ── 1. first_visit (анонимы, 5-15) ──────────────────────────────
        for _ in range(random.randint(5, 15)):
            ts = base + timedelta(seconds=random.randint(0, 86399))
            logs.append(_log_entry(None, "first_visit", "Первый заход на форум", ts))

        # ── 2. register (новые пользователи, 5-8) ──────────────────────
        remaining_days = days_in_month - day_num
        remaining_users = len(users_pool) - pool_idx
        max_today = remaining_users - remaining_days * 5
        n_reg = max(5, min(random.randint(5, 8), max_today))

        for _ in range(n_reg):
            if pool_idx >= len(users_pool):
                break
            u = users_pool[pool_idx]
            pool_idx += 1
            # Регистрация утром: 00:01 - 07:00
            reg_ts = base + timedelta(seconds=random.randint(60, 25200))
            u["joined_at"] = reg_ts.isoformat()
            registered.append(u)
            today_registered.append(u)

            logs.append(_log_entry(u["id"], "register", f"Пользователь {u['username']} зарегистрировался", reg_ts, code=201))

            # Авто-логин через 1-30 сек после регистрации
            login_ts = reg_ts + timedelta(seconds=random.randint(1, 30))
            # Сессия длится от 2 до 8 часов, но заканчивается внутри дня (до 23:55)
            session_dur = random.randint(7200, 28800)
            logout_ts = min(login_ts + timedelta(seconds=session_dur), base + timedelta(hours=23, minutes=55))
            session_bounds[u["id"]] = (login_ts, logout_ts)

            logs.append(_log_entry(u["id"], "login", f"Пользователь {u['username']} вошёл (после регистрации)", login_ts))

        # ── 3. login (ранее зарегистрированные пользователи) ────────────
        # Гарантируем возврат пользователей вчерашней когорты для ненулевого D1 retention
        d1_returns = []
        if yesterday_registered:
            # 40-70% вчерашних возвращаются сегодня
            n_d1 = max(1, min(len(yesterday_registered), random.randint(2, max(2, len(yesterday_registered) - 1))))
            d1_returns = random.sample(yesterday_registered, n_d1)

        other_candidates = [u for u in registered if u["id"] not in session_bounds and u not in d1_returns]
        random.shuffle(other_candidates)

        # Всего существующих залогинившихся: d1_returns + часть других кандидатов
        existing_logins = d1_returns + other_candidates[:random.randint(3, 7)]
        for u in existing_logins:
            # Логин в интервале 01:00 - 14:00
            login_ts = base + timedelta(seconds=random.randint(3600, 50400))
            session_dur = random.randint(7200, 32400)  # 2 - 9 часов
            logout_ts = min(login_ts + timedelta(seconds=session_dur), base + timedelta(hours=23, minutes=55))
            session_bounds[u["id"]] = (login_ts, logout_ts)

            logs.append(_log_entry(u["id"], "login", f"Пользователь {u['username']} вошёл в систему", login_ts))

        active_uids = list(session_bounds.keys())

        # ── 4. create_topic (success, 7-12) ─────────────────────────────
        # Создаём с запасом (7-12), чтобы после удаления min 5 всегда оставались темы для постов
        n_ct = random.randint(7, 12)
        for _ in range(min(n_ct, len(active_uids))):
            uid = random.choice(active_uids)
            u = user_map[uid]
            tid = str(uuid.uuid4())
            li_ts, lo_ts = session_bounds[uid]
            span = int((lo_ts - li_ts).total_seconds())
            # Создаём тему в первой трети сессии (30 сек .. 35% длины сессии)
            sec_offset = random.randint(30, max(31, int(span * 0.35)))
            ts = li_ts + timedelta(seconds=sec_offset)

            topic = {
                "id": tid,
                "forum_id": forum["id"],
                "author_id": uid,
                "slug": fake.slug()[:200],
                "title": fake.sentence(nb_words=6)[:255],
                "status": "open",
                "created_at": ts.isoformat(),
                "last_post_at": ts.isoformat(),
            }
            topics.append(topic)
            available_topics.append(tid)
            topic_created_map[tid] = ts

            logs.append(_log_entry(uid, "create_topic", f"Создана тема «{topic['title']}»", ts, target=tid, code=201))

        # ── 5. create_topic (error — без логина, мин. 2, 2-5 в день) ─────
        for _ in range(random.randint(2, 5)):
            ts = base + timedelta(seconds=random.randint(28800, 72000))
            logs.append(_log_entry(
                None, "create_topic",
                "Попытка создать тему без авторизации — отклонено",
                ts, status="error", code=401
            ))

        # ── 6. create_post (~50/50 авторизованные / анонимы, 14-24) ──────
        n_post = random.randint(14, 24)
        n_auth_posts = n_post // 2 + random.randint(0, 1)
        for i in range(n_post):
            if not available_topics:
                break
            tid = random.choice(available_topics)
            t_created = topic_created_map[tid]
            pid = str(uuid.uuid4())

            if i < n_auth_posts and active_uids:
                # Находим пользователя, чья сессия перекрывается со временем после создания темы
                eligible_users = [
                    uid for uid in active_uids
                    if session_bounds[uid][1] > max(session_bounds[uid][0], t_created) + timedelta(seconds=30)
                ]
                if eligible_users:
                    uid = random.choice(eligible_users)
                    u = user_map[uid]
                    li_ts, lo_ts = session_bounds[uid]
                    start_sec = max(int((li_ts - base).total_seconds()), int((t_created - base).total_seconds())) + 10
                    end_sec = int((lo_ts - base).total_seconds()) - 10
                    post_sec = random.randint(start_sec, max(start_sec + 1, end_sec))
                    ts = base + timedelta(seconds=post_sec)

                    post = {
                        "id": pid, "topic_id": tid,
                        "author_id": uid, "author_guest_name": None,
                        "body": fake.paragraph(nb_sentences=3),
                        "created_at": ts.isoformat(),
                    }
                    desc = f"Пользователь {u['username']} написал сообщение"
                    posts.append(post)
                    logs.append(_log_entry(uid, "create_post", desc, ts, target=pid, code=201))
                    continue

            # Иначе — анонимный пост
            guest = fake.first_name()
            t_sec = int((t_created - base).total_seconds())
            start_sec = max(0, t_sec + 10)
            post_sec = random.randint(start_sec, 86300)
            ts = base + timedelta(seconds=post_sec)

            post = {
                "id": pid, "topic_id": tid,
                "author_id": None, "author_guest_name": guest,
                "body": fake.paragraph(nb_sentences=3),
                "created_at": ts.isoformat(),
            }
            desc = f"Аноним «{guest}» написал сообщение"
            posts.append(post)
            logs.append(_log_entry(None, "create_post", desc, ts, target=pid, code=201))

        # ── 7. view_topic (8-20 действий) ───────────────────────────────
        for _ in range(random.randint(8, 20)):
            if not available_topics:
                break
            tid = random.choice(available_topics)
            t_created = topic_created_map[tid]

            # 50% просмотров от залогиненных пользователей, 50% от анонимов
            if random.random() > 0.5 and active_uids:
                eligible_users = [
                    uid for uid in active_uids
                    if session_bounds[uid][1] > max(session_bounds[uid][0], t_created) + timedelta(seconds=20)
                ]
                if eligible_users:
                    uid = random.choice(eligible_users)
                    li_ts, lo_ts = session_bounds[uid]
                    start_sec = max(int((li_ts - base).total_seconds()), int((t_created - base).total_seconds())) + 5
                    end_sec = int((lo_ts - base).total_seconds()) - 5
                    view_sec = random.randint(start_sec, max(start_sec + 1, end_sec))
                    ts = base + timedelta(seconds=view_sec)
                    logs.append(_log_entry(uid, "view_topic", f"Просмотр темы {tid[:8]}…", ts, target=tid))
                    continue

            # Анонимный просмотр
            t_sec = int((t_created - base).total_seconds())
            start_sec = max(0, t_sec + 5)
            ts = base + timedelta(seconds=random.randint(start_sec, 86300))
            logs.append(_log_entry(None, "view_topic", f"Просмотр темы {tid[:8]}…", ts, target=tid))

        # ── 8. delete_topic (мин. 5, 5-7 в день) ─────────────────────────
        candidate_topics = [
            tid for tid in available_topics
            if any(session_bounds[u][1] > topic_created_map[tid] + timedelta(seconds=30) for u in active_uids)
        ]
        n_del = random.randint(5, min(7, len(candidate_topics))) if len(candidate_topics) >= 5 else len(candidate_topics)
        topics_to_delete = random.sample(candidate_topics, n_del) if candidate_topics else []

        for tid in topics_to_delete:
            t_created = topic_created_map[tid]
            eligible_users = [
                uid for uid in active_uids
                if session_bounds[uid][1] > max(session_bounds[uid][0], t_created) + timedelta(seconds=30)
            ]
            if eligible_users:
                uid = random.choice(eligible_users)
                li_ts, lo_ts = session_bounds[uid]
                start_sec = max(int((li_ts - base).total_seconds()), int((t_created - base).total_seconds())) + 15
                end_sec = int((lo_ts - base).total_seconds()) - 5
                del_sec = random.randint(start_sec, max(start_sec + 1, end_sec))
                ts = base + timedelta(seconds=del_sec)
            else:
                uid = random.choice(active_uids)
                li_ts, lo_ts = session_bounds[uid]
                ts = lo_ts - timedelta(seconds=random.randint(10, 60))

            available_topics.remove(tid)
            logs.append(_log_entry(uid, "delete_topic", f"Удаление темы {tid[:8]}…", ts, target=tid))

        # ── 9. logout (все залогиненные пользователи завершают сессию) ───
        for uid, (li_ts, lo_ts) in session_bounds.items():
            u = user_map[uid]
            logs.append(_log_entry(uid, "logout", f"Пользователь {u['username']} вышел", lo_ts))

        yesterday_registered = today_registered

    # Сортируем по времени
    logs.sort(key=lambda x: x["created_at"])

    # Сохраняем всё
    _dump("registered_users.json", [u for u in users_pool if u["joined_at"]])
    _dump("topics.json", topics)
    _dump("posts.json", posts)
    _dump("logs.json", logs)

    print(f"Generated: {pool_idx} users, {len(topics)} topics, "
          f"{len(posts)} posts, {len(logs)} log entries")


# ─── Task 3: validate_generated_data ────────────────────────────────────────

def _validate_generated_data(**context):
    """Валидация сгенерированных данных через pandera + бизнес-правила."""
    logs_data = _load("logs.json")
    users_data = _load("registered_users.json")
    topics_data = _load("topics.json")
    posts_data = _load("posts.json")

    df = pd.DataFrame(logs_data)
    registered_ids = {u["id"] for u in users_data}

    # ── Pandera: схема ───────────────────────────────────────────────────
    schema = DataFrameSchema({
        "id":              Column(str, nullable=False),
        "action_type":     Column(str, Check.isin(ACTION_TYPES)),
        "description":     Column(str, nullable=False),
        "response_status": Column(str, Check.isin(["success", "error"])),
        "response_code":   Column("Int64", Check.in_range(100, 599)),
        "created_at":      Column(str, nullable=False),
    })
    schema.validate(df, lazy=True)
    print("Pandera schema validation passed")

    # ── Нет логаута без логина ───────────────────────────────────────────
    logout_uids = set(df[df["action_type"] == "logout"]["user_id"].dropna())
    login_uids = set(df[df["action_type"] == "login"]["user_id"].dropna())
    orphan = logout_uids - login_uids
    assert not orphan, f"Logout without login: {orphan}"
    print("No logout without login")

    # ── Успешное создание темы только залогиненным ────────────────────────
    ok_topics = df[(df["action_type"] == "create_topic") & (df["response_status"] == "success")]
    assert ok_topics["user_id"].notna().all(), "Successful create_topic with null user_id"
    print("Successful topic creation only by registered users")

    # ── Ошибочное создание темы — без user_id ────────────────────────────
    err_topics = df[(df["action_type"] == "create_topic") & (df["response_status"] == "error")]
    assert err_topics["user_id"].isna().all(), "Error create_topic should have null user_id"
    print(f"{len(err_topics)} error create_topic entries (all anonymous)")

    # ── Минимум 5 действий каждого типа в день ───────────────────────────
    df["day"] = pd.to_datetime(df["created_at"]).dt.date
    daily = df.groupby(["day", "action_type"]).size().unstack(fill_value=0)
    for action in ACTION_TYPES:
        if action in daily.columns:
            min_val = int(daily[action].min())
            assert min_val >= 5, f"{action}: min/day = {min_val}, need ≥ 5"
    print("Minimum 5 actions per type per day")

    # ── Минимум 2 ошибки create_topic в день ─────────────────────────────
    err_daily = err_topics.groupby(pd.to_datetime(err_topics["created_at"]).dt.date).size()
    assert err_daily.min() >= 2, f"Min error create_topic/day = {err_daily.min()}, need ≥ 2"
    print("Minimum 2 error create_topic per day")

    # ── Примерно равное распределение auth/anon постов ────────────────────
    post_logs = df[(df["action_type"] == "create_post") & (df["response_status"] == "success")]
    n_auth = int(post_logs["user_id"].notna().sum())
    n_anon = int(post_logs["user_id"].isna().sum())
    total = n_auth + n_anon
    ratio = n_anon / total if total > 0 else 0
    print(f" Posts: auth={n_auth}, anon={n_anon}, anon_ratio={ratio:.1%}")
    assert 0.3 <= ratio <= 0.7, f"Anon ratio {ratio:.1%} not in [30%-70%]"
    print("Roughly equal auth/anon post distribution")

    # ── Проверка строгой хронологии сессий: login <= действие <= logout ──
    df_auth = df[df["user_id"].notna()].copy()
    df_auth["ts"] = pd.to_datetime(df_auth["created_at"])
    df_auth["day"] = df_auth["ts"].dt.date
    df_auth = df_auth.sort_values("ts")

    for (day, uid), u_actions in df_auth.groupby(["day", "user_id"]):
        logins = u_actions[u_actions["action_type"] == "login"]["ts"].tolist()
        logouts = u_actions[u_actions["action_type"] == "logout"]["ts"].tolist()

        assert len(logins) > 0, f"User {uid} performed actions on {day} without login"
        assert len(logouts) > 0, f"User {uid} has no logout on {day}"
        assert len(logins) == len(logouts), f"Mismatched login/logout count for {uid} on {day}"

        for li, lo in zip(logins, logouts):
            assert li < lo, f"Login {li} is not before logout {lo} for {uid}"
            # Проверяем, что нет промежуточных действий вне границ сессии [li, lo]
            outside = u_actions[
                (u_actions["action_type"].isin(["create_topic", "create_post", "delete_topic", "view_topic"]))
                & ((u_actions["ts"] < li) | (u_actions["ts"] > lo))
            ]
            assert len(outside) == 0, f"User {uid} on {day} has actions outside session: {outside}"

    print("Strict session chronology validated (login < actions < logout)")

    # ── Проверка хронологии: темы созданы до постов в них ────────────────
    topic_map = {t["id"]: pd.to_datetime(t["created_at"]) for t in topics_data}
    for p in posts_data:
        topic_created = topic_map.get(p["topic_id"])
        if topic_created is not None:
            post_created = pd.to_datetime(p["created_at"])
            assert post_created >= topic_created, (
                f"Post {p['id']} ({post_created}) created before topic {p['topic_id']} ({topic_created})"
            )
    print("Strict topic-post chronology validated (topic.created_at <= post.created_at)")

    print("\nAll validations passed!")


# ─── Task 4: load_to_postgres ────────────────────────────────────────────────

def _load_to_postgres(**context):
    """Загружает сгенерированные данные в PostgreSQL (idempotent)."""
    from psycopg2.extras import execute_values

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        # Очищаем таблицы (idempotent)
        cur.execute("TRUNCATE user_logs, posts, topics, forums, users CASCADE;")

        # forums
        forum = _load("forum.json")
        cur.execute(
            "INSERT INTO forums (id, slug, name, description, position) VALUES (%s,%s,%s,%s,%s)",
            (forum["id"], forum["slug"], forum["name"], forum["description"], forum["position"]),
        )

        # users
        users = _load("registered_users.json")
        execute_values(cur,
            "INSERT INTO users (id,username,email,display_name,status,joined_at) VALUES %s",
            [(u["id"], u["username"], u["email"], u["display_name"], u["status"], u["joined_at"])
             for u in users],
        )

        # topics
        topics = _load("topics.json")
        execute_values(cur,
            "INSERT INTO topics (id,forum_id,author_id,slug,title,status,created_at,last_post_at) VALUES %s",
            [(t["id"], t["forum_id"], t["author_id"], t["slug"], t["title"],
              t["status"], t["created_at"], t["last_post_at"]) for t in topics],
        )

        # posts
        posts = _load("posts.json")
        execute_values(cur,
            "INSERT INTO posts (id,topic_id,author_id,author_guest_name,body,created_at) VALUES %s",
            [(p["id"], p["topic_id"], p["author_id"], p["author_guest_name"],
              p["body"], p["created_at"]) for p in posts],
        )

        # user_logs
        logs = _load("logs.json")
        execute_values(cur,
            """INSERT INTO user_logs
               (id,user_id,session_or_ip,action_type,description,
                target_object_id,response_status,response_code,created_at) VALUES %s""",
            [(l["id"], l["user_id"], l["session_or_ip"], l["action_type"],
              l["description"], l["target_object_id"], l["response_status"],
              l["response_code"], l["created_at"]) for l in logs],
        )

        conn.commit()
        print(f"✅ Loaded: {len(users)} users, {len(topics)} topics, "
              f"{len(posts)} posts, {len(logs)} logs")

    except Exception as e:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()

# ─── DAG definition ─────────────────────────────────────────────────────────

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="forum_data_generation",
    default_args=default_args,
    description="Генерация синтетических данных форума за один месяц",
    start_date=datetime(2026, 10, 1),
    schedule=None,          # запуск вручную
    catchup=False,
    tags=["forum", "generation"],
    params={
        "year": 2026,
        "month": 10,
        "n_users": 250,
    },
) as dag:

    t1 = PythonOperator(task_id="generate_users",          python_callable=_generate_users)
    t2 = PythonOperator(task_id="generate_logs",            python_callable=_generate_logs)
    t3 = PythonOperator(task_id="validate_generated_data",  python_callable=_validate_generated_data)
    t4 = PythonOperator(task_id="load_to_postgres",         python_callable=_load_to_postgres)

    t1 >> t2 >> t3 >> t4
