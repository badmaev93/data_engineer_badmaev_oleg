import random
import uuid
from datetime import timedelta
from .helpers import fake, _log_entry
from .session_actions import pick_session_ts

def gen_topics(base_dt, forum_id, session_bounds, user_map, available_topics, topic_created_map):
    """Создает темы авторизованными пользователями и ошибки (401) от анонимов пропорционально числу активных сессий."""
    active_uids = list(session_bounds.keys())
    n_active = len(active_uids)
    topics, logs = [], []

    # Гарантируем запас тем (минимум 7), чтобы после удалений всегда оставались темы для постов и просмотров
    min_topics = max(7, int(n_active * 0.4))
    max_topics = max(12, int(n_active * 0.8))
    n_topics = random.randint(min_topics, max_topics)

    for _ in range(n_topics):
        uid = random.choice(active_uids)
        li_ts, lo_ts = session_bounds[uid]
        span = int((lo_ts - li_ts).total_seconds())
        ts = li_ts + timedelta(seconds=random.randint(30, max(31, int(span * 0.35))))
        tid = str(uuid.uuid4())
        title = fake.sentence(nb_words=6)[:255]

        topics.append({
            "id": tid, "forum_id": forum_id, "author_id": uid,
            "slug": fake.slug()[:200], "title": title, "status": "open",
            "created_at": ts.isoformat(), "last_post_at": ts.isoformat(),
        })
        available_topics.append(tid)
        topic_created_map[tid] = ts
        logs.append(_log_entry(uid, "create_topic", f"Создана тема «{title}»", ts, target=tid, code=201))

    min_err = max(2, int(n_active * 0.1))
    max_err = max(5, int(n_active * 0.25))
    for _ in range(random.randint(min_err, max_err)):
        ts = base_dt + timedelta(seconds=random.randint(28800, 72000))
        logs.append(_log_entry(None, "create_topic", "Отказ: создание темы без авторизации", ts, status="error", code=401))

    return topics, logs

def gen_posts(base_dt, available_topics, topic_created_map, session_bounds, user_map):
    """Создает сообщения (~50/50 авторизованные и анонимные) пропорционально числу активных пользователей."""
    active_uids = list(session_bounds.keys())
    n_active = len(active_uids)

    min_posts = max(14, int(n_active * 1.2))
    max_posts = max(24, int(n_active * 2.2))
    n_post = random.randint(min_posts, max_posts)
    n_auth = n_post // 2 + random.randint(0, 1)
    posts, logs = [], []

    for i in range(n_post):
        if not available_topics:
            break
        tid = random.choice(available_topics)
        t_created = topic_created_map[tid]
        pid = str(uuid.uuid4())
        uid, ts = (pick_session_ts(session_bounds, active_uids, min_ts=t_created) if i < n_auth else (None, None))

        if uid and ts:
            guest, desc = None, f"Сообщение от {user_map[uid]['username']}"
        else:
            uid, guest = None, fake.first_name()
            start_sec = max(0, int((t_created - base_dt).total_seconds()) + 10)
            ts = base_dt + timedelta(seconds=random.randint(start_sec, 86300))
            desc = f"Сообщение от анонима {guest}"

        posts.append({
            "id": pid, "topic_id": tid, "author_id": uid,
            "author_guest_name": guest, "body": fake.paragraph(nb_sentences=3),
            "created_at": ts.isoformat(),
        })
        logs.append(_log_entry(uid, "create_post", desc, ts, target=pid, code=201))

    return posts, logs

def gen_views_and_deletes(base_dt, available_topics, topic_created_map, session_bounds) -> list[dict]:
    """Генерирует просмотры и удаления тем пропорционально числу активных пользователей."""
    active_uids = list(session_bounds.keys())
    n_active = len(active_uids)
    logs = []

    min_views = max(8, int(n_active * 1.0))
    max_views = max(20, int(n_active * 2.5))
    for _ in range(random.randint(min_views, max_views)):
        if not available_topics:
            break
        tid = random.choice(available_topics)
        t_created = topic_created_map[tid]
        uid, ts = (pick_session_ts(session_bounds, active_uids, min_ts=t_created, offset_start=5) if random.random() > 0.5 else (None, None))
        if not ts:
            start_sec = max(0, int((t_created - base_dt).total_seconds()) + 5)
            ts = base_dt + timedelta(seconds=random.randint(start_sec, 86300))
        logs.append(_log_entry(uid, "view_topic", f"Просмотр темы {tid[:8]}", ts, target=tid))

    candidates = [tid for tid in available_topics if pick_session_ts(session_bounds, active_uids, min_ts=topic_created_map[tid])[0]]
    min_del = 5
    max_del = max(7, int(len(candidates) * 0.3))
    n_del = random.randint(min_del, min(max_del, len(candidates))) if len(candidates) >= min_del else len(candidates)
    for tid in random.sample(candidates, n_del):
        uid, ts = pick_session_ts(session_bounds, active_uids, min_ts=topic_created_map[tid], offset_start=15)
        available_topics.remove(tid)
        logs.append(_log_entry(uid, "delete_topic", f"Удаление темы {tid[:8]}", ts, target=tid))

    return logs