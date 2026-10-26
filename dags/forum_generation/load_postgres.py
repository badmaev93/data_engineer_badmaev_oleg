import logging
from airflow.providers.postgres.hooks.postgres import PostgresHook
from forum_generation.helpers import CONN_ID, _load

logger = logging.getLogger(__name__)

def _load_to_postgres(**context):
    """Загружает данные в PostgreSQL."""
    from psycopg2.extras import execute_values

    hook = PostgresHook(postgres_conn_id=CONN_ID)
    conn = hook.get_conn()
    cur = conn.cursor()

    try:
        cur.execute("TRUNCATE user_logs, posts, topics, forums, users CASCADE;")

        forum = _load("forum.json")
        cur.execute(
            "INSERT INTO forums (id, slug, name, description, position) VALUES (%s,%s,%s,%s,%s)",
            (forum["id"], forum["slug"], forum["name"], forum["description"], forum["position"]),
        )

        users = _load("registered_users.json")
        execute_values(cur,
            "INSERT INTO users (id,username,email,display_name,status,joined_at) VALUES %s",
            [(u["id"], u["username"], u["email"], u["display_name"], u["status"], u["joined_at"])
             for u in users],
        )

        topics = _load("topics.json")
        execute_values(cur,
            "INSERT INTO topics (id,forum_id,author_id,slug,title,status,created_at,last_post_at) VALUES %s",
            [(t["id"], t["forum_id"], t["author_id"], t["slug"], t["title"],
              t["status"], t["created_at"], t["last_post_at"]) for t in topics],
        )

        posts = _load("posts.json")
        execute_values(cur,
            "INSERT INTO posts (id,topic_id,author_id,author_guest_name,body,created_at) VALUES %s",
            [(p["id"], p["topic_id"], p["author_id"], p["author_guest_name"],
              p["body"], p["created_at"]) for p in posts],
        )

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
        logger.info("Загружено: %d пользователей, %d тем, %d сообщений, %d логов",
                    len(users), len(topics), len(posts), len(logs))

    except Exception:
        conn.rollback()
        raise
    finally:
        cur.close()
        conn.close()
