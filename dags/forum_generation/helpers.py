import json
import os
import uuid
from faker import Faker

TMP_DIR = "/tmp/forum_data"
CONN_ID = "forum_db"

ACTION_TYPES = [
    "first_visit", "register", "login", "logout",
    "create_topic", "view_topic", "delete_topic", "create_post",
]

fake = Faker("ru_RU")

def _dump(filename, data):
    """Сохраняет данные в JSON."""
    os.makedirs(TMP_DIR, exist_ok=True)
    with open(os.path.join(TMP_DIR, filename), "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, default=str)

def _load(filename):
    """Загружает данные из JSON."""
    with open(os.path.join(TMP_DIR, filename), "r", encoding="utf-8") as f:
        return json.load(f)

def _log_entry(user_id, action, desc, ts, target=None, status="success", code=200):
    """Формирует запись лога."""
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
