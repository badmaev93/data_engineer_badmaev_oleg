import uuid
from calendar import monthrange
from forum_generation.helpers import fake, _dump

def _generate_users(**context):
    """Создаёт пул пользователей и форум."""
    params = context["params"]
    year = params.get("year", 2026)
    month = params.get("month", 10)
    _, days_in_month = monthrange(year, month)

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
