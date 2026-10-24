"""
Тесты второго уровня: исполнение бизнес-логики генерации данных.

Изолированно запускает _generate_users и _generate_logs с подменой окружения,
затем проверяет ВСЕ инварианты из ТЗ на реальных сгенерированных данных.

Запуск:
  cd farpost && python3 -m pytest tests/test_logic_execution.py -v --tb=short
"""

import json
import os
import random
import sys
import tempfile
from collections import defaultdict
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from unittest.mock import MagicMock, patch

import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dags"))

# Подменяем TMP_DIR на временный каталог для изоляции тестов
_tmpdir = tempfile.mkdtemp(prefix="farpost_test_")

# Мокаем модули Airflow (не нужны для юнит-тестов)
class _FakeModule:
    def __getattr__(self, name):
        return _FakeModule()
    def __call__(self, *a, **kw):
        return _FakeModule()

for mod in [
    "airflow", "airflow.models", "airflow.operators",
    "airflow.operators.python", "airflow.sensors",
    "airflow.sensors.base", "airflow.providers",
    "airflow.providers.postgres",
    "airflow.providers.postgres.hooks",
    "airflow.providers.postgres.hooks.postgres",
]:
    sys.modules[mod] = _FakeModule()

# Патчим DAG, PythonOperator, BaseSensorOperator, PostgresHook
sys.modules["airflow"].DAG = type("DAG", (), {"__init__": lambda *a, **kw: None, "__enter__": lambda s: s, "__exit__": lambda *a: None})

class FakePythonOp:
    def __init__(self, **kw): pass
    def __rshift__(self, other): return other
sys.modules["airflow.operators.python"].PythonOperator = FakePythonOp
sys.modules["airflow.sensors.base"].BaseSensorOperator = type("BSO", (), {"__init__": lambda *a, **kw: None})
sys.modules["airflow.providers.postgres.hooks.postgres"].PostgresHook = _FakeModule

# Теперь импортируем DAG-модуль
import importlib
import forum_data_generation as gen_mod

# Переопределяем TMP_DIR
gen_mod.TMP_DIR = _tmpdir


# ══════════════════════════════════════════════════════════════════════════════
# Fixtures
# ══════════════════════════════════════════════════════════════════════════════

YEAR, MONTH = 2026, 10

@pytest.fixture(scope="module")
def generated_data():
    """Генерирует данные за целый месяц (один раз для всех тестов)."""
    random.seed(42)  # Детерминированный seed для воспроизводимости

    context = {"params": {"year": YEAR, "month": MONTH, "n_users": 250}}

    # Запускаем генерацию
    gen_mod._generate_users(**context)
    gen_mod._generate_logs(**context)

    # Загружаем результаты
    logs = gen_mod._load("logs.json")
    users = gen_mod._load("registered_users.json")
    topics = gen_mod._load("topics.json")
    posts = gen_mod._load("posts.json")

    df = pd.DataFrame(logs)
    df["created_at_dt"] = pd.to_datetime(df["created_at"])
    df["day"] = df["created_at_dt"].dt.date

    return {
        "logs": logs,
        "users": users,
        "topics": topics,
        "posts": posts,
        "df": df,
    }


# ══════════════════════════════════════════════════════════════════════════════
# Тесты на данные: пункт 3 ТЗ
# ══════════════════════════════════════════════════════════════════════════════

class TestGeneratedDataVolume:
    """Проверяет объёмы и покрытие сгенерированных данных."""

    def test_covers_full_month(self, generated_data):
        """Данные покрывают все дни октября 2026."""
        df = generated_data["df"]
        from calendar import monthrange
        _, days = monthrange(YEAR, MONTH)
        unique_days = sorted(df["day"].unique())
        expected_days = [datetime(YEAR, MONTH, d).date() for d in range(1, days + 1)]
        assert unique_days == expected_days, \
            f"Не все дни покрыты: {len(unique_days)} из {days}"

    def test_has_registered_users(self, generated_data):
        assert len(generated_data["users"]) > 0

    def test_has_topics(self, generated_data):
        assert len(generated_data["topics"]) > 0

    def test_has_posts(self, generated_data):
        assert len(generated_data["posts"]) > 0

    def test_has_logs(self, generated_data):
        assert len(generated_data["logs"]) > 100


class TestMinActionsPerDay:
    """ТЗ п. 3: Минимум 5 действий каждого типа в день."""

    @pytest.fixture(autouse=True)
    def _data(self, generated_data):
        self.df = generated_data["df"]

    def _min_daily(self, action):
        daily = self.df[self.df["action_type"] == action].groupby("day").size()
        return daily.min() if len(daily) > 0 else 0

    def test_first_visit_min_5(self):
        assert self._min_daily("first_visit") >= 5

    def test_register_min_5(self):
        assert self._min_daily("register") >= 5

    def test_login_min_5(self):
        assert self._min_daily("login") >= 5

    def test_logout_min_5(self):
        assert self._min_daily("logout") >= 5

    def test_create_topic_min_5(self):
        assert self._min_daily("create_topic") >= 5

    def test_view_topic_min_5(self):
        assert self._min_daily("view_topic") >= 5

    def test_delete_topic_min_5(self):
        assert self._min_daily("delete_topic") >= 5

    def test_create_post_min_5(self):
        assert self._min_daily("create_post") >= 5


class TestErrorCreateTopic:
    """ТЗ п. 3: Минимум 2 ошибки create_topic (без логина) в день."""

    def test_min_2_error_create_topic_per_day(self, generated_data):
        df = generated_data["df"]
        errors = df[
            (df["action_type"] == "create_topic") &
            (df["response_status"] == "error")
        ]
        daily = errors.groupby("day").size()
        assert daily.min() >= 2, f"Минимум ошибок create_topic/день = {daily.min()}"

    def test_error_create_topic_is_anonymous(self, generated_data):
        """Ошибочные create_topic должны быть от анонимов (user_id=None)."""
        df = generated_data["df"]
        errors = df[
            (df["action_type"] == "create_topic") &
            (df["response_status"] == "error")
        ]
        assert errors["user_id"].isna().all(), \
            "Все ошибочные create_topic должны иметь user_id=None"

    def test_error_create_topic_has_401(self, generated_data):
        """Ошибочные create_topic должны иметь код 401."""
        df = generated_data["df"]
        errors = df[
            (df["action_type"] == "create_topic") &
            (df["response_status"] == "error")
        ]
        assert (errors["response_code"] == 401).all()


class TestPostDistribution:
    """ТЗ п. 3: Примерно равное распределение auth/anon постов."""

    def test_anon_ratio_30_to_70(self, generated_data):
        df = generated_data["df"]
        posts = df[
            (df["action_type"] == "create_post") &
            (df["response_status"] == "success")
        ]
        n_auth = posts["user_id"].notna().sum()
        n_anon = posts["user_id"].isna().sum()
        total = n_auth + n_anon
        assert total > 0
        ratio = n_anon / total
        assert 0.3 <= ratio <= 0.7, \
            f"Anon ratio = {ratio:.1%}, должно быть в [30%, 70%]"


# ══════════════════════════════════════════════════════════════════════════════
# Тесты на данные: пункт 5 ТЗ (хронология / валидация)
# ══════════════════════════════════════════════════════════════════════════════

class TestNoLogoutWithoutLogin:
    """ТЗ п. 5: Нет логаута без логина."""

    def test_no_orphan_logouts(self, generated_data):
        df = generated_data["df"]
        logout_uids = set(df[df["action_type"] == "logout"]["user_id"].dropna())
        login_uids = set(df[df["action_type"] == "login"]["user_id"].dropna())
        orphan = logout_uids - login_uids
        assert not orphan, f"Logout без login: {orphan}"


class TestSuccessCreateTopicOnlyByLoggedIn:
    """ТЗ п. 5: Успешное создание темы только залогиненными."""

    def test_success_create_topic_has_user_id(self, generated_data):
        df = generated_data["df"]
        ok = df[
            (df["action_type"] == "create_topic") &
            (df["response_status"] == "success")
        ]
        assert ok["user_id"].notna().all(), \
            "Все успешные create_topic должны иметь user_id"


class TestSessionChronology:
    """ТЗ п. 5: login < действие < logout для каждого user+day."""

    def test_all_actions_within_session(self, generated_data):
        """Каждое действие авторизованного пользователя внутри [login, logout]."""
        df = generated_data["df"].copy()
        df_auth = df[df["user_id"].notna()].copy()
        df_auth["ts"] = pd.to_datetime(df_auth["created_at"])

        violations = []
        for (day, uid), u_df in df_auth.groupby(["day", "user_id"]):
            u_actions = u_df.sort_values("ts")
            logins = u_actions[u_actions["action_type"] == "login"]["ts"].tolist()
            logouts = u_actions[u_actions["action_type"] == "logout"]["ts"].tolist()

            if not logins or not logouts:
                continue  # Может быть только register (без login в этот день)

            for li, lo in zip(logins, logouts):
                if li >= lo:
                    violations.append(f"User {uid} on {day}: login {li} >= logout {lo}")
                    continue

                # Проверяем, что все действия (кроме register) находятся между li и lo
                action_types_check = ["create_topic", "create_post", "delete_topic", "view_topic"]
                outside = u_actions[
                    (u_actions["action_type"].isin(action_types_check)) &
                    ((u_actions["ts"] < li) | (u_actions["ts"] > lo))
                ]
                if len(outside) > 0:
                    violations.append(
                        f"User {uid} on {day}: {len(outside)} actions outside session [{li}, {lo}]"
                    )

        assert not violations, f"Нарушения хронологии сессий:\n" + "\n".join(violations[:10])


class TestTopicPostChronology:
    """ТЗ п. 5: Пост не создан раньше темы."""

    def test_posts_after_topic_creation(self, generated_data):
        topics = generated_data["topics"]
        posts = generated_data["posts"]

        topic_map = {t["id"]: pd.to_datetime(t["created_at"]) for t in topics}

        violations = []
        for p in posts:
            t_created = topic_map.get(p["topic_id"])
            if t_created is not None:
                p_created = pd.to_datetime(p["created_at"])
                if p_created < t_created:
                    violations.append(
                        f"Post {p['id'][:8]} at {p_created} < Topic {p['topic_id'][:8]} at {t_created}"
                    )

        assert not violations, \
            f"Посты до создания тем:\n" + "\n".join(violations[:10])


class TestD1RetentionCohort:
    """Проверяет, что данные позволяют рассчитать ненулевой D1 retention."""

    def test_d1_retention_non_trivial(self, generated_data):
        """Хотя бы в один день D1 retention > 0."""
        df = generated_data["df"].copy()
        df["ts"] = pd.to_datetime(df["created_at"])
        df["day"] = df["ts"].dt.date

        days = sorted(df["day"].unique())
        has_nonzero = False

        for i in range(1, len(days)):
            prev_day = days[i - 1]
            cur_day = days[i]

            reg_yesterday = set(
                df[
                    (df["day"] == prev_day) &
                    (df["action_type"] == "register") &
                    (df["response_status"] == "success")
                ]["user_id"].dropna()
            )

            if not reg_yesterday:
                continue

            active_today = set(
                df[df["day"] == cur_day]["user_id"].dropna()
            )

            returned = reg_yesterday & active_today
            if len(returned) > 0:
                has_nonzero = True
                break

        assert has_nonzero, "D1 retention = 0 для всех дней — данные не содержат возвратов"


class TestActionTypeCounts:
    """Проверяет полноту типов действий."""

    def test_all_8_action_types_present(self, generated_data):
        df = generated_data["df"]
        expected = {
            "first_visit", "register", "login", "logout",
            "create_topic", "view_topic", "delete_topic", "create_post",
        }
        actual = set(df["action_type"].unique())
        assert expected == actual, f"Ожидалось {expected}, получено {actual}"


class TestResponseCodes:
    """Проверяет корректность HTTP-кодов."""

    def test_all_codes_in_range(self, generated_data):
        df = generated_data["df"]
        codes = df["response_code"].astype(int)
        assert (codes >= 100).all() and (codes <= 599).all()

    def test_success_codes_are_2xx(self, generated_data):
        df = generated_data["df"]
        success = df[df["response_status"] == "success"]["response_code"].astype(int)
        assert ((success >= 200) & (success <= 299)).all()

    def test_error_codes_are_4xx(self, generated_data):
        df = generated_data["df"]
        errors = df[df["response_status"] == "error"]["response_code"].astype(int)
        assert ((errors >= 400) & (errors <= 499)).all()


class TestUsersData:
    """Проверяет целостность данных пользователей."""

    def test_all_user_ids_unique(self, generated_data):
        users = generated_data["users"]
        ids = [u["id"] for u in users]
        assert len(ids) == len(set(ids))

    def test_all_emails_unique(self, generated_data):
        users = generated_data["users"]
        emails = [u["email"] for u in users]
        assert len(emails) == len(set(emails))

    def test_all_usernames_unique(self, generated_data):
        users = generated_data["users"]
        names = [u["username"] for u in users]
        assert len(names) == len(set(names))

    def test_all_users_have_joined_at(self, generated_data):
        users = generated_data["users"]
        for u in users:
            assert u["joined_at"] is not None, f"User {u['id']} has no joined_at"


class TestTopicsData:
    """Проверяет целостность данных тем."""

    def test_topic_ids_unique(self, generated_data):
        topics = generated_data["topics"]
        ids = [t["id"] for t in topics]
        assert len(ids) == len(set(ids))

    def test_topic_has_author(self, generated_data):
        topics = generated_data["topics"]
        for t in topics:
            assert t["author_id"] is not None, f"Topic {t['id']} has no author"

    def test_topic_author_is_registered(self, generated_data):
        topics = generated_data["topics"]
        user_ids = {u["id"] for u in generated_data["users"]}
        for t in topics:
            assert t["author_id"] in user_ids, \
                f"Topic author {t['author_id']} not in registered users"


class TestPostsData:
    """Проверяет целостность данных постов."""

    def test_post_ids_unique(self, generated_data):
        posts = generated_data["posts"]
        ids = [p["id"] for p in posts]
        assert len(ids) == len(set(ids))

    def test_posts_reference_existing_topics(self, generated_data):
        posts = generated_data["posts"]
        topic_ids = {t["id"] for t in generated_data["topics"]}
        for p in posts:
            assert p["topic_id"] in topic_ids, \
                f"Post {p['id']} references non-existent topic {p['topic_id']}"

    def test_anon_posts_have_guest_name(self, generated_data):
        posts = generated_data["posts"]
        anon = [p for p in posts if p["author_id"] is None]
        for p in anon:
            assert p["author_guest_name"], \
                f"Anon post {p['id']} has no guest name"

    def test_auth_posts_have_user_id(self, generated_data):
        posts = generated_data["posts"]
        auth = [p for p in posts if p["author_id"] is not None]
        user_ids = {u["id"] for u in generated_data["users"]}
        for p in auth:
            assert p["author_id"] in user_ids


class TestLogEntriesIntegrity:
    """Проверяет целостность записей лога."""

    def test_log_ids_unique(self, generated_data):
        logs = generated_data["logs"]
        ids = [l["id"] for l in logs]
        assert len(ids) == len(set(ids))

    def test_logs_sorted_chronologically(self, generated_data):
        logs = generated_data["logs"]
        timestamps = [l["created_at"] for l in logs]
        assert timestamps == sorted(timestamps), "Логи не отсортированы хронологически"

    def test_all_logs_within_month(self, generated_data):
        """Все логи в пределах октября 2026."""
        df = generated_data["df"]
        from calendar import monthrange
        _, days = monthrange(YEAR, MONTH)
        start = datetime(YEAR, MONTH, 1)
        end = datetime(YEAR, MONTH, days, 23, 59, 59)

        assert (df["created_at_dt"] >= start).all(), "Есть логи раньше начала месяца"
        assert (df["created_at_dt"] <= end).all(), "Есть логи позже конца месяца"

    def test_description_not_empty(self, generated_data):
        df = generated_data["df"]
        assert (df["description"].str.len() > 0).all(), "Есть логи с пустым description"

    def test_session_or_ip_not_empty(self, generated_data):
        df = generated_data["df"]
        assert (df["session_or_ip"].str.len() > 0).all()


class TestLoginLogoutPairing:
    """Проверяет паринг login/logout по каждому user+day."""

    def test_equal_login_logout_per_user_per_day(self, generated_data):
        """Каждый пользователь имеет одинаковое кол-во login и logout в день."""
        df = generated_data["df"]
        df_auth = df[df["user_id"].notna()]

        violations = []
        for (day, uid), u_df in df_auth.groupby(["day", "user_id"]):
            n_login = (u_df["action_type"] == "login").sum()
            n_logout = (u_df["action_type"] == "logout").sum()

            # Пользователь мог только зарегистрироваться (и логин) и разлогиниться
            if n_login != n_logout and n_login > 0:
                violations.append(f"Day {day}, User {uid[:8]}: login={n_login}, logout={n_logout}")

        assert not violations, \
            f"Непарные login/logout:\n" + "\n".join(violations[:10])

class TestLoadToPostgres:
    """ТЗ п. 4: Проверяем загрузку в Postgres."""

    @patch("forum_data_generation.PostgresHook")
    def test_load_to_postgres_rollback_on_exception(self, mock_hook_class):
        from forum_data_generation import _load_to_postgres

        # Настраиваем моки для хука и коннекта
        mock_hook = mock_hook_class.return_value
        mock_conn = MagicMock()
        mock_hook.get_conn.return_value = mock_conn

        mock_cur = MagicMock()
        mock_conn.cursor.return_value = mock_cur

        # Заставляем execute бросать исключение (при попытке TRUNCATE)
        mock_cur.execute.side_effect = Exception("DB error")

        # Запускаем функцию и ожидаем исключение
        with pytest.raises(Exception, match="DB error"):
            _load_to_postgres()

        # Проверяем, что был вызван rollback
        mock_conn.rollback.assert_called_once()
        mock_cur.close.assert_called_once()
        mock_conn.close.assert_called_once()
