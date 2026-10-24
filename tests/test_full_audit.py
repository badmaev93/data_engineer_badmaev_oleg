"""
Полный набор тестов для проверки проекта farpost по ТЗ Data Engineer.

Тесты разбиты по пунктам ТЗ:
  1.x — Схема БД
  2   — Docker / Infra
  3   — DAG forum_data_generation
  4   — DAG forum_log_aggregation
  5   — Качество данных (pandera)

Запуск:
  cd farpost && python -m pytest tests/test_full_audit.py -v
"""

import ast
import os
import re
from pathlib import Path

import pytest
import yaml

ROOT = Path(__file__).resolve().parent.parent

# ══════════════════════════════════════════════════════════════════════════════
# Helpers
# ══════════════════════════════════════════════════════════════════════════════

def _read(relpath: str) -> str:
    return (ROOT / relpath).read_text(encoding="utf-8")


def _read_sql() -> str:
    return _read("schema.sql")


def _read_dag1() -> str:
    return _read("dags/forum_data_generation.py")


def _read_dag2() -> str:
    return _read("dags/forum_log_aggregation.py")


def _read_readme() -> str:
    return _read("README.md")


def _read_compose() -> str:
    return _read("docker-compose.yml")


def _parse_compose() -> dict:
    return yaml.safe_load(_read_compose())


# ══════════════════════════════════════════════════════════════════════════════
# 1. СХЕМА БД (schema.sql)
# ══════════════════════════════════════════════════════════════════════════════

class TestSchemaTables:
    """Проверяет наличие обязательных таблиц."""

    @pytest.fixture(autouse=True)
    def _sql(self):
        self.sql = _read_sql().lower()

    def test_users_table_exists(self):
        assert "create table users" in self.sql

    def test_forums_table_exists(self):
        assert "create table forums" in self.sql

    def test_topics_table_exists(self):
        assert "create table topics" in self.sql

    def test_posts_table_exists(self):
        assert "create table posts" in self.sql

    def test_user_logs_table_exists(self):
        assert "create table user_logs" in self.sql


class TestSchemaPoint1_1:
    """1.1: Тему может создавать только залогиненный пользователь."""

    def test_topics_author_id_not_null(self):
        sql = _read_sql()
        # Ищем определение topics — author_id NOT NULL + REFERENCES users
        topics_block = re.search(
            r"CREATE TABLE topics\s*\((.*?)\);",
            sql, re.DOTALL | re.IGNORECASE
        )
        assert topics_block, "CREATE TABLE topics не найден"
        body = topics_block.group(1)
        assert re.search(r"author_id\s+UUID\s+NOT\s+NULL", body, re.IGNORECASE), \
            "topics.author_id должен быть NOT NULL"
        assert "references users" in body.lower(), \
            "topics.author_id должен ссылаться на users"


class TestSchemaPoint1_2:
    """1.2: Сообщение может писать залогиненный или аноним."""

    def test_posts_author_id_nullable(self):
        sql = _read_sql()
        posts_block = re.search(
            r"CREATE TABLE posts\s*\((.*?)\);",
            sql, re.DOTALL | re.IGNORECASE
        )
        assert posts_block, "CREATE TABLE posts не найден"
        body = posts_block.group(1)
        # author_id не должен иметь NOT NULL
        author_line = [
            line for line in body.split("\n")
            if "author_id" in line.lower() and "reply_to" not in line.lower()
        ]
        assert len(author_line) >= 1, "Поле author_id не найдено в posts"
        assert "not null" not in author_line[0].lower(), \
            "posts.author_id должен быть nullable (без NOT NULL)"

    def test_posts_author_guest_name_exists(self):
        sql = _read_sql().lower()
        assert "author_guest_name" in sql, \
            "В posts должно быть поле author_guest_name для анонимов"


class TestSchemaPoint1_3:
    """1.3: Поля таблицы user_logs."""

    @pytest.fixture(autouse=True)
    def _sql(self):
        self.sql = _read_sql()
        # Ищем всё тело CREATE TABLE user_logs ... PARTITION BY RANGE (...)
        block = re.search(
            r"CREATE TABLE user_logs\s*\((.*?)\)\s*PARTITION BY RANGE\s*\([^)]+\)",
            self.sql, re.DOTALL | re.IGNORECASE
        )
        assert block, "CREATE TABLE user_logs не найден"
        self.body = block.group(1).lower()

    def test_has_user_id(self):
        assert "user_id" in self.body

    def test_has_action_type(self):
        assert "action_type" in self.body

    def test_has_description(self):
        assert "description" in self.body

    def test_has_target_object_id(self):
        assert "target_object_id" in self.body

    def test_has_response_status(self):
        assert "response_status" in self.body

    def test_has_response_code(self):
        assert "response_code" in self.body

    def test_has_created_at(self):
        assert "created_at" in self.body

    def test_eight_action_types_in_enum(self):
        """Проверяет, что ENUM log_action_type содержит ровно 8 типов действий."""
        enum_match = re.search(
            r"CREATE TYPE log_action_type AS ENUM\s*\((.*?)\);",
            self.sql, re.DOTALL | re.IGNORECASE
        )
        assert enum_match, "ENUM log_action_type не найден"
        values = re.findall(r"'(\w+)'", enum_match.group(1))
        expected = {
            "first_visit", "register", "login", "logout",
            "create_topic", "view_topic", "delete_topic", "create_post",
        }
        assert set(values) == expected, \
            f"Ожидались 8 действий: {expected}, нашли: {set(values)}"


class TestSchemaPoint1_4:
    """1.4: Партиционирование user_logs."""

    @pytest.fixture(autouse=True)
    def _sql(self):
        self.sql = _read_sql().lower()

    def test_partition_by_range(self):
        assert "partition by range" in self.sql, \
            "user_logs должна иметь PARTITION BY RANGE"

    def test_partitioned_by_created_at(self):
        assert "partition by range (created_at)" in self.sql, \
            "Партиционирование должно быть по created_at"

    def test_has_monthly_partitions(self):
        partitions = re.findall(r"partition of user_logs", self.sql)
        assert len(partitions) >= 2, \
            "Должно быть как минимум 2 партиции (месячные)"

    def test_has_default_partition(self):
        assert "partition of user_logs default" in self.sql, \
            "Должна быть DEFAULT партиция"

    def test_primary_key_includes_created_at(self):
        """PK партиционированной таблицы должен включать ключ партиционирования."""
        assert "primary key (id, created_at)" in self.sql, \
            "PRIMARY KEY должен включать created_at для партиционирования"


class TestSchemaPoint1_5:
    """1.5: Индексы."""

    @pytest.fixture(autouse=True)
    def _sql(self):
        self.sql = _read_sql().lower()

    def test_brin_index_on_created_at(self):
        assert "using brin (created_at)" in self.sql, \
            "Должен быть BRIN индекс по created_at"

    def test_partial_index_user_id(self):
        """Partial-индекс по user_id (WHERE user_id IS NOT NULL)."""
        assert re.search(
            r"create index.*user_logs.*user_id.*where user_id is not null",
            self.sql, re.DOTALL
        ), "Должен быть partial индекс по user_id (WHERE user_id IS NOT NULL)"

    def test_partial_index_target_object(self):
        assert re.search(
            r"create index.*target_object.*where target_object_id is not null",
            self.sql, re.DOTALL
        ), "Должен быть partial индекс по target_object_id"

    def test_composite_index_for_analytics(self):
        """Составной индекс для аналитики (status + action_type)."""
        assert re.search(
            r"create index.*response_status.*action_type",
            self.sql, re.DOTALL
        ), "Должен быть составной индекс (response_status, action_type)"


# ══════════════════════════════════════════════════════════════════════════════
# 2. DOCKER / INFRA
# ══════════════════════════════════════════════════════════════════════════════

class TestDockerCompose:
    """Проверяет docker-compose.yml."""

    @pytest.fixture(autouse=True)
    def _compose(self):
        self.compose = _parse_compose()
        self.raw = _read_compose()

    def test_postgres_service_exists(self):
        assert "postgres" in self.compose["services"]

    def test_postgres_image(self):
        img = self.compose["services"]["postgres"]["image"]
        assert "postgres" in img.lower()

    def test_airflow_webserver_exists(self):
        assert "airflow-webserver" in self.compose["services"]

    def test_airflow_scheduler_exists(self):
        assert "airflow-scheduler" in self.compose["services"]

    def test_airflow_init_exists(self):
        assert "airflow-init" in self.compose["services"]

    def test_dags_volume_mounted(self):
        """DAGs directory монтируется в контейнер."""
        assert "./dags:/opt/airflow/dags" in self.raw

    def test_output_volume_mounted(self):
        assert "./output:/opt/airflow/output" in self.raw

    def test_forum_db_connection_created(self):
        """airflow-init создает connection forum_db."""
        init_cmd = str(self.compose["services"]["airflow-init"].get("command", ""))
        assert "forum_db" in init_cmd, \
            "airflow-init должен создавать connection forum_db"

    def test_user_uid_set(self):
        """Для Linux-совместимости user должен быть задан."""
        assert "AIRFLOW_UID" in self.raw or "50000" in self.raw

    def test_schema_sql_mounted(self):
        pg_volumes = self.compose["services"]["postgres"].get("volumes", [])
        schema_mounted = any("schema.sql" in str(v) for v in pg_volumes)
        assert schema_mounted, "schema.sql должен монтироваться в контейнер postgres"

    def test_init_db_mounted(self):
        pg_volumes = self.compose["services"]["postgres"].get("volumes", [])
        init_mounted = any("init-db" in str(v) for v in pg_volumes)
        assert init_mounted, "init-db/ должен монтироваться в docker-entrypoint-initdb.d"

    def test_postgres_healthcheck(self):
        hc = self.compose["services"]["postgres"].get("healthcheck")
        assert hc, "PostgreSQL должен иметь healthcheck"

    def test_services_depend_on_postgres(self):
        """Airflow-сервисы зависят от postgres."""
        # Через x-airflow-common или напрямую
        assert "depends_on" in self.raw


class TestDockerfile:
    """Проверяет Dockerfile."""

    def test_dockerfile_exists(self):
        assert (ROOT / "Dockerfile").exists()

    def test_based_on_airflow(self):
        content = _read("Dockerfile").lower()
        assert "airflow" in content, "Dockerfile должен быть основан на образе Airflow"

    def test_requirements_installed(self):
        content = _read("Dockerfile")
        assert "requirements.txt" in content


class TestRequirements:
    """Проверяет requirements.txt."""

    @pytest.fixture(autouse=True)
    def _req(self):
        self.req = _read("requirements.txt").lower()

    def test_postgres_provider(self):
        assert "apache-airflow-providers-postgres" in self.req

    def test_faker(self):
        assert "faker" in self.req

    def test_pandera(self):
        assert "pandera" in self.req

    def test_pyarrow(self):
        assert "pyarrow" in self.req


class TestInitDB:
    """Проверяет init-db/01-init-forum-db.sh."""

    def test_script_exists(self):
        assert (ROOT / "init-db" / "01-init-forum-db.sh").exists()

    def test_script_executable(self):
        path = ROOT / "init-db" / "01-init-forum-db.sh"
        assert os.access(path, os.X_OK), \
            "01-init-forum-db.sh должен иметь бит исполнения (chmod +x)"

    def test_creates_farpost_db(self):
        content = _read("init-db/01-init-forum-db.sh").lower()
        assert "farpost" in content, "Скрипт должен создавать БД farpost"

    def test_applies_schema(self):
        content = _read("init-db/01-init-forum-db.sh")
        assert "schema.sql" in content, "Скрипт должен применять schema.sql"


# ══════════════════════════════════════════════════════════════════════════════
# 3. DAG: forum_data_generation
# ══════════════════════════════════════════════════════════════════════════════

class TestDAG1Structure:
    """Проверяет структуру DAG forum_data_generation."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag1()

    def test_dag_id(self):
        assert 'dag_id="forum_data_generation"' in self.code

    def test_has_generate_users_task(self):
        assert "generate_users" in self.code

    def test_has_generate_logs_task(self):
        assert "generate_logs" in self.code

    def test_has_validate_generated_data_task(self):
        assert "validate_generated_data" in self.code

    def test_has_load_to_postgres_task(self):
        assert "load_to_postgres" in self.code

    def test_correct_task_order(self):
        """generate_users >> generate_logs >> validate >> load."""
        assert re.search(
            r"t1\s*>>\s*t2\s*>>\s*t3\s*>>\s*t4", self.code
        ), "Порядок задач: t1 >> t2 >> t3 >> t4"

    def test_schedule_is_none(self):
        """DAG генерации запускается только вручную."""
        assert "schedule=None" in self.code or "schedule_interval=None" in self.code

    def test_has_params_year_month(self):
        assert '"year"' in self.code
        assert '"month"' in self.code


class TestDAG1DataGeneration:
    """Проверяет логику генерации данных (п. 3 ТЗ)."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag1()

    def test_covers_full_month(self):
        """Генерация покрывает один календарный месяц."""
        assert "monthrange" in self.code, "Должен использоваться monthrange для полного месяца"
        assert "days_in_month" in self.code

    def test_min_5_actions_each_type_per_day(self):
        """Рандомайзер с порогом >= 5 для каждого типа."""
        # Проверяем что в generate_logs есть randint с нижней границей >= 5
        # для основных действий
        assert "random.randint(5," in self.code or "random.randint(7," in self.code or \
               "random.randint(8," in self.code, \
            "Рандомайзер должен обеспечивать минимум 5 действий"

    def test_min_2_error_create_topic(self):
        """Минимум 2 ошибки create_topic в день."""
        assert re.search(r"random\.randint\(2,\s*\d+\)", self.code), \
            "Должен быть randint(2, N) для ошибочных create_topic"

    def test_approximately_equal_auth_anon_posts(self):
        """Примерно равное распределение auth/anon постов (~50/50)."""
        assert "n_auth_posts" in self.code or "n_post // 2" in self.code, \
            "Должна быть логика ~50/50 распределения auth/anon постов"

    def test_randomizer_used(self):
        """Количества действий выбираются рандомайзером."""
        assert self.code.count("random.randint") >= 5, \
            "Должно быть достаточно вызовов random.randint"

    def test_session_bounds_exist(self):
        """Для хронологии сессий используется session_bounds."""
        assert "session_bounds" in self.code, \
            "Должен использоваться session_bounds для привязки действий к сессиям"

    def test_login_before_actions(self):
        """Действия создаются внутри окна сессии (li_ts, lo_ts)."""
        assert "li_ts, lo_ts = session_bounds" in self.code, \
            "Время действий должно извлекаться из session_bounds"

    def test_logout_uses_session_bounds(self):
        """Logout использует заранее вычисленный lo_ts."""
        assert re.search(
            r"for uid.*session_bounds.*items\(\)", self.code, re.DOTALL
        ), "Logout должен перебирать session_bounds.items()"

    def test_d1_retention_cohort(self):
        """Часть вчерашних зарегистрированных возвращается сегодня."""
        assert "yesterday_registered" in self.code, \
            "Для D1 retention нужен трекинг yesterday_registered"
        assert "d1_return" in self.code or "d1_returns" in self.code, \
            "Должна быть логика возврата вчерашних пользователей"

    def test_topic_created_before_post(self):
        """Пост/просмотр создаётся после создания темы."""
        assert "t_created" in self.code or "topic_created_map" in self.code, \
            "Должен быть трекинг времени создания тем"

    def test_error_create_topic_anonymous(self):
        """Ошибки create_topic — без user_id (None)."""
        # Находим блок ошибочных create_topic
        assert re.search(
            r'_log_entry\(\s*None.*"create_topic".*"error".*401',
            self.code, re.DOTALL
        ), "Ошибочные create_topic должны иметь user_id=None и code=401"


class TestDAG1Validation:
    """Проверяет валидацию в validate_generated_data."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag1()

    def test_pandera_used(self):
        assert "pandera" in self.code or "DataFrameSchema" in self.code

    def test_no_logout_without_login_check(self):
        assert "logout_uids" in self.code and "login_uids" in self.code, \
            "Должна быть проверка: нет логаута без логина"

    def test_successful_topic_only_by_logged_in(self):
        assert "ok_topics" in self.code, \
            "Должна быть проверка: успешные create_topic только у залогиненных"

    def test_session_chronology_check(self):
        """Проверка хронологии: login < действие < logout."""
        assert "session chronology" in self.code.lower() or \
               ("login" in self.code and "logout" in self.code and "outside" in self.code), \
            "Должна быть проверка хронологии сессий"

    def test_topic_post_chronology_check(self):
        """Проверка: пост не создан раньше темы."""
        assert "topic_map" in self.code or "topic-post chronology" in self.code.lower()

    def test_min_5_per_type_per_day_check(self):
        assert "min_val" in self.code or ">= 5" in self.code, \
            "Должна быть проверка минимум 5 действий каждого типа в день"

    def test_min_2_error_per_day_check(self):
        assert "err_daily" in self.code, \
            "Должна быть проверка минимум 2 ошибки create_topic в день"

    def test_anon_ratio_check(self):
        assert "ratio" in self.code and "0.3" in self.code, \
            "Должна быть проверка распределения auth/anon постов"


# ══════════════════════════════════════════════════════════════════════════════
# 4. DAG: forum_log_aggregation
# ══════════════════════════════════════════════════════════════════════════════

class TestDAG2Structure:
    """Проверяет структуру DAG forum_log_aggregation."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag2()

    def test_dag_id(self):
        assert 'dag_id="forum_log_aggregation"' in self.code

    def test_schedule_daily(self):
        assert '@daily' in self.code or 'schedule="@daily"' in self.code

    def test_has_6_tasks(self):
        """Должно быть 6 тасок в DAG."""
        expected = [
            "check_data_availability",
            "extract_raw_logs",
            "validate_data",
            "transform_aggregates",
            "export_to_csv",
            "alert_on_anomaly",
        ]
        for task in expected:
            assert task in self.code, f"Таска '{task}' не найдена"

    def test_correct_task_order(self):
        assert "check >> extract >> validate >> transform >> export >> alert" in self.code or \
               re.search(r"check\s*>>\s*extract\s*>>\s*validate\s*>>\s*transform\s*>>\s*export\s*>>\s*alert", self.code)

    def test_has_params_date_from_to(self):
        assert '"date_from"' in self.code
        assert '"date_to"' in self.code

    def test_sensor_exists(self):
        assert "DataAvailabilitySensor" in self.code or "BaseSensorOperator" in self.code


class TestDAG2Sensor:
    """Проверяет сенсор check_data_availability."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag2()

    def test_sensor_inherits_base(self):
        assert "BaseSensorOperator" in self.code

    def test_sensor_has_poke_method(self):
        assert "def poke" in self.code

    def test_sensor_uses_postgres_hook(self):
        assert "PostgresHook" in self.code

    def test_sensor_no_created_at_cast(self):
        """Сенсор НЕ кастит created_at::date (partition pruning)."""
        # Ищем SQL в сенсоре — не должно быть created_at::date
        sensor_code = self.code[self.code.index("class DataAvailabilitySensor"):
                                self.code.index("def _extract")]
        assert "created_at::date" not in sensor_code, \
            "Сенсор не должен кастить created_at::date (ломает Partition Pruning)"


class TestDAG2ExtractRawLogs:
    """Проверяет extract_raw_logs."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag2()

    def test_captures_extra_day(self):
        """Захватывает date_from - 1 день."""
        assert "timedelta(days=1)" in self.code, \
            "Должен захватывать date_from - 1 день для расчёта метрик"

    def test_no_created_at_cast_in_sql(self):
        """SQL не кастит колонку created_at (partition pruning)."""
        extract_code = self.code[self.code.index("def _extract"):
                                  self.code.index("def _validate")]
        assert "created_at::date" not in extract_code, \
            "SQL в extract не должен кастить created_at::date"

    def test_saves_parquet(self):
        assert "to_parquet" in self.code

    def test_uses_sargable_where(self):
        """WHERE clause использует SARGable предикаты."""
        assert "created_at >=" in self.code and "created_at <" in self.code


class TestDAG2TransformAggregates:
    """Проверяет transform_aggregates — все 7 метрик из ТЗ."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag2()

    def test_metric_new_accounts(self):
        assert "new_accounts" in self.code

    def test_metric_anon_messages_pct(self):
        assert "anon_messages_pct" in self.code

    def test_metric_total_messages(self):
        assert "total_messages" in self.code

    def test_metric_topics_growth_pct(self):
        assert "topics_growth_pct" in self.code

    def test_metric_avg_session_duration_sec(self):
        assert "avg_session_duration_sec" in self.code or "avg_session" in self.code

    def test_metric_d1_retention_pct(self):
        assert "d1_retention_pct" in self.code

    def test_metric_day(self):
        assert '"day"' in self.code

    def test_target_days_filter(self):
        """Агрегация только по целевым дням [d_from .. d_to], а не по всем."""
        assert "target_days" in self.code or "d_from <= d <= d_to" in self.code

    def test_topics_growth_uses_prev_day(self):
        """topics_growth_pct сравнивает с предыдущим днём."""
        assert "prev_day" in self.code and "topics_yesterday" in self.code

    def test_d1_retention_uses_prev_day(self):
        """d1_retention_pct ищет вчерашних зарегистрированных."""
        assert "reg_yesterday" in self.code

    def test_session_duration_login_logout_pairing(self):
        """Сессия = login→logout по user_id."""
        transform_code = self.code[self.code.index("def _transform"):]
        assert "login" in transform_code and "logout" in transform_code
        assert "zip" in transform_code, \
            "Login/logout парятся через zip"


class TestDAG2ExportCSV:
    """Проверяет export_to_csv."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag2()

    def test_saves_csv(self):
        assert "to_csv" in self.code

    def test_output_dir(self):
        assert "/opt/airflow/output" in self.code or "OUTPUT_DIR" in self.code


class TestDAG2AlertOnAnomaly:
    """Проверяет alert_on_anomaly."""

    @pytest.fixture(autouse=True)
    def _code(self):
        self.code = _read_dag2()

    def test_alert_on_high_anon_pct(self):
        assert "80" in self.code, "Алерт на anon_messages_pct > 80%"

    def test_alert_on_zero_messages(self):
        assert "total_messages" in self.code and "== 0" in self.code

    def test_alert_function_exists(self):
        assert "def _alert_on_anomaly" in self.code


# ══════════════════════════════════════════════════════════════════════════════
# 5. КАЧЕСТВО ДАННЫХ (pandera)
# ══════════════════════════════════════════════════════════════════════════════

class TestDataQuality:
    """Проверяет использование pandera для валидации (п. 5 ТЗ)."""

    def test_pandera_imported_in_dag1(self):
        code = _read_dag1()
        assert "import pandera" in code or "from pandera" in code

    def test_pandera_imported_in_dag2(self):
        code = _read_dag2()
        assert "import pandera" in code or "from pandera" in code

    def test_dag1_uses_dataframeschema(self):
        code = _read_dag1()
        assert "DataFrameSchema" in code

    def test_dag2_uses_dataframeschema(self):
        code = _read_dag2()
        assert "DataFrameSchema" in code

    def test_dag2_coerce_true(self):
        """coerce=True для безопасной работы с datetime64 разной точности."""
        code = _read_dag2()
        assert "coerce=True" in code

    def test_dag1_validates_action_types(self):
        code = _read_dag1()
        assert "Check.isin" in code

    def test_dag2_validates_action_types(self):
        code = _read_dag2()
        assert "Check.isin" in code

    def test_dag1_validates_null_constraints(self):
        code = _read_dag1()
        assert "nullable=False" in code

    def test_dag2_validates_null_constraints(self):
        code = _read_dag2()
        assert "nullable=False" in code

    def test_dag2_validates_response_code_range(self):
        code = _read_dag2()
        assert "in_range" in code or "Check.in_range" in code


# ══════════════════════════════════════════════════════════════════════════════
# README
# ══════════════════════════════════════════════════════════════════════════════

class TestREADME:
    """Проверяет README.md (требования к сдаче)."""

    @pytest.fixture(autouse=True)
    def _readme(self):
        self.readme = _read_readme()
        self.lower = self.readme.lower()

    def test_readme_exists(self):
        assert (ROOT / "README.md").exists()

    def test_has_launch_instructions(self):
        assert "docker-compose up" in self.readme

    def test_has_project_structure(self):
        assert "структура" in self.lower or "structure" in self.lower

    def test_has_partitioning_rationale(self):
        assert "партиционирован" in self.lower or "partitioning" in self.lower

    def test_has_index_rationale(self):
        assert "индекс" in self.lower or "index" in self.lower

    def test_mentions_brin(self):
        assert "brin" in self.lower

    def test_mentions_partial_index(self):
        assert "partial" in self.lower

    def test_mentions_partition_pruning(self):
        assert "partition pruning" in self.lower or "pruning" in self.lower

    def test_describes_dag1(self):
        assert "forum_data_generation" in self.readme

    def test_describes_dag2(self):
        assert "forum_log_aggregation" in self.readme

    def test_linux_permissions_note(self):
        assert "chmod" in self.readme or "AIRFLOW_UID" in self.readme

    def test_describes_csv_output(self):
        assert "csv" in self.lower

    def test_describes_all_csv_columns(self):
        """README описывает все 7 столбцов итогового CSV."""
        columns = [
            "day", "new_accounts", "anon_messages_pct",
            "total_messages", "topics_growth_pct",
            "avg_session_duration_sec", "d1_retention_pct",
        ]
        for col in columns:
            assert col in self.readme, f"README должен описывать колонку '{col}'"


# ══════════════════════════════════════════════════════════════════════════════
# Синтаксис Python (оба DAG'а парсятся без ошибок)
# ══════════════════════════════════════════════════════════════════════════════

class TestPythonSyntax:
    """Проверяет, что файлы DAG'ов парсятся без синтаксических ошибок."""

    def test_dag1_parses(self):
        code = _read_dag1()
        try:
            ast.parse(code)
        except SyntaxError as e:
            pytest.fail(f"forum_data_generation.py содержит SyntaxError: {e}")

    def test_dag2_parses(self):
        code = _read_dag2()
        try:
            ast.parse(code)
        except SyntaxError as e:
            pytest.fail(f"forum_log_aggregation.py содержит SyntaxError: {e}")


# ══════════════════════════════════════════════════════════════════════════════
# Дополнительные Edge-case тесты
# ══════════════════════════════════════════════════════════════════════════════

class TestEdgeCases:
    """Проверяет граничные случаи и потенциальные проблемы."""

    def test_no_hardcoded_dates_in_dag1_logic(self):
        """Генерация не зависит от хардкодённых дат (кроме default params)."""
        code = _read_dag1()
        # Проверяем что monthrange используется динамически
        assert "monthrange(year, month)" in code

    def test_dag2_handles_empty_prev_day(self):
        """topics_growth_pct = None если нет данных за prev_day."""
        code = _read_dag2()
        assert "topics_growth_pct = None" in code or "topics_growth_pct =  None" in code

    def test_dag2_handles_empty_retention(self):
        """d1_retention_pct = None если нет вчерашних регистраций."""
        code = _read_dag2()
        assert "d1_retention_pct = None" in code or "d1_retention_pct =  None" in code

    def test_dag2_handles_zero_messages(self):
        """anon_messages_pct не делит на ноль."""
        code = _read_dag2()
        assert "if total_messages" in code or "total_messages > 0" in code

    def test_dag2_handles_no_sessions(self):
        """avg_session = None если нет сессий."""
        code = _read_dag2()
        assert "if durations" in code or "len(durations)" in code

    def test_dag1_pool_idx_overflow_protection(self):
        """Защита от выхода за пределы пула пользователей."""
        code = _read_dag1()
        assert "pool_idx >= len(users_pool)" in code

    def test_dag1_idempotent_load(self):
        """load_to_postgres — idempotent (TRUNCATE перед загрузкой)."""
        code = _read_dag1()
        assert "truncate" in code.lower()

    def test_gitignore_exists(self):
        assert (ROOT / ".gitignore").exists()

    def test_output_gitkeep_exists(self):
        assert (ROOT / "output" / ".gitkeep").exists()

    def test_logs_gitkeep_exists(self):
        assert (ROOT / "logs" / ".gitkeep").exists()

    def test_dag2_resolve_dates_supports_both_modes(self):
        """_resolve_dates поддерживает и params, и ds (logical_date)."""
        code = _read_dag2()
        assert "params" in code and '"ds"' in code or "'ds'" in code

    def test_dag1_uses_faker(self):
        """Генерация использует Faker для реалистичных данных."""
        code = _read_dag1()
        assert "Faker" in code or "faker" in code

    def test_schema_session_or_ip_field(self):
        """user_logs имеет поле session_or_ip."""
        sql = _read_sql().lower()
        assert "session_or_ip" in sql

    def test_dag1_logout_clamp_to_day_end(self):
        """logout_ts не выходит за пределы текущего дня."""
        code = _read_dag1()
        # Проверяем min(..., base + ...) для ограничения logout внутри дня
        assert "min(login_ts + timedelta" in code or "hours=23" in code


class TestConsistencyBetweenFiles:
    """Проверяет согласованность между файлами."""

    def test_conn_id_matches(self):
        """Оба DAG'а и docker-compose используют одинаковый conn_id."""
        dag1 = _read_dag1()
        dag2 = _read_dag2()
        compose = _read_compose()

        assert 'CONN_ID = "forum_db"' in dag1
        assert 'CONN_ID = "forum_db"' in dag2
        assert "forum_db" in compose

    def test_action_types_match_enum(self):
        """ACTION_TYPES в DAG1 совпадает с ENUM в schema.sql."""
        sql = _read_sql()
        enum_match = re.search(
            r"CREATE TYPE log_action_type AS ENUM\s*\((.*?)\);",
            sql, re.DOTALL | re.IGNORECASE
        )
        enum_values = set(re.findall(r"'(\w+)'", enum_match.group(1)))

        dag1 = _read_dag1()
        dag1_match = re.search(
            r"ACTION_TYPES\s*=\s*\[(.*?)\]",
            dag1, re.DOTALL
        )
        dag1_values = set(re.findall(r'"(\w+)"', dag1_match.group(1)))

        assert enum_values == dag1_values, \
            f"ACTION_TYPES в DAG1 {dag1_values} != ENUM в schema {enum_values}"

    def test_action_types_in_dag2_validation(self):
        """DAG2 validate_data проверяет те же action_type что в ENUM."""
        dag2 = _read_dag2()
        sql = _read_sql()
        enum_match = re.search(
            r"CREATE TYPE log_action_type AS ENUM\s*\((.*?)\);",
            sql, re.DOTALL | re.IGNORECASE
        )
        enum_values = set(re.findall(r"'(\w+)'", enum_match.group(1)))

        # Находим Check.isin в _validate_data
        isin_match = re.search(
            r'Check\.isin\(\[\s*(.*?)\]\)',
            dag2[dag2.index("def _validate_data"):], re.DOTALL
        )
        assert isin_match, "Check.isin не найден в _validate_data"
        dag2_values = set(re.findall(r'"(\w+)"', isin_match.group(1)))

        assert enum_values == dag2_values, \
            f"action_types в DAG2 validation {dag2_values} != ENUM {enum_values}"

    def test_db_name_consistent(self):
        """Имя БД farpost — везде одинаковое."""
        compose = _read_compose()
        init_db = _read("init-db/01-init-forum-db.sh")
        assert "farpost" in compose
        assert "farpost" in init_db
