"""
Тесты исполнения бизнес-логики агрегации (DAG forum_log_aggregation).

Проверяет:
  - _resolve_dates (params vs logical_date)
  - DataAvailabilitySensor (poke логика, partition pruning в SQL)
  - _extract_raw_logs (смещение на -1 день, запись в parquet)
  - _validate_data (Pandera валидация, coerce, негативные тесты на schema/business rules)
  - _transform_aggregates (расчёт всех 7 метрик, работа @daily для 1 дня, граничные случаи)
  - _export_to_csv (корректность файла, структуры и колонок)
  - _alert_on_anomaly (срабатывание алертов при аномалиях и тишина при норме)

Запуск:
  cd farpost && python3 -m pytest tests/test_aggregation_execution.py -v --tb=short
"""

import io
import os
import shutil
import sys
import tempfile
from contextlib import redirect_stdout
from datetime import date, datetime, timedelta
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pandera as pa
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dags"))

# Импортируем DAG-модуль агрегации
import forum_log_aggregation as agg_mod


@pytest.fixture(autouse=True)
def setup_tmp_dirs():
    """Изолированные временные папки для каждого теста."""
    tmp_dir = tempfile.mkdtemp(prefix="farpost_agg_tmp_")
    out_dir = tempfile.mkdtemp(prefix="farpost_agg_out_")
    orig_tmp = agg_mod.TMP_DIR
    orig_out = agg_mod.OUTPUT_DIR
    agg_mod.TMP_DIR = tmp_dir
    agg_mod.OUTPUT_DIR = out_dir
    yield {"tmp_dir": tmp_dir, "out_dir": out_dir}
    agg_mod.TMP_DIR = orig_tmp
    agg_mod.OUTPUT_DIR = orig_out
    shutil.rmtree(tmp_dir, ignore_errors=True)
    shutil.rmtree(out_dir, ignore_errors=True)


# ══════════════════════════════════════════════════════════════════════════════
# 1. Тесты _resolve_dates
# ══════════════════════════════════════════════════════════════════════════════

class TestResolveDates:
    def test_uses_params_when_provided(self):
        ctx = {"params": {"date_from": "2026-10-05", "date_to": "2026-10-20"}, "ds": "2026-10-25"}
        d_from, d_to = agg_mod._resolve_dates(ctx)
        assert d_from == "2026-10-05"
        assert d_to == "2026-10-20"

    def test_falls_back_to_ds_when_params_empty(self):
        ctx = {"params": {"date_from": "", "date_to": ""}, "ds": "2026-10-15"}
        d_from, d_to = agg_mod._resolve_dates(ctx)
        assert d_from == "2026-10-15"
        assert d_to == "2026-10-15"

    def test_falls_back_to_ds_when_params_missing(self):
        ctx = {"params": {}, "ds": "2026-10-15"}
        d_from, d_to = agg_mod._resolve_dates(ctx)
        assert d_from == "2026-10-15"
        assert d_to == "2026-10-15"


# ══════════════════════════════════════════════════════════════════════════════
# 2. Тесты DataAvailabilitySensor
# ══════════════════════════════════════════════════════════════════════════════

class TestDataAvailabilitySensor:
    def test_poke_returns_true_when_data_exists(self):
        sensor = agg_mod.DataAvailabilitySensor(conn_id="forum_db", task_id="test_sensor")
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (42,)

        with patch("forum_log_aggregation.PostgresHook", return_value=mock_hook):
            res = sensor.poke({"params": {"date_from": "2026-10-10", "date_to": "2026-10-10"}, "ds": "2026-10-10"})
            assert res is True
            mock_hook.get_first.assert_called_once()
            sql, params = mock_hook.get_first.call_args[0][0], mock_hook.get_first.call_args[1]["parameters"]
            assert "created_at >= %s::timestamptz" in sql
            assert "created_at < (%s::date + INTERVAL '1 day')::timestamptz" in sql
            assert params == ("2026-10-10", "2026-10-10")

    def test_poke_returns_false_when_data_absent(self):
        sensor = agg_mod.DataAvailabilitySensor(conn_id="forum_db", task_id="test_sensor")
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = (0,)

        with patch("forum_log_aggregation.PostgresHook", return_value=mock_hook):
            res = sensor.poke({"params": {}, "ds": "2026-10-10"})
            assert res is False

    def test_poke_returns_false_when_row_is_none(self):
        sensor = agg_mod.DataAvailabilitySensor(conn_id="forum_db", task_id="test_sensor")
        mock_hook = MagicMock()
        mock_hook.get_first.return_value = None

        with patch("forum_log_aggregation.PostgresHook", return_value=mock_hook):
            res = sensor.poke({"params": {}, "ds": "2026-10-10"})
            assert res is False


# ══════════════════════════════════════════════════════════════════════════════
# 3. Тесты _extract_raw_logs
# ══════════════════════════════════════════════════════════════════════════════

class TestExtractRawLogs:
    def test_extract_queries_with_minus_one_day_offset(self, setup_tmp_dirs):
        mock_hook = MagicMock()
        sample_df = pd.DataFrame([{
            "id": "1", "user_id": "u1", "session_or_ip": "1.1.1.1",
            "action_type": "login", "description": "desc",
            "target_object_id": None, "response_status": "success",
            "response_code": 200, "created_at": pd.Timestamp("2026-10-14 12:00:00", tz="UTC")
        }])
        mock_hook.get_pandas_df.return_value = sample_df

        ctx = {"params": {"date_from": "2026-10-15", "date_to": "2026-10-15"}, "ds": "2026-10-15"}

        with patch("forum_log_aggregation.PostgresHook", return_value=mock_hook):
            agg_mod._extract_raw_logs(**ctx)
            mock_hook.get_pandas_df.assert_called_once()
            _, kwargs = mock_hook.get_pandas_df.call_args
            params = kwargs["parameters"]
            # ext_from должен быть 2026-10-14 (date_from - 1 день)
            assert params == ("2026-10-14", "2026-10-15")

            saved_path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
            assert os.path.exists(saved_path)
            loaded_df = pd.read_parquet(saved_path)
            assert len(loaded_df) == 1


# ══════════════════════════════════════════════════════════════════════════════
# 4. Тесты _validate_data (Pandera + бизнес-правила)
# ══════════════════════════════════════════════════════════════════════════════

class TestValidateData:
    def _create_valid_row(self, **overrides):
        row = {
            "id": "some-uuid",
            "user_id": "u-123",
            "session_or_ip": "192.168.1.1",
            "action_type": "login",
            "description": "Пользователь вошел",
            "target_object_id": None,
            "response_status": "success",
            "response_code": 200,
            "created_at": pd.Timestamp("2026-10-15 10:00:00", tz="UTC"),
        }
        row.update(overrides)
        return row

    def test_valid_data_passes(self, setup_tmp_dirs):
        df = pd.DataFrame([
            self._create_valid_row(),
            self._create_valid_row(action_type="create_topic", target_object_id="t-1", response_code=201),
            self._create_valid_row(action_type="create_post", user_id=None, response_code=201),
        ])
        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        # Должно пройти без исключений
        agg_mod._validate_data()

    def test_coerces_microsecond_timestamps(self, setup_tmp_dirs):
        """Проверяет coerce=True на datetime64[us, UTC]."""
        df = pd.DataFrame([self._create_valid_row()])
        # Явно задаем us точность (микросекунды Postgres)
        df["created_at"] = df["created_at"].astype("datetime64[us, UTC]")
        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        agg_mod._validate_data()

    def test_fails_on_invalid_action_type(self, setup_tmp_dirs):
        df = pd.DataFrame([self._create_valid_row(action_type="hacked_system")])
        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        with pytest.raises(pa.errors.SchemaErrors):
            agg_mod._validate_data()

    def test_fails_on_invalid_response_status(self, setup_tmp_dirs):
        df = pd.DataFrame([self._create_valid_row(response_status="pending")])
        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        with pytest.raises(pa.errors.SchemaErrors):
            agg_mod._validate_data()

    def test_fails_on_response_code_out_of_range(self, setup_tmp_dirs):
        df = pd.DataFrame([self._create_valid_row(response_code=99)])
        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        with pytest.raises(pa.errors.SchemaErrors):
            agg_mod._validate_data()

    def test_fails_on_successful_create_topic_without_user(self, setup_tmp_dirs):
        """Бизнес-правило п. 1.1 / п. 5: успешный create_topic не может быть анонимным."""
        df = pd.DataFrame([self._create_valid_row(
            action_type="create_topic",
            response_status="success",
            user_id=None,
        )])
        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        with pytest.raises(AssertionError, match="Found successful create_topic without user_id"):
            agg_mod._validate_data()


# ══════════════════════════════════════════════════════════════════════════════
# 5. Тесты _transform_aggregates
# ══════════════════════════════════════════════════════════════════════════════

class TestTransformAggregates:
    def _build_log_df(self, records):
        df = pd.DataFrame(records)
        df["created_at"] = pd.to_datetime(df["created_at"], utc=True)
        return df

    def test_daily_run_calculates_topics_growth_and_d1_retention(self, setup_tmp_dirs):
        """
        Критический тест Бага №1:
        При запуске за 1 день (date_from=date_to=2026-10-15) и наличии в df данных
        за 2026-10-14 и 2026-10-15:
        1. В итоговом датафрейме ровно 1 строка (2026-10-15).
        2. topics_growth_pct вычислен корректно (не None!).
        3. d1_retention_pct вычислен корректно (не None!).
        """
        records = [
            # ── 2026-10-14 (вчера) ─────────────────────────────
            # 2 регистрации: u1, u2
            {"id": "1", "user_id": "u1", "action_type": "register", "response_status": "success", "created_at": "2026-10-14 02:00:00"},
            {"id": "2", "user_id": "u2", "action_type": "register", "response_status": "success", "created_at": "2026-10-14 03:00:00"},
            # 2 темы вчера
            {"id": "3", "user_id": "u1", "action_type": "create_topic", "response_status": "success", "created_at": "2026-10-14 04:00:00"},
            {"id": "4", "user_id": "u2", "action_type": "create_topic", "response_status": "success", "created_at": "2026-10-14 05:00:00"},

            # ── 2026-10-15 (сегодня) ───────────────────────────
            # 1 регистрация: u3
            {"id": "5", "user_id": "u3", "action_type": "register", "response_status": "success", "created_at": "2026-10-15 01:00:00"},
            # u1 вернулся сегодня (сессия 08:00 -> 09:00 = 3600 сек)
            {"id": "6", "user_id": "u1", "action_type": "login", "response_status": "success", "created_at": "2026-10-15 08:00:00"},
            {"id": "7", "user_id": "u1", "action_type": "logout", "response_status": "success", "created_at": "2026-10-15 09:00:00"},
            # u2 НЕ вернулся сегодня
            # 3 темы сегодня (+50% рост от вчерашних 2 тем: (3 - 2)/2 * 100 = 50.0%)
            {"id": "8", "user_id": "u1", "action_type": "create_topic", "response_status": "success", "created_at": "2026-10-15 10:00:00"},
            {"id": "9", "user_id": "u1", "action_type": "create_topic", "response_status": "success", "created_at": "2026-10-15 11:00:00"},
            {"id": "10", "user_id": "u3", "action_type": "create_topic", "response_status": "success", "created_at": "2026-10-15 12:00:00"},
            # 4 сообщения: 1 аноним, 3 авторизованных -> anon_messages_pct = 25.0%
            {"id": "11", "user_id": None, "action_type": "create_post", "response_status": "success", "created_at": "2026-10-15 13:00:00"},
            {"id": "12", "user_id": "u1", "action_type": "create_post", "response_status": "success", "created_at": "2026-10-15 14:00:00"},
            {"id": "13", "user_id": "u1", "action_type": "create_post", "response_status": "success", "created_at": "2026-10-15 15:00:00"},
            {"id": "14", "user_id": "u3", "action_type": "create_post", "response_status": "success", "created_at": "2026-10-15 16:00:00"},
        ]
        df = self._build_log_df(records)
        df["session_or_ip"] = "1.1.1.1"
        df["description"] = "desc"
        df["target_object_id"] = None
        df["response_code"] = 200

        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        ctx = {"params": {"date_from": "2026-10-15", "date_to": "2026-10-15"}, "ds": "2026-10-15"}
        agg_mod._transform_aggregates(**ctx)

        agg_path = os.path.join(setup_tmp_dirs["tmp_dir"], "aggregates.parquet")
        assert os.path.exists(agg_path)
        res = pd.read_parquet(agg_path)

        # Ровно 1 целевой день
        assert len(res) == 1
        row = res.iloc[0]

        assert row["day"] == "2026-10-15"
        assert row["new_accounts"] == 1
        assert row["total_messages"] == 4
        assert row["anon_messages_pct"] == 25.0
        # (3 - 2) / 2 * 100 = 50.0%
        assert row["topics_growth_pct"] == 50.0
        # Сессия u1 длилась ровно 3600 секунд
        assert row["avg_session_duration_sec"] == 3600.0
        # Из 2 вчерашних вернулся 1 (u1) -> 1 / 2 * 100 = 50.0%
        assert row["d1_retention_pct"] == 50.0

    def test_handles_boundary_and_zero_values(self, setup_tmp_dirs):
        """Проверяет граничные случаи: 0 сообщений, 0 тем вчера, нет сессий."""
        records = [
            # День без тем вчера, без сообщений сегодня
            {"id": "1", "user_id": "u1", "action_type": "create_topic", "response_status": "success", "created_at": "2026-10-01 10:00:00"},
        ]
        df = self._build_log_df(records)
        df["session_or_ip"] = "1.1.1.1"
        df["description"] = "desc"
        df["target_object_id"] = None
        df["response_code"] = 200
        path = os.path.join(setup_tmp_dirs["tmp_dir"], "raw_logs.parquet")
        df.to_parquet(path, index=False)

        ctx = {"params": {"date_from": "2026-10-01", "date_to": "2026-10-01"}, "ds": "2026-10-01"}
        agg_mod._transform_aggregates(**ctx)

        res = pd.read_parquet(os.path.join(setup_tmp_dirs["tmp_dir"], "aggregates.parquet"))
        row = res.iloc[0]

        assert row["total_messages"] == 0
        assert row["anon_messages_pct"] == 0.0
        assert pd.isna(row["topics_growth_pct"]) or row["topics_growth_pct"] is None
        assert pd.isna(row["avg_session_duration_sec"]) or row["avg_session_duration_sec"] is None
        assert pd.isna(row["d1_retention_pct"]) or row["d1_retention_pct"] is None


# ══════════════════════════════════════════════════════════════════════════════
# 6. Тесты _export_to_csv
# ══════════════════════════════════════════════════════════════════════════════

class TestExportToCsv:
    def test_exports_correct_csv_structure(self, setup_tmp_dirs):
        sample_agg = pd.DataFrame([{
            "day": "2026-10-15",
            "new_accounts": 5,
            "anon_messages_pct": 45.5,
            "total_messages": 20,
            "topics_growth_pct": 10.0,
            "avg_session_duration_sec": 1200.5,
            "d1_retention_pct": 60.0,
        }])
        sample_agg.to_parquet(os.path.join(setup_tmp_dirs["tmp_dir"], "aggregates.parquet"), index=False)

        ctx = {"params": {"date_from": "2026-10-15", "date_to": "2026-10-15"}, "ds": "2026-10-15"}
        agg_mod._export_to_csv(**ctx)

        expected_file = os.path.join(setup_tmp_dirs["out_dir"], "aggregation_2026-10-15_to_2026-10-15.csv")
        assert os.path.exists(expected_file), f"Файл {expected_file} не найден"

        df_csv = pd.read_csv(expected_file)
        expected_cols = [
            "day", "new_accounts", "anon_messages_pct", "total_messages",
            "topics_growth_pct", "avg_session_duration_sec", "d1_retention_pct"
        ]
        assert list(df_csv.columns) == expected_cols
        assert len(df_csv) == 1
        assert df_csv.iloc[0]["new_accounts"] == 5
        assert df_csv.iloc[0]["d1_retention_pct"] == 60.0


# ══════════════════════════════════════════════════════════════════════════════
# 7. Тесты _alert_on_anomaly
# ══════════════════════════════════════════════════════════════════════════════

class TestAlertOnAnomaly:
    def _run_alert(self, row_dict, setup_tmp_dirs):
        df = pd.DataFrame([row_dict])
        df.to_parquet(os.path.join(setup_tmp_dirs["tmp_dir"], "aggregates.parquet"), index=False)

        buf = io.StringIO()
        with redirect_stdout(buf):
            agg_mod._alert_on_anomaly()
        return buf.getvalue()

    def test_no_anomalies_detected(self, setup_tmp_dirs):
        output = self._run_alert({
            "day": "2026-10-15",
            "new_accounts": 10,
            "anon_messages_pct": 40.0,
            "total_messages": 30,
            "topics_growth_pct": 15.0,
            "avg_session_duration_sec": 300.0,
            "d1_retention_pct": 50.0,
        }, setup_tmp_dirs)
        assert "No anomalies detected" in output
        assert "⚠️" not in output

    def test_triggers_high_anon_messages_pct(self, setup_tmp_dirs):
        output = self._run_alert({
            "day": "2026-10-15",
            "new_accounts": 10,
            "anon_messages_pct": 85.0,  # > 80%
            "total_messages": 30,
            "topics_growth_pct": 15.0,
            "avg_session_duration_sec": 300.0,
            "d1_retention_pct": 50.0,
        }, setup_tmp_dirs)
        assert "anon_messages_pct = 85.0% (> 80%)" in output

    def test_triggers_total_messages_zero(self, setup_tmp_dirs):
        output = self._run_alert({
            "day": "2026-10-15",
            "new_accounts": 10,
            "anon_messages_pct": 0.0,
            "total_messages": 0,  # == 0
            "topics_growth_pct": 0.0,
            "avg_session_duration_sec": 300.0,
            "d1_retention_pct": 50.0,
        }, setup_tmp_dirs)
        assert "total_messages = 0" in output

    def test_triggers_short_session_duration(self, setup_tmp_dirs):
        output = self._run_alert({
            "day": "2026-10-15",
            "new_accounts": 10,
            "anon_messages_pct": 40.0,
            "total_messages": 20,
            "topics_growth_pct": 0.0,
            "avg_session_duration_sec": 5.0,  # < 10s
            "d1_retention_pct": 50.0,
        }, setup_tmp_dirs)
        assert "avg_session_duration = 5.0s (< 10s)" in output

    def test_triggers_huge_topics_growth(self, setup_tmp_dirs):
        output = self._run_alert({
            "day": "2026-10-15",
            "new_accounts": 10,
            "anon_messages_pct": 40.0,
            "total_messages": 20,
            "topics_growth_pct": 250.0,  # > 200%
            "avg_session_duration_sec": 300.0,
            "d1_retention_pct": 50.0,
        }, setup_tmp_dirs)
        assert "topics_growth_pct = 250.0% (|x| > 200%)" in output

    def test_triggers_low_d1_retention(self, setup_tmp_dirs):
        output = self._run_alert({
            "day": "2026-10-15",
            "new_accounts": 10,
            "anon_messages_pct": 40.0,
            "total_messages": 20,
            "topics_growth_pct": 0.0,
            "avg_session_duration_sec": 300.0,
            "d1_retention_pct": 3.0,  # < 5%
        }, setup_tmp_dirs)
        assert "d1_retention_pct = 3.0% (< 5%)" in output
