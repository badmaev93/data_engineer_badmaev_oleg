"""
End-to-End интеграционный тест полного пайплайна (DAG 1 -> DB -> DAG 2 -> CSV).

Симулирует полный жизненный цикл данных:
  1. DAG 1 forum_data_generation:
     - _generate_users
     - _generate_logs
     - _validate_generated_data (проверка pandera и бизнес-правил)
  2. Загрузка в симулятор БД
  3. DAG 2 forum_log_aggregation:
     - check_data_availability (Sensor poke)
     - _extract_raw_logs (с -1 днем)
     - _validate_data (Pandera валидация извлеченных логов)
     - _transform_aggregates (расчет метрик)
     - _export_to_csv (экспорт в CSV)
     - _alert_on_anomaly (проверка алертов)
  4. Проверка итогового CSV на соответствие ТЗ:
     - Точное соответствие 7 колонок
     - Проверка типов и диапазонов значений
     - Проверка, что D1 retention и темп роста тем рассчитываются корректно
"""

import os
import random
import shutil
import sys
import tempfile
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "dags"))

import forum_data_generation as gen_mod
import forum_log_aggregation as agg_mod


@pytest.fixture(scope="module")
def run_full_pipeline():
    """Запускает оба DAG последовательно и возвращает пути к сгенерированным артефактам."""
    random.seed(42)

    gen_tmp = tempfile.mkdtemp(prefix="farpost_e2e_gen_")
    agg_tmp = tempfile.mkdtemp(prefix="farpost_e2e_agg_")
    out_dir = tempfile.mkdtemp(prefix="farpost_e2e_out_")

    orig_gen_tmp = gen_mod.TMP_DIR
    orig_agg_tmp = agg_mod.TMP_DIR
    orig_agg_out = agg_mod.OUTPUT_DIR

    gen_mod.TMP_DIR = gen_tmp
    agg_mod.TMP_DIR = agg_tmp
    agg_mod.OUTPUT_DIR = out_dir

    try:
        # ─── ШАГ 1: DAG 1 forum_data_generation ───────────────────────────────
        ctx_gen = {"params": {"year": 2026, "month": 10, "n_users": 250}}

        # Task 1: generate_users
        gen_mod._generate_users(**ctx_gen)
        # Task 2: generate_logs
        gen_mod._generate_logs(**ctx_gen)
        # Task 3: validate_generated_data (pandera + бизнес-правила)
        gen_mod._validate_generated_data(**ctx_gen)

        # Читаем результат генерации (симуляция таблиц БД)
        raw_logs = gen_mod._load("logs.json")
        db_logs_df = pd.DataFrame(raw_logs)
        db_logs_df["created_at"] = pd.to_datetime(db_logs_df["created_at"], utc=True)

        # ─── ШАГ 2: Симуляция PostgresHook для DAG 2 ──────────────────────────
        def mock_get_pandas_df(sql, parameters=None):
            date_from_str, date_to_str = parameters
            start_ts = pd.Timestamp(date_from_str, tz="UTC")
            # + 1 day
            end_date = pd.Timestamp(date_to_str, tz="UTC") + pd.Timedelta(days=1)
            filtered = db_logs_df[
                (db_logs_df["created_at"] >= start_ts) &
                (db_logs_df["created_at"] < end_date)
            ].copy()
            return filtered.sort_values("created_at")

        def mock_get_first(sql, parameters=None):
            df = mock_get_pandas_df(sql, parameters)
            return (len(df),)

        mock_hook = MagicMock()
        mock_hook.get_pandas_df.side_effect = mock_get_pandas_df
        mock_hook.get_first.side_effect = mock_get_first

        # ─── ШАГ 3: DAG 2 forum_log_aggregation (Полный месяц) ────────────────
        ctx_agg_month = {
            "params": {"date_from": "2026-10-01", "date_to": "2026-10-31"},
            "ds": "2026-10-31",
        }

        with patch("forum_log_aggregation.PostgresHook", return_value=mock_hook):
            # Task 1: Sensor
            sensor = agg_mod.DataAvailabilitySensor(conn_id="forum_db", task_id="check_avail")
            assert sensor.poke(ctx_agg_month) is True

            # Task 2: extract_raw_logs
            agg_mod._extract_raw_logs(**ctx_agg_month)

            # Task 3: validate_data (Pandera)
            agg_mod._validate_data(**ctx_agg_month)

            # Task 4: transform_aggregates
            agg_mod._transform_aggregates(**ctx_agg_month)

            # Task 5: export_to_csv
            agg_mod._export_to_csv(**ctx_agg_month)

            # Task 6: alert_on_anomaly
            agg_mod._alert_on_anomaly(**ctx_agg_month)

        month_csv_path = os.path.join(out_dir, "aggregation_2026-10-01_to_2026-10-31.csv")

        # ─── ШАГ 4: DAG 2 forum_log_aggregation (@daily за 1 день) ───────────
        ctx_agg_daily = {
            "params": {"date_from": "2026-10-15", "date_to": "2026-10-15"},
            "ds": "2026-10-15",
        }

        with patch("forum_log_aggregation.PostgresHook", return_value=mock_hook):
            agg_mod._extract_raw_logs(**ctx_agg_daily)
            agg_mod._validate_data(**ctx_agg_daily)
            agg_mod._transform_aggregates(**ctx_agg_daily)
            agg_mod._export_to_csv(**ctx_agg_daily)

        daily_csv_path = os.path.join(out_dir, "aggregation_2026-10-15_to_2026-10-15.csv")

        yield {
            "month_csv": pd.read_csv(month_csv_path),
            "daily_csv": pd.read_csv(daily_csv_path),
            "month_csv_path": month_csv_path,
            "daily_csv_path": daily_csv_path,
        }

    finally:
        gen_mod.TMP_DIR = orig_gen_tmp
        agg_mod.TMP_DIR = orig_agg_tmp
        agg_mod.OUTPUT_DIR = orig_agg_out
        shutil.rmtree(gen_tmp, ignore_errors=True)
        shutil.rmtree(agg_tmp, ignore_errors=True)
        shutil.rmtree(out_dir, ignore_errors=True)


class TestFullPipelineMonthRun:
    """Проверяет результат агрегации за полный месяц (31 день)."""

    def test_file_exists(self, run_full_pipeline):
        assert os.path.exists(run_full_pipeline["month_csv_path"])

    def test_exact_7_columns(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        expected = [
            "day", "new_accounts", "anon_messages_pct", "total_messages",
            "topics_growth_pct", "avg_session_duration_sec", "d1_retention_pct"
        ]
        assert list(df.columns) == expected

    def test_covers_all_31_days(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        assert len(df) == 31
        expected_days = [f"2026-10-{d:02d}" for d in range(1, 32)]
        assert df["day"].tolist() == expected_days

    def test_new_accounts_positive(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        assert (df["new_accounts"] >= 5).all(), "Каждый день должно быть >= 5 регистраций"

    def test_total_messages_positive(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        assert (df["total_messages"] >= 5).all()

    def test_anon_messages_pct_within_range(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        assert (df["anon_messages_pct"] >= 0.0).all()
        assert (df["anon_messages_pct"] <= 100.0).all()

    def test_avg_session_duration_positive(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        valid_durations = df["avg_session_duration_sec"].dropna()
        assert len(valid_durations) > 0
        assert (valid_durations > 0).all()

    def test_d1_retention_has_valid_values(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        # Со 2-го дня retention должен рассчитываться
        day_2_to_31 = df.iloc[1:]
        valid_retention = day_2_to_31["d1_retention_pct"].dropna()
        assert len(valid_retention) > 0
        assert (valid_retention >= 0.0).all()
        assert (valid_retention <= 100.0).all()
        # Должен быть ненулевой retention хотя бы в части дней
        assert (valid_retention > 0.0).any()

    def test_topics_growth_pct_calculated_from_day_2(self, run_full_pipeline):
        df = run_full_pipeline["month_csv"]
        day_2_to_31 = df.iloc[1:]
        valid_growth = day_2_to_31["topics_growth_pct"].dropna()
        assert len(valid_growth) == 30, "Со 2-го по 31-й день topics_growth_pct должен быть вычислен для каждого дня"


class TestFullPipelineDailyRun:
    """Проверяет результат ежедневного запуска (@daily) за 1 день."""

    def test_file_exists(self, run_full_pipeline):
        assert os.path.exists(run_full_pipeline["daily_csv_path"])

    def test_exact_single_row(self, run_full_pipeline):
        df = run_full_pipeline["daily_csv"]
        assert len(df) == 1
        assert df.iloc[0]["day"] == "2026-10-15"

    def test_topics_growth_pct_not_null_on_daily_run(self, run_full_pipeline):
        """Гарантирует решение Бага №1: topics_growth_pct не None при запуске за 1 день!"""
        df = run_full_pipeline["daily_csv"]
        val = df.iloc[0]["topics_growth_pct"]
        assert pd.notna(val), f"topics_growth_pct не должен быть NaN на daily run: {val}"

    def test_d1_retention_pct_not_null_on_daily_run(self, run_full_pipeline):
        """Гарантирует решение Бага №1: d1_retention_pct не None при запуске за 1 день!"""
        df = run_full_pipeline["daily_csv"]
        val = df.iloc[0]["d1_retention_pct"]
        assert pd.notna(val), f"d1_retention_pct не должен быть NaN на daily run: {val}"
        assert val > 0.0, f"d1_retention_pct должен быть > 0: {val}"
