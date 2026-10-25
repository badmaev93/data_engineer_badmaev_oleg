import pandas as pd
import os
from datetime import datetime, timedelta

from .helpers import _resolve_dates, TMP_DIR

def _calc_new_accounts(success_df):
    return int((success_df["action_type"] == "register").sum())


def _calc_messages_stats(success_df):
    msg = success_df[success_df["action_type"] == "create_post"]
    total_messages = len(msg)
    anon_messages = int(msg["user_id"].isna().sum())
    anon_messages_pct = (
        round(anon_messages / total_messages * 100, 2)
        if total_messages
        else 0.0
    )
    return total_messages, anon_messages_pct


def _calc_topics_growth(success_today_df, success_prev_df):
    topics_today = int((success_today_df["action_type"] == "create_topic").sum())
    topics_yesterday = (
        int((success_prev_df["action_type"] == "create_topic").sum())
        if len(success_prev_df)
        else 0
    )
    if topics_yesterday > 0:
        return round((topics_today - topics_yesterday) / topics_yesterday * 100, 2)
    return None


def _calc_avg_session_duration(success_df):
    logins = success_df[success_df["action_type"] == "login"]
    logouts = success_df[success_df["action_type"] == "logout"]

    durations = []

    # Optimize by sorting once and grouping, avoiding O(N^2) dataframe filtering in a loop
    l_sorted = logins.sort_values(["user_id", "created_at"])
    lo_sorted = logouts.sort_values(["user_id", "created_at"])

    l_grouped = l_sorted.groupby("user_id", sort=False)["created_at"].agg(list)
    lo_grouped = lo_sorted.groupby("user_id", sort=False)["created_at"].agg(list)

    for uid, u_logins in l_grouped.items():
        u_logouts = lo_grouped.get(uid, [])
        for li_ts, lo_ts in zip(u_logins, u_logouts):
            dur = (lo_ts - li_ts).total_seconds()
            if dur > 0:
                durations.append(dur)

    return round(sum(durations) / len(durations), 2) if durations else None


def _calc_d1_retention(day_df, prev_df):
    reg_yesterday = (
        prev_df[
            (prev_df["action_type"] == "register")
            & (prev_df["response_status"] == "success")
        ]["user_id"]
        .dropna()
        .unique()
    )

    if len(reg_yesterday) > 0:
        active_today = set(day_df["user_id"].dropna().unique())
        returned = sum(1 for uid in reg_yesterday if uid in active_today)
        return round(returned / len(reg_yesterday) * 100, 2)
    return None


def _transform_aggregates(**context):
    """Вычисляет ежедневные метрики из логов."""
    date_from, date_to = _resolve_dates(context)
    d_from = datetime.strptime(date_from, "%Y-%m-%d").date()
    d_to = datetime.strptime(date_to, "%Y-%m-%d").date()

    df = pd.read_parquet(os.path.join(TMP_DIR, "raw_logs.parquet"))
    df["day"] = df["created_at"].dt.date

    # Агрегируем только целевые дни [d_from .. d_to], используя prev_day из df
    target_days = sorted([d for d in df["day"].unique() if d_from <= d <= d_to])
    rows = []

    for day in target_days:
        day_df = df[df["day"] == day]
        success = day_df[day_df["response_status"] == "success"]
        prev_day = day - timedelta(days=1)
        prev_df = df[(df["day"] == prev_day)]
        success_prev_df = prev_df[prev_df["response_status"] == "success"]

        new_accounts = _calc_new_accounts(success)
        total_messages, anon_messages_pct = _calc_messages_stats(success)
        topics_growth_pct = _calc_topics_growth(success, success_prev_df)
        avg_session = _calc_avg_session_duration(success)
        d1_retention_pct = _calc_d1_retention(day_df, prev_df)

        rows.append({
            "day": str(day),
            "new_accounts": new_accounts,
            "anon_messages_pct": anon_messages_pct,
            "total_messages": total_messages,
            "topics_growth_pct": topics_growth_pct,
            "avg_session_duration_sec": avg_session,
            "d1_retention_pct": d1_retention_pct,
        })

    result = pd.DataFrame(rows)
    result.to_parquet(os.path.join(TMP_DIR, "aggregates.parquet"), index=False)
    print(f"Aggregated {len(result)} day(s)")
    print(result.to_string(index=False))
