import pandas as pd

def calc_new_accounts(success_df) -> int:
    return int((success_df["action_type"] == "register").sum())


def calc_messages_stats(success_df) -> tuple[int, float]:
    msg = success_df[success_df["action_type"] == "create_post"]
    total_messages = len(msg)
    anon_messages = int(msg["user_id"].isna().sum())
    anon_messages_pct = (
        round(anon_messages / total_messages * 100, 2)
        if total_messages
        else 0.0
    )
    return total_messages, anon_messages_pct


def calc_topics_growth(success_today_df, success_prev_df) -> float | None:
    topics_today = int((success_today_df["action_type"] == "create_topic").sum())
    topics_yesterday = (
        int((success_prev_df["action_type"] == "create_topic").sum())
        if len(success_prev_df)
        else 0
    )
    if topics_yesterday > 0:
        return round((topics_today - topics_yesterday) / topics_yesterday * 100, 2)
    return None


def calc_avg_session_duration(success_df) -> float | None:
    # Оптимизация: векторизованное вычисление продолжительности сессий
    # вместо вложенных циклов по каждому пользователю
    logins = success_df[success_df["action_type"] == "login"].dropna(subset=["user_id"]).copy()
    logouts = success_df[success_df["action_type"] == "logout"].dropna(subset=["user_id"]).copy()

    if logins.empty or logouts.empty:
        return None

    logins = logins.sort_values(["user_id", "created_at"])
    logouts = logouts.sort_values(["user_id", "created_at"])

    logins["seq"] = logins.groupby("user_id").cumcount()
    logouts["seq"] = logouts.groupby("user_id").cumcount()

    merged = pd.merge(logins, logouts, on=["user_id", "seq"], suffixes=("_in", "_out"))

    durations = (merged["created_at_out"] - merged["created_at_in"]).dt.total_seconds()
    durations = durations[durations > 0]

    return round(durations.mean(), 2) if not durations.empty else None


def calc_d1_retention(day_df, prev_df) -> float | None:
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


def build_daily_metrics(day, day_df, prev_df) -> dict:
    success = day_df[day_df["response_status"] == "success"]
    success_prev = prev_df[prev_df["response_status"] == "success"]

    return {
        "day": str(day),
        "new_accounts": calc_new_accounts(success),
        "anon_messages_pct": calc_messages_stats(success)[1],
        "total_messages": calc_messages_stats(success)[0],
        "topics_growth_pct": calc_topics_growth(success, success_prev),
        "avg_session_duration_sec": calc_avg_session_duration(success),
        "d1_retention_pct": calc_d1_retention(day_df, prev_df),
    }
