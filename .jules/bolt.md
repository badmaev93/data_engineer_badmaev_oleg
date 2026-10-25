## 2023-11-20 - O(N^2) DataFrame filtering in Airflow DAG
**Learning:** In pandas data processing scripts (e.g. `dags/forum_log_aggregation.py`), filtering a full DataFrame inside a loop over unique users scales poorly (O(N^2)). Using `.sort_values()` followed by `.groupby(..., sort=False).agg(list)` is significantly faster (O(N log N) + O(N)) and preserves the chronological order when sorting by `user_id` and `created_at` simultaneously.
**Action:** When calculating duration or pairing sequential events per user in pandas, avoid `df[df['id'] == uid]`. Sort the combined keys and use `groupby().agg(list)` instead.
## 2026-10-24 - Pandas iterrows optimization
**Learning:** `itertuples()` is significantly faster than `iterrows()` for iterating through Pandas DataFrames. When migrating from `iterrows()` to `itertuples()`, checking for missing numerical values requires updating from `is not None` to `pd.notna()`, as `itertuples` returns namedtuples where missing values are represented as `np.nan` instead of `None`.
**Action:** Use `pd.notna()` for safe missing value checks when iterating with `itertuples()`.
