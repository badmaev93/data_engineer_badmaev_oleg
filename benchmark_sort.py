import pandas as pd
import numpy as np
import time

# Create a dummy dataframe
n = 100000
dates = pd.date_range('2023-01-01', periods=10, freq='D')

df = pd.DataFrame({
    'user_id': np.random.randint(1, 1000, n),
    'created_at': pd.to_datetime(np.random.randint(
        pd.Timestamp('2023-01-01').value,
        pd.Timestamp('2023-01-10').value,
        n
    )),
    'action_type': np.random.choice(['login', 'logout', 'create_post'], n)
})

df_auth = df.copy()
df_auth["ts"] = pd.to_datetime(df_auth["created_at"])
df_auth["day"] = df_auth["ts"].dt.date

# Baseline
start = time.time()
for (day, uid), u_df in df_auth.groupby(["day", "user_id"]):
    u_actions = u_df.sort_values("ts")
    # simulate some work
    pass
baseline_time = time.time() - start
print(f"Baseline time: {baseline_time:.4f} seconds")

# Optimized
start = time.time()
df_auth_sorted = df_auth.sort_values("ts")
for (day, uid), u_df in df_auth_sorted.groupby(["day", "user_id"]):
    u_actions = u_df
    # simulate some work
    pass
optimized_time = time.time() - start
print(f"Optimized time: {optimized_time:.4f} seconds")
print(f"Speedup: {baseline_time / optimized_time:.2f}x")
