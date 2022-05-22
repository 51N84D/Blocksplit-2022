# %%
from ex_01_graph_connector import AaveGraphConnector
from ex_02_data_cleaner import AaveDataCleaner
import pandas as pd
import numpy as np

# %%
aave_graph = AaveGraphConnector()
data_cleaner = AaveDataCleaner()

tokens = ["dai", "usdc", "usdt"]
token_dfs = {}
for token in tokens:
    graph_data = aave_graph.get_reserve_stats(token=token, lookback_hours=100)
    cleaned_df = data_cleaner.clean_data(raw_data=graph_data, token=token)
    token_dfs[token] = cleaned_df

# %%
# Cut the data up into 6 hour chunks
# Get start of data
start_idx = None
end_idx = None
for token, df in token_dfs.items():
    if start_idx is None or df.index[0] < start_idx:
        start_idx = df.index[0]

    if end_idx is None or df.index[-1] > end_idx:
        end_idx = df.index[-1]
# %%
# Split up time series into chunks to run strategy on
interval_dates: pd.DatetimeIndex = pd.date_range(start=start_idx,
                                                 end=end_idx,
                                                 freq="360min")
# %%
data_slices = []
for datetime_idx in range(len(interval_dates) - 1):
    interval_dates[datetime_idx], interval_dates[datetime_idx + 1]
    token_df_slices = {}
    for token, df in token_dfs.items():
        token_df_slices[token] = df[
            (df.index > interval_dates[datetime_idx])
            & (df.index < interval_dates[datetime_idx + 1])]

    data_slices.append((token_df_slices, interval_dates[datetime_idx],
                        interval_dates[datetime_idx + 1]))
# %%
strategy_decisions = []
for data_slice in data_slices:
    # Choose the token with the highest mean interest rate
    aave_data = data_slice[0]
    start_date = data_slice[1]
    end_date = data_slice[2]
    best_token = None
    best_mean = 0
    for token, df in aave_data.items():
        if df.supply_rate.mean() > best_mean:
            best_mean = df.supply_rate.mean()
            best_token = token

    strategy_decisions.append((best_token, start_date, end_date))

# %%
