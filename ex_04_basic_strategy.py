# %%
from ex_03_multiple_tokens import get_token_data
import pandas as pd


# %%
def get_start_end_dates(token_dfs):
    # Cut the data up into 6 hour chunks
    # Get start of data
    start_date = None
    end_date = None
    for token, df in token_dfs.items():
        if start_date is None or df.index[0] < start_date:
            start_date = df.index[0]

        if end_date is None or df.index[-1] > end_date:
            end_date = df.index[-1]
    return (start_date, end_date)


# %%
def slice_data(interval_dates, token_dfs):
    data_slices = []
    for datetime_idx in range(len(interval_dates) - 1):
        token_df_slices = {}
        for token, df in token_dfs.items():
            token_df_slices[token] = df[
                (df.index > interval_dates[datetime_idx])
                & (df.index < interval_dates[datetime_idx + 1])]

        data_slices.append(token_df_slices)
    return data_slices


# %%
def get_strategy_decision(aave_data):
    # Choose the token with the highest mean interest rate
    best_token = None
    best_mean = 0
    for token, df in aave_data.items():
        if df.supply_rate.mean() > best_mean:
            best_mean = df.supply_rate.mean()
            best_token = token

    return best_token


# %%
token_dfs = get_token_data(100)

# Split up time series into chunks to run strategy on
(start_date, end_date) = get_start_end_dates(token_dfs=token_dfs)
interval_dates: pd.DatetimeIndex = pd.date_range(start=start_date,
                                                 end=end_date,
                                                 freq="360min")

data_slices = slice_data(interval_dates=interval_dates, token_dfs=token_dfs)

# %%
for aave_data in data_slices:
    best_token = get_strategy_decision(aave_data=aave_data)
    print(best_token)

# %%
