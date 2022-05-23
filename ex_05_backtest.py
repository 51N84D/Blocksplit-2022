# %%
from ex_03_multiple_tokens import get_token_data
from ex_04_basic_strategy import get_start_end_dates, slice_data, get_strategy_decision
import pandas as pd

# %%
token_dfs = get_token_data(lookback_hours=1000)

# Split up time series into chunks to run strategy on
(start_date, end_date) = get_start_end_dates(token_dfs=token_dfs)
interval_dates: pd.DatetimeIndex = pd.date_range(start=start_date,
                                                 end=end_date,
                                                 freq="360min")

data_slices = slice_data(interval_dates=interval_dates, token_dfs=token_dfs)


# %%
# Simulate each interest accrual
def compound_interest(principal, start_date, end_date, interest_rate):
    time_in_years = (end_date - start_date).total_seconds() / (60 * 60 * 24 *
                                                               365)
    return (interest_rate * time_in_years + 1) * principal


# %%
def backtest(data_slices, interval_dates, principal=1e5, token_choice=None):
    """Given some principal amount, simulate a strategy and compute
    the final value.

    Args:
        data_slices (_type_): This is our data sliced up into chunks
        interval_dates (_type_): This is our list of periods that we update the strategy
        principal (_type_, optional): Defaults to 1e5.
        token_choice (_type_, optional): This is just an argument that let's us compare our
                                         strategy results to a fixed strategy (e.g. only DAI)
    """
    print("starting amount: ", principal)
    if token_choice is None:
        use_strategy = True
    else:
        use_strategy = False

    # Iterate through each update period
    for datetime_idx in range(1, len(interval_dates) - 1):
        # Get strategy decision at index i using data from index i-1 -> i
        previous_data = data_slices[datetime_idx - 1]
        if use_strategy:
            token_choice = get_strategy_decision(previous_data)

        # Simulate position from index i -> i+1
        current_data = data_slices[datetime_idx]

        # Compute interest on the current period
        # NOTE: we make an approximation here that the supply rate at the
        #       beginning of the period is fixed throughout the period.
        #       Of course, this isn't true. The supply rate actually
        #       changes through the course of the period.
        principal = compound_interest(
            principal=principal,
            start_date=interval_dates[datetime_idx],
            end_date=interval_dates[datetime_idx + 1],
            interest_rate=current_data[token_choice].supply_rate.iloc[0])
    print("ending amount: ", principal)


# %%
print("------- Backtest strategy -------")
backtest(data_slices=data_slices, interval_dates=interval_dates)
print("------- Backtest DAI -------")
backtest(data_slices=data_slices,
         interval_dates=interval_dates,
         token_choice="dai")
print("---------------------------------")
print("------- Backtest USDC -------")
backtest(data_slices=data_slices,
         interval_dates=interval_dates,
         token_choice="usdc")
print("---------------------------------")
print("------- Backtest USDT -------")
backtest(data_slices=data_slices,
         interval_dates=interval_dates,
         token_choice="usdt")
print("---------------------------------")

# %%
