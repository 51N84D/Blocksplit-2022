# %%
from typing import Sequence
import pandas as pd
import math
from ex_01_graph_connector import AaveGraphConnector

# %%
"""Define a mapping from token name to number of decimals in the contract.
We need this because all of the subgraph data is decimal-adjusted
"""
TOKEN_TO_DECIMALS = {"dai": 18, "usdc": 6, "usdt": 6}

# %%


class AaveDataCleaner:
    """Class for cleaning up the data from the subgraph and returning it
        as a pandas DF
    """
    def _decimal_adj(self, decimals) -> float:
        return math.pow(10, decimals)

    def decimal_adj(self, val, token):
        return val * self.decimal_adj(TOKEN_TO_DECIMALS[token])

    def decimal_unadj(self, val, token):
        return val / self._decimal_adj(TOKEN_TO_DECIMALS[token])

    def clean_data(self, raw_data: Sequence[dict], token: str):
        """
        Main function for cleaning data. Takes the raw data from the subgraph
            and returns it as a pandas DF 
        """
        df = pd.DataFrame(raw_data)

        # If there is no data, there's no need to clean
        if len(df) == 0:
            return df

        else:
            # Convert the timestamp string to a pandas datetime object
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s')

            # Adjust every column by the constant factor that Aave uses
            for col in [
                    'variableBorrowRate', 'stableBorrowRate', 'liquidityRate',
                    'averageStableBorrowRate'
            ]:
                df[col] = df[col].apply(lambda x: int(x))
                df[col] = df[col] / (10**27)

            # Convert all big int strings to type int
            for col in [
                    'totalLiquidity', 'totalATokenSupply',
                    'totalLiquidityAsCollateral', 'totalScaledVariableDebt',
                    'totalCurrentVariableDebt', 'totalPrincipalStableDebt'
            ]:
                df[col] = df[col].apply(lambda x: int(x))

            # Convert utilization rate to float
            df["utilizationRate"] = df["utilizationRate"].apply(
                lambda x: float(x))

            # Rename all columns
            df.rename(columns={
                "liquidityRate": "supply_rate",
                "totalPrincipalStableDebt": "total_stable_debt_token",
                "totalCurrentVariableDebt": "total_variable_debt_token",
                "stableBorrowRate": "borrow_rate_stable",
                "variableBorrowRate": "borrow_rate_variable",
                "utilizationRate": "utilization_rate",
            },
                      inplace=True)

            # Decimal adjust columns
            df["total_stable_debt_token"] = df[
                "total_stable_debt_token"].apply(
                    lambda x: self.decimal_unadj(val=x, token=token))
            df["total_variable_debt_token"] = df[
                "total_variable_debt_token"].apply(
                    lambda x: self.decimal_unadj(val=x, token=token))

            # Make all column names lowercase
            df.columns = map(str.lower, df.columns)

            # Use the timestamp as the index of the dataframe
            df = df.set_index('timestamp').sort_index(ascending=True)

            # --------------------------------------------------------------------
        return df


# %%
aave_graph = AaveGraphConnector()
graph_data = aave_graph.get_reserve_stats(token="dai", lookback_hours=100)

# %%
dai_data_cleaner = AaveDataCleaner()
cleaned_dai_df = dai_data_cleaner.clean_data(raw_data=graph_data, token="dai")
# %%
