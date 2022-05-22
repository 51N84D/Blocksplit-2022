# %%
from typing import Sequence
import pandas as pd
import math
from ex_01_graph_connector import AaveGraphConnector

# %%
TOKEN_TO_DECIMALS = {"dai": 18, "usdc": 6, "usdt": 6}

# %%


class AaveDataCleaner:
    def __init__(self) -> None:
        ...

    def _decimal_adj(self, decimals) -> float:
        return math.pow(10, decimals)

    def decimal_adj(self, val, token):
        return val * self.decimal_adj(TOKEN_TO_DECIMALS[token])

    def decimal_unadj(self, val, token):
        return val / self._decimal_adj(TOKEN_TO_DECIMALS[token])

    def clean_data(self, raw_data: Sequence[dict], token: str):
        df = pd.DataFrame(raw_data)

        if len(df) == 0:
            return df

        else:
            df["timestamp"] = pd.to_datetime(df["timestamp"], unit='s')

            for col in [
                    'variableBorrowRate', 'stableBorrowRate', 'liquidityRate',
                    'averageStableBorrowRate'
            ]:
                df[col] = df[col].apply(lambda x: int(x))
                df[col] = df[col] / (10**27)

            for col in [
                    'totalLiquidity', 'totalATokenSupply',
                    'totalLiquidityAsCollateral', 'totalScaledVariableDebt',
                    'totalCurrentVariableDebt', 'totalPrincipalStableDebt'
            ]:
                df[col] = df[col].apply(lambda x: int(x))

            df["utilizationRate"] = df["utilizationRate"].apply(
                lambda x: float(x))

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

            df.columns = map(str.lower, df.columns)

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
