# %%
from ex_01_graph_connector import AaveGraphConnector
from ex_02_data_cleaner import AaveDataCleaner

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
