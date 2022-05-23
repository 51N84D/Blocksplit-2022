# %%
from ex_01_graph_connector import AaveGraphConnector
from ex_02_data_cleaner import AaveDataCleaner


# %%
def get_token_data(lookback_hours, tokens=["dai", "usdc", "usdt"]):
    """
    Function for getting (clean) subgraph data from multiple tokens
    """
    aave_graph = AaveGraphConnector()
    data_cleaner = AaveDataCleaner()

    token_dfs = {}
    for token in tokens:
        graph_data = aave_graph.get_reserve_stats(
            token=token, lookback_hours=lookback_hours)
        cleaned_df = data_cleaner.clean_data(raw_data=graph_data, token=token)
        token_dfs[token] = cleaned_df

    return token_dfs


# %%