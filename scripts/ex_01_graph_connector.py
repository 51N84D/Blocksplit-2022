# %%
"""
In this first example script, we define a class that allows us to query the Aave subgraph.
This uses The Graph's Hosted Service. 
In general, if you want to get data for a specific protocol or app, The Graph is a good
    first place to look.

"""
# %%
import requests
from datetime import datetime
import pandas as pd

# %%
"""
Here we define a mapping from token name to token address
We will need the addresses to get the right data from the subgraph
"""
TOKEN_TO_ADDRESS = {
    "dai": "0x6B175474E89094C44Da98b954EedeAC495271d0F",
    "usdc": "0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48",
    "usdt": "0xdAC17F958D2ee523a2206206994597C13D831ec7"
}

# %%


class AaveGraphConnector:
    """Class for getting data from the Aave subgraph
    """
    def __init__(self):
        self.url = "https://api.thegraph.com/subgraphs/name/aave/protocol-v2"
        self.lending_pool_address = "0xB53C1a33016B2DC2fF3653530bfF1848a515c8c5".lower(
        )

    def _get_graph_data(self, query: str) -> dict:
        """Makes API call using the given query and returns the request 
        response.
        """

        headers = {'Content-Type': 'application/json'}

        response: requests.Response = requests.post(url=self.url,
                                                    json={'query': query},
                                                    headers=headers)

        if response.status_code == 200:
            return response.json()
        else:
            raise Exception(
                'Query failed and return code is {}.      {}'.format(
                    response.status_code, query))

    def get_query(self, token_address: str, start_timestamp: int,
                  end_timestamp: int):
        """
        Constructs the GraphQL query


        You can play around with the Aave subgraph here: https://thegraph.com/hosted-service/subgraph/aave/protocol-v2?version=current

        Let's break it down a bit.
        - `reserve` is the outermost object we want.
        - `(id: "{token_address})`means we're filtering for reserve objects that have 
            `token_address` as their ID.
        - `symbol`, `aEmissionPerSecond`, etc. are the fields of the reserve object that we want
        - The same logic applies to `paramsHistory`:
            - Our filter conditions go in the parentheses, in this case we are only
                selecting the first 1000, we're sorting by timestamp, and we're 
                only selecting elements that are between our start/end timestamps
        - Similarly, `variableBorrowRate`, `stableBorrowRate`, etc. are the fields of the paramHistory
            that we want to keep. 
        """

        query = f"""query {{
            reserve (id: "{token_address}") {{
                symbol
                aEmissionPerSecond
                paramsHistory(first: 1000, orderBy: timestamp, orderDirection: desc, where: {{
                timestamp_gt : {start_timestamp},
                timestamp_lt : {end_timestamp}
                }}) {{
                        variableBorrowRate
                        stableBorrowRate
                        averageStableBorrowRate
                        utilizationRate
                        liquidityRate
                        totalLiquidity
                        totalATokenSupply
                        totalLiquidityAsCollateral
                        totalScaledVariableDebt
                        totalCurrentVariableDebt
                        totalPrincipalStableDebt
                        timestamp
                }}
            }}
            }}"""
        return query

    def get_data(self,
                 token_address: str,
                 start_date: pd.Timestamp,
                 end_date: pd.Timestamp,
                 verbose=False):
        """
        Repeatedly query the subgraph until we have all data for our dates
        (since we can only get a fixed amount of data per query)
        """
        records = []
        start_timestamp = int(start_date.timestamp())
        end_timestamp = int(end_date.timestamp())
        print(f"Fetching data for token_address {token_address}")
        while end_timestamp >= start_date.timestamp():
            json_data = self._get_graph_data(
                self.get_query(token_address=token_address,
                               start_timestamp=start_timestamp,
                               end_timestamp=end_timestamp))

            if "data" not in json_data.keys():
                json_data = self._get_graph_data(
                    self.get_query(token_address=token_address,
                                   start_timestamp=start_timestamp,
                                   end_timestamp=end_timestamp))

            record = json_data['data']['reserve']['paramsHistory']
            for a in record:
                a["aEmissionPerSecond"] = json_data['data']['reserve'][
                    "aEmissionPerSecond"]

            if len(record) == 0:
                if verbose:
                    print("GraphQL ran out of records!")
                break

            end_timestamp = int(record[-1]['timestamp'])
            records.extend(record)
            timestamp_iso: str = datetime.fromtimestamp(
                end_timestamp).isoformat()

            if verbose:
                print(f"Parsed {len(record)} records untill {timestamp_iso}")

        if verbose:
            print(f"Collected {len(records)} records!")

        return records

    def get_reserve_stats(self, token: str, lookback_hours: int):
        """
        Get the start and end dates based on how far we want to lookback,
        then query the data
        """

        end_date = pd.Timestamp.now(tz='UTC')
        start_date = end_date - pd.Timedelta(hours=lookback_hours)

        token_address = TOKEN_TO_ADDRESS[token].lower(
        ) + self.lending_pool_address

        return self.get_data(token_address=token_address,
                             start_date=start_date,
                             end_date=end_date)


# %%
aave_graph = AaveGraphConnector()
graph_data = aave_graph.get_reserve_stats(token="dai", lookback_hours=100)
# %%
