"""Pipeline that pulls some data from the OpenDota API it:

    1. Pulls a list of proPlayers
    2. Saves this to a csv file
    3. Uses the account_ids from that list to get match history for each pro
    4. Then combined the acount_id with the match_history data
    5. And saves it to a csv file (so the 2 dataframes could be joined on account_id)


Equivalent code without the use of micropipe:

---
import pandas as pd
import requests

# get the list of pro players from the api
base_url = "https://api.opendota.com/api"
data = requests.get(f"{base_url}/proPlayers")

# turn it into a dataframe and save as csv
players = pd.DataFrame(data.json())
players.to_csv("pro_players.csv", index=False)

# extract a list of account ids and turn it into a list of tuple of 
# (account_id, url) pairs
account_ids = players["account_id"].astype(str).tolist()[:10]
account_id_urls = [
    (account_id, f"{base_url}/players/{account_id}/matches")
    for account_id in account_ids
]

# for each pair in the previous list get the match history, add 
# account_id column, and add the resulting df to our list of dfs
dfs = []
for account_id, url in account_id_urls:
    df = pd.DataFrame(requests.get(url).json())
    df.assign(account_id=account_id)
    dfs.append(df)

# concat the entire list of dataframes and save as csv
pd.concat(dfs).to_csv("pro_matches.csv", index=False)
---
"""
import pandas as pd

from micropipe import Pipeline, stage

# create a pipeline
pipeline = Pipeline(
    stages=[
        # inject the proPlayers endpoint to start flow
        stage.FlowGenerator(value=["https://api.opendota.com/api/proPlayers"]),
        # call the endpoint and turn to response into json
        stage.ApiCall(lambda resp: resp.json()),
        # transform the json response into a dataframe
        stage.Transform(lambda fv: pd.DataFrame(fv.value)),
        # save dataframe as csv file
        stage.Passthrough(lambda fv: fv.value.to_csv("pro_players.csv", index=False)),
        # extract just the account_id's
        stage.Transform(lambda fv: fv.value["account_id"].astype(str).tolist()[:10]),
        # flatten the list of account ids so we can work with each individually
        # also add the account_id to meta_data so we can keep track of it
        stage.Flatten(meta_func=lambda acc_id, _: {"account_id": acc_id}),
        # generate urls to call using the previously flattened list of account_ids
        stage.UrlGenerator(
            template_url="https://api.opendota.com/api/players/{account_id}/matches",
            params=lambda fv: {"account_id": fv.value},
        ),
        # rate limit to 1 per sec (60 per min) as OpenDota requires
        stage.RateLimit(max_per_sec=1),
        # call the endpoints
        stage.ApiCall(lambda r: r.json()),
        # turn list of matches into a df
        stage.Transform(lambda fv: pd.DataFrame(fv.value)),
        # assign account_id column from meta_data
        stage.Transform(lambda fv: fv.value.assign(account_id=fv.meta["account_id"])),
        # collect the dataframes into a list of dataframes using CollectDeque
        # which stores the list in a local disk cache, so we don't use too much memory
        stage.CollectDeque(),
        # concat the list of dataframes into a single dataframe
        stage.Transform(lambda fv: pd.concat(fv.value)),
        # save the resulting dataframe to disk
        stage.Passthrough(
            func=lambda fv: fv.value.to_csv("pro_matches.csv", index=False),
        ),
    ]
)

# or 'awaiting' anything
pipeline.flow_sync()
