"""Pipeline that pulls some data from the OpenDota API it:

    1. Pulls a list of proPlayers
    2. Saves this to a csv file
    3. Uses the account_ids from that list to get match history for each pro
    4. Then combined the acount_id with the match_history data
    5. And saves it to a csv file (so the 2 dataframes could be joined on account_id)

"""
import pandas as pd

from micropipe import Pipeline, stages

# create a pipeline
pipeline = Pipeline(
    stages=[
        # call the endpoint and turn to response into json
        stages.Request(lambda resp: resp.json()),
        # transform the json response into a dataframe
        stages.Transform(lambda fv: pd.DataFrame(fv.value)),
        # save dataframe as csv file
        stages.Passthrough(lambda fv: fv.value.to_csv("pro_players.csv", index=False)),
        # extract just the account_id's
        stages.Transform(lambda fv: fv.value["account_id"].astype(str).tolist()[:10]),
        # flatten the list of account ids so we can work with each individually
        # also add the account_id to meta_data so we can keep track of it
        stages.Flatten(meta_func=lambda acc_id, _: {"account_id": acc_id}),
        # generate urls to call using the previously flattened list of account_ids
        stages.UrlGenerator(
            template_url="https://api.opendota.com/api/players/{account_id}/matches",
            params=lambda fv: {"account_id": fv.value},
        ),
        # rate limit to 1 per sec (60 per min) as OpenDota requires
        stages.RateLimit(max_per_sec=1),
        # call the endpoints
        stages.Request(lambda r: r.json()),
        # turn list of matches into a df
        stages.Transform(lambda fv: pd.DataFrame(fv.value)),
        # assign account_id column from meta_data
        stages.Transform(lambda fv: fv.value.assign(account_id=fv.meta["account_id"])),
        # collect the dataframes into a list of dataframes using CollectDeque
        # which stores the list in a local disk cache, so we don't use too much memory
        stages.CollectDeque(),
        # concat the list of dataframes into a single dataframe
        stages.Transform(lambda fv: pd.concat(fv.value)),
        # save the resulting dataframe to disk
        stages.Passthrough(
            func=lambda fv: fv.value.to_csv("pro_matches.csv", index=False),
        ),
    ]
)

pipeline.pump(["https://api.opendota.com/api/proPlayers"])
