"""Simple pipeline that pulls posts from the https://jsonplaceholder.typicode.com/posts endpoint
and saves it in a pandas DataFrame.

"""
import pandas as pd

from micropipe import Pipeline, stages

# create a pipeline
pipeline = Pipeline(
    stages=[
        # generate some 'flow' by injecting the URL for the API we want to call
        stages.FlowGenerator(value=["https://jsonplaceholder.typicode.com/posts"]),
        # actually call the API, using the default GET method,
        # once we have a response decode it using resp.json()
        stages.ApiCall(lambda resp: resp.json()),
        # transform the list of posts into a pandas dataframe
        stages.Transform(lambda fv: pd.DataFrame(fv.value)),
        # use a passthrough stage to write the DF to a csv file
        stages.Passthrough(lambda fv: fv.value.to_csv("posts.csv")),
    ]
)

# let the pipeline 'flow' (sync means we don't have to worry about an event loop)
# or 'awaiting' anything
output = pipeline.flow_sync()

# the output is a list of FlowValues (hence fv), print it for the sake of visualisation
for fv in output:
    print(fv.value)
