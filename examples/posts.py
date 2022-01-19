"""Simple pipeline that pulls posts from the https://jsonplaceholder.typicode.com/posts endpoint
and saves it in a pandas DataFrame.

Equivalent code without the use of micropipe:

---
import pandas as pd

df = pd.read_json("https://jsonplaceholder.typicode.com/posts")
df.to_csv("posts_clean.csv")
print(df)
---
"""
import pandas as pd

from micropipe import Pipeline, stage

# create a pipeline
pipeline = Pipeline(
    stages=[
        # generate some 'flow' by injecting the URL for the API we want to call
        stage.FlowGenerator(value=["https://jsonplaceholder.typicode.com/posts"]),
        # actually call the API, using the default GET method,
        # once we have a response decode it using resp.json()
        stage.ApiCall(lambda resp: resp.json()),
        # transform the list of posts into a pandas dataframe
        stage.Transform(lambda fv: pd.DataFrame(fv.value)),
        # use a passthrough stage to write the DF to a csv file
        stage.Passthrough(lambda fv: fv.value.to_csv("posts.csv")),
    ]
)

# let the pipeline 'flow' (sync means we don't have to worry about an event loop)
# or 'awaiting' anything
output = pipeline.flow_sync()

# the output is a list of FlowValues (hence fv), print it for the sake of visualisation
for fv in output:
    print(fv.value)
