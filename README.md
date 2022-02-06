# micropipe

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![codecov](https://codecov.io/gh/ProfHercules/micropipe/branch/main/graph/badge.svg?token=G1DZ9BWUWK)](https://codecov.io/gh/ProfHercules/micropipe)
[![CI](https://github.com/ProfHercules/micropipe/actions/workflows/codecov.yml/badge.svg?branch=main&event=push)](https://github.com/ProfHercules/micropipe/actions/workflows/codecov.yml)
![Libraries.io dependency status for GitHub repo](https://img.shields.io/librariesio/github/profhercules/micropipe)
![GitHub last commit](https://img.shields.io/github/last-commit/profhercules/micropipe)

`micropipe` helps you build quick & consistent linear ETL sequences using independent "stages".

Each stage is concerned only with itself, stages don't care what comes before or after them.

# Example (from `examples/posts.py`):

```python
import pandas as pd

from micropipe import Pipeline, stages

# create a pipeline
pipeline = Pipeline(
    stages=[
        # run a request (GET by default)
        # using `resp.json()` to decode the response
        stages.Request(lambda resp: resp.json()),
        # transform the list of posts into a pandas DataFrame
        stages.Transform(lambda fv: pd.DataFrame(fv.value)),
        # use a passthrough stage to write the DF to a csv file
        stages.Passthrough(lambda fv: fv.value.to_csv("posts.csv")),
    ]
)

# once the pipeline is defined, simply call `pump` with either an
# `Iterator` or an `AsyncIterator`, to begin processing data
output = pipeline.pump(["https://jsonplaceholder.typicode.com/posts"])

# the `pump` method returns the output from the final stage,
# so we can print it out if we want:
for fv in output:
    print(fv.value)
```

# Note:

This project is still in it's infancy, use at your own discretion.

# TODO

- [ ] Improve parallelization (currently we only use asyncio)
- [ ] Add more complex examples
- [ ] Add documentation
- [ ] Add integration tests for various combinations of stages
- [ ] Write a test for `RateLimiter` stage
- [x] Add unit tests for every stage
- [x] Add some basic examples
