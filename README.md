# micropipe

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)
[![codecov](https://codecov.io/gh/ProfHercules/micropipe/branch/main/graph/badge.svg?token=G1DZ9BWUWK)](https://codecov.io/gh/ProfHercules/micropipe)
[![CI](https://github.com/ProfHercules/micropipe/actions/workflows/codecov.yml/badge.svg?branch=main&event=push)](https://github.com/ProfHercules/micropipe/actions/workflows/codecov.yml)
[![security: bandit](https://img.shields.io/badge/security-bandit-yellow.svg)](https://github.com/PyCQA/bandit)
![Libraries.io dependency status for GitHub repo](https://img.shields.io/librariesio/github/profhercules/micropipe)
![GitHub last commit](https://img.shields.io/github/last-commit/profhercules/micropipe)

`micropipe` helps you build quick & consistent linear ETL sequences using independent "stages".

Each stage is concerned only with itself, stages don't interact with each other.

## Basics:

Define a `Pipeline` that contains some `stage`s, each stage does exactly 1 thing (usually described by the name of the stage). Stages do not (and cannot) know about earlier or later stages.

You start a `Pipeline` by calling `pump(values)` on it, where `values` is any `Iterator` or `AsyncIterator`.
Each value in `values` is wrapped with a `FlowValue` (a thin wrapper that allows `micropipe` to associate meta-data with each value). The first stage in `Pipeline` starts processing input as soon as the first value flows down the pipe.

Each stage consumes `FlowValue[I]`s and emits new `FlowValue[O]`s (where `I` is the input type of the stage and `O` the output type). When a stage consumes an `EndFlow` value, it emits it's own `EndFlow` value and stops processing. `EndFlow` is thus used to signal that there is nothing left to process.

There are no guarantees on the order in which stages process `FlowValues` except that an `EndFlow` will always be emitted last. If ordering is important you may associate ordering meta-data with each value using a `MetaFunc`.

A `MetaFunc` (`Callable[[O, MetaData], MetaData]`) is a Python `Callable` that takes as arguments (1) the `O`utput produced by the current stage and (2) the previous `meta` data associated with the value, and returns a `MetaData` object (a simple Python dictionary containing anything)

For example, the following function associates the current time in seconds with each `FlowValue`:

```python
def meta_process_time(output: str, meta: dict) -> dict:
    return { 'time': time.time() }
```

# Current stages:

The following is a list of the stages currently implemented.

- `CollectList`
  - takes an optional `batch_size` argument
  - consumes input values and once `batch_size` number have been collected, emits a `List[I]` of length `batch_size`.
  - `batch_size` of 0 means it'll collect **all** previous values and emit only a single list.
  - `batch_size` of 1 is not allowed
- `CollectDeque`
  - the same as `CollectList` except emits a `diskcache.Deque` instead of a list.
  - useful for _large_ amounts of data
- `Filter`
  - uses a given `should_keep(FlowValue) -> bool` function to filter out values
- `Flatten`
  - Basically the inverse of `Collect_` stages
  - Consumes a `List[I]`, and for each value emits a new `I`.
- `Passthrough`
  - does nothing to the consumed values, simply emits them again, but calls a supplied `func(FlowValue)` on each
  - used, for example, to checkpoint values in the pipeline
  - can be used to build more complex pipelines
  - optional `copy_type` argument allows you to protect `Passthrough`d values from modification
- `RateLimit`
  - consumes values and emits them at a rate less than or equal to `max_per_sec`
- `Request`
  - consumes `str` values, assuming they are valid urls, calls them using the supplied `http_method` (GET, POST, etc), and then decodes the result using the supplied `decode_func` (eg. `resp.json()`).
- `Transform`
  - applies an arbitrary Python function to consumed `FlowValue`s and emits the result
- `UrlGenerator`
  - builds valid urls given a template `str` and a function that returns a `dict` of params.
  - template arguments that match keys in `params` are replaces with their values, remaining `params` are encoded in a query string (eg. `?len=10&size=20`)

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

- [ ] Improve parallelization (currently we only use asyncio, which is only single threaded)
- [ ] Add more complex examples
- [ ] Add documentation
- [ ] Add integration tests for various combinations of stages
- [x] Write a test for `RateLimiter` stage
- [x] Add unit tests for every stage
- [x] Add some basic examples
