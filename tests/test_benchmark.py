import math

import pandas as pd

from micropipe import Pipeline, stages


def basic_example():
    lim = 10_000
    pipeline = Pipeline(
        [
            stages.FlowGenerator(value=range(lim)),
            stages.Transform(lambda fv: fv.value ** 2),
            stages.Filter(lambda fv: fv.value % 2 == 0),
            stages.Transform(lambda fv: math.sin(fv.value)),
            stages.Transform(lambda fv: fv.value ** 2),
            stages.Transform(lambda fv: math.asin(fv.value)),
            stages.CollectList(),
            stages.Transform(lambda fv: sum(fv.value)),
        ]
    )
    result = pipeline.flow_sync()
    assert len(result) == 1
    assert result[0].value == sum(
        [math.asin(math.sin(i ** 2) ** 2) for i in range(lim) if i % 2 == 0]
    )


def test_execution_speed(benchmark):
    benchmark(basic_example)
