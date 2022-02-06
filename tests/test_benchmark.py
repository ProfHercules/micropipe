import math

from micropipe import Pipeline, stages


def basic_example():
    pipeline = Pipeline(
        stages.Transform(lambda fv: fv.value ** 2),
        stages.Filter(lambda fv: fv.value % 2 == 0),
        stages.Transform(lambda fv: math.sin(fv.value)),
        stages.Transform(lambda fv: fv.value ** 2),
        stages.Transform(lambda fv: math.asin(fv.value)),
        stages.CollectList(),
        stages.Transform(lambda fv: sum(fv.value)),
    )

    lim = 1_000

    result = pipeline.pump(range(lim))

    assert len(result) == 1
    assert result[0].value == sum(
        [math.asin(math.sin(i ** 2) ** 2) for i in range(lim) if i % 2 == 0]
    )


def test_execution_speed(benchmark):
    benchmark(basic_example)
