import math

from micropipe import Pipeline, stages


def basic_example(lim: int = 10_000):
    pipeline = Pipeline(
        stages.Transform(lambda fv: fv.value ** 2),
        stages.Filter(lambda fv: fv.value % 2 == 0),
        stages.Transform(lambda fv: math.sin(fv.value)),
        stages.Transform(lambda fv: fv.value ** 2),
        stages.Transform(lambda fv: math.asin(fv.value)),
        stages.CollectList(),
        stages.Transform(lambda fv: sum(fv.value)),
    )

    result = pipeline.pump(range(lim))

    return result


def test_execution_speed(benchmark):
    lim = 10_000
    result = benchmark(basic_example, lim=10_000)

    assert len(result) == 1
    assert result[0].value == sum(
        [math.asin(math.sin(i ** 2) ** 2) for i in range(lim) if i % 2 == 0]
    )
