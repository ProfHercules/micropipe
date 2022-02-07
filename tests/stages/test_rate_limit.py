import asyncio
import time

import pytest

from micropipe.stages import RateLimit
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_rate_limit():
    input_queue = asyncio.Queue()
    # associate the current time with each flow_value using meta_func
    meta_func = lambda _, __: {"time": time.time_ns()}

    stage = RateLimit(max_per_sec=1, meta_func=meta_func)

    for i in range(5):
        input_queue.put_nowait(FlowValue(i))
    input_queue.put_nowait(EndFlow())

    start = time.time_ns()
    await stage.flow(input_queue.get)
    duration = time.time_ns() - start

    assert duration >= 5 * 1_000_000_000  # for 5 values, should take at least 5 seconds
    assert duration <= 7 * 1_000_000_000  # but we don't want too much overhead

    assert stage._output_queue.qsize() == 6

    first_val = stage._output_queue.get_nowait()
    assert first_val.value == 0

    SECOND: int = 1_000_000_000

    prev_time = first_val.meta["time"]

    # now we use the associated meta data "time" to ensure each value was processed _at least_
    # 1 second after the previous
    while not stage._output_queue.empty():
        val = stage._output_queue.get_nowait()
        if isinstance(val, EndFlow):
            break
        assert val.meta["time"] > prev_time + SECOND
        prev_time = val.meta["time"]


def test_rate_limit_not_zero():
    with pytest.raises(ValueError):
        _ = RateLimit(max_per_sec=0)
