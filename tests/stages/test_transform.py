import asyncio

import pytest

from micropipe.stages import Transform
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_transform():
    def transformer(flow_val: FlowValue[int]) -> int:
        return flow_val.value ** 2

    stage = Transform[int, int](transformer)

    input_queue = asyncio.Queue()
    for i in range(10):
        input_queue.put_nowait(FlowValue(i))
    input_queue.put_nowait(EndFlow())

    await stage.flow(input_queue.get)

    for i in range(10):
        assert stage._output_queue.get_nowait().value == transformer(FlowValue(i))
