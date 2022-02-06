import asyncio

import pytest

from micropipe.stages import Filter
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_filter():
    stage = Filter(should_keep=lambda x: x.value % 2 == 0)
    input_queue = asyncio.Queue()

    for i in range(10):
        input_queue.put_nowait(FlowValue(i))
    input_queue.put_nowait(EndFlow())

    await stage.flow(input_queue.get)
    assert stage._output_queue.qsize() == 6
    assert stage._output_queue.get_nowait().value == 0
    assert stage._output_queue.get_nowait().value == 2
