import asyncio

import pytest

from micropipe.stages import Flatten
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_flatten():
    stage = Flatten()

    input_queue = asyncio.Queue()
    input_queue.put_nowait(FlowValue([i for i in range(10)]))
    input_queue.put_nowait(EndFlow())

    assert input_queue.qsize() == 2

    await stage.flow(input_queue.get)

    assert stage._output_queue.qsize() == 11
    assert stage._output_queue.get_nowait().value == 0
    assert stage._output_queue.get_nowait().value == 1
