import pytest

from micropipe import FlattenStage
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_flatten():
    stage = FlattenStage()

    stage._input_queue.put_nowait(FlowValue([i for i in range(10)]))
    stage._input_queue.put_nowait(EndFlow())

    assert stage._input_queue.qsize() == 2

    await stage._flow()

    assert stage._output_queue.qsize() == 11
    assert stage._output_queue.get_nowait().value == 0
    assert stage._output_queue.get_nowait().value == 1