import pytest

from micropipe import FilterStage
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_filter():
    stage = FilterStage(should_keep=lambda x: x.value % 2 == 0)
    for i in range(10):
        stage._input_queue.put_nowait(FlowValue(i))
    stage._input_queue.put_nowait(EndFlow())

    await stage._flow()
    assert stage._output_queue.qsize() == 6
    assert stage._output_queue.get_nowait().value == 0
    assert stage._output_queue.get_nowait().value == 2