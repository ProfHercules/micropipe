import diskcache
import pytest

from micropipe.stages import CollectDeque, CollectList
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_collect_list():
    stage = CollectList()
    for i in range(10):
        stage._input_queue.put_nowait(FlowValue(i))
    stage._input_queue.put_nowait(EndFlow())

    await stage._flow()

    assert stage._output_queue.qsize() == 2
    assert stage._output_queue.get_nowait() == FlowValue([i for i in range(10)])
    assert stage._output_queue.get_nowait() == EndFlow()


@pytest.mark.asyncio
async def test_collect_deque():
    stage = CollectDeque()
    for i in range(10):
        stage._input_queue.put_nowait(FlowValue(i))
    stage._input_queue.put_nowait(EndFlow())

    await stage._flow()

    assert stage._output_queue.qsize() == 2
    val = stage._output_queue.get_nowait()
    assert isinstance(val.value, diskcache.Deque)
    assert len(val.value) == 10
    assert val.value.peek() == 9
    assert val.value.peekleft() == 0
