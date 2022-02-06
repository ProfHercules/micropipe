import asyncio

import diskcache
import pytest

from micropipe.stages import CollectDeque, CollectList
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_collect_list():
    input_queue = asyncio.Queue()

    for i in range(10):
        input_queue.put_nowait(FlowValue(i))
    input_queue.put_nowait(EndFlow())

    stage = CollectList()
    await stage.flow(input_queue.get)

    assert stage._output_queue.qsize() == 2
    assert stage._output_queue.get_nowait() == FlowValue([i for i in range(10)])
    assert stage._output_queue.get_nowait() == EndFlow()


@pytest.mark.asyncio
async def test_collect_list_batch_size():
    input_queue = asyncio.Queue()

    for i in range(10):
        input_queue.put_nowait(FlowValue(i))
    input_queue.put_nowait(EndFlow())

    stage = CollectList(batch_size=5)
    await stage.flow(input_queue.get)

    assert stage._output_queue.qsize() == 3
    assert stage._output_queue.get_nowait() == FlowValue([i for i in range(5)])
    assert stage._output_queue.get_nowait() == FlowValue([i for i in range(5, 10)])
    assert stage._output_queue.get_nowait() == EndFlow()


@pytest.mark.asyncio
async def test_collect_deque():
    input_queue = asyncio.Queue()

    for i in range(10):
        input_queue.put_nowait(FlowValue(i))
    input_queue.put_nowait(EndFlow())

    stage = CollectDeque()
    await stage.flow(input_queue.get)

    assert stage._output_queue.qsize() == 2
    val = stage._output_queue.get_nowait()
    assert isinstance(val.value, diskcache.Deque)
    assert len(val.value) == 10
    assert val.value.peek() == 9
    assert val.value.peekleft() == 0


@pytest.mark.asyncio
async def test_collect_deque_batch_size():
    input_queue = asyncio.Queue()

    for i in range(10):
        input_queue.put_nowait(FlowValue(i))
    input_queue.put_nowait(EndFlow())

    stage = CollectDeque(batch_size=5)
    await stage.flow(input_queue.get)

    assert stage._output_queue.qsize() == 3

    # first batch
    val = stage._output_queue.get_nowait()
    assert isinstance(val.value, diskcache.Deque)
    assert len(val.value) == 5
    assert val.value.peek() == 4
    assert val.value.peekleft() == 0

    # second batch
    val = stage._output_queue.get_nowait()
    assert isinstance(val.value, diskcache.Deque)
    assert len(val.value) == 5
    assert val.value.peek() == 9
    assert val.value.peekleft() == 5
