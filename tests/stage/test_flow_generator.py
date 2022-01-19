import pytest
from async_timeout import asyncio

from micropipe import EndFlow, FlowGeneratorStage, FlowValue


@pytest.mark.asyncio
async def test_flow_generator_single():
    gen = FlowGeneratorStage(value=["foo"])
    await gen._flow()

    assert gen._output_queue.get_nowait() == FlowValue("foo")
    assert gen._output_queue.get_nowait() == EndFlow()


@pytest.mark.asyncio
async def test_flow_generator_multi():
    gen = FlowGeneratorStage(value=["foo", "bar", "baz"])
    await gen._flow()

    assert gen._output_queue.get_nowait() == FlowValue("foo")
    assert gen._output_queue.get_nowait() == FlowValue("bar")
    assert gen._output_queue.get_nowait() == FlowValue("baz")
    assert gen._output_queue.get_nowait() == EndFlow()


@pytest.mark.asyncio
async def test_flow_generator_async_multi():
    async def gen_nums(n: int):
        for i in range(n):
            yield i
            await asyncio.sleep(0.1)

    gen = FlowGeneratorStage(value=gen_nums(3))
    await gen._flow()

    assert gen._output_queue.qsize() == 4

    assert gen._output_queue.get_nowait() == FlowValue(0)
    assert gen._output_queue.get_nowait() == FlowValue(1)
    assert gen._output_queue.get_nowait() == FlowValue(2)
    assert gen._output_queue.get_nowait() == EndFlow()
