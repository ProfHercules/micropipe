import asyncio

import aiohttp
import pytest
from aioresponses import aioresponses

from micropipe.stages import Request
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_request():
    session = aiohttp.ClientSession()
    stage = Request(decode_func=lambda resp: resp.json(), session=session)

    input_queue = asyncio.Queue()
    input_queue.put_nowait(FlowValue("http://test.example.com"))
    input_queue.put_nowait(EndFlow())

    data = {"id": 1, "name": "test"}

    with aioresponses() as m:
        m.get("http://test.example.com", payload=data)

        await stage.flow(input_queue.get)
        assert stage._output_queue.qsize() == 2
        assert stage._output_queue.get_nowait() == FlowValue(data)
        assert stage._output_queue.get_nowait() == EndFlow()

    await session.close()
