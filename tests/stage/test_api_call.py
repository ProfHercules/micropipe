import aiohttp
import pytest
from aioresponses import aioresponses

from micropipe import ApiCall
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_api_call():
    session = aiohttp.ClientSession()
    stage = ApiCall(decode_func=lambda resp: resp.json(), session=session)

    stage._input_queue.put_nowait(FlowValue("http://test.example.com"))
    stage._input_queue.put_nowait(EndFlow())

    data = {"id": 1, "name": "test"}

    with aioresponses() as m:
        m.get("http://test.example.com", payload=data)

        await stage._flow()
        assert stage._output_queue.qsize() == 2
        assert stage._output_queue.get_nowait() == FlowValue(data)
        assert stage._output_queue.get_nowait() == EndFlow()

    await session.close()
