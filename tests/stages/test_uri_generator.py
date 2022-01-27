import pytest

from micropipe.stages import UrlGenerator
from micropipe.types import EndFlow, FlowValue


@pytest.mark.asyncio
async def test_uri_generator():
    stage = UrlGenerator(
        template_url="https://jsonplaceholder.typicode.com/users/{id}",
        params=lambda id: {"id": str(id.value)},
    )
    for i in range(10):
        stage._input_queue.put_nowait(FlowValue(i))
    stage._input_queue.put_nowait(EndFlow())

    await stage._flow()

    assert stage._output_queue.qsize() == 11

    for id in range(10):
        fv = stage._output_queue.get_nowait()
        assert fv.value == f"https://jsonplaceholder.typicode.com/users/{id}"
