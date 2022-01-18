from typing import Dict

import pytest

from micropipe import EndFlow, FlowValue, PassthroughStage
from micropipe.stage.passthrough import CopyMode


# CopyMode.NONE means the object can be modified by Passthrough func
@pytest.mark.asyncio
async def test_passthrough_no_copy_modify():
    stage = PassthroughStage[Dict[str, str]](
        lambda x: x.value.__setitem__("foo", "bar"),
        copy_mode=CopyMode.NONE,
    )

    stage._input_queue.put_nowait(FlowValue({}))
    stage._input_queue.put_nowait(EndFlow())

    assert stage._input_queue.qsize() == 2
    await stage._flow()
    assert stage._output_queue.qsize() == 2

    mockval = stage._output_queue.get_nowait().value
    # object was modified by passthrough
    assert mockval.get("foo") == "bar"


# CopyMode.SHALLOW means basic attrs cannot be modified
@pytest.mark.asyncio
async def test_passthrough_shallow_copy_shallow_modify():
    original, modified = {"foo": "bar"}, {"foo": "baz"}

    stage = PassthroughStage[Dict[str, str]](
        lambda x: x.__setattr__("value", modified),
        copy_mode=CopyMode.SHALLOW,
    )

    stage._input_queue.put_nowait(FlowValue(original))
    stage._input_queue.put_nowait(EndFlow())

    assert stage._input_queue.qsize() == 2
    await stage._flow()
    assert stage._output_queue.qsize() == 2

    mockval = stage._output_queue.get_nowait().value
    # object was not modified by passthrough
    assert mockval.get("foo") == original.get("foo")


# CopyMode.SHALLOW means child references on the object can be modified by Passthrough func
@pytest.mark.asyncio
async def test_passthrough_shallow_copy_deep_modify():
    original = {"foo": {}}

    stage = PassthroughStage[Dict[str, dict]](
        lambda x: x.value["foo"].__setitem__("bar", "baz"),
        copy_mode=CopyMode.SHALLOW,
    )

    stage._input_queue.put_nowait(FlowValue(original))
    stage._input_queue.put_nowait(EndFlow())

    assert stage._input_queue.qsize() == 2
    await stage._flow()
    assert stage._output_queue.qsize() == 2

    mockval = stage._output_queue.get_nowait().value
    # object was modified by passthrough
    assert mockval.get("foo") == {"bar": "baz"}


# CopyMode.DEEP means the object is safe from modifications
@pytest.mark.asyncio
async def test_passthrough_deep_copy_deep_modify():
    original = {"foo": {}}

    stage = PassthroughStage[Dict[str, dict]](
        lambda x: x.value["foo"].__setitem__("bar", "baz"),
        copy_mode=CopyMode.DEEP,
    )

    stage._input_queue.put_nowait(FlowValue(original))
    stage._input_queue.put_nowait(EndFlow())

    assert stage._input_queue.qsize() == 2
    await stage._flow()
    assert stage._output_queue.qsize() == 2

    mockval = stage._output_queue.get_nowait().value
    # object was modified by passthrough
    assert mockval.get("foo") == {}
