from micropipe import EndFlow, FlowValue
from micropipe.stages import FlowGenerator


def test_flow_generator():
    gen = FlowGenerator(iterator=[1, 2, 3])
    # there should be 4 items, the 3 values + EndFlow()
    assert gen.output_queue.qsize() == 4

    assert gen.output_queue.get_nowait() == FlowValue(1, {})
    assert gen.output_queue.get_nowait() == FlowValue(2, {})
    assert gen.output_queue.get_nowait() == FlowValue(3, {})

    assert isinstance(gen.output_queue.get_nowait(), EndFlow)
