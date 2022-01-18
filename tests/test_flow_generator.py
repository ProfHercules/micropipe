from micropipe import EndFlow, FlowGeneratorStage, FlowValue


def test_flow_generator():
    gen = FlowGeneratorStage(iterator=[1, 2, 3])
    # there should be 4 items, the 3 values + EndFlow()
    assert gen._output_queue.qsize() == 4

    assert gen._output_queue.get_nowait() == FlowValue(1)
    assert gen._output_queue.get_nowait() == FlowValue(2)
    assert gen._output_queue.get_nowait() == FlowValue(3)

    assert isinstance(gen._output_queue.get_nowait(), EndFlow)
