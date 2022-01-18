from typing import Generic, Iterable, TypeVar

from micropipe.stage.base import BaseStage
from micropipe.types import EndFlow

I = TypeVar("I")  # input


class FlowGeneratorStage(Generic[I], BaseStage[None, I]):
    def __init__(
        self,
        iterator: Iterable[I],
        **kwargs,
    ):
        super(FlowGeneratorStage, self).__init__(**kwargs)

        for i in iterator:
            val = self._wrap_flow_value(i, {})
            self._output_queue.put_nowait(val)

        self._output_queue.put_nowait(EndFlow())
