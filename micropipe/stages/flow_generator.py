import logging
from typing import Generic, Iterable, Optional, TypeVar

from micropipe.base_stage import PipelineStage
from micropipe.types import EndFlow, FlowValue


I = TypeVar("I")  # input


class FlowGenerator(Generic[I], PipelineStage[None, I]):
    def __init__(
        self,
        iterator: Iterable[I],
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=None, logger=logger)

        for i in iterator:
            self._output_queue.put_nowait(FlowValue(i, {}))

        self._output_queue.put_nowait(EndFlow())
