import logging
from typing import Any, Generic, Iterable, Optional, TypeVar

from micropipe.base import Pipeline, PipelineStage
from micropipe.exceptions import PipelineException
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

    def _flow(self, pipeline: Pipeline, prev_stage: Optional[PipelineStage[Any, I]]):
        # prev should be none since we generate flow
        assert prev_stage is None
        self._logger = pipeline.logger
        if hasattr(self, "prev_stage") and self._prev_stage is not None:
            self._logger.error("[%s] has to be the first stage.", self.name)
            raise PipelineException(f"[{self.name}] has to be the first stage.")

        self._logger.info("[%s] Generating flow", self.name)
