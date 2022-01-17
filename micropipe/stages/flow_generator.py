import logging
from typing import Any, Generic, Iterable, Optional, TypeVar

from micropipe.base import Pipeline, PipelineStage
from micropipe.common import EndFlow, FlowValue, PipelineException

I = TypeVar("I")  # input


class FlowGenerator(Generic[I], PipelineStage[None, I]):
    def __init__(
        self,
        iterator: Iterable[I],
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=None, logger=logger)

        for i in iterator:
            self.output_queue.put_nowait(FlowValue(i, {}))

        self.output_queue.put_nowait(EndFlow())

    def __call__(self) -> None:
        pass

    def flow(self, pipeline: Pipeline, prev_stage: Optional[PipelineStage[Any, I]]):
        # prev should be none since we generate flow
        assert prev_stage is None
        self.logger = pipeline.logger
        if hasattr(self, "prev_stage") and self.prev_stage is not None:
            self.logger.error("[%s] has to be the first stage.", self.name)
            raise PipelineException(f"[{self.name}] has to be the first stage.")

        self.logger.info("[%s] Generating flow", self.name)
