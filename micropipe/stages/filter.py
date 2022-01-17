import logging
from typing import Callable, Generic, Optional, TypeVar

from micropipe.base import MetaFunc, PipelineStage
from micropipe.common import FlowValue

I = TypeVar("I")  # input


class Filter(Generic[I], PipelineStage[I, I]):
    should_keep: Callable[[FlowValue[I]], bool]

    def __init__(
        self,
        should_keep: Callable[[FlowValue[I]], bool],
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.should_keep = should_keep

    async def task_handler(self, flow_val: FlowValue[I]) -> bool:
        if self.should_keep(flow_val):
            out_val = self.wrap_flow_value(flow_val.value, flow_val.meta)
            await self.output_queue.put(out_val)

        return True
