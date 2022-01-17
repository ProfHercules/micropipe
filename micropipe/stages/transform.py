import logging
from typing import Callable, Generic, Optional, TypeVar

from micropipe.base import MetaFunc, PipelineStage
from micropipe.common import FlowValue

I = TypeVar("I")  # input
O = TypeVar("O")  # output


class Transform(Generic[I, O], PipelineStage[I, O]):
    transformer: Callable[[FlowValue[I]], O]

    def __init__(
        self,
        transformer: Callable[[FlowValue[I]], O],
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.transformer = transformer

    async def task_handler(self, flow_val: FlowValue[I]) -> bool:
        transformed: O = self.transformer(flow_val)
        result = self.wrap_flow_value(transformed, flow_val.meta)
        await self.output_queue.put(result)

        return True
