import logging
from typing import Callable, Generic, Optional, TypeVar

from micropipe.base import PipelineStage
from micropipe.types import FlowValue, MetaFunc

I = TypeVar("I")  # input


class Passthrough(Generic[I], PipelineStage[I, I]):
    __func: Callable[[FlowValue[I]], None]

    def __init__(
        self,
        func: Callable[[FlowValue[I]], None],
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.__func = func

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        result = self._wrap_flow_value(flow_val.value, flow_val.meta)
        await self._output_queue.put(result)

        hash_before = hash(flow_val)
        self.__func(flow_val)
        assert hash_before == hash(flow_val)

        return True
