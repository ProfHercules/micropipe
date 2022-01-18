from typing import Callable, Generic, TypeVar

from micropipe.stage.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")  # input


class PassthroughStage(Generic[I], BaseStage[I, I]):
    __func: Callable[[FlowValue[I]], None]

    def __init__(
        self,
        func: Callable[[FlowValue[I]], None],
        **kwargs,
    ):
        super(PassthroughStage, self).__init__(**kwargs)
        self.__func = func

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        result = self._wrap_flow_value(flow_val.value, flow_val.meta)
        await self._output_queue.put(result)

        hash_before = hash(flow_val)
        self.__func(flow_val)
        assert hash_before == hash(flow_val)

        return True
