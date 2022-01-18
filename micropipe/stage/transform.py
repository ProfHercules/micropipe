from typing import Callable, Generic, TypeVar

from micropipe.stage.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")  # input
O = TypeVar("O")  # output


class TransformStage(Generic[I, O], BaseStage[I, O]):
    __transformer: Callable[[FlowValue[I]], O]

    def __init__(
        self,
        transformer: Callable[[FlowValue[I]], O],
        **kwargs,
    ):
        super(TransformStage, self).__init__(**kwargs)
        self.__transformer = transformer

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        transformed: O = self.__transformer(flow_val)
        result = self._wrap_flow_value(transformed, flow_val.meta)
        await self._output_queue.put(result)

        return True
