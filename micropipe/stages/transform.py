from __future__ import annotations

from typing import Callable, Generic, TypeVar

from micropipe.stages.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")  # input
O = TypeVar("O")  # output


class Transform(BaseStage[I, O], Generic[I, O]):
    __transformer: Callable[[FlowValue[I]], O]

    def __init__(
        self,
        transformer: Callable[[FlowValue[I]], O],
        **kwargs,
    ):
        super(Transform, self).__init__(**kwargs)
        self.__transformer = transformer

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        transformed = self.__transformer(flow_val)
        await self._output(transformed, flow_val.meta)

        return True
