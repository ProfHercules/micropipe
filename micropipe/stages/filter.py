from __future__ import annotations

from typing import Callable, Generic, TypeVar

from micropipe.stages.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")  # input


class Filter(BaseStage[I, I], Generic[I]):
    __should_keep: Callable[[FlowValue[I]], bool]

    def __init__(
        self,
        should_keep: Callable[[FlowValue[I]], bool],
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.__should_keep = should_keep

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        if self.__should_keep(flow_val):
            await self._output(flow_val.value, flow_val.meta)

        return True
