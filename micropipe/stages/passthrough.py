from __future__ import annotations

from copy import copy, deepcopy
from typing import Callable, Generic, TypeVar

from micropipe.stages.base import BaseStage
from micropipe.types import CopyMode, FlowValue

I = TypeVar("I")  # input


class Passthrough(BaseStage[I, I], Generic[I]):
    __func: Callable[[FlowValue[I]], None]
    __copy_mode: CopyMode

    def __init__(
        self,
        func: Callable[[FlowValue[I]], None],
        copy_mode: CopyMode = CopyMode.NONE,
        **kwargs,
    ):
        super(Passthrough, self).__init__(**kwargs)
        self.__func = func
        self.__copy_mode = copy_mode

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        result = self._wrap_flow_value(flow_val.value, flow_val.meta)
        await self._output_queue.put(result)

        if self.__copy_mode is CopyMode.NONE:
            self.__func(flow_val)
        elif self.__copy_mode is CopyMode.SHALLOW:
            self.__func(copy(flow_val))
        elif self.__copy_mode is CopyMode.DEEP:
            self.__func(deepcopy(flow_val))

        return True
