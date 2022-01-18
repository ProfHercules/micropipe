from __future__ import annotations

from copy import copy, deepcopy
from enum import Enum
from typing import Callable, Generic, TypeVar

from micropipe.stage.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")  # input


class CopyMode(Enum):
    SHALLOW = 1
    DEEP = 2
    NONE = 3


class PassthroughStage(Generic[I], BaseStage[I, I]):
    __func: Callable[[FlowValue[I]], None]
    __copy_mode: CopyMode

    def __init__(
        self,
        func: Callable[[FlowValue[I]], None],
        copy_mode: CopyMode = CopyMode.NONE,
        **kwargs,
    ):
        super(PassthroughStage, self).__init__(**kwargs)
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
