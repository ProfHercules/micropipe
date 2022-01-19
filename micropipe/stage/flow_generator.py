from __future__ import annotations

from typing import AsyncIterable, Generic, Iterable, TypeVar, Union

from micropipe.stage.base import BaseStage
from micropipe.types import EndFlow

I = TypeVar("I")  # input


class FlowGenerator(Generic[I], BaseStage[None, I]):
    __value: Union[Iterable[I], AsyncIterable[I]]

    def __init__(
        self,
        value: Union[Iterable[I], AsyncIterable[I]],
        **kwargs,
    ):
        super(FlowGenerator, self).__init__(**kwargs)
        self.__value = value

    async def _flow(self) -> None:
        value = self.__value
        if isinstance(value, Iterable):
            for i in value:
                val = self._wrap_flow_value(i)
                await self._output_queue.put(val)
        elif isinstance(value, AsyncIterable):
            async for i in value:
                val = self._wrap_flow_value(i)
                await self._output_queue.put(val)
        else:
            raise NotImplementedError("value must be either Iterable or AsyncIterable")

        await self._output_queue.put(EndFlow())
