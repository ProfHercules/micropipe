from __future__ import annotations

from typing import Generic, List, Optional, TypeVar, Union

from diskcache import Deque

from micropipe.stage.base import BaseStage
from micropipe.types import EndFlow, FlowValue

I = TypeVar("I")  # input


class CollectListStage(Generic[I], BaseStage[I, List[I]]):
    async def _flow(self) -> None:
        output: List[I] = []

        while True:
            v = await self._input_queue.get()

            if isinstance(v, EndFlow):
                break
            # else
            output.append(v.value)

        self._logger.info("[%s] Output list complete", self.name)
        out = FlowValue(output)

        await self._output_queue.put(out)
        await self._output_queue.put(EndFlow())


class CollectDequeStage(Generic[I], BaseStage[I, Deque]):
    __cache_directory: Optional[str]

    def __init__(
        self,
        cache_directory: Optional[str] = None,
        **kwargs,
    ):
        super(CollectDequeStage, self).__init__(**kwargs)
        self.__cache_directory = cache_directory

    async def _flow(self) -> None:
        cache = Deque(directory=self.__cache_directory)

        while True:
            v: Union[FlowValue, EndFlow] = await self._input_queue.get()

            if isinstance(v, EndFlow):
                break
            # else
            cache.append(v.value)

        self._logger.info("[%s] Deque collection completed", self.name)
        out = FlowValue(cache)

        await self._output_queue.put(out)
        await self._output_queue.put(EndFlow())
