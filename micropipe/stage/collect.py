from typing import Generic, List, TypeVar, Union

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
    async def _flow(self) -> None:
        cache = Deque()

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
