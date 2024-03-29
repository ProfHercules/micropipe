from __future__ import annotations

from typing import (
    Callable,
    Generic,
    List,
    MutableSequence,
    Optional,
    Sequence,
    TypeVar,
    Union,
)

from diskcache import Deque

from micropipe.stages.base import BaseStage
from micropipe.types import EndFlow, FlowValue, TaskGetter

I = TypeVar("I")  # input
# currently O may be 'Sequence' in the case of Deque
O = TypeVar("O", bound=Union[Sequence, MutableSequence])  # input


# --- abstract class defined here ---


class _CollectBase(BaseStage[I, O], Generic[I, O]):
    __new_batch: Callable[[], O]
    _batch_size: int

    def __init__(
        self,
        new_batch: Callable[[], O],
        batch_size: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if batch_size == 1:
            raise ValueError("A batch size of 1 is pointless")
        self.__new_batch = new_batch
        self._batch_size = batch_size

    async def flow(self, task_getter: TaskGetter) -> None:
        batch = self.__new_batch()

        while True:
            v = await task_getter()

            if isinstance(v, EndFlow):
                break
            # else
            batch.append(v.value)

            # 0 means we never execute this branch, since
            # len(batch) > 0 (always)
            if len(batch) == self._batch_size:
                await self._output(batch)
                batch = self.__new_batch()

        if len(batch) > 0:
            await self._output(batch)

        self._logger.info("[%s] Collect complete", self.name)

        await self._mark_done()

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        pass


# -- concrete classes below here ---


class CollectList(_CollectBase[I, List[I]], Generic[I]):
    def __init__(
        self,
        batch_size: int = 0,
        **kwargs,
    ):
        super().__init__(
            lambda: [],
            batch_size,
            **kwargs,
        )


class CollectDeque(_CollectBase[I, Deque], Generic[I]):
    def __init__(
        self,
        batch_size: int = 0,
        cache_directory: Optional[str] = None,
        **kwargs,
    ):
        super().__init__(
            lambda: Deque(directory=cache_directory),
            batch_size,
            **kwargs,
        )
