from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Generic, Optional, TypeVar, Union

from tqdm.asyncio import tqdm

from micropipe.types import EndFlow, FlowValue, MetaData, MetaFunc, TaskGetter

I = TypeVar("I")  # input
O = TypeVar("O")  # output


class BaseStage(Generic[I, O], ABC):
    _output_queue: asyncio.Queue[Union[FlowValue[O], EndFlow]]
    _meta_func: Optional[MetaFunc]
    _logger: logging.Logger

    def __init__(
        self,
        meta_func: Optional[MetaFunc] = None,
    ):
        self._output_queue = asyncio.Queue()
        self._meta_func = meta_func
        self._logger = logging.getLogger()

    @property
    def name(self) -> str:
        return self.__class__.__name__

    @property
    def output_task_getter(self) -> TaskGetter:
        return self._output_queue.get

    async def flow(self, task_getter: TaskGetter) -> None:
        tasks = []
        scheduled = 0

        while True:
            value = await task_getter()

            if isinstance(value, EndFlow):
                break
            # else
            task = asyncio.create_task(self._task_handler(value))
            scheduled += 1
            tasks.append(task)

        results = await tqdm.gather(*tasks)
        success: int = sum(map(int, results))

        pct = round(float(success) / float(scheduled) * 100.0, 1)

        self._logger.info(
            "[%s] %d / %d (%.1f %%) tasks completed successfully.",
            self.name,
            success,
            scheduled,
            pct,
        )

        await self._mark_done()

    async def _mark_done(self) -> None:
        await self._output_queue.put(EndFlow())

    async def _output(self, value: O, meta: Optional[MetaData] = None) -> None:
        if self._meta_func:
            old_meta = {} if meta is None else meta
            meta = self._meta_func(value, old_meta)
        await self._output_queue.put(FlowValue(value, meta))

    @abstractmethod
    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        ...
