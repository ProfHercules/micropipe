from __future__ import annotations

import asyncio
import time
from typing import Generic, TypeVar

from tqdm.asyncio import tqdm

from micropipe.stages.base import BaseStage
from micropipe.types import EndFlow, FlowValue, TaskGetter

I = TypeVar("I")  # input


class RateLimit(BaseStage[I, I], Generic[I]):
    __max_per_sec: float

    def __init__(
        self,
        max_per_sec: float,
        concurrency_limit: int = 0,
        **kwargs,
    ):
        super().__init__(**kwargs)
        if max_per_sec <= 0.0:
            raise ValueError("`max_per_sec` must be a positive value > 0")

        self.__max_per_sec = max_per_sec

        # limit the size of the output queue to ensure the next stage can have
        # at most *concurrency_limit* concurrent *task_handler*s running
        if concurrency_limit >= 0:
            self._output_queue = asyncio.Queue(concurrency_limit)

    async def __limited_generator(self, task_getter: TaskGetter):
        sleep_time = 1.0 / self.__max_per_sec

        while True:
            start = time.monotonic()
            value = await task_getter()
            yield value

            sleep_for = sleep_time - (time.monotonic() - start)

            if sleep_for > 0.0:
                await asyncio.sleep(sleep_for)

    async def flow(self, task_getter: TaskGetter):
        async for value in tqdm(self.__limited_generator(task_getter), desc=self.name):
            if isinstance(value, EndFlow):
                break
            # else
            await self._output(value)

        await self._mark_done()

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        pass
