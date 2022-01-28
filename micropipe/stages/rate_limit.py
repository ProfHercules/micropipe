from __future__ import annotations

import asyncio
import time
from typing import Generic, TypeVar

from tqdm.asyncio import tqdm

from micropipe.stages.base import BaseStage
from micropipe.types import EndFlow

I = TypeVar("I")  # input


class RateLimit(BaseStage[I, I], Generic[I]):
    __max_per_sec: float

    def __init__(
        self,
        max_per_sec: float,
        concurrency_limit: int = 0,
        **kwargs,
    ):
        super(RateLimit, self).__init__(**kwargs)
        self.__max_per_sec = max_per_sec
        assert self.__max_per_sec > 0.0

        # limit the size of the output queue to ensure the next stage can have
        # at most *concurrency_limit* concurrent *task_handler*s running
        if concurrency_limit >= 0:
            self._output_queue = asyncio.Queue(concurrency_limit)

    async def __limited_generator(self):
        sleep_time = 1.0 / self.__max_per_sec

        while True:
            start = time.monotonic()
            value = await self._input_queue.get()
            yield value

            sleep_for = sleep_time - (time.monotonic() - start)

            if sleep_for > 0.0:
                await asyncio.sleep(sleep_for)

    async def _flow(self):
        async for value in tqdm(self.__limited_generator(), desc=self.name):
            if isinstance(value, EndFlow):
                break
            # else
            await self._output_queue.put(value)

        await self._output_queue.put(EndFlow())
