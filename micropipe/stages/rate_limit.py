import asyncio
import logging
import time
from typing import Generic, Optional, TypeVar, Union

from micropipe.base import Pipeline, PipelineStage
from micropipe.types import EndFlow, FlowValue, MetaFunc
from tqdm.asyncio import tqdm

I = TypeVar("I")  # input


class RateLimit(Generic[I], PipelineStage[I, I]):
    __max_per_sec: float

    def __init__(
        self,
        max_per_sec: float,
        concurrency_limit: int = 0,
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.__max_per_sec = max_per_sec

        # limit the size of the output queue to ensure the next stage can have
        # at most *concurrency_limit* concurrent *task_handler*s running
        if concurrency_limit >= 0:
            self._output_queue = asyncio.Queue(concurrency_limit)

    async def __rate_limiter(self):
        sleep_time = 1.0 / self.__max_per_sec

        while True:
            start = time.monotonic()
            value: Union[
                FlowValue, EndFlow
            ] = await self._prev_stage._output_queue.get()
            yield value

            sleep_for = sleep_time - (time.monotonic() - start)

            if sleep_for > 0.0:
                await asyncio.sleep(sleep_for)

    async def _worker(self, pipeline: Pipeline):
        async for value in tqdm(self.__rate_limiter(), desc=self.name):
            if isinstance(value, EndFlow):
                break
            # else
            await self._output_queue.put(value)

        await self._output_queue.put(EndFlow())
