import asyncio
import logging
import time
from typing import Generic, Optional, TypeVar, Union

from micropipe.base import MetaFunc, Pipeline, PipelineStage
from micropipe.common import EndFlow, FlowValue
from tqdm.asyncio import tqdm

I = TypeVar("I")  # input


class RateLimit(Generic[I], PipelineStage[I, I]):
    max_per_sec: float

    def __init__(
        self,
        max_per_sec: float,
        concurrency_limit: int = 0,
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.max_per_sec = max_per_sec

        # limit the size of the output queue to ensure the next stage can have
        # at most *concurrency_limit* concurrent *task_handler*s running
        if concurrency_limit >= 0:
            self.output_queue = asyncio.Queue(concurrency_limit)

    async def rate_limiter(self):
        sleep_time = 1.0 / self.max_per_sec

        while True:
            start = time.monotonic()
            value: Union[FlowValue, EndFlow] = await self.prev_stage.output_queue.get()
            yield value

            sleep_for = sleep_time - (time.monotonic() - start)

            if sleep_for > 0.0:
                await asyncio.sleep(sleep_for)

    async def worker(self, pipeline: Pipeline):
        async for value in tqdm(self.rate_limiter(), desc=self.name):
            if isinstance(value, EndFlow):
                break
            # else
            await self.output_queue.put(value)

        await self.output_queue.put(EndFlow())
