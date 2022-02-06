from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterable, Generic, Iterable, List, Tuple, TypeVar, Union

import coloredlogs
from tqdm.asyncio import tqdm

from micropipe.stages.base import BaseStage
from micropipe.types import EndFlow, FlowValue

T = TypeVar("T")  # input


class Pipeline(Generic[T]):
    logger: logging.Logger
    stages: Tuple[BaseStage, ...]
    tasks: List[asyncio.Task]
    input_queue: asyncio.Queue[Union[FlowValue[T], EndFlow]]

    def __init__(
        self,
        *stages: BaseStage,
    ):
        if len(stages) == 0:
            raise ValueError("You must specify some stages")
        self.stages = stages
        self.logger = logging.getLogger()
        self.tasks = []
        self.input_queue = asyncio.Queue()

        coloredlogs.install(level=self.logger.level, logger=self.logger)

    async def _flow_generator(self, value: Union[Iterable[T], AsyncIterable[T]]):
        if isinstance(value, Iterable):
            for i in value:
                await self.input_queue.put(FlowValue(i))
        elif isinstance(value, AsyncIterable):
            async for i in value:
                await self.input_queue.put(FlowValue(i))
        else:
            raise NotImplementedError("value must be either Iterable or AsyncIterable")

        await self.input_queue.put(EndFlow())

    async def pump_async(self, value: Union[Iterable[T], AsyncIterable[T]]):
        self.logger.info(f"[Pipeline] Starting flow with {len(self.stages)} stages")

        flow_gen_task = asyncio.create_task(self._flow_generator(value))
        self.tasks.append(flow_gen_task)

        task_getter = self.input_queue.get
        first_stage = asyncio.create_task(self.stages[0].flow(task_getter))
        self.tasks.append(first_stage)

        for i in range(1, len(self.stages)):
            task_getter = self.stages[i - 1].output_task_getter
            flow = self.stages[i].flow(task_getter)
            task = asyncio.create_task(flow)
            self.tasks.append(task)

        if len(self.tasks) > 0:
            await tqdm.gather(*self.tasks)

        self.logger.info("[Pipeline] %d stages completed.", len(self.stages))

        # read final stage
        final_stage = self.stages[-1]
        output = []

        while not final_stage._output_queue.empty():
            res = final_stage._output_queue.get_nowait()

            if not isinstance(res, EndFlow):
                output.append(res)

        return output

    def pump(self, value: Union[Iterable[T], AsyncIterable[T]]):
        loop = asyncio.get_event_loop()
        output = loop.run_until_complete(self.pump_async(value))
        return output
