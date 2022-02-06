from __future__ import annotations

import asyncio
import logging
from typing import AsyncIterable, Iterable, List, Tuple, TypeVar, Union

import coloredlogs
from tqdm.asyncio import tqdm

from micropipe.stages.base import BaseStage
from micropipe.types import EndFlow, FlowValue

I = TypeVar("I")  # input


class Pipeline:
    logger: logging.Logger
    stages: Tuple[BaseStage, ...]
    tasks: List[asyncio.Task]

    def __init__(
        self,
        *stages: BaseStage,
    ):
        assert len(stages) > 0
        self.stages = stages
        self.logger = logging.getLogger()
        self.tasks = []

        coloredlogs.install(level=self.logger.level, logger=self.logger)

    def pump(self, value: Union[Iterable[I], AsyncIterable[I]]):
        loop = asyncio.get_event_loop()
        output = loop.run_until_complete(self.pump_async(value))
        return output

    async def __pump(self, value: Union[Iterable[I], AsyncIterable[I]]):
        if isinstance(value, Iterable):
            for i in value:
                await self.stages[0]._input_queue.put(FlowValue(i))
        elif isinstance(value, AsyncIterable):
            async for i in value:
                await self.stages[0]._input_queue.put(FlowValue(i))
        else:
            raise NotImplementedError("value must be either Iterable or AsyncIterable")

        await self.stages[0]._input_queue.put(EndFlow())

    async def pump_async(self, value: Union[Iterable[I], AsyncIterable[I]]):
        self.logger.info(f"[Pipeline] Starting flow with {len(self.stages)} stages")

        flow_gen_task = asyncio.create_task(self.__pump(value))
        self.tasks.append(flow_gen_task)

        first_stage = asyncio.create_task(self.stages[0]._flow())
        self.tasks.append(first_stage)

        for i in range(1, len(self.stages)):
            task = self.stages[i]._connect(self.stages[i - 1])
            self.tasks.append(task)

        if len(self.tasks) > 0:
            await tqdm.gather(*self.tasks)

        self.logger.info("[Pipeline] %d stages completed.", len(self.stages))
        return self.stages[-1]._read()
