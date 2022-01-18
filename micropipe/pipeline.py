from __future__ import annotations

import asyncio
import logging
from typing import List

import coloredlogs
from tqdm.asyncio import tqdm

from micropipe.stage import FlowGeneratorStage
from micropipe.stage.base import BaseStage


class Pipeline:
    logger: logging.Logger
    stages: List[BaseStage]
    tasks: List[asyncio.Task]

    def __init__(
        self,
        stages: List[BaseStage],
        logger: logging.Logger,
    ):
        assert len(stages) > 0
        assert isinstance(stages[0], FlowGeneratorStage)
        self.stages = stages
        self.logger = logger
        self.tasks = []

        coloredlogs.install(level=logger.level, logger=logger)

    async def flow(self):
        self.logger.info(f"[Pipeline] Starting flow with {len(self.stages)} stages")

        for i in range(1, len(self.stages)):
            task = self.stages[i]._connect(self.stages[i - 1])
            self.tasks.append(task)

        if len(self.tasks) > 0:
            await tqdm.gather(*self.tasks)

        self.logger.info("[Pipeline] %d stages completed.", len(self.stages))
        return self.stages[-1]._read()

    def flow_sync(self):
        loop = asyncio.get_event_loop()
        output = loop.run_until_complete(self.flow())
        return output
