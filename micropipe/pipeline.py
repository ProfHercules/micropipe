from __future__ import annotations

import asyncio
import logging
from typing import List, Optional

import coloredlogs
from tqdm.asyncio import tqdm

from micropipe.stages import FlowGenerator
from micropipe.stages.base import BaseStage


class Pipeline:
    logger: logging.Logger
    stages: List[BaseStage]
    tasks: List[asyncio.Task]

    def __init__(
        self,
        stages: List[BaseStage],
        logger: Optional[logging.Logger] = None,
    ):
        assert len(stages) > 0
        assert isinstance(stages[0], FlowGenerator)
        self.stages = stages
        self.logger = logging.getLogger() if logger is None else logger
        self.tasks = []

        coloredlogs.install(level=self.logger.level, logger=logger)

    async def flow(self):
        self.logger.info(f"[Pipeline] Starting flow with {len(self.stages)} stages")

        flow_gen_task = asyncio.create_task(self.stages[0]._flow())
        self.tasks.append(flow_gen_task)

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
