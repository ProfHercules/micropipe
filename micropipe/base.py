from __future__ import annotations

import asyncio
from asyncio.log import logger
import logging
from typing import Any, Callable, Dict, Generic, List, Optional, TypeVar, Union

import coloredlogs
from aiohttp import ClientSession
from micropipe.common import EndFlow, FlowQueue, FlowValue
from tqdm.asyncio import tqdm

I = TypeVar("I")  # input
O = TypeVar("O")  # output


class Pipeline:
    logger: logging.Logger
    stages: List[PipelineStage]
    session: Optional[ClientSession]
    tasks: List[asyncio.Task]
    values_flowed: int
    values_lost: List[FlowValue[Any]]

    def __init__(
        self,
        stages: List[PipelineStage],
        logger: logging.Logger,
        session: Optional[ClientSession] = None,
    ):
        self.stages = stages
        self.logger = logger
        self.session = session
        self.tasks = []

        coloredlogs.install(level=logger.level, logger=logger)

        self.values_flowed = 0
        self.values_lost = []

    async def flow(self):
        self.logger.info(f"[Pipeline] Starting flow with {len(self.stages)} stages")

        prev_stage: Optional[PipelineStage] = None
        for stage in self.stages:
            flow_worker = stage.flow(self, prev_stage)
            if prev_stage:
                task = asyncio.create_task(flow_worker)
                self.tasks.append(task)

            prev_stage = stage

        if self.tasks:
            await tqdm.gather(*self.tasks)

        self.logger.info("[Pipeline] %d stages completed.", len(self.stages))
        pct = (1 - float(len(self.values_lost)) / float(self.values_flowed)) * 100.0
        self.logger.info(
            "[Pipeline] %d lost of %d values flowed (%.2f %% success).",
            len(self.values_lost),
            self.values_flowed,
            pct,
        )
        self.logger.debug(f"Values Lost:\n{self.values_lost}")
        return self.stages[-1].read()


# func(output_val, meta_data) -> FlowVal(output_val)
MetaFunc = Callable[[O, Dict[str, Any]], Dict[str, Any]]


class PipelineStage(Generic[I, O]):
    prev_stage: Optional[PipelineStage[Any, I]]
    output_queue: FlowQueue[O]
    meta_func: Optional[MetaFunc]
    logger: logging.Logger

    def __init__(
        self,
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self.output_queue = asyncio.Queue()
        self.meta_func = meta_func
        self.logger = logging.getLogger() if logger is None else logger
        self.prev_stage = None

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def read(self):
        output = []
        while not self.output_queue.empty():
            res = self.output_queue.get_nowait()

            if not isinstance(res, EndFlow):
                output.append(res)

        return output

    def flow(self, pipeline: Pipeline, prev_stage: Optional[PipelineStage[Any, I]]):
        assert prev_stage is not None
        self.prev_stage = prev_stage
        self.logger = pipeline.logger

        return self.worker(pipeline)

    async def worker(self, pipeline: Pipeline):
        assert self.prev_stage is not None
        prev_stage = self.prev_stage
        tasks = []
        scheduled = 0

        while True:
            value = await prev_stage.output_queue.get()

            if isinstance(value, EndFlow):
                break
            # else
            task = asyncio.create_task(self.task_shield(pipeline, value))
            scheduled += 1
            tasks.append(task)

        results = await tqdm.gather(*tasks)
        success: int = sum(map(int, results))

        pct = round(float(success) / float(scheduled) * 100.0, 1)

        self.logger.info(
            "[%s] %d / %d (%.1f %%) tasks completed successfully.",
            self.name,
            success,
            scheduled,
            pct,
        )

        await self.output_queue.put(EndFlow())

    def wrap_flow_value(self, out_val: O, meta: Dict[str, Any]) -> FlowValue[O]:
        if self.meta_func:
            meta = self.meta_func(out_val, meta)
        return FlowValue(out_val, meta)

    async def task_shield(self, pipeline: Pipeline, flow_val: FlowValue[I]) -> bool:
        success = True
        try:
            success = await self.task_handler(flow_val)
        except Exception as e:
            self.logger.exception("[%s] TaskHandler Exception: %s", self.name, e)
            success = False

        if success:
            pipeline.values_flowed += 1
        else:
            new_meta = flow_val.meta
            new_meta["drop_stage"] = self.name
            pipeline.values_lost.append(FlowValue(flow_val.value, new_meta))

        return success

    async def task_handler(self, flow_val: FlowValue[I]) -> bool:
        ...
        # try:
        #     # tapped = self.apply_flow_tap(in_val)
        #     # intermediate = ...
        #     # inject = self.apply_injector(in_val, intermediate)
        #     # await self.output_queue.put(inject)
        #     pass
        # except:
        #     # log it
        #     pass
