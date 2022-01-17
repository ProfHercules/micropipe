from __future__ import annotations

import asyncio
import logging
from typing import Any, Dict, Generic, List, Optional, TypeVar

import aiohttp
import coloredlogs
from aiohttp import ClientSession
from tqdm.asyncio import tqdm

from micropipe.types import EndFlow, FlowQueue, FlowValue, MetaFunc

I = TypeVar("I")  # input
O = TypeVar("O")  # output


class Pipeline:
    logger: logging.Logger
    stages: List[PipelineStage]
    session: Optional[ClientSession]
    tasks: List[asyncio.Task]
    values_flowed: int
    values_lost: List[FlowValue[Any]]

    did_create_session: bool

    def __init__(
        self,
        stages: List[PipelineStage],
        logger: logging.Logger,
        session: Optional[ClientSession] = None,
    ):
        assert len(stages) > 0
        self.stages = stages
        self.logger = logger
        self.session = session
        self.tasks = []

        coloredlogs.install(level=logger.level, logger=logger)

        self.values_flowed = 0
        self.values_lost = []

        self.did_create_session = False

    async def flow(self):
        self.logger.info(f"[Pipeline] Starting flow with {len(self.stages)} stages")

        if self.session == None:
            should_create_session = any(
                map(lambda s: hasattr(s, "session"), self.stages)
            )
            if should_create_session:
                self.did_create_session = True
                self.session = aiohttp.ClientSession()

        prev_stage: Optional[PipelineStage] = None
        for stage in self.stages:
            flow_worker = stage._flow(self, prev_stage)
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

        if self.did_create_session and self.session.closed == False:
            await self.session.close()

        return self.stages[-1]._read()

    def flow_sync(self):
        loop = asyncio.get_event_loop()
        output = loop.run_until_complete(self.flow())
        return output


class PipelineStage(Generic[I, O]):
    _prev_stage: Optional[PipelineStage[Any, I]]
    _output_queue: FlowQueue[O]
    _meta_func: Optional[MetaFunc]
    _logger: logging.Logger

    def __init__(
        self,
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._output_queue = asyncio.Queue()
        self._meta_func = meta_func
        self._logger = logging.getLogger() if logger is None else logger
        self._prev_stage = None

    @property
    def name(self) -> str:
        return self.__class__.__name__

    def _read(self) -> List[FlowValue[O]]:
        output = []
        while not self._output_queue.empty():
            res = self._output_queue.get_nowait()

            if not isinstance(res, EndFlow):
                output.append(res)

        return output

    def _flow(self, pipeline: Pipeline, prev_stage: Optional[PipelineStage[Any, I]]):
        assert prev_stage is not None
        self._prev_stage = prev_stage
        self._logger = pipeline.logger

        return self._worker(pipeline)

    async def _worker(self, pipeline: Pipeline):
        assert self._prev_stage is not None
        prev_stage = self._prev_stage
        tasks = []
        scheduled = 0

        while True:
            value = await prev_stage._output_queue.get()

            if isinstance(value, EndFlow):
                break
            # else
            task = asyncio.create_task(self._task_shield(pipeline, value))
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

        await self._output_queue.put(EndFlow())

    def _wrap_flow_value(self, out_val: O, meta: Dict[str, Any]) -> FlowValue[O]:
        if self._meta_func:
            meta = self._meta_func(out_val, meta)
        return FlowValue(out_val, meta)

    async def _task_shield(self, pipeline: Pipeline, flow_val: FlowValue[I]) -> bool:
        success = True
        try:
            success = await self._task_handler(flow_val)
        except Exception as e:
            self._logger.exception("[%s] TaskHandler Exception: %s", self.name, e)
            success = False

        if success:
            pipeline.values_flowed += 1
        else:
            new_meta = flow_val.meta
            new_meta["drop_stage"] = self.name
            pipeline.values_lost.append(FlowValue(flow_val.value, new_meta))

        return success

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
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
