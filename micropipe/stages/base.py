from __future__ import annotations

import asyncio
import logging
from abc import ABC, abstractmethod
from typing import Any, Generic, List, Optional, TypeVar, Union

from tqdm.asyncio import tqdm

from micropipe.types import EndFlow, FlowValue, MetaData, MetaFunc

I = TypeVar("I")  # input
O = TypeVar("O")  # output


class BaseStage(Generic[I, O], ABC):
    _input_queue: asyncio.Queue[Union[FlowValue[I], EndFlow]]
    _output_queue: asyncio.Queue[Union[FlowValue[O], EndFlow]]
    _meta_func: Optional[MetaFunc]
    _logger: logging.Logger

    def __init__(
        self,
        input_queue: Optional[asyncio.Queue] = None,
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        self._input_queue = asyncio.Queue() if input_queue is None else input_queue
        self._output_queue = asyncio.Queue()
        self._meta_func = meta_func
        self._logger = logging.getLogger() if logger is None else logger

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

    def _connect(self, prev_stage: BaseStage[Any, I]) -> asyncio.Task:
        self._input_queue = prev_stage._output_queue
        return asyncio.create_task(self._flow())

    def _wrap_flow_value(self, out_val: O, meta: MetaData = {}) -> FlowValue[O]:
        if self._meta_func:
            meta = self._meta_func(out_val, meta)
        return FlowValue(out_val, meta)

    async def _flow(self) -> None:
        tasks = []
        scheduled = 0

        while True:
            value = await self._input_queue.get()

            if isinstance(value, EndFlow):
                break
            # else
            task = asyncio.create_task(self._task_handler(value))
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

    @abstractmethod
    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        ...
        raise NotImplementedError()  # default _task_handler should not be called
