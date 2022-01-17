import logging
from typing import Any, Callable, Generic, Optional, TypeVar

import aiofiles
from micropipe.base_stage import PipelineStage
from micropipe.types import FlowValue, MetaFunc

I = TypeVar("I")  # input


class StoreFile(Generic[I], PipelineStage[I, I]):
    __line_formatter: Callable[[FlowValue[I]], str]
    __filename: Callable[[FlowValue[I]], str]
    __mode: Any

    def __init__(
        self,
        line_formatter: Callable[[FlowValue[I]], str],
        filename: Callable[[FlowValue[I]], str],
        mode: Any,
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.__line_formatter = line_formatter
        self.__filename = filename
        self.__mode = mode

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        filename = self.__filename(flow_val)

        line = self.__line_formatter(flow_val)

        if not line.endswith("\n"):
            line = f"{line}\n"

        async with aiofiles.open(filename, mode=self.__mode) as f:
            await f.write(line)

        await self._output_queue.put(flow_val)

        return True
