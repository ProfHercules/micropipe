import logging
from typing import Any, Callable, Generic, Optional, TypeVar

import aiofiles
from micropipe.base import MetaFunc, PipelineStage
from micropipe.common import FlowValue

I = TypeVar("I")  # input


class StoreFile(Generic[I], PipelineStage[I, I]):
    line_formatter: Callable[[FlowValue[I]], str]
    filename: Callable[[FlowValue[I]], str]
    mode: Any

    def __init__(
        self,
        line_formatter: Callable[[FlowValue[I]], str],
        filename: Callable[[FlowValue[I]], str],
        mode: Any,
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.line_formatter = line_formatter
        self.filename = filename
        self.mode = mode

    async def task_handler(self, flow_val: FlowValue[I]) -> bool:
        filename = self.filename(flow_val)

        line = self.line_formatter(flow_val)

        if not line.endswith("\n"):
            line = f"{line}\n"

        async with aiofiles.open(filename, mode=self.mode) as f:
            await f.write(line)

        await self.output_queue.put(flow_val)

        return True
