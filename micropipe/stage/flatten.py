from __future__ import annotations

from typing import Generic, List, TypeVar

from micropipe.stage.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")


class Flatten(Generic[I], BaseStage[List[I], I]):
    async def _task_handler(self, flow_val: FlowValue[List[I]]) -> bool:
        for item in flow_val.value:
            result = self._wrap_flow_value(item, flow_val.meta)
            await self._output_queue.put(result)

        return True
