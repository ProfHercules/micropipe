from __future__ import annotations

from typing import Generic, List, TypeVar

from micropipe.stages.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")


class Flatten(BaseStage[List[I], I], Generic[I]):
    async def _task_handler(self, flow_val: FlowValue[List[I]]) -> bool:
        for item in flow_val.value:
            await self._output(item, flow_val.meta)

        return True
