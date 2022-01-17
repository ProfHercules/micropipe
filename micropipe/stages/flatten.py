from typing import Generic, List, TypeVar

from micropipe.base import PipelineStage
from micropipe.common import FlowValue

I = TypeVar("I")


class Flatten(Generic[I], PipelineStage[List[I], I]):
    async def task_handler(self, flow_val: FlowValue[List[I]]) -> bool:
        for item in flow_val.value:
            result = self.wrap_flow_value(item, flow_val.meta)
            await self.output_queue.put(result)

        return True
