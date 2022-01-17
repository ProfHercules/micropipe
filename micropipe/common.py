import asyncio
import json
from typing import Any, Dict, Generic, TypeVar, Union


class EndFlow:
    pass


T = TypeVar("T")


class FlowValue(Generic[T]):
    value: T
    meta: Dict[str, Any]

    def __init__(self, value: T, meta: Dict[str, Any]):
        self.value = value
        self.meta = meta

    def __repr__(self) -> str:
        return self.toJSON()

    def toJSON(self):
        try:
            return json.dumps(
                self,
                default=lambda o: o.__dict__,
                sort_keys=True,
                indent=4,
            )
        except:
            return "Cannot dump value to JSON"


FlowQueue = asyncio.Queue[Union[FlowValue[T], EndFlow]]


class PipelineException(Exception):
    pass
