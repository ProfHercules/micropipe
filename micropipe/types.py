from __future__ import annotations

import asyncio
import json
from typing import Any, Callable, Dict, Generic, Optional, TypeVar, Union

T = TypeVar("T")
O = TypeVar("O")

# helper type aliases
MetaData = Dict[str, Any]
MetaFunc = Callable[[O, MetaData], MetaData]

# classes
class EndFlow:
    def __eq__(self, __o: object) -> bool:
        return isinstance(__o, EndFlow)


class FlowValue(Generic[T]):
    value: T
    meta: MetaData

    def __init__(self, value: T, meta: Optional[MetaData] = None):
        self.value = value
        self.meta = {} if meta is None else meta

    def __repr__(self) -> str:
        return self.toJSON()

    def __eq__(self, __o: object) -> bool:
        return (
            isinstance(__o, FlowValue)
            and __o.value == self.value
            and __o.meta == self.meta
        )

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


# type alias dependant on above classes
FlowQueue = asyncio.Queue[Union[FlowValue[T], EndFlow]]
