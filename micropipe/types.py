from __future__ import annotations

import json
from asyncio import Queue
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

    def __eq__(self, __o: object) -> bool:
        return (
            isinstance(__o, FlowValue)
            and __o.value == self.value
            and __o.meta == self.meta
        )
