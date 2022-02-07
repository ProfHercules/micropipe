from __future__ import annotations

from enum import Enum
from typing import Any, Awaitable, Callable, Dict, Generic, Optional, TypeVar, Union

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


class CopyMode(Enum):
    SHALLOW = 1
    DEEP = 2
    NONE = 3


class HttpMethod(Enum):
    GET = "GET"
    POST = "POST"
    HEAD = "HEAD"
    PUT = "PUT"
    DELETE = "DELETE"
    CONNECT = "CONNECT"
    OPTIONS = "OPTIONS"
    TRACE = "TRACE"
    PATCH = "PATCH"


# helper alias that depends on above classes
TaskGetter = Callable[..., Awaitable[Union[FlowValue[T], EndFlow]]]
