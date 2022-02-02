from __future__ import annotations

from typing import Any, Callable, Generic, Iterable, Optional, Tuple, TypeVar, Union

import aiosqlite

from micropipe.stages.base import BaseStage
from micropipe.types import FlowValue, RowFlowType

I = TypeVar("I")  # input


class SQLiteQuery(
    BaseStage[I, Union[aiosqlite.Row, Iterable[aiosqlite.Row]]], Generic[I]
):
    # we want a function like: lambda fv: ("SELECT * FROM ...;", args1, arg2, ...)
    __query_builder: Callable[
        [FlowValue[I]],
        # return a tuple of str with an optional iterable of (any or iterable of any)
        Union[str, Tuple[str, Optional[Union[Iterable[Any], Iterable[Iterable[Any]]]]]],
    ]
    __con_builder: Callable[[FlowValue[I]], aiosqlite.Connection]
    __row_flow: RowFlowType

    def __init__(
        self,
        query_builder: Callable[
            [FlowValue[I]],
            Union[
                str, Tuple[str, Optional[Union[Iterable[Any], Iterable[Iterable[Any]]]]]
            ],
        ],
        con_builder: Callable[[FlowValue[I]], aiosqlite.Connection],
        row_flow: RowFlowType = RowFlowType.EACH_ROW,
        **kwargs,
    ):
        super(SQLiteQuery, self).__init__(**kwargs)
        self.__query_builder = query_builder
        self.__con_builder = con_builder
        self.__row_flow = row_flow

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        async with self.__con_builder(flow_val) as db:
            query = self.__query_builder(flow_val)

            cursor: Optional[aiosqlite.Cursor] = None

            if isinstance(query, str):
                cursor = await db.execute(query)
            else:
                # now we either have a str and args or str and list of args
                query_str, maybe_args = query

                if maybe_args is None:
                    cursor = await db.execute(query_str)
                # check if we're dealing with nested iterable
                elif any(isinstance(i, iter) for i in maybe_args):
                    cursor = await db.executemany(query_str, maybe_args)
                else:
                    cursor = await db.execute(query_str, maybe_args)

            if self.__row_flow is RowFlowType.EACH_ROW:
                async for row in cursor:
                    fv = self._wrap_flow_value(row)
                    await self._output_queue.put(fv)
            else:
                rows = await cursor.fetchall()
                fv = self._wrap_flow_value(rows)
                await self._output_queue.put(fv)

        return True
