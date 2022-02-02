import sqlite3
from typing import List, Tuple

import aiosqlite
import pytest

from micropipe.stages import SQLiteQuery
from micropipe.types import EndFlow, FlowValue, RowFlowType


def setup_test_table(test_data: List[Tuple[str, float, bool]]):
    con = sqlite3.connect("test.db")
    cur = con.cursor()
    cur.execute("DROP TABLE IF EXISTS test;")
    cur.execute("CREATE TABLE test (name text, value real, is_cool bool)")
    cur.executemany("INSERT INTO test VALUES (?, ?, ?)", test_data)
    con.commit()
    cur.close()
    con.close()


@pytest.mark.asyncio
async def test_sqlite_single_rows():
    test_data = [("foo", 50, True), ("bar", 100, True), ("fizz", 200, False)]
    setup_test_table(test_data)

    stage = SQLiteQuery(
        query_builder=lambda fv: fv.value,
        con_builder=lambda _: aiosqlite.connect("test.db"),
        row_flow=RowFlowType.EACH_ROW,
    )

    stage._input_queue.put_nowait(FlowValue("SELECT * FROM test ORDER BY value"))
    stage._input_queue.put_nowait(EndFlow())

    await stage._flow()

    rows = sorted(
        [fv.value for fv in stage._read()],
        key=lambda v: v[1],  # sort by 2nd elem in tuple (i.e. value)
    )

    assert len(rows) == 3
    assert rows == test_data


@pytest.mark.asyncio
async def test_sqlite_all_rows():
    test_data = [("foo", 50, True), ("bar", 100, True), ("fizz", 200, False)]
    setup_test_table(test_data)

    stage = SQLiteQuery(
        query_builder=lambda fv: fv.value,
        con_builder=lambda _: aiosqlite.connect("test.db"),
        row_flow=RowFlowType.ALL_ROWS,
    )

    stage._input_queue.put_nowait(FlowValue("SELECT * FROM test ORDER BY value"))
    stage._input_queue.put_nowait(EndFlow())

    await stage._flow()

    iter_rows = stage._read()[0].value

    rows = sorted(
        [row for row in iter_rows],
        key=lambda v: v[1],  # sort by 2nd elem in tuple (i.e. value)
    )

    assert len(rows) == 3
    assert rows == test_data
