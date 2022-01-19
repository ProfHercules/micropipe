from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Generic, Literal, Optional, TypeVar

import aiohttp
from aiohttp import ClientResponse, ClientSession

from micropipe.exceptions import PipelineException
from micropipe.stage.base import BaseStage
from micropipe.types import FlowValue

O = TypeVar("O")  # output

HTTP_METHOD = Literal[
    "GET",
    "HEAD",
    "POST",
    "PUT",
    "DELETE",
    "CONNECT",
    "OPTIONS",
    "TRACE",
    "PATCH",
]


class ApiCall(Generic[O], BaseStage[str, O]):
    __method: HTTP_METHOD
    __decode_func: Callable[[ClientResponse], Awaitable[O]]
    __session: Optional[ClientSession]
    __retry_limit: int
    __retry_timout_sec: int

    def __init__(
        self,
        decode_func: Callable[[ClientResponse], Awaitable[O]],
        method: HTTP_METHOD = "GET",
        retry_limit: int = 5,
        retry_timout_sec: int = 15,
        session: Optional[ClientSession] = None,
        **kwargs,
    ):
        super(ApiCall, self).__init__(**kwargs)
        self.__method = method
        self.__decode_func = decode_func
        self.__retry_limit = retry_limit
        self.__retry_timout_sec = retry_timout_sec
        self.__session = session

    async def _flow(self) -> None:
        should_close_session = False
        if self.__session is None:
            self.__session = aiohttp.ClientSession()
            should_close_session = True
        await super()._flow()
        if should_close_session:
            assert self.__session is not None
            await self.__session.close()

    async def _task_handler(self, flow_val: FlowValue[str]) -> bool:
        remaining_tries = max(self.__retry_limit + 1, 1)

        while remaining_tries > 0:
            remaining_tries -= 1
            try:
                assert self.__session is not None
                response = await self.__session.request(self.__method, flow_val.value)
                status = response.status

                if status < 200 or status >= 300:
                    raise PipelineException(f"HTTP Status Code: {status}")

                decoded = await self.__decode_func(response)
                result = self._wrap_flow_value(decoded, flow_val.meta)

                await self._output_queue.put(result)
                return True

            except Exception as e:
                self._logger.warning(
                    "[%s] TaskHandler Exception: %s (%d tries remaining)",
                    self.name,
                    e,
                    remaining_tries,
                )
                if remaining_tries > 0:
                    await asyncio.sleep(self.__retry_timout_sec)

        self._logger.warning(
            "[%s] Max retries reached, losing value from flow.",
            self.name,
        )
        return False
