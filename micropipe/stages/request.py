from __future__ import annotations

import asyncio
from typing import Awaitable, Callable, Generic, Optional, TypeVar

import aiohttp
from aiohttp import ClientResponse, ClientSession

from micropipe.stages.base import BaseStage
from micropipe.types import FlowValue, HttpMethod, TaskGetter

O = TypeVar("O")  # output


class Request(BaseStage[str, O], Generic[O]):
    __method: HttpMethod
    __decode_func: Callable[[ClientResponse], Awaitable[O]]
    __session: Optional[ClientSession]
    __retry_limit: int
    __retry_timout_sec: int

    def __init__(
        self,
        decode_func: Callable[[ClientResponse], Awaitable[O]],
        method: HttpMethod = HttpMethod.GET,
        retry_limit: int = 5,
        retry_timout_sec: int = 15,
        session: Optional[ClientSession] = None,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.__method = method
        self.__decode_func = decode_func
        self.__retry_limit = retry_limit
        self.__retry_timout_sec = retry_timout_sec
        self.__session = session

    async def flow(self, task_getter: TaskGetter) -> None:
        should_close_session = False
        if self.__session is None:
            self.__session = aiohttp.ClientSession()
            should_close_session = True
        await super().flow(task_getter)
        if should_close_session:
            if self.__session is None:
                raise TypeError("`Request` tried to close a None session!")
            await self.__session.close()

    async def _task_handler(self, flow_val: FlowValue[str]) -> bool:
        if self.__session is None:
            raise TypeError("`Request` session is None.")

        session = self.__session

        remaining_tries = max(self.__retry_limit + 1, 1)

        while remaining_tries > 0:
            remaining_tries -= 1
            try:
                response = await session.request(self.__method.name, flow_val.value)
                status = response.status

                if status < 200 or status >= 300:
                    raise RuntimeError(f"HTTP Status Code: {status}")

                decoded = await self.__decode_func(response)
                await self._output(decoded, flow_val.meta)

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
