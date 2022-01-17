import asyncio
import logging
from typing import Any, Awaitable, Callable, Generic, Literal, Optional, TypeVar

from aiohttp import ClientResponse, ClientSession
from micropipe.base import Pipeline, PipelineStage
from micropipe.exceptions import PipelineException
from micropipe.types import FlowValue, MetaFunc

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


class ApiCall(Generic[O], PipelineStage[str, O]):
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
        meta_func: Optional[MetaFunc] = None,
        logger: Optional[logging.Logger] = None,
    ):
        super().__init__(meta_func=meta_func, logger=logger)
        self.__method = method
        self.__decode_func = decode_func
        self.__retry_limit = retry_limit
        self.__retry_timout_sec = retry_timout_sec
        self.__session = None

    def _flow(self, pipeline: Pipeline, prev_stage: Optional[PipelineStage[Any, str]]):
        self.__session = pipeline.session
        return super()._flow(pipeline, prev_stage)

    async def _task_handler(self, flow_val: FlowValue[str]) -> bool:
        assert self.__session is not None
        session = self.__session
        remaining_tries = max(self.__retry_limit + 1, 1)

        while remaining_tries > 0:
            remaining_tries -= 1
            try:
                response = await session.request(self.__method, flow_val.value)
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
