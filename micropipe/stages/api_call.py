import asyncio
import logging
from typing import Any, Awaitable, Callable, Generic, Literal, Optional, TypeVar

from aiohttp import ClientResponse, ClientSession
from micropipe.base import MetaFunc, Pipeline, PipelineStage
from micropipe.common import FlowValue, PipelineException

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
    method: HTTP_METHOD
    decode_func: Callable[[ClientResponse], Awaitable[O]]
    session: Optional[ClientSession]
    retry_limit: int
    retry_timout_sec: int

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
        self.method = method
        self.decode_func = decode_func
        self.retry_limit = retry_limit
        self.retry_timout_sec = retry_timout_sec
        self.session = None

    def flow(self, pipeline: Pipeline, prev_stage: Optional[PipelineStage[Any, str]]):
        self.session = pipeline.session
        return super().flow(pipeline, prev_stage)

    async def task_handler(self, flow_val: FlowValue[str]) -> bool:
        assert self.session is not None
        session = self.session
        remaining_tries = max(self.retry_limit + 1, 1)

        while remaining_tries > 0:
            remaining_tries -= 1
            try:
                response = await session.request(self.method, flow_val.value)
                status = response.status

                if status < 200 or status >= 300:
                    raise PipelineException(f"HTTP Status Code: {status}")

                decoded = await self.decode_func(response)
                result = self.wrap_flow_value(decoded, flow_val.meta)

                await self.output_queue.put(result)
                return True

            except Exception as e:
                self.logger.warning(
                    "[%s] TaskHandler Exception: %s (%d tries remaining)",
                    self.name,
                    e,
                    remaining_tries,
                )
                if remaining_tries > 0:
                    await asyncio.sleep(self.retry_timout_sec)

        self.logger.warning(
            "[%s] Max retries reached, losing value from flow.",
            self.name,
        )
        return False
