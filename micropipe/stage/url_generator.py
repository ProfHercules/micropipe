import re
import urllib.parse
from typing import Callable, Dict, Generic, TypeVar

from micropipe.stage.base import BaseStage
from micropipe.types import FlowValue

I = TypeVar("I")  # input


class UrlGeneratorStage(Generic[I], BaseStage[I, str]):
    __template_url: str
    __params: Callable[[FlowValue[I]], Dict[str, str]]

    def __init__(
        self,
        template_url: str,
        params: Callable[[FlowValue[I]], Dict[str, str]],
        **kwargs,
    ):
        super(UrlGeneratorStage, self).__init__(**kwargs)
        self.__template_url = template_url
        self.__params = params

    def __gen_url(self, flow_val: FlowValue[I]) -> str:
        regex = re.compile(r"({[A-z0-9_-]+})")

        params = self.__params(flow_val)
        matches = regex.findall(self.__template_url)
        url = self.__template_url

        for match in matches:
            key = str(match[1:-1])

            if key in params:
                url = url.replace(match, params[key])
                del params[key]

        if len(params.keys()) > 0:
            p = urllib.parse.urlencode(params)
            url = f"{url}?{p}"

        return url

    async def _task_handler(self, flow_val: FlowValue[I]) -> bool:
        url = self.__gen_url(flow_val)

        result = self._wrap_flow_value(url, flow_val.meta)
        await self._output_queue.put(result)

        return True
