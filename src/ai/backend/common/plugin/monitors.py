from __future__ import annotations

from abc import ABCMeta, abstractmethod
from typing import Union

from . import AbstractPlugin, BasePluginContext

__all__ = (
    'AbstractStatReporterPlugin',
    'AbstractErrorReporterPlugin',
    'StatPluginContext',
    'ErrorPluginContext',
)


class AbstractStatReporterPlugin(AbstractPlugin, metaclass=ABCMeta):

    async def init(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    @abstractmethod
    async def report_stats(self, report_type: str, metric: Union[int, float], *args) -> None:
        pass


class AbstractErrorReporterPlugin(AbstractPlugin, metaclass=ABCMeta):
    async def init(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    @abstractmethod
    async def capture_exception(self, *args) -> None:
        pass

    @abstractmethod
    async def capture_message(self, message: str) -> None:
        pass


class StatPluginContext(BasePluginContext[AbstractStatReporterPlugin]):
    plugin_group = 'backendai_stats_v20'

    async def report_stats(self, report_type: str, metric: Union[int, float], *args) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.report_stats(report_type, metric, *args)


class ErrorPluginContext(BasePluginContext[AbstractErrorReporterPlugin]):
    plugin_group = 'backendai_error_v20'

    async def capture_exception(self, *args) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.capture_exception(*args)

    async def capture_message(self, message: str) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.capture_message(message)
