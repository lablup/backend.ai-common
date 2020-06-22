from __future__ import annotations

from abc import ABCMeta, abstractmethod
import enum
from typing import (
    Any,
    Mapping,
    Union,
)

from . import AbstractPlugin, BasePluginContext

__all__ = (
    'AbstractStatReporterPlugin',
    'AbstractErrorReporterPlugin',
    'StatPluginContext',
    'ErrorPluginContext',
)


class StatMetricTypes(enum.Enum):
    INCREMENT = 0
    GAUGE = 1


class AbstractStatReporterPlugin(AbstractPlugin, metaclass=ABCMeta):

    async def init(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    @abstractmethod
    async def report_stats(
        self,
        metric_type: StatMetricTypes,
        metric_name: str,
        value: Union[float, int] = None,
    ):
        pass


class AbstractErrorReporterPlugin(AbstractPlugin, metaclass=ABCMeta):
    async def init(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    @abstractmethod
    async def capture_exception(
        self,
        exc_instance: Exception = None,
        context: Mapping[str, Any] = None,
    ) -> None:
        pass

    @abstractmethod
    async def capture_message(self, message: str) -> None:
        pass


class StatPluginContext(BasePluginContext[AbstractStatReporterPlugin]):
    plugin_group = 'backendai_stats_v20'

    async def report_stats(
        self,
        metric_type: StatMetricTypes,
        metric_name: str,
        value: Union[float, int] = None,
    ):
        for plugin_instance in self.plugins.values():
            await plugin_instance.report_stats(metric_type, metric_name, value)


class ErrorPluginContext(BasePluginContext[AbstractErrorReporterPlugin]):
    plugin_group = 'backendai_error_v20'

    async def capture_exception(
        self,
        exc_instance: Exception = None,
        context: Mapping[str, Any] = None,
    ) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.capture_exception(exc_instance, context)

    async def capture_message(self, message: str) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.capture_message(message)
