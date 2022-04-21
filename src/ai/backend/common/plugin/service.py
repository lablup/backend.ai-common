from __future__ import annotations

from abc import ABCMeta, abstractmethod
import logging
from pathlib import Path
from typing import Any, ClassVar, Mapping, Union

from . import AbstractPlugin, BasePluginContext
from ..logging import BraceStyleAdapter
from ..types import MountTypes


__all__ = (
    'AbstractServicePlugin',
    'BaseServicePluginContext',
)

log = BraceStyleAdapter(logging.getLogger(__name__))


class AbstractServicePlugin(AbstractPlugin, metaclass=ABCMeta):
    service_name: ClassVar[str]
    fname_pattern: ClassVar[str]
    mount_dst: ClassVar[Union[str, Path]]
    mount_type: ClassVar[MountTypes]

    @classmethod
    def get_plugin_config(cls) -> Mapping[str, Any]:
        # TODO: should be abstractmethod and implemented in extended classes
        config = {}
        return config

    @property
    @abstractmethod
    def src_path(self) -> Path:
        pass

    @property
    @abstractmethod
    def dst_path(self) -> Path:
        pass

    @abstractmethod
    async def mount(self) -> None:
        pass

    async def init(self, context: Any = None) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def update_plugin_config(self, plugin_config: Mapping[str, Any]) -> None:
        self.plugin_config = plugin_config

    def update_service(self) -> None:
        # TODO: implement update the service or package
        pass


class BaseServicePluginContext(BasePluginContext[AbstractServicePlugin]):
    plugin_group = 'backendai_service_plugin_v20'

    async def init(self, context: Any = None) -> None:
        pass

    async def mount(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.mount()
