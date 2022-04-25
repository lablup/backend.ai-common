from __future__ import annotations

from abc import ABCMeta, abstractmethod
import logging
from pathlib import Path
from typing import Any, ClassVar, Mapping, TypeVar, Union

from . import AbstractPlugin, BasePluginContext
from ..logging import BraceStyleAdapter
from ..types import MountTypes


__all__ = (
    'AbstractContainerAddon',
    'BaseContainerAddonContext',
)

log = BraceStyleAdapter(logging.getLogger(__name__))


class AbstractContainerAddon(AbstractPlugin, metaclass=ABCMeta):
    addon_name: ClassVar[str]
    fname_pattern: ClassVar[str]
    mount_dst: ClassVar[Union[str, Path]]
    mount_type: ClassVar[MountTypes]

    @classmethod
    def get_plugin_config(cls) -> Mapping[str, Any]:
        # TODO: should be abstractmethod and implemented in extended classes
        config: Mapping[str, Any] = {}
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

    def update_addon(self) -> None:
        # TODO: implement update the container addon
        pass


T = TypeVar('T', bound=AbstractContainerAddon)


class BaseContainerAddonContext(BasePluginContext[T]):
    plugin_group = 'backendai_container_addon_v20'

    async def init(self, context: Any = None) -> None:
        pass

    async def mount(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.mount()
