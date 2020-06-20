from __future__ import annotations

from abc import ABCMeta, abstractmethod
import asyncio
import functools
import logging
import pkg_resources
from typing import (
    Any,
    ClassVar,
    Container,
    Dict,
    Iterator,
    Mapping,
    Set,
    Tuple,
    Type,
)

from ..gateway.etcd import ConfigServer
from ..logging_utils import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__name__))

__all__ = (
    'AbstractPlugin',
    'AbstractPluginContext',
    'discover_plugins',
)


class AbstractPlugin(metaclass=ABCMeta):
    """
    The minimum generic plugin interface.
    """

    plugin_config: Mapping[str, Any]
    """
    ``plugin_config`` contains the plugin-specific configuration read from the etcd.
    This configuration may be updated at runtime via the ``update_plugin_config()`` method,
    which is called when the etcd updates are detected.
    """

    local_config: Mapping[str, Any]
    """
    ``local_config`` contains the configuration read from the disk TOML file of the current daemon.
    This configuration is only updated when restarting the daemon and thus plugins should assume
    that it's read-only and immutable during its lifetime.
    e.g., If the plugin is running with the manager, it's the validated content of manager.toml file.
    """

    def __init__(self, plugin_config: Mapping[str, Any], local_config: Mapping[str, Any]) -> None:
        """
        Instantiate the plugin with the given initial configuration.
        """
        self.plugin_config = plugin_config
        self.local_config = local_config

    @abstractmethod
    async def init(self) -> None:
        """
        Initialize any resource used by the plugin.
        """
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        """
        Clean up any resource used by the plugin upon server cleanup.
        """
        pass

    @abstractmethod
    async def update_plugin_config(self, plugin_config: Mapping[str, Any]) -> None:
        """
        Handle runtime configuration updates.
        The config parameter contains both the updated parts
        and unchanged parts of the configuration.

        The default implementation is just to replace the config property,
        but actual plugins may trigger other operations to reflect config changes
        and/or inspect the differences of configs before replacing the current config.
        """
        self.plugin_config = plugin_config


class BasePluginContext:
    """
    A minimal plugin manager which controls the lifecycles of the given plugins
    and watches & applies the configuration changes in etcd.

    The subclasses must redefine ``plugin_group``.
    """

    config_server: ConfigServer
    local_config: Mapping[str, Any]
    plugins: Dict[str, AbstractPlugin]
    plugin_group: ClassVar[str] = 'backendai_XXX_v10'

    _config_watchers: Set[asyncio.Task]

    def __init__(self, config_server: ConfigServer, local_config: Mapping[str, Any]) -> None:
        self.config_server = config_server
        self.local_config = local_config
        self.plugins = {}
        self._config_watchers = set()

    async def init(self) -> None:
        hook_plugins = discover_plugins(self.plugin_group)
        for plugin_name, plugin_entry in hook_plugins:
            plugin_config = await self.config_server.etcd.get_prefix(f"config/plugins/{plugin_name}")
            plugin_instance = plugin_entry(plugin_config, self.local_config)
            await plugin_instance.init()
            await self.watch_config_changes(plugin_name)

    async def cleanup(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.cleanup()
        for wtask in {*self._config_watchers}:
            wtask.cancel()
            await wtask

    async def _watcher(self, plugin_name: str) -> None:
        try:
            async for ev in self.config_server.watch_prefix(f"config/plugins/{plugin_name}"):
                print(f"config-watcher: {plugin_name}: {ev}")
                # TODO: aggregate a chunk of changes happening in a short period of time??
        except asyncio.CancelledError:
            print(f"config-watcher: {plugin_name}: <cancelled>")
            pass

    async def watch_config_changes(self, plugin_name: str) -> None:
        wtask = asyncio.create_task(self._watcher(plugin_name))
        wtask.add_done_callback(functools.partial(self._config_watchers.discard, wtask))
        self._config_watchers.add(wtask)


def discover_plugins(
    plugin_group: str,
    blocklist: Container[str] = None,
) -> Iterator[Tuple[str, Type[AbstractPlugin]]]:
    if blocklist is None:
        blocklist = set()
    for entrypoint in pkg_resources.iter_entry_points(plugin_group):
        if entrypoint.name in blocklist:
            continue
        log.info('loading plugin (group:{}): {}', plugin_group, entrypoint.name)
        yield entrypoint.name, entrypoint.load()
