import asyncio
import inspect
import logging
import pkg_resources
import typing

from .logging import BraceStyleAdapter
from .monitor import AbstractErrorMonitor, AbstractStatsMonitor

log = BraceStyleAdapter(logging.getLogger('ai.backend.common.plugin'))


plugin_base_classes = {
    'stats_monitor': AbstractStatsMonitor,
    'error_monitor': AbstractErrorMonitor,
}


class PluginRegistry:

    def __init__(self, plugin_name):
        self._plugins = []
        self._base_class = plugin_base_classes[plugin_name]
        self._methods = {}
        methods = inspect.getmembers(self._base_class,
                                     predicate=inspect.ismethod)
        for name, method in methods:
            self._methods[name] = method

    def register(self, plugin):
        assert isinstance(plugin, self._base_class), \
            (f'Wrong type of plugin '
             f'(plugin: {type(plugin)} / registry: {self._base_class})')
        self._plugins.append(plugin)

    def __getattr__(self, name):
        try:
            method = self._methods[name]
        except KeyError:
            raise AttributeError(f"'{self._base_class.__name__}' object "
                                 f"has no attribute '{name}'")

        if inspect.iscoroutinefunction(method):
            async def _callback_dispatcher(*args, **kwargs):
                return await asyncio.gather(*[getattr(plugin, name)(*args, kwargs)
                                              for plugin in self._plugins],
                                            return_exceptions=True)
        else:
            def _callback_dispatcher(*args, **kwargs):
                results = []
                for plugin in self._plugins:
                    result = getattr(plugin, name)(*args, **kwargs)
                    results.append(result)
                return results

        return _callback_dispatcher


def discover_entrypoints(plugins, disable_plugins=None):
    if disable_plugins is None:
        disable_plugins = []
    for plugin_name in plugins:
        plugin_group = f'backendai_{plugin_name}_v10'
        for entrypoint in pkg_resources.iter_entry_points(plugin_group):
            if entrypoint.name in disable_plugins:
                continue
            yield plugin_group, plugin_name, entrypoint


def install_plugins(plugins, app, install_type, config):
    """
    Automatically install plugins to the app.

    :param plugins: List of plugin names to discover and install plugins
    :param app: Any type of app to install plugins
    :param install_type: The way to install plugins to app
    :param config: Config object to initialize plugins
    :return:

    You should note that app can be any type of object. For instance,
    when used in manager, app param is the instance of aiohttp.web.Application,
    but it is the instance of subclass of aiozmq.rpc.AttrHandler in agents.

    Therefore, you should specify :install_type: to install plugins into different
    types of apps correctly. Currently we support two types of :install_type:,
    which are 'attr' and 'dict'. For 'attr', plugins will be installed to app
    as its attributes. For 'dict', plugins will be installed as following:
    app[plugin_name] = plugin.
    """
    try:
        disable_plugins = config.disable_plugins
        if not disable_plugins:
            disable_plugins = []
    except AttributeError:
        disable_plugins = []
    for plugin_name in plugins:
        plugin_group = f'backendai_{plugin_name}_v10'
        registry = PluginRegistry(plugin_name)
        for entrypoint in pkg_resources.iter_entry_points(plugin_group):
            if entrypoint.name in disable_plugins:
                continue
            log.info('Installing plugin: {}.{}', plugin_group, entrypoint.name)
            plugin_module = entrypoint.load()
            plugin = getattr(plugin_module, 'get_plugin')(config)
            registry.register(plugin)
            if install_type == 'attr':
                setattr(app, plugin_name, registry)
            elif install_type == 'dict':
                assert isinstance(app, typing.MutableMapping), \
                    (f"app must be an instance of MutableMapping "
                     f"for 'dict' install_type.")
                app[plugin_name] = registry
            else:
                raise ValueError(f'Invalid install type: {install_type}')


def add_plugin_args(parser, plugins):
    for plugin_name in plugins:
        plugin_group = f'backendai_{plugin_name}_v10'
        for entrypoint in pkg_resources.iter_entry_points(plugin_group):
            plugin_module = entrypoint.load()
            _add_plugin_args = getattr(plugin_module, 'add_plugin_args', None)
            if _add_plugin_args:
                _add_plugin_args(parser)
