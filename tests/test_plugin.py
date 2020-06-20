import asyncio
import functools
from typing import (
    Any,
    Mapping,
)
from unittest.mock import AsyncMock

from ai.backend.common.plugin import (
    AbstractPlugin,
    BasePluginContext,
)

import pytest


class DummyPlugin(AbstractPlugin):

    def __init__(self, plugin_config, local_config) -> None:
        super().__init__(plugin_config, local_config)

    async def init(self) -> None:
        pass

    async def cleanup(self) -> None:
        pass

    async def update_plugin_config(self, plugin_config: Mapping[str, Any]) -> None:
        pass


class DummyEntrypoint:

    def __init__(self, name: str, load_result: Any) -> None:
        self.name = name
        self._load_result = load_result

    def load(self) -> Any:
        return self._load_result


def mock_entrypoints_with_instance(plugin_group_name: str, *, mocked_plugin):
    # Since mocked_plugin is already an instance constructed via AsyncMock,
    # we emulate the original constructor using a lambda fucntion.
    yield DummyEntrypoint('dummy', lambda plugin_config, local_config: mocked_plugin)


def mock_entrypoints_with_class(plugin_group_name: str, *, plugin_cls):
    yield DummyEntrypoint('dummy', plugin_cls)


@pytest.mark.asyncio
async def test_plugin_context_init_cleanup(etcd, mocker):
    mocked_plugin = AsyncMock(DummyPlugin)
    mocked_entrypoints = functools.partial(mock_entrypoints_with_instance,
                                           mocked_plugin=mocked_plugin)
    mocker.patch('ai.backend.common.plugin.pkg_resources.iter_entry_points', mocked_entrypoints)
    ctx = BasePluginContext(etcd, {})
    try:
        assert not ctx.plugins
        await ctx.init()
        assert ctx.plugins
        ctx.plugins['dummy'].init.assert_awaited_once()
    finally:
        await ctx.cleanup()
        ctx.plugins['dummy'].cleanup.assert_awaited_once()


@pytest.mark.asyncio
async def test_plugin_context_config(etcd, mocker):
    mocked_entrypoints = functools.partial(mock_entrypoints_with_class, plugin_cls=DummyPlugin)
    mocker.patch('ai.backend.common.plugin.pkg_resources.iter_entry_points', mocked_entrypoints)
    await etcd.put('config/plugins/dummy/etcd-key', 'etcd-value')
    ctx = BasePluginContext(
        etcd,
        {'local-key': 'local-value'},
    )
    try:
        assert not ctx.plugins
        await ctx.init()
        assert ctx.plugins
        assert isinstance(ctx.plugins['dummy'], DummyPlugin)
        ctx.plugins['dummy'].local_config['local-key'] == 'local-value'
        ctx.plugins['dummy'].plugin_config['etcd-key'] == 'etcd-value'
    finally:
        await ctx.cleanup()


@pytest.mark.asyncio
async def test_plugin_context_config_autoupdate(etcd, mocker):
    mocked_plugin = AsyncMock(DummyPlugin)
    mocked_entrypoints = functools.partial(mock_entrypoints_with_instance,
                                           mocked_plugin=mocked_plugin)
    mocker.patch('ai.backend.common.plugin.pkg_resources.iter_entry_points', mocked_entrypoints)
    await etcd.put_prefix('config/plugins/dummy', {'a': '1', 'b': '2'})
    ctx = BasePluginContext(
        etcd,
        {'local-key': 'local-value'},
    )
    try:
        await ctx.init()
        await asyncio.sleep(0.01)
        await etcd.put_prefix('config/plugins/dummy', {'a': '3', 'b': '4'})
        await asyncio.sleep(0.6)  # we should see the update only once
        await etcd.put_prefix('config/plugins/dummy', {'a': '5', 'b': '6'})
        await asyncio.sleep(0.3)
        args_list = mocked_plugin.update_plugin_config.await_args_list
        assert len(args_list) == 2
        assert args_list[0].args[0] == {'a': '3', 'b': '4'}
        assert args_list[1].args[0] == {'a': '5', 'b': '6'}
    finally:
        await ctx.cleanup()
