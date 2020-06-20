import functools
from typing import (
    Any,
    Mapping,
    Type,
)
from unittest.mock import AsyncMock, MagicMock

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
    mocked_entrypoints = functools.partial(mock_entrypoints_with_instance, mocked_plugin=mocked_plugin)
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
