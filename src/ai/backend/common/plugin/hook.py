from __future__ import annotations

from abc import ABCMeta, abstractmethod
import enum
from typing import (
    Any,
    Dict,
    Iterator,
    Optional,
    Protocol,
    Sequence,
    Tuple,
)

import attr

from . import AbstractPlugin, AbstractPluginContext, discover_plugins


class HookHandler(Protocol):
    """
    The handler should accept a single argument containing
    a tuple of parameters passed to the handler.
    If it decides to cancel the ongoing event, it should raise
    :class:`HookDenied` exception.
    """

    async def __call__(self, args: Tuple[Any, ...]) -> None:
        ...


class HookPlugin(AbstractPlugin, metaclass=ABCMeta):
    """
    The abstract interface for hook plugins.
    """

    @abstractmethod
    def get_handlers(self) -> Sequence[Tuple[str, HookHandler]]:
        """
        Returns a sequence of pairs of the event name
        and its corresponding handler function.
        """
        pass


class HookDenied(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


class HookResultStatus(enum.Enum):
    PASSED = 0
    DENIED = 1


@attr.s(auto_attribs=True, slots=True, frozen=True)
class HookResult:
    status: HookResultStatus
    src_plugin: Optional[str] = None
    reason: Optional[str] = None


class HookPluginContext(AbstractPluginContext):
    """
    A manager for hook plugins with convenient handler invocation.
    """

    plugins: Dict[str, HookPlugin]

    def __init__(self) -> None:
        self.plugins = {}

    async def init(self) -> None:
        hook_plugins = discover_plugins('backendai_hook_v10')
        for plugin_name, plugin_instance in hook_plugins:
            await plugin_instance.init()

    async def cleanup(self) -> None:
        pass

    def _get_handlers(self, event_name: str) -> Iterator[Tuple[str, HookHandler]]:
        for plugin_name, plugin_instance in self.plugins.items():
            for hooked_event_name, hook_handler in plugin_instance.get_handlers():
                if event_name != hooked_event_name:
                    continue
                yield plugin_name, hook_handler

    async def dispatch_cancellable_event(
        self, event_name: str, args: Tuple[Any, ...],
    ) -> HookResult:
        """
        Invoke the handlers that matches with the given ``event_name``.
        If any of the handlers raises :class:`HookDenied`,
        the event caller should seize the processing.
        """
        for plugin_name, hook_handler in self._get_handlers(event_name):
            try:
                await hook_handler(args)
            except HookDenied as e:
                return HookResult(
                    status=HookResultStatus.DENIED,
                    src_plugin=plugin_name,
                    reason=e.reason,
                )
        return HookResult(
            status=HookResultStatus.PASSED,
        )

    async def dispatch_notification_event(
        self, event_name: str, args: Tuple[Any, ...],
    ) -> None:
        """
        Invoke the handlers that matches with the given ``event_name``.
        Regardless of the handler results, the processing continues.
        """
        for plugin_name, hook_handler in self._get_handlers(event_name):
            await hook_handler(args)
