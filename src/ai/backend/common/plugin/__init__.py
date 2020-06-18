from abc import ABCMeta, abstractmethod
import pkg_resources
from typing import (
    Any,
    Container,
    Iterator,
    Mapping,
    Tuple,
)


class AbstractPlugin(metaclass=ABCMeta):
    """
    The minimum generic plugin interface.
    """

    def __init__(self, config: Mapping[str, Any]) -> None:
        self.config = config

    @abstractmethod
    async def init(self) -> None:
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        pass


class AbstractPluginContext(metaclass=ABCMeta):
    """
    A minimal plugin manager which controls the lifecycles of the given plugins.
    """

    @abstractmethod
    async def init(self) -> None:
        pass

    @abstractmethod
    async def cleanup(self) -> None:
        pass


def discover_plugins(
    plugin_group: str,
    blocklist: Container[str] = None,
) -> Iterator[Tuple[str, AbstractPlugin]]:
    if blocklist is None:
        blocklist = set()
    for entrypoint in pkg_resources.iter_entry_points(plugin_group):
        if entrypoint.name in blocklist:
            continue
        yield entrypoint.name, entrypoint.load()
