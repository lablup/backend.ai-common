from abc import ABCMeta, abstractmethod
from ctypes import Union
import pkg_resources
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Tuple

from . import AbstractPlugin, BasePluginContext
from ..types import MountTypes

class AbstractServicePlugin(AbstractPlugin, metaclass=ABCMeta):
    service_name: str
    runner_dirname: Union[str, Path]
    filename_pattern: str
    match_distro: Callable[[Mapping[str, Any], str], Tuple[str, Any]]
    resolve_krunner_filepath: Callable[[Union[str, Path]], Path]
    mount_dst: Union[str, Path]
    kernel_pkg = 'ai.backend.agent'
    _rx_distro = re.compile(r"\.([a-z-]+\d+\.\d+)\.")

    async def init(self, context: Any = None) -> None:
        pass

    async def cleanup(self) -> None:
        pass


    async def mount(
        self,
        distro: str,
        mount_to_rsc_spec: Callable,
    ) -> None:
        def find_artifacts() -> Mapping[str, str]:
            artifacts = {}
            artifact_path = Path(pkg_resources.resource_filename(
                self.kernel_pkg, Path().resolve().parent / self.runner_dirname))
            for p in artifact_path.glob(self.filename_pattern):
                m = self._rx_distro.search(p.name)
                if m is not None:
                    artifacts[m.group(1)] = p.name
            return artifacts

        def resolve_filepath(cands: Mapping[str, str]) -> Path:
            _, cand = self.match_distro(cands, distro)
            return self.resolve_krunner_filepath(Path(self.runner_dirname) / cand)

        cands = find_artifacts()
        exec_path = resolve_filepath(cands)
        mount_to_rsc_spec(MountTypes.BIND, exec_path, self.mount_dst)


class ServicePluginContext(BasePluginContext[AbstractServicePlugin]):
    distro: str
    mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None]
    plugin_group = 'backendai_service_mount_v20'

    async def mount(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.mount(self.distro, self.mount_to_rsc_spec)
