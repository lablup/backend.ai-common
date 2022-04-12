from abc import ABCMeta
from ctypes import Union
from pkg_resources import resource_filename
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Tuple

from . import AbstractPlugin, BasePluginContext
from ..types import MountTypes


class AbstractServicePlugin(AbstractPlugin, metaclass=ABCMeta):
    service_name: str
    filename_pattern: str
    mount_dst: Union[str, Path]
    service_info: Mapping[str, Any]
    child_env: Mapping[str, Any]
    runner_dirname = 'runner'
    find_exec = False
    kernel_pkg = 'ai.backend.agent'
    _rx_distro = re.compile(r"\.([a-z-]+\d+\.\d+)\.")


    async def init(self, context: Any = None) -> None:
        pass


    async def cleanup(self) -> None:
        pass


    async def mount(
        self,
        distro: str,
        match_distro_data: Callable,
        resolve_krunner_filepath: Callable,
        mount_to_rsc_spec: Callable,
    ) -> None:
        def find_artifacts() -> Mapping[str, str]:
            artifacts = {}
            pattern = Path().resolve().parent / self.runner_dirname
            artifact_path = Path(resource_filename(self.kernel_pkg, pattern))
            for pth in artifact_path.glob(self.filename_pattern):
                matched = self._rx_distro.search(pth.name)
                if matched is not None:
                    artifacts[matched.group(1)] = pth.name
            return artifacts

        def resolve_filepath(cands: Mapping[str, str]) -> Path:
            _, cand = match_distro_data(cands, distro)
            return resolve_krunner_filepath(Path(self.runner_dirname) / cand)

        src_path: Union[str, Path]
        if self.find_exec:
            cands = find_artifacts()
            src_path = resolve_filepath(cands)
        else:
            src_path = self.filename_pattern
        mount_to_rsc_spec(MountTypes.BIND, src_path, self.mount_dst)


    async def init_service(self) -> None:
        pass


    async def prepare_service(self) -> None:
        pass


    def update_service(self) -> None:
        # TODO: implement update the service or package
        pass


class ServicePluginContext(BasePluginContext[AbstractServicePlugin]):
    distro: str
    match_distro_data: Callable[[Mapping[str, Any], str], Tuple[str, Any]]
    resolve_krunner_filepath: Callable[[Union[str, Path]], Path]
    mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None]
    plugin_group = 'backendai_service_plugin_v20'


    async def mount(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.mount(
                self.distro,
                self.match_distro_data,
                self.resolve_krunner_filepath,
                self.mount_to_rsc_spec,
            )


    async def init_services(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.init_service()


    async def prepare_services(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.prepare_service()
