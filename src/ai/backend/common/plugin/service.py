from abc import ABCMeta, abstractmethod
from functools import partial
from pkg_resources import resource_filename
from pathlib import Path
import re
from typing import Any, Callable, Mapping, Sequence, Tuple, Union

from . import AbstractPlugin, BasePluginContext
from ..types import MountTypes, ServicePortProtocols


class AbstractServicePlugin(AbstractPlugin, metaclass=ABCMeta):
    service_name: str
    fname_pattern: str
    mount_dst: Union[str, Path]
    runner_dirname = 'runner'
    kernel_pkg = 'ai.backend.agent'


    @abstractmethod
    def get_fname_pattern(self, arch: str, libc_style: str) -> str:
        """
        Each service should define own artifact name pattern since a number of artifacts have
        a file or path pattern which depends on an architecture or libc style.
        """
        pass


    async def init(self, context: Any = None) -> None:
        pass


    async def cleanup(self) -> None:
        pass


    async def mount(
        self,
        arch: str,
        libc_style: str,
        resolve_krunner_filepath: Callable[[Union[str, Path]], Path],
        mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None],
    ) -> None:
        filename_pattern = self.get_fname_pattern(arch, libc_style)
        src_path = resolve_krunner_filepath(Path(self.runner_dirname) / filename_pattern)
        mount_to_rsc_spec(MountTypes.BIND, src_path, self.mount_dst)


    def update_service(self) -> None:
        # TODO: implement update the service or package
        pass


class AbstractArtifactServicePlugin(AbstractServicePlugin, metaclass=ABCMeta):
    
    async def mount(
        self,
        arch: str,
        libc_style: str,
        distro: str,
        match_distro_data: Callable[[Mapping[str, Any], str], Tuple[str, Any]],
        resolve_krunner_filepath: Callable[[Union[str, Path]], Path],
        mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None],
    ) -> None:

        filename_pattern = self.get_fname_pattern(arch, libc_style)

        def find_artifacts() -> Mapping[str, str]:
            artifacts = {}
            pattern = Path().resolve().parent / self.runner_dirname
            artifact_path = Path(resource_filename(self.kernel_pkg, pattern))
            rx_distro = re.compile(r"\.([a-z-]+\d+\.\d+)\.")
            for pth in artifact_path.glob(filename_pattern):
                matched = rx_distro.search(pth.name)
                if matched is not None:
                    artifacts[matched.group(1)] = pth.name
            return artifacts

        cands = find_artifacts()
        _, filename_cand = match_distro_data(cands, distro)
        src_path = resolve_krunner_filepath(Path(self.runner_dirname) / filename_cand)
        mount_to_rsc_spec(MountTypes.BIND, src_path, self.mount_dst)


class AbstractIntrinsicServicePlugin(AbstractServicePlugin, metaclass=ABCMeta):
    protocol: ServicePortProtocols
    ports: Tuple[int]
    host_ports = (None,)


    @property
    def port_map(self) -> Mapping[str, Any]:
        return {
            'name': self.service_name,
            'protocol': self.protocol,
            'container_ports': self.ports,
            'host_ports': self.host_ports,
        }

    @abstractmethod
    async def init_service(self, child_env) -> None:
        pass


    @abstractmethod
    async def prepare_service(self, ports: Tuple[int] | None) -> Tuple[Sequence[str], Mapping[str, str]]:
        pass


    def mount(self) -> None:
        pass


class PylibServicePlugin(AbstractServicePlugin):
    pylib_path = '/opt/backend.ai/lib/python{pyver}/site-packages/'

    def get_mount_dst(self, krunner_pyver: str) -> Path:
        return Path(self.pylib_path.format(pyver=krunner_pyver)) / 'ai' / 'backend' / self.mount_dst

    def get_fname_pattern(self, arch: str, libc_style: str) -> str:
        pass

    async def mount(
        self,
        krunner_pyver: str,
        resolve_krunner_filepath: Callable[[Union[str, Path]], Path],
        mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None],
    ) -> None:
        src_path = resolve_krunner_filepath(self.fname_pattern)
        mount_to_rsc_spec(MountTypes.BIND, src_path, self.get_mount_dst(krunner_pyver))


class ServicePluginContext(BasePluginContext[AbstractServicePlugin]):
    plugin_group = 'backendai_service_plugin_v20'
    distro: str
    arch: str
    libc_style: str
    krunner_pyver: str
    match_distro_data: Callable[[Mapping[str, Any], str], Tuple[str, Any]]
    resolve_krunner_filepath: Callable[[Union[str, Path]], Path]
    mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None]

    async def init(self, context: Any = None) -> None:
        pass
        

    async def mount(self) -> None:
        for plugin_instance in self.plugins.values():
            if isinstance(plugin_instance, PylibServicePlugin):
                mount_func = partial(plugin_instance.mount, self.krunner_pyver)
            elif isinstance(plugin_instance, AbstractIntrinsicServicePlugin):
                # TODO: intrinsic services should be mounted
                pass
            elif isinstance(plugin_instance, AbstractServicePlugin):
                mount_func = partial(
                    plugin_instance.mount,
                    self.arch,
                    self.libc_style,
                )
                if isinstance(plugin_instance, AbstractArtifactServicePlugin):
                    mount_func = partial(
                        mount_func,
                        self.distro,
                        self.match_distro_data,
                    )
            await mount_func(
                resolve_krunner_filepath=self.resolve_krunner_filepath,
                mount_to_rsc_spec=self.mount_to_rsc_spec,
            )


    async def init_services(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.init_service()


    async def prepare_services(self) -> None:
        for plugin_instance in self.plugins.values():
            await plugin_instance.prepare_service()
