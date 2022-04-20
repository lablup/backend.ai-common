from abc import ABCMeta, abstractmethod
from functools import partial
import logging
from pathlib import Path
from typing import Any, Callable, Container, Mapping, Sequence, Set, Tuple, Union

from . import AbstractPlugin, BasePluginContext
from ..logging import BraceStyleAdapter
from ..types import MountTypes, ServicePortProtocols


__all__ = (
    'AbstractServicePlugin',
    'AbstractArtifactServicePlugin',
    'AbstractIntrinsicServicePlugin',
    'AbstractIntrinsicServicePlugin',
    'VolumeMountServicePlugin',
    'PylibServicePlugin',
    'ServicePluginContext',
)

log = BraceStyleAdapter(logging.getLogger(__name__))


class DuplicatedServiceName(Exception):

    def __str__(self) -> str:
        return f'Service name ``{self.args[0]}`` is already in used'


class AbstractServicePlugin(AbstractPlugin, metaclass=ABCMeta):
    service_name: str
    fname_pattern: str
    mount_dst: Union[str, Path]
    runner_dirname = 'runner'
    kernel_pkg = 'ai.backend.agent'


    @classmethod
    def check_duplicate_name(cls, service_name_set: Set | None = None) -> None:
        if service_name_set is None:
            service_name_set = set()
        for subcls in cls.__subclasses__():
            try:
                sname = subcls.service_name
            except AttributeError:
                subcls.check_duplicate_name(service_name_set)
                continue
            if sname in service_name_set:
                raise DuplicatedServiceName(sname)
            service_name_set.add(sname)


    @abstractmethod
    def get_fname_pattern(self, arch: str, libc_style: str) -> str:
        """
        Each service should define own artifact name pattern since a number of artifacts have
        a file or path pattern which depends on an architecture or libc style.
        """
        pass

    
    @classmethod
    def get_plugin_config(cls) -> Mapping[str, Any]:
        # TODO: should be abstractmethod and implemented in extended classes
        config = {}
        return config


    async def init(self, context: Any = None) -> None:
        pass


    async def cleanup(self) -> None:
        pass


    async def update_plugin_config(self, plugin_config: Mapping[str, Any]) -> None:
        self.plugin_config = plugin_config


    async def mount(
        self,
        arch: str,
        libc_style: str,
        resolve_krunner_filepath: Callable[[str], Path],
        mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None],
    ) -> None:
        filename_pattern = self.get_fname_pattern(arch, libc_style)
        src_path = resolve_krunner_filepath(str(Path(self.runner_dirname) / filename_pattern))
        mount_to_rsc_spec(MountTypes.BIND, src_path, self.mount_dst)


    def update_service(self) -> None:
        # TODO: implement update the service or package
        pass


class AbstractArtifactServicePlugin(AbstractServicePlugin, metaclass=ABCMeta):

    @abstractmethod
    def find_artifacts(self, filename_pattern: str) -> Mapping[str, str]:
        """
        This method finds local files match with given ``filename_pattern``.
        It depends on the location of runtime, it should be implemented in the local.
        """
        pass


    async def mount(
        self,
        arch: str,
        libc_style: str,
        distro: str,
        match_distro_data: Callable[[Mapping[str, Any], str], Tuple[str, Any]],
        resolve_krunner_filepath: Callable[[str], Path],
        mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None],
    ) -> None:

        filename_pattern = self.get_fname_pattern(arch, libc_style)
        cands = self.find_artifacts(filename_pattern)

        _, filename_cand = match_distro_data(cands, distro)
        src_path = resolve_krunner_filepath(str(Path(self.runner_dirname) / filename_cand))
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

    def get_fname_pattern(self, arch: str, libc_style: str) -> str:
        pass

    async def mount(self) -> None:
        pass


class VolumeMountServicePlugin(AbstractServicePlugin):
    mount_dst = '/opt/backend.ai'

    def get_fname_pattern(self, arch: str, libc_style: str) -> str:
        pass

    async def mount(
        self,
        krunner_volume: str,
        mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None],
    ) -> None:
        mount_to_rsc_spec(MountTypes.VOLUME, krunner_volume, self.mount_dst)


class PylibServicePlugin(AbstractServicePlugin):
    pylib_path = '/opt/backend.ai/lib/python{pyver}/site-packages/'

    def get_mount_dst(self, krunner_pyver: str) -> Path:
        return Path(self.pylib_path.format(pyver=krunner_pyver)) / 'ai' / 'backend' / self.mount_dst

    def get_fname_pattern(self, arch: str, libc_style: str) -> str:
        pass

    async def mount(
        self,
        krunner_pyver: str,
        resolve_krunner_filepath: Callable[[str], Path],
        mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None],
    ) -> None:
        src_path = resolve_krunner_filepath(self.fname_pattern)
        mount_to_rsc_spec(MountTypes.BIND, src_path, self.get_mount_dst(krunner_pyver))


class ServicePluginContext(BasePluginContext[AbstractServicePlugin]):
    plugin_group = 'backendai_service_plugin_v20'
    distro: str
    arch: str
    libc_style: str
    krunner_volume: str
    krunner_pyver: str
    match_distro_data: Callable[[Mapping[str, Any], str], Tuple[str, Any]]
    resolve_krunner_filepath: Callable[[str], Path]
    mount_to_rsc_spec: Callable[[MountTypes, Union[str, Path], Union[str, Path]], None]


    async def init(self, context: Any = None) -> None:
        pass


    async def mount(self) -> None:
        mount_func: partial
        for plugin_name, plugin_instance in self.plugins.items():
            match plugin_instance:
                case PylibServicePlugin():
                    mount_func = partial(plugin_instance.mount, self.krunner_pyver)
                case VolumeMountServicePlugin():
                    await plugin_instance.mount(self.krunner_volume, self.mount_to_rsc_spec)
                    continue
                case AbstractIntrinsicServicePlugin():
                    # TODO: intrinsic services should be mounted
                    await plugin_instance.mount()
                    continue
                case AbstractArtifactServicePlugin():
                    mount_func = partial(
                        plugin_instance.mount,
                        self.arch,
                        self.libc_style,
                        self.distro,
                        self.match_distro_data,
                    )
                case AbstractServicePlugin():
                    mount_func = partial(
                        plugin_instance.mount,
                        self.arch,
                        self.libc_style,
                    )
                case _:
                    log.exception(
                        'Service plugin ``{}`` has invalid type {}.',
                        plugin_name,
                        type(plugin_instance).__name__,
                    )
                    continue

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
