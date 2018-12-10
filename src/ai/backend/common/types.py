from decimal import Decimal
import enum
import re
from typing import Hashable, Mapping, Iterable, Sequence, Set, NewType, Tuple, Union

import attr


DeviceId = NewType('DeviceId', Hashable)


class BinarySize(int):
    '''
    A wrapper around Python integers to represent binary sizes for storage and
    memory in various places.

    Its string representation and parser, ``from_str()`` classmethod, does not use
    any locale-specific digit delimeters -- it supports only standard Python
    digit delimeters.
    '''

    suffix_map = {
        'y': 2 ** 80, 'Y': 2 ** 80,  # yotta
        'z': 2 ** 70, 'Z': 2 ** 70,  # zetta
        'e': 2 ** 60, 'E': 2 ** 60,  # exa
        'p': 2 ** 50, 'P': 2 ** 50,  # peta
        't': 2 ** 40, 'T': 2 ** 40,  # tera
        'g': 2 ** 30, 'G': 2 ** 30,  # giga
        'm': 2 ** 20, 'M': 2 ** 20,  # mega
        'k': 2 ** 10, 'K': 2 ** 10,  # kilo
        ' ': 1,
    }
    suffices = (' ', 'K', 'M', 'G', 'T', 'P', 'E', 'Z', 'Y')
    endings = ('ibytes', 'ibyte', 'ib', 'bytes', 'byte', 'b')

    @classmethod
    def from_str(cls, expr):
        assert isinstance(expr, str)
        orig_expr = expr
        expr = expr.strip().replace('_', '')
        try:
            return int(expr)
        except ValueError:
            expr = expr.lower()
            for ending in cls.endings:
                if expr.endswith(ending):
                    length = len(ending) + 1
                    suffix = expr[-length]
                    expr = float(expr[:-length])
                    break
            else:
                # when there is no unit ending (e.g., "2K")
                if not str.isnumeric(expr[-1]):
                    suffix = expr[-1]
                    expr = float(expr[:-1])
                else:
                    suffix = ' '
            try:
                multiplier = cls.suffix_map[suffix]
                return cls(expr * multiplier)
            except KeyError:
                raise ValueError('Unconvertible value', orig_expr)

    def __str__(self):
        scale = self
        suffix_idx = 0
        while scale >= 1024:
            scale //= 1024
            suffix_idx += 1
        if suffix_idx == 0:
            if self == 1:
                return f'{int(self)} byte'
            else:
                return f'{int(self)} bytes'
        else:
            suffix = type(self).suffices[suffix_idx]
            multiplier = type(self).suffix_map[suffix]
            d = Decimal(self) / Decimal(multiplier)
            if d == d.to_integral():
                value = d.quantize(Decimal(1))
            else:
                value = d.quantize(Decimal('.00'))
            return f'{value} {suffix.upper()}iB'


class PlatformTagSet(Mapping):

    __slots__ = ('_data', )
    _rx_ver = re.compile(r'^(?P<tag>[a-zA-Z]+)(?P<version>\d+(?:\.\d+)*[a-z0-9]*)?$')

    def __init__(self, tags: Iterable[str]):
        self._data = dict()
        rx = type(self)._rx_ver
        for t in tags:
            match = rx.search(t)
            if match is None:
                raise ValueError('invalid tag-version string', t)
            key = match.group('tag')
            value = match.group('version')
            if key in self._data:
                raise ValueError('duplicate platform tag with different versions', t)
            if value is None:
                value = ''
            self._data[key] = value

    def has(self, key: str, version: str = None):
        if version is None:
            return key in self._data
        _v = self._data.get(key, None)
        return _v == version

    def __getitem__(self, key: str):
        return self._data[key]

    def __iter__(self):
        return iter(self._data)

    def __len__(self):
        return len(self._data)

    def __eq__(self, other):
        if isinstance(other, (set, frozenset)):
            return set(self._data.keys()) == other
        return self._data == other


class ImageRef:

    __slots__ = ('_registry', '_name', '_tag', '_tag_set')

    _rx_slug = re.compile(r'^[A-Za-z0-9](?:[A-Za-z0-9-._]*[A-Za-z0-9])?$')
    _rx_kernel_prefix = re.compile(r'^(?:.+/)?kernel-.+$')

    @staticmethod
    def _parse_image_tag(s: str) -> Tuple[str, str]:
        if s.startswith('kernel-'):
            s = s[7:]
        image_tag = s.split(':', maxsplit=1)
        if len(image_tag) == 1:
            image = image_tag[0]
            tag = None
        else:
            image = image_tag[0]
            tag = image_tag[1]
        return image, tag

    @classmethod
    def is_kernel(cls, s: str) -> bool:
        '''
        Checks if the given string in "RepoTags" field values from Docker's
        image listing API follows Backend.AI's kernel image name.
        '''
        if not cls._rx_kernel_prefix.search(s):
            return False
        try:
            ref = cls(s)
        except ValueError:
            return False
        if ref.tag is not None and ref.tag == 'latest':
            return False
        return True

    def __init__(self, s: str):
        rx_slug = type(self)._rx_slug
        parts = s.rsplit('/', maxsplit=1)
        if len(parts) == 1:
            self._registry = 'lablup'
            self._name, self._tag = ImageRef._parse_image_tag(parts[0])
            if not rx_slug.search(self._name):
                raise ValueError('Invalid image name')
            if self._tag is not None and not rx_slug.search(self._tag):
                raise ValueError('Invalid iamge tag')
        else:
            self._registry = parts[0]
            if not rx_slug.search(self._registry):
                raise ValueError('Invalid image registry')
            self._name, self._tag = ImageRef._parse_image_tag(parts[1])
            if not rx_slug.search(self._name):
                raise ValueError('Invalid image name')
            if self._tag is not None and not rx_slug.search(self._tag):
                raise ValueError('Invalid image tag')
        self._update_tag_set()

    async def resolve(self, etcd: 'ai.backend.common.etcd.AsyncEtcd'):
        '''
        Resolve the tag using etcd so that the current instance indicates
        a concrete, latest image.

        Note that alias resolving does not take the registry component into
        account.
        '''
        async def resolve_alias(alias_key):
            alias_target = None
            while True:
                prev_alias_key = alias_key
                alias_key = await etcd.get(f'images/_aliases/{alias_key}')
                if alias_key is None:
                    alias_target = prev_alias_key
                    break
            return alias_target

        name_or_alias = self.short
        alias_target = await resolve_alias(name_or_alias)
        if alias_target == name_or_alias and name_or_alias.rfind(':') == -1:
            alias_target = await resolve_alias(f'{name_or_alias}:latest')
        assert alias_target is not None
        name, _, tag = alias_target.partition(':')
        hash = await etcd.get(f'images/{name}/tags/{tag}')
        if hash is None:
            raise RuntimeError(f'{name_or_alias}: Unregistered image '
                               'or unknown alias.')
        self._name = name
        self._tag = tag
        self._update_tag_set()

    def _update_tag_set(self):
        if self._tag is None:
            self._tag_set = (None, PlatformTagSet([]))
            return
        tags = self._tag.split('-')
        self._tag_set = (tags[0], PlatformTagSet(tags[1:]))

    def resolve_required(self) -> bool:
        return (self._tag is None or self._tag == 'latest')

    @property
    def canonical(self) -> str:
        # e.g., lablup/kernel-python:3.6-ubuntu
        return f'{self.registry}/kernel-{self.name}:{self.tag}'

    @property
    def name(self) -> str:
        # e.g., python
        return self._name

    @property
    def tag(self) -> str:
        # e.g., 3.6-ubuntu
        return self._tag

    @property
    def tag_set(self) -> Tuple[str, PlatformTagSet]:
        # e.g., '3.6', {'ubuntu', 'cuda', ...}
        return self._tag_set

    @property
    def registry(self) -> str:
        # e.g., lablup
        return self._registry

    @property
    def long(self) -> str:
        # e.g., lablup/python:3.6-ubuntu
        return f'{self.registry}/{self.name}:{self.tag}'

    @property
    def short(self) -> str:
        '''
        Returns the image reference string without the registry part.
        '''
        # e.g., python:3.6-ubuntu
        return f'{self.name}:{self.tag}'

    def __str__(self) -> str:
        return self.canonical

    def __repr__(self) -> str:
        return f'<ImageRef: "{self.canonical}">'

    def __eq__(self, other) -> bool:
        if self.resolve_required() or other.resolve_required():
            raise ValueError('You must compare resolved image references.')
        return (
            (self._name == other._name) and
            (self._tag == other._tag) and
            (self._registry == other._registry)
        )

    def __hash__(self) -> int:
        return hash((self._name, self._tag, self._registry))


class DeviceTypes(enum.Enum):
    CPU = 'cpu'
    MEMORY = 'mem'


DeviceType = NewType('DeviceType', Union[str, DeviceTypes])


@attr.s(auto_attribs=True, slots=True)
class ShareRequest:
    '''
    Represents resource demands in "share" for a specific type of resource.

    **share** is a relative value where its realization is responsible to
    the specific resource type.  For example, "1.5 CUDA GPU" may be either
    physically a fraction of one CUDA GPU or fractions spanning across two (or
    more) CUDA GPUs depending on the actual GPU capacity detected/analyzed by
    the CUDA accelerator plugin.

    The version and feature set fields are optional -- they are considered
    meaningful only when specified.  If specified, they indicate the minimum
    compatible versions required to execute the given compute session.
    '''
    device_type: DeviceType
    device_share: Decimal
    feature_version: str = None
    feature_set: Set[str] = attr.Factory(set)
    lib_version: str = None
    driver_version: str = None


class MountPermission(enum.Enum):
    READ_ONLY = 'ro'
    READ_WRITE = 'rw'
    WRITE_DELETE = 'wd'


@attr.s(auto_attribs=True, slots=True)
class VFolderRequest:
    '''
    Represents vfolders for a new compute session request.
    '''
    vfolder_id: str
    vfolder_host: str
    permission: MountPermission


@attr.s(auto_attribs=True, slots=True)
class ResourceRequest:
    '''
    Represents resource demands for a new compute session request.

    CPU and memory shares are relative values, but you may think
    1 CPU-share is roughly equivalent to 1 CPU core and 1 memory-share is for 1
    GiB of the main memory.  Note that this mapping may be changed in the
    future depending the hardware performance changes.
    '''
    shares: Mapping[str, ShareRequest] = attr.Factory(dict)
    vfolders: Sequence[VFolderRequest] = attr.Factory(list)
    scratch_disk_size: BinarySize = None
    ignore_numa: bool = False
    extra: str = ''

    def to_json(self):
        pass

    @classmethod
    def from_json(cls):
        pass


@attr.s(auto_attribs=True, slots=True)
class SessionRequest:
    '''
    Represents a new compute session request.
    '''
    scaling_group: str
    cluster_size: int
    master_image: str
    master_resource_spec: ResourceRequest
    # for multi-container sessions
    worker_image: str = None
    worker_resource_spec: ResourceRequest = None
