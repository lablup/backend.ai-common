from collections import UserDict
from decimal import Decimal
import enum
import math
import numbers
from packaging import version
import re
from typing import (
    Any, Hashable, Mapping,
    Iterable, Sequence, Set,
    NewType, Tuple, Union
)

import attr
import itertools

from . import etcd
from .docker import (
    default_registry, default_repository,
    is_known_registry, get_known_registries,
)
from .exception import AliasResolutionFailed

__all__ = (
    'BinarySize',
    'ImageRef',
    'PlatformTagSet',
    'DeviceId',
    'SlotType',
    'IntrinsicSlotTypes',
    'Allocation',
    'ResourceAllocations',
    'ResourceSlot',
    'MountPermission',

    # TODO: to be updated
    'ShareRequest',
    'SessionRequest',
)


class IntrinsicSlotTypes(str, enum.Enum):
    CPU = 'cpu'
    MEMORY = 'mem'


class HandlerForUnknownSlotType(str, enum.Enum):
    DROP = 'drop'
    ERROR = 'error'


DeviceId = NewType('DeviceId', Hashable)
SlotType = NewType('DeviceType', Union[str, IntrinsicSlotTypes])

Quantum = Decimal('0.000')


class MountPermission(str, enum.Enum):
    READ_ONLY = 'ro'
    READ_WRITE = 'rw'
    RW_DELETE = 'wd'


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
        if isinstance(expr, numbers.Integral):
            return cls(expr)
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
                    expr = Decimal(expr[:-length])
                    break
            else:
                # when there is no unit ending (e.g., "2K")
                if not str.isnumeric(expr[-1]):
                    suffix = expr[-1]
                    expr = Decimal(expr[:-1])
                else:
                    suffix = ' '
            try:
                multiplier = cls.suffix_map[suffix]
                return cls(expr * multiplier)
            except KeyError:
                raise ValueError('Unconvertible value', orig_expr)

    def _preformat(self):
        scale = self
        suffix_idx = 0
        while scale >= 1024:
            scale //= 1024
            suffix_idx += 1
        return suffix_idx

    @staticmethod
    def _quantize(val, multiplier):
        d = Decimal(val) / Decimal(multiplier)
        if d == d.to_integral():
            value = d.quantize(Decimal(1))
        else:
            value = d.quantize(Decimal('.00')).normalize()
        return value

    def __str__(self):
        suffix_idx = self._preformat()
        if suffix_idx == 0:
            if self == 1:
                return f'{int(self)} byte'
            else:
                return f'{int(self)} bytes'
        else:
            suffix = type(self).suffices[suffix_idx]
            multiplier = type(self).suffix_map[suffix]
            value = self._quantize(self, multiplier)
            return f'{value} {suffix.upper()}iB'

    def __format__(self, format_spec):
        if len(format_spec) != 1:
            raise ValueError('format-string for BinarySize can be only one character.')
        if format_spec == 's':
            # automatically scaled
            suffix_idx = self._preformat()
            if suffix_idx == 0:
                return f'{int(self)}'
            suffix = type(self).suffices[suffix_idx]
            multiplier = type(self).suffix_map[suffix]
            value = self._quantize(self, multiplier)
            return f'{value}{suffix.lower()}'
        else:
            # use the given scale
            suffix = format_spec.lower()
            multiplier = type(self).suffix_map.get(suffix)
            if multiplier is None:
                raise ValueError('Unsupported scale unit.', suffix)
            value = self._quantize(self, multiplier)
            return f'{value}{suffix.lower()}'.strip()
        return super().__format__(format_spec)


Allocation = NewType('Allocation', Union[Decimal, BinarySize])

ResourceAllocations = NewType('ResourceAllocations',
    Mapping[SlotType, Mapping[DeviceId, Allocation]])
'''
Represents the mappings of resource slot types and
their device-allocation pairs.

Example:

 + "cpu"
   - "0": 1.0
   - "1": 1.0
 + "mem"
   - "node0": "4g"
   - "node1": "4g"
 + "cuda.smp"
   - "0": 20
   - "1": 10
 + "cuda.mem"
   - "0": "8g"
   - "1": "2g"
'''


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

    __slots__ = ('_registry', '_name', '_tag', '_tag_set', '_sha')

    _rx_slug = re.compile(r'^[A-Za-z0-9](?:[A-Za-z0-9-._]*[A-Za-z0-9])?$')

    @classmethod
    async def resolve_alias(cls, alias_key: str, etcd: etcd.AsyncEtcd):
        '''
        Resolve the tag using etcd so that the current instance indicates
        a concrete, latest image.

        Note that alias resolving does not take the registry component into
        account.
        '''
        alias_target = None
        repeats = 0
        while repeats < 8:
            prev_alias_key = alias_key
            alias_key = await etcd.get(f'images/_aliases/{alias_key}')
            if alias_key is None:
                alias_target = prev_alias_key
                break
            repeats += 1
        else:
            raise AliasResolutionFailed('Could not resolve the given image name!')
        known_registries = await get_known_registries(etcd)
        return cls(alias_target, known_registries)

    def __init__(self, value: str,
                 known_registries: Union[Mapping[str, Any], Sequence[str]] = None):
        rx_slug = type(self)._rx_slug
        if '://' in value or value.startswith('//'):
            raise ValueError('ImageRef should not contain the protocol scheme.')
        parts = value.split('/', maxsplit=1)
        if len(parts) == 1:
            self._registry = default_registry
            self._name, self._tag = ImageRef._parse_image_tag(value, True)
            if not rx_slug.search(self._tag):
                raise ValueError('Invalid image tag')
        else:
            if is_known_registry(parts[0], known_registries):
                self._registry = parts[0]
                using_default = (parts[0].endswith('.docker.io') or parts[0] == 'docker.io')
                self._name, self._tag = ImageRef._parse_image_tag(parts[1], using_default)
            else:
                self._registry = default_registry
                self._name, self._tag = ImageRef._parse_image_tag(value, True)
            if not rx_slug.search(self._tag):
                raise ValueError('Invalid image tag')
        self._update_tag_set()

    @staticmethod
    def _parse_image_tag(s: str, using_default_registry: bool = False) -> Tuple[str, str]:
        image_tag = s.rsplit(':', maxsplit=1)
        if len(image_tag) == 1:
            image = image_tag[0]
            tag = 'latest'
        else:
            image = image_tag[0]
            tag = image_tag[1]
        if not image:
            raise ValueError('Empty image repository/name')
        if ('/' not in image) and using_default_registry:
            image = default_repository + '/' + image
        return image, tag

    def _update_tag_set(self):
        if self._tag is None:
            self._tag_set = (None, PlatformTagSet([]))
            return
        tags = self._tag.split('-')
        self._tag_set = (tags[0], PlatformTagSet(tags[1:]))

    def generate_aliases(self) -> Mapping[str, 'ImageRef']:
        basename = self.name.split('/')[-1]
        possible_names = basename.rsplit('-')
        if len(possible_names) > 1:
            possible_names = [basename, possible_names[1]]

        possible_ptags = []
        tag_set = self.tag_set
        if not tag_set[0]:
            pass
        else:
            possible_ptags.append([tag_set[0]])
            for tag_key in tag_set[1]:
                tag_ver = tag_set[1][tag_key]
                tag_list = ['', tag_key, tag_key + tag_ver]
                if '.' in tag_ver:
                    tag_list.append(tag_key + tag_ver.rsplit('.')[0])
                elif tag_key == 'py' and len(tag_ver) > 1:
                    tag_list.append(tag_key + tag_ver[0])
                if 'cuda' in tag_key:
                    tag_list.append('gpu')
                possible_ptags.append(tag_list)

        ret = {}
        for name in possible_names:
            ret[name] = self
        for name, ptags in itertools.product(
                possible_names,
                itertools.product(*possible_ptags)):
            ret[f"{name}:{'-'.join(t for t in ptags if t)}"] = self
        return ret

    @staticmethod
    def merge_aliases(genned_aliases_1, genned_aliases_2) -> Mapping[str, 'ImageRef']:
        ret = {}
        aliases_set_1, aliases_set_2 = set(genned_aliases_1.keys()), set(genned_aliases_2.keys())
        aliases_dup = aliases_set_1 & aliases_set_2

        for alias in aliases_dup:
            ret[alias] = max(genned_aliases_1[alias], genned_aliases_2[alias])

        for alias in aliases_set_1 - aliases_dup:
            ret[alias] = genned_aliases_1[alias]
        for alias in aliases_set_2 - aliases_dup:
            ret[alias] = genned_aliases_2[alias]

        return ret

    @property
    def canonical(self) -> str:
        # e.g., registry.docker.io/lablup/kernel-python:3.6-ubuntu
        return f'{self.registry}/{self.name}:{self.tag}'

    @property
    def tag_path(self) -> str:
        '''
        Return the string key that can be used to fetch image metadata from etcd.
        '''
        return f'images/{etcd.quote(self.registry)}/' \
               f'{etcd.quote(self.name)}/{self.tag}'

    @property
    def registry(self) -> str:
        # e.g., lablup
        return self._registry

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
    def short(self) -> str:
        '''
        Returns the image reference string without the registry part.
        '''
        # e.g., python:3.6-ubuntu
        return f'{self.name}:{self.tag}' if self.tag is not None else self.name

    def __str__(self) -> str:
        return self.canonical

    def __repr__(self) -> str:
        return f'<ImageRef: "{self.canonical}">'

    def __hash__(self) -> int:
        return hash((self._name, self._tag, self._registry))

    def __eq__(self, other) -> bool:
        return (self._registry == other._registry and
                self._name == other._name and
                self._tag == other._tag)

    def __ne__(self, other) -> bool:
        return (self._registry != other._registry or
                self._name != other._name or
                self._tag != other._tag)

    def __lt__(self, other) -> bool:
        if self == other:   # call __eq__ first for resolved check
            return False
        if self.name != other.name:
            raise ValueError('only the image-refs with same names can be compared.')
        if self.tag_set[0] != other.tag_set[0]:
            return version.parse(self.tag_set[0]) < version.parse(other.tag_set[0])
        ptagset_self, ptagset_other = self.tag_set[1], other.tag_set[1]
        for key_self in ptagset_self:
            if ptagset_other.has(key_self):
                version_self, version_other = ptagset_self.get(key_self), ptagset_other.get(key_self)
                if version_self and version_other:
                    parsed_version_self, parsed_version_other = version.parse(version_self), version.parse(version_other)
                    if parsed_version_self != parsed_version_other:
                        return parsed_version_self < parsed_version_other
        return len(ptagset_self) > len(ptagset_other)


class ResourceSlot(UserDict):

    __slots__ = ('data', 'numeric')

    def __init__(self, *args, numeric=False):
        super().__init__(*args)
        self.numeric = numeric

    def __add__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be added together.')
        return type(self)({
            k: self.get(k, 0) + other.get(k, 0)
            for k in (self.keys() | other.keys())
        }, numeric=True)

    def __sub__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be operands of subtraction.')
        if other.keys() > self.keys():
            raise ValueError('Cannot subtract resource slot with more keys!')
        return type(self)({
            k: self.data[k] - other.get(k, 0)
            for k in self.keys()
        }, numeric=True)

    def __eq__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() != other.keys():
            raise False
        self_values = [self.data[k] for k in sorted(self.data.keys())]
        other_values = [other.data[k] for k in sorted(other.data.keys())]
        return self_values == other_values

    def __ne__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() != other.keys():
            return True
        return not self.__eq__(other)

    def eq_contains(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot contain other.')
        common_keys = sorted(other.keys() & self.keys())
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values

    def eq_contained(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be contained in other.')
        common_keys = sorted(other.keys() & self.keys())
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values

    def __le__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be smaller than other.')
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return not any(s > o for s, o in zip(self_values, other_values))

    def __lt__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be smaller than other.')
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return (not any(s > o for s, o in zip(self_values, other_values)) and
                not (self_values == other_values))

    def __ge__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot be larger than other.')
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return not any(s < o for s, o in zip(self_values, other_values))

    def __gt__(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot be larger than other.')
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return (not any(s < o for s, o in zip(self_values, other_values)) and
                not (self_values == other_values))

    def lte_unlimited(self, other):
        if not self.numeric or not other.numeric:
            raise TypeError('Only numeric slots can be compared.')
        if other.keys() != self.keys():
            raise ValueError('To allow unlimited resource comparison, keys must be same.')
        for self_value, other_value in zip((self.data[k] for k in self.keys()),
                                           (other.data[k] for k in self.keys())):
            # If the right operand has zero or null values,
            # treat them like infinity.
            if other_value is None or other_value == 0:
                continue
            if self_value > other_value:
                return False
        return True

    # as_numeric series methods are to preserve accuracy of values.
    # as_humanized series methods are to pretty-print values.

    def as_numeric(self, slot_types, *,
                   unknown: HandlerForUnknownSlotType = 'error',
                   fill_missing: bool = False):
        data = {}
        unknown_handler = HandlerForUnknownSlotType(unknown)
        for k, v in self.data.items():
            unit = slot_types.get(k)
            if unit is None:
                if unknown_handler == 'drop':
                    continue
                elif unknown_handler == 'error':
                    raise ValueError('unit unknown for slot', k)
            data[k] = ResourceSlot.value_as_numeric(v, unit)
        if fill_missing:
            for k in slot_types.keys():
                if k not in data:
                    data[k] = 0
        return type(self)(data, numeric=True)

    @staticmethod
    def value_as_numeric(value, unit):
        if value in (float('inf'), 'inf'):
            return Decimal('Infinity')
        if unit == 'bytes':
            if isinstance(value, (Decimal, int)):
                return int(value)
            value = int(BinarySize.from_str(value))
        else:
            value = Decimal(value).quantize(Quantum).normalize()
        return value

    @staticmethod
    def _humanize(src_data, slot_types, fill_missing):
        data = {}
        for k, v in src_data.items():
            unit = slot_types.get(k, 'count')
            if unit == 'bytes':
                try:
                    v = '{:s}'.format(BinarySize(v))
                except ValueError:
                    v = _stringify_number(v)
            else:
                v = _stringify_number(v)
            data[k] = v
        if fill_missing:
            for k in slot_types.keys():
                if k not in data:
                    data[k] = '0'
        return data

    def as_humanized(self, slot_types, *,
                     fill_missing: bool = True):
        data = self._humanize(self.data, slot_types, fill_missing)
        return type(self)(data, numeric=False)

    def as_json_humanized(self, slot_types, *,
                          fill_missing: bool = True):
        data = self._humanize(self.data, slot_types, fill_missing)
        return data

    def as_json_numeric(self, slot_types, *,
                        unknown: HandlerForUnknownSlotType = 'error',
                        fill_missing: bool = False):
        data = {}
        unknown_handler = HandlerForUnknownSlotType(unknown)
        for k, v in self.data.items():
            unit = slot_types.get(k)
            if unit is None:
                if unknown_handler == 'drop':
                    continue
                elif unknown_handler == 'error':
                    raise ValueError('unit unknown for slot', k)
            v = ResourceSlot.value_as_numeric(v, unit)
            data[k] = _stringify_number(v)
        if fill_missing:
            for k in slot_types.keys():
                if k not in data:
                    data[k] = '0'
        return data

    # legacy:
    # mem: Decimal = Decimal(0)  # multiple of GiBytes
    # cpu: Decimal = Decimal(0)  # multiple of CPU cores
    # accel_slots: Optional[Mapping[str, Decimal]] = None


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
    slot_type: SlotType
    device_share: Decimal
    feature_version: str = None
    feature_set: Set[str] = attr.Factory(set)
    lib_version: str = None
    driver_version: str = None


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


def _stringify_number(v):
    '''
    Stringify a number, preventing unwanted scientific notations.
    '''
    if isinstance(v, (float, Decimal)):
        if math.isinf(v) and v > 0:
            v = 'inf'
        elif math.isinf(v) and v < 0:
            v = '-inf'
        else:
            v = '{:f}'.format(v)
    elif isinstance(v, BinarySize):
        v = '{:d}'.format(int(v))
    elif isinstance(v, int):
        v = '{:d}'.format(v)
    else:
        v = str(v)
    return v
