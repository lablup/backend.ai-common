from collections import UserDict, namedtuple
from decimal import Decimal
import enum
import ipaddress
import math
import numbers
from typing import (
    Any, Hashable, Mapping, Optional,
    Sequence, Set,
    NewType, Union
)

import attr

__all__ = (
    'BinarySize',
    'HostPortPair',
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


class DefaultForUnspecified(enum.Enum):
    LIMITED = 0
    UNLIMITED = 1


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


class HostPortPair(namedtuple('HostPortPair', 'host port')):

    def as_sockaddr(self):
        return str(self.host), self.port

    def __str__(self):
        if isinstance(self.host, ipaddress.IPv6Address):
            return f'[{self.host}]:{self.port}'
        return f'{self.host}:{self.port}'


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
        if isinstance(expr, (Decimal, numbers.Integral)):
            return cls(expr)
        assert isinstance(expr, str)
        if expr.lower().startswith('inf'):
            return Decimal('Infinity')
        orig_expr = expr
        expr = expr.strip().replace('_', '')
        try:
            return int(expr)
        except ValueError:
            expr = expr.lower()
            try:
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
            except ArithmeticError:
                raise ValueError('Unconvertible value', orig_expr)
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


class ResourceSlot(UserDict):

    __slots__ = ('data', )

    def __init__(self, *args):
        super().__init__(*args)

    def __add__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can add ResourceSlot to ResourceSlot.'
        return type(self)({
            k: self.get(k, 0) + other.get(k, 0)
            for k in (self.keys() | other.keys())
        })

    def __sub__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can subtract ResourceSlot from ResourceSlot.'
        if other.keys() > self.keys():
            raise ValueError('Cannot subtract resource slot with more keys!')
        return type(self)({
            k: self.data[k] - other.get(k, 0)
            for k in self.keys()
        })

    def __eq__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() != other.keys():
            return False
        self_values = [self.data[k] for k in sorted(self.data.keys())]
        other_values = [other.data[k] for k in sorted(other.data.keys())]
        return self_values == other_values

    def __ne__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() != other.keys():
            return True
        return not self.__eq__(other)

    def eq_contains(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot contain other.')
        common_keys = sorted(other.keys() & self.keys())
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values

    def eq_contained(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be contained in other.')
        common_keys = sorted(other.keys() & self.keys())
        self_values = [self.data[k] for k in common_keys]
        other_values = [other.data[k] for k in common_keys]
        return self_values == other_values

    def __le__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be smaller than other.')
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return not any(s > o for s, o in zip(self_values, other_values))

    def __lt__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() > other.keys():
            raise ValueError('Slots with more keys cannot be smaller than other.')
        self_values = [self.data[k] for k in self.keys()]
        other_values = [other.data[k] for k in self.keys()]
        return (not any(s > o for s, o in zip(self_values, other_values)) and
                not (self_values == other_values))

    def __ge__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot be larger than other.')
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return not any(s < o for s, o in zip(self_values, other_values))

    def __gt__(self, other):
        assert isinstance(other, ResourceSlot), 'Only can compare ResourceSlot objects.'
        if self.keys() < other.keys():
            raise ValueError('Slots with less keys cannot be larger than other.')
        self_values = [self.data[k] for k in other.keys()]
        other_values = [other.data[k] for k in other.keys()]
        return (not any(s < o for s, o in zip(self_values, other_values)) and
                not (self_values == other_values))

    def filter_slots(self, known_slots):
        if isinstance(known_slots, Mapping):
            slots = {*known_slots.keys()}
        else:
            slots = {*known_slots}
        data = {
            k: v for k, v in self.data.items()
            if k in slots
        }
        return type(self)(data)

    @classmethod
    def _normalize_value(cls, value: Any, unit: str) -> Decimal:
        try:
            if unit == 'bytes':
                if isinstance(value, (Decimal, int)):
                    return int(value)
                value = Decimal(BinarySize.from_str(value))
            else:
                value = Decimal(value)
                if value.is_finite():
                    value = value.quantize(Quantum).normalize()
        except ArithmeticError:
            raise ValueError('Cannot convert to decimal', value)
        return value

    @classmethod
    def _humanize_value(cls, value: Decimal, unit: str) -> str:
        if unit == 'bytes':
            try:
                value = '{:s}'.format(BinarySize(value))
            except ValueError:
                value = _stringify_number(value)
        else:
            value = _stringify_number(value)
        return value

    @classmethod
    def _guess_slot_type(cls, key: str) -> str:
        if 'mem' in key:
            return 'bytes'
        return 'count'

    @classmethod
    def from_policy(cls, policy: Mapping[str, Any], slot_types: Mapping):
        try:
            data = {
                k: cls._normalize_value(v, slot_types[k])
                for k, v in policy['total_resource_slots'].items()
                if v is not None and k in slot_types
            }
            # fill missing (depending on the policy for unspecified)
            fill = Decimal(0)
            if policy['default_for_unspecified'] == DefaultForUnspecified.UNLIMITED:
                fill = Decimal('Infinity')
            for k in slot_types.keys():
                if k not in data:
                    data[k] = fill
        except KeyError as e:
            raise ValueError('unit unknown for slot', e.args[0])
        return cls(data)

    @classmethod
    def from_user_input(cls, obj: Mapping[str, Any], slot_types: Optional[Mapping]):
        try:
            if slot_types is None:
                data = {
                    k: cls._normalize_value(v, cls._guess_slot_type(k)) for k, v in obj.items()
                    if v is not None
                }
            else:
                data = {
                    k: cls._normalize_value(v, slot_types[k]) for k, v in obj.items()
                    if v is not None
                }
                # fill missing
                for k in slot_types.keys():
                    if k not in data:
                        data[k] = Decimal(0)
        except KeyError as e:
            raise ValueError('unit unknown for slot', e.args[0])
        return cls(data)

    def to_humanized(self, slot_types: Mapping) -> Mapping[str, str]:
        try:
            return {
                k: type(self)._humanize_value(v, slot_types[k]) for k, v in self.data.items()
                if v is not None
            }
        except KeyError as e:
            raise ValueError('unit unknown for slot', e.args[0])

    @classmethod
    def from_json(cls, obj: Mapping[str, Any]):
        data = {
            k: Decimal(v) for k, v in obj.items()
            if v is not None
        }
        return cls(data)

    def to_json(self) -> Mapping[str, str]:
        return {
            k: str(Decimal(v)) for k, v in self.data.items()
            if v is not None
        }


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
            v = 'Infinity'
        elif math.isinf(v) and v < 0:
            v = '-Infinity'
        else:
            v = '{:f}'.format(v)
    elif isinstance(v, BinarySize):
        v = '{:d}'.format(int(v))
    elif isinstance(v, int):
        v = '{:d}'.format(v)
    else:
        v = str(v)
    return v
