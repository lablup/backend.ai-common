from collections import UserDict, namedtuple
from decimal import Decimal
import enum
import ipaddress
import math
import numbers
from typing import (
    Any, Optional, Union,
    Tuple, Sequence,
    Mapping,
    NewType, Type, TypeVar,
)
from typing_extensions import TypedDict
import uuid

import attr

__all__ = (
    'aobject',
    'DeviceId',
    'ContainerId',
    'KernelId',
    'MetricKey',
    'MetricValue',
    'MovingStatValue',
    'PID',
    'HostPID',
    'ContainerPID',
    'BinarySize',
    'HostPortPair',
    'DeviceId',
    'SlotName',
    'IntrinsicSlotNames',
    'ResourceSlot',
    'MountPermission',
    'KernelCreationConfig',
    'KernelCreationResult',
)


T_aobj = TypeVar('T_aobj', bound='aobject')


class aobject(object):
    '''
    An "asynchronous" object which guarantees to invoke both ``def __init__(self, ...)`` and
    ``async def __ainit(self)__`` to ensure asynchronous initialization of the object.

    You can create an instance of subclasses of aboject in two ways:

    .. code-block:: python

       o = await SomeAObj(...)

    .. code-block:: python

       o = await SomeAObj.new(...)

    The latter is supported to avoid type checking errors (currently confirmed in mypy 0.720+).
    '''

    @classmethod
    async def new(cls: Type[T_aobj], *args, **kwargs) -> T_aobj:
        '''
        We can do ``await SomeAObject(...)``, but this makes mypy
        to complain about its return type with ``await`` statement.
        This is a copy of ``__new__()`` to workaround it.
        '''
        instance = super().__new__(cls)
        instance.__init__(*args, **kwargs)
        await instance.__ainit__()
        return instance

    def __init__(self, *args, **kwargs) -> None:
        pass

    async def __ainit__(self) -> None:
        '''
        Automatically called when creating the instance using
        ``await SubclassOfAObject(...)``
        where the arguments are passed to ``__init__()`` as in
        the vanilla Python classes.
        '''
        pass


PID = NewType('PID', int)
HostPID = NewType('HostPID', PID)
ContainerPID = NewType('ContainerPID', PID)

ContainerId = NewType('ContainerId', str)
KernelId = NewType('KernelId', uuid.UUID)
SessionId = NewType('SessionId', str)

AgentId = NewType('AgentId', str)
DeviceName = NewType('DeviceName', str)
DeviceId = NewType('DeviceId', str)
SlotName = NewType('SlotName', str)
MetricKey = NewType('MetricKey', str)

AccessKey = NewType('AccessKey', str)
SecretKey = NewType('SecretKey', str)


class SlotTypes(str, enum.Enum):
    COUNT = 'count'
    BYTES = 'bytes'


class SessionTypes(str, enum.Enum):
    INTERACTIVE = 'interactive'
    BATCH = 'batch'


class SessionResult(str, enum.Enum):
    UNDEFINED = 'undefined'
    SUCCESS = 'success'
    FAILURE = 'failure'


class MovingStatValue(TypedDict):
    min: str
    max: str
    sum: str
    avg: str
    diff: str
    rate: str
    version: Optional[int]  # for legacy client compatibility


MetricValue = TypedDict('MetricValue', {
    'current': str,
    'capacity': Optional[str],
    'pct': Optional[str],
    'unit_hint': str,
    'stats.min': str,
    'stats.max': str,
    'stats.sum': str,
    'stats.avg': str,
    'stats.diff': str,
    'stats.rate': str,
}, total=False)


class IntrinsicSlotNames(enum.Enum):
    CPU = SlotName('cpu')
    MEMORY = SlotName('mem')


class DefaultForUnspecified(str, enum.Enum):
    LIMITED = 'LIMITED'
    UNLIMITED = 'UNLIMITED'


class HandlerForUnknownSlotName(str, enum.Enum):
    DROP = 'drop'
    ERROR = 'error'


Quantum = Decimal('0.000')


class MountPermission(str, enum.Enum):
    READ_ONLY = 'ro'
    READ_WRITE = 'rw'
    RW_DELETE = 'wd'


class MountTypes(str, enum.Enum):
    VOLUME = 'volume'
    BIND = 'bind'
    TMPFS = 'tmpfs'


class HostPortPair(namedtuple('HostPortPair', 'host port')):

    def as_sockaddr(self) -> Tuple[str, int]:
        return str(self.host), self.port

    def __str__(self) -> str:
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


class ResourceSlot(UserDict):

    __slots__ = ('data', )

    def __init__(self, *args) -> None:
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

    def filter_slots(self, known_slots) -> 'ResourceSlot':
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
                if isinstance(value, Decimal):
                    return Decimal(value) if value.is_finite() else value
                if isinstance(value, int):
                    return Decimal(value)
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
                result = '{:s}'.format(BinarySize(value))
            except (OverflowError, ValueError):
                result = _stringify_number(value)
        else:
            result = _stringify_number(value)
        return result

    @classmethod
    def _guess_slot_type(cls, key: str) -> str:
        if 'mem' in key:
            return 'bytes'
        return 'count'

    @classmethod
    def from_policy(cls, policy: Mapping[str, Any], slot_types: Mapping) -> 'ResourceSlot':
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
    def from_user_input(cls, obj: Mapping[str, Any], slot_types: Optional[Mapping]) -> 'ResourceSlot':
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
    def from_json(cls, obj: Mapping[str, Any]) -> 'ResourceSlot':
        data = {
            k: Decimal(v) for k, v in obj.items()
            if v is not None
        }
        return cls(data)

    def to_json(self) -> Mapping[str, str]:
        return {
            k: _stringify_number(Decimal(v)) for k, v in self.data.items()
            if v is not None
        }


@attr.s(auto_attribs=True, slots=True)
class VFolderRequest:
    '''
    Represents vfolders for a new compute session request.
    '''
    vfolder_id: str
    vfolder_host: str
    permission: MountPermission


class ImageRegistry(TypedDict):
    name: str
    url: str
    username: Optional[str]
    password: Optional[str]


class ImageConfig(TypedDict):
    canonical: str
    digest: str
    repo_digest: Optional[str]
    registry: ImageRegistry
    labels: Mapping[str, str]


class ServicePort(TypedDict):
    name: str
    protocol: str
    container_ports: Sequence[int]
    host_ports: Sequence[Optional[int]]


class DeviceModelInfo(TypedDict):
    device_id: DeviceId
    model_name: str


class KernelCreationResult(TypedDict):
    id: KernelId
    container_id: ContainerId
    service_ports: Sequence[ServicePort]
    kernel_host: str
    resource_spec: Mapping[str, Any]
    attached_devices: Mapping[DeviceName, Sequence[DeviceModelInfo]]
    repl_in_port: int
    repl_out_port: int
    stdin_port: int     # legacy
    stdout_port: int    # legacy


class KernelCreationConfig(TypedDict):
    image: ImageConfig
    session_type: str                  # value of SessionTypes
    resource_slots: Mapping[str, str]  # json form of ResourceSlot
    resource_opts: Mapping[str, str]   # json form of resource options
    environ: Mapping[str, str]
    mounts: Sequence[str]              # list of mount expressions
    idle_timeout: int
    startup_command: Optional[str]
    internal_data: Optional[Mapping[str, Any]]


def _stringify_number(v: Union[BinarySize, int, float, Decimal]) -> str:
    '''
    Stringify a number, preventing unwanted scientific notations.
    '''
    if isinstance(v, (float, Decimal)):
        if math.isinf(v) and v > 0:
            result = 'Infinity'
        elif math.isinf(v) and v < 0:
            result = '-Infinity'
        else:
            result = '{:f}'.format(v)
    elif isinstance(v, BinarySize):
        result = '{:d}'.format(int(v))
    elif isinstance(v, int):
        result = '{:d}'.format(v)
    else:
        result = str(v)
    return result


class Sentinel(object):
    pass
