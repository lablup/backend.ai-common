from decimal import Decimal
import enum
from typing import Hashable, Mapping, Sequence, Set, TypeVar

import attr


DeviceId = TypeVar('DeviceId', int, str, Hashable)


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


@attr.s(auto_attribs=True, slots=True)
class AcceleratorRequest:
    '''
    Represents accelerator demands for a new compute session request.

    **device_share** is a relative value where its realization is responsible to
    the specific accelerator plugin.  For example, "1.5 CUDA GPU" may be either
    physically a fraction of one CUDA GPU or fractions spanning across two (or
    more) CUDA GPUs depending on the actual GPU capacity detected/analyzed by
    the CUDA accelerator plugin.

    The version and feature set fields are optional -- they are considered
    meaningful only when specified.  If specified, they indicate the minimum
    compatible versions required to execute the given compute session.
    '''
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
    cpu_share: Decimal
    memory_share: Decimal
    accelerators: Mapping[str, AcceleratorRequest] = attr.Factory(dict)
    vfolders: Sequence[VFolderRequest] = attr.Factory(list)
    scratch_disk_size: BinarySize = None


@attr.s(auto_attribs=True, slots=True)
class SessionRequest:
    '''
    Represents a new compute session request.
    '''
    scaling_group: str
    image: str
    resource_spec: ResourceRequest
