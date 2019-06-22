'''
An extension module to Trafaret which provides additional type checkers.
'''

import collections
import ipaddress
import os
from pathlib import Path as _Path
from typing import Any, Mapping, Sequence, Tuple
import pwd

import trafaret as t

from .types import BinarySize as _BinarySize

__all__ = (
    'BinarySize',
    'HostPortPair',
    'Path',
    'PortRange',
    'UID',
)


class BinarySize(t.Trafaret):

    def check_and_return(self, value: Any) -> _BinarySize:
        try:
            return _BinarySize.from_str(value)
        except ValueError:
            self._failure('value is not a valid binary size', value=value)


_HostPortPair = collections.namedtuple('_HostPortPair', 'host port')


class Path(t.Trafaret):

    def __init__(self, *, type: str,
                 auto_create: bool = False,
                 allow_nonexisting: bool = False,
                 allow_devnull: bool = False):
        self._type = type
        if auto_create and type != 'dir':
            raise TypeError('Only directory paths can be set auto-created.')
        self._auto_create = auto_create
        self._allow_nonexisting = allow_nonexisting
        self._allow_devnull = allow_devnull

    def check_and_return(self, value: Any) -> _Path:
        try:
            p = _Path(value).resolve()
        except (TypeError, ValueError):
            self._failure('cannot parse value as a path', value=value)
        if self._type == 'dir':
            if self._auto_create:
                p.mkdir(parents=True, exist_ok=True)
            if not self._allow_nonexisting and not p.is_dir():
                self._failure('value is not a directory', value=value)
        elif self._type == 'file':
            if not self._allow_devnull and str(p) == os.devnull:
                # it may be not a regular file but a char-device.
                return p
            if not self._allow_nonexisting and not p.is_file():
                self._failure('value is not a regular file', value=value)
        return p


class HostPortPair(t.Trafaret):

    def check_and_return(self, value: Any) -> Tuple[ipaddress._BaseAddress, int]:
        if isinstance(value, str):
            pair = value.rsplit(':', maxsplit=1)
            if len(pair) == 1:
                self._failure('value as string must contain both address and number', value=value)
            host, port = pair[0], pair[1]
        elif isinstance(value, Sequence):
            if len(value) != 2:
                self._failure('value as array must contain only two values for address and number', value=value)
            host, port = value[0], value[1]
        elif isinstance(value, Mapping):
            try:
                host, port = value['host'], value['port']
            except KeyError:
                self._failure('value as map must contain "host" and "port" keys', value=value)
        else:
            self._failure('urecognized value type', value=value)
        try:
            host = ipaddress.ip_address(host.strip('[]'))
        except ValueError:
            pass  # just treat as a string hostname
        try:
            port = t.Int[1:65535].check(port)
        except t.DataError:
            self._failure('port number must be between 1 and 65535', value=value)
        return _HostPortPair(host, port)


class PortRange(t.Trafaret):

    def check_and_return(self, value: Any) -> Tuple[int, int]:
        if isinstance(value, str):
            try:
                value = tuple(map(int, value.split('-')))
            except (TypeError, ValueError):
                self._failure('value as string should be a hyphen-separated pair of integers', value=value)
        elif isinstance(value, Sequence):
            if len(value) != 2:
                self._failure('value as array must contain only two values', value=value)
        else:
            self._failure('urecognized value type', value=value)
        try:
            min_port = t.Int[1:65535].check(value[0])
            max_port = t.Int[1:65535].check(value[1])
        except t.DataError:
            self._failure('each value must be a valid port number')
        if not (min_port < max_port):
            self._failure('first value must be less than second value', value=value)
        return min_port, max_port


class UID(t.Trafaret):

    def check_and_return(self, value: Any) -> int:
        if isinstance(value, int):
            if value == -1:
                return os.getuid()
        elif isinstance(value, str):
            if not value:
                return os.getuid()
            try:
                value = int(value)
            except ValueError:
                try:
                    return pwd.getpwnam(value).pw_uid
                except KeyError:
                    self._failure('no such user in system', value=value)
            else:
                return self.check_and_return(value)
        else:
            self._failure('value must be either int or str', value=value)
        return value
