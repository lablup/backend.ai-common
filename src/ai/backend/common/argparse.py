import asyncio
import argparse
from collections import namedtuple
import ipaddress
import pathlib
import socket
from typing import Tuple
import threading

try:
    import aiodns
    _aiodns_available = True
    _aiodns_ctx = threading.local()
    _aiodns_ctx.resolver = None
except ImportError:
    _aiodns_available = False
    _aiodns_ctx = None


def port_no(s: str) -> int:
    try:
        port = int(s)
        assert port > 0
        assert port < 65536
    except (ValueError, AssertionError):
        msg = f'{s!r} is not a valid port number.'
        raise argparse.ArgumentTypeError(msg)
    return port


def port_range(s: str) -> Tuple[int, int]:
    try:
        port_range = tuple(map(int, s.split('-')))
    except (TypeError, ValueError):
        msg = f'{s!r} should be a hyphen-separated pair of integers.'
        raise argparse.ArgumentTypeError(msg)
    if len(port_range) != 2:
        msg = f'{s!r} should have exactly two integers.'
        raise argparse.ArgumentTypeError(msg)
    if not (0 < port_range[0] < 65536):
        msg = f'{port_range[0]} is not a valid port number.'
        raise argparse.ArgumentTypeError(msg)
    if not (0 < port_range[1] < 65536):
        msg = f'{port_range[1]} is not a valid port number.'
        raise argparse.ArgumentTypeError(msg)
    if not (port_range[0] < port_range[1]):
        msg = f'{port_range[0]} should be less than {port_range[1]}.'
        raise argparse.ArgumentTypeError(msg)
    return port_range


def positive_int(s: str) -> int:
    try:
        val = int(s)
        assert val > 0
    except (ValueError, AssertionError):
        msg = f'{s!r} is not a positive integer.'
        raise argparse.ArgumentTypeError(msg)
    return val


def non_negative_int(s: str) -> int:
    try:
        val = int(s)
        assert val >= 0
    except (ValueError, AssertionError):
        msg = f'{s!r} is not a non-negative integer.'
        raise argparse.ArgumentTypeError(msg)
    return val


class HostPortPair(namedtuple('_HostPortPair', 'host port')):

    def __format__(self, spec):
        return self.__str__()

    def __str__(self):
        if isinstance(self.host, ipaddress.IPv6Address):
            return f'[{self.host}]:{self.port}'
        return f'{self.host}:{self.port}'

    def as_sockaddr(self):
        # Translate this to a tuple of host/port pair without hostname resolving.
        return str(self.host), self.port

    def resolve(self):
        if isinstance(self.host, ipaddress._BaseAddress):
            # Already resolved one.
            return self
        # Resolve now and return a new HostPortPair.
        addrs = socket.getaddrinfo(self.host, 80)
        ip = ipaddress.ip_address(addrs[0][4][0])
        return HostPortPair(ip, self.port)

    async def resolve_async(self):
        if isinstance(self.host, ipaddress._BaseAddress):
            return self
        loop = asyncio.get_event_loop()
        if _aiodns_available:
            if _aiodns_ctx.resolver is None:
                _aiodns_ctx.resolver = aiodns.DNSResolver(loop=loop)
            else:
                assert _aiodns_ctx.resolver.loop is loop
            result = await _aiodns_ctx.resolver.gethostbyname(self.host, 0)
            ip = ipaddress.ip_address(result.addresses[0])
            return HostPortPair(ip, self.port)
        else:
            addrs = await loop.getaddrinfo(self.host, 80)
            ip = ipaddress.ip_address(addrs[0][4][0])
            return HostPortPair(ip, self.port)


def host_port_pair(s: str) -> Tuple[ipaddress._BaseAddress, int]:
    pieces = s.rsplit(':', maxsplit=1)
    if len(pieces) == 1:
        msg = f'{s!r} should contain both IP address and port number.'
        raise argparse.ArgumentTypeError(msg)
    elif len(pieces) == 2:
        # strip potential brackets in IPv6 hostname-port strings (RFC 3986).
        host = pieces[0].strip('[]')
        try:
            host = ipaddress.ip_address(host)
        except ValueError:
            # Let it be just a hostname.
            host = host
        try:
            port = int(pieces[1])
            assert port > 0
            assert port < 65536
        except (ValueError, AssertionError):
            msg = f'{pieces[1]!r} is not a valid port number.'
            raise argparse.ArgumentTypeError(msg)
    return HostPortPair(host, port)


def ipaddr(s: str) -> ipaddress._BaseAddress:
    try:
        ip = ipaddress.ip_address(s.strip('[]'))
    except ValueError:
        msg = f'{s!r} is not a valid IP address.'
        raise argparse.ArgumentTypeError(msg)
    return ip


def path(val: str) -> pathlib.Path:
    if val is None:
        return None
    p = pathlib.Path(val)
    if not p.exists():
        msg = f'{val!r} is not a valid file/dir path.'
        raise argparse.ArgumentTypeError(msg)
    return p
