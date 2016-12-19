#! /usr/bin/env python3

import argparse
from collections import namedtuple
import ipaddress
import pathlib


def port_no(s):
    try:
        port = int(s)
        assert port > 0
        assert port < 65536
    except (ValueError, AssertionError):
        msg = '{!r} is not a valid port number.'.format(s)
        raise argparse.ArgumentTypeError(msg)
    return port


def positive_int(s):
    try:
        val = int(s)
        assert val > 0
    except (ValueError, AssertionError):
        msg = '{!r} is not a positive integer.'.format(s)
        raise argparse.ArgumentTypeError(msg)
    return val


class HostPortPair(namedtuple('_HostPortPair', 'ip port')):
    def __format__(self, spec):
        if self.ip.version == 6:
            return f'[{self.ip}]:{self.port}'
        return f'{self.ip}:{self.port}'

    def as_sockaddr(self):
        return str(self.ip), self.port


def host_port_pair(s):
    pieces = s.rsplit(':', maxsplit=1)
    if len(pieces) == 1:
        msg = f'{s!r} should contain both IP address and port number.'
        raise argparse.ArgumentTypeError(msg)
    elif len(pieces) == 2:
        try:
            # strip brackets in IPv6 hostname-port strings (RFC 3986).
            ip = ipaddress.ip_address(pieces[0].strip('[]'))
        except ValueError:
            msg = f'{pieces[0]!r} is not a valid IP address.'
            raise argparse.ArgumentTypeError(msg)
        try:
            port = int(pieces[1])
            assert port > 0
            assert port < 65536
        except (ValueError, AssertionError):
            msg = f'{pieces[1]!r} is not a valid port number.'
            raise argparse.ArgumentTypeError(msg)
    return HostPortPair(ip, port)


def ipaddr(s):
    try:
        ip = ipaddress.ip_address(s.strip('[]'))
    except ValueError as e:
        msg = f'{s!r} is not a valid IP address.'
        raise argparse.ArgumentTypeError(msg)
    return ip


def path(val):
    if val is None:
        return None
    p = pathlib.Path(val)
    if not p.exists():
        msg = f'{val!r} is not a valid file/dir path.'
        raise argparse.ArgumentTypeError(msg)
    return p
