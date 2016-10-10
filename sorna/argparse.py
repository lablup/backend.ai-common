#! /usr/bin/env python3

import argparse
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

def host_port_pair(s):
    pieces = s.split(':', maxsplit=1)
    if len(pieces) == 1:
        msg = '{!r} should contain both IP address and port number.'.format(s)
        raise argparse.ArgumentTypeError(msg)
    elif len(pieces) == 2:
        ipaddr = pieces[0]
        try:
            port = int(pieces[1])
            assert port > 0
            assert port < 65536
        except (ValueError, AssertionError):
            msg = '{!r} is not a valid port number.'.format(s)
            raise argparse.ArgumentTypeError(msg)
    return (ipaddr, port)

def ipaddr(s):
    try:
        addr = ipaddress.ip_address(s)
    except ValueError as e:
        msg = '{!r} is not a valid IP address.'.format(s)
        raise argparse.ArgumentTypeError(msg)
    return addr

def path(val):
    if val is None:
        return None
    p = pathlib.Path(val)
    if not p.exists():
        msg = '{!r} is not a valid file/dir path.'.format(val)
        raise argparse.ArgumentTypeError(msg)
    return p

