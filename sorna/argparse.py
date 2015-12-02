#! /usr/bin/env python3

import argparse

def port_no(s):
    try:
        port = int(s)
        assert port > 0
        assert port < 65536
    except (ValueError, AssertionError) as e:
        raise argparse.ArgumentTypeError('The port number should be a positive integer between 0 and 65536.') from e
    return port

def positive_int(s):
    try:
        val = int(s)
        assert val > 0
    except (ValueError, AssertionError) as e:
        raise argparse.ArgumentTypeError('The value should be a positive integer.') from e
    return val

def host_port_pair(s):
    pieces = s.split(':', maxsplit=1)
    if len(pieces) == 1:
        raise argparse.ArgumentTypeError('%r should contain both IP address and port number.', s)
    elif len(pieces) == 2:
        ipaddr = pieces[0]
        try:
            port = int(pieces[1])
            assert port > 0
            assert port < 65536
        except (ValueError, AssertionError) as e:
            raise argparse.ArgumentTypeError('The port number should be a positive integer between 0 and 65536.') from e
    return (ipaddr, port)
