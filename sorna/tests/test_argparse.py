import argparse
import ipaddress

import pytest

from sorna.argparse import (
    port_no, positive_int, HostPortPair, host_port_pair, ipaddr, path
)


def test_port_no():
    assert port_no(1) == 1
    assert port_no(20) == 20
    assert port_no(65535) == 65535

    with pytest.raises(argparse.ArgumentTypeError):
        port_no(-1)
    with pytest.raises(argparse.ArgumentTypeError):
        port_no(0)
    with pytest.raises(argparse.ArgumentTypeError):
        port_no(65536)
    with pytest.raises(argparse.ArgumentTypeError):
        port_no(65537)


def test_positive_int():
    assert positive_int(1)
    assert positive_int(100000)

    with pytest.raises(argparse.ArgumentTypeError):
        positive_int(0)
    with pytest.raises(argparse.ArgumentTypeError):
        positive_int(-1)
    with pytest.raises(argparse.ArgumentTypeError):
        positive_int(-10)


def test_host_port_pair_object():
    ip = ipaddress.ip_address('1.2.3.4')
    pair = HostPortPair(ip, 8000)

    assert pair.as_sockaddr() == ('1.2.3.4', 8000)
    assert '{}'.format(pair) == '1.2.3.4:8000'


def test_host_port_pair_ftn():
    ip = ipaddress.ip_address('1.2.3.4')
    assert host_port_pair('1.2.3.4:5000') == HostPortPair(ip, 5000)

    with pytest.raises(argparse.ArgumentTypeError):
        host_port_pair('1.2.3')
    with pytest.raises(argparse.ArgumentTypeError):
        host_port_pair('1.2.3.4')
    with pytest.raises(argparse.ArgumentTypeError):
        host_port_pair('1.2.3.4:5:6')
    with pytest.raises(argparse.ArgumentTypeError):
        host_port_pair('1.2.3.4:5:0')
    with pytest.raises(argparse.ArgumentTypeError):
        host_port_pair('1.2.3.4:5:65536')


def test_ipaddr():
    assert ipaddr('[192.168.0.1]') == ipaddress.ip_address('192.168.0.1')
    assert ipaddr('192.168.0.1') == ipaddress.ip_address('192.168.0.1')
    assert ipaddr('2001:DB8::1') == ipaddress.ip_address('2001:DB8::1')

    with pytest.raises(argparse.ArgumentTypeError):
        ipaddr('50')
    with pytest.raises(argparse.ArgumentTypeError):
        ipaddr('1.1')
    with pytest.raises(argparse.ArgumentTypeError):
        ipaddr('1.1.1')


def test_path(tmpdir):
    assert path(None) is None
    assert path(tmpdir) == tmpdir
    with pytest.raises(argparse.ArgumentTypeError):
        assert path('/path/not/exist/')
