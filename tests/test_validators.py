from ipaddress import IPv4Address

import multidict
import pytest
import trafaret as t

from ai.backend.common import validators as tx


def test_aliased_key():
    iv = t.Dict({
        t.Key('x') >> 'z': t.Int,
        tx.AliasedKey(['y', 'Y']): t.Int,
    })
    assert iv.check({'x': 1, 'y': 2}) == {'z': 1, 'y': 2}

    with pytest.raises(t.DataError) as e:
        iv.check({'x': 1})
    err_data = e.value.as_dict()
    assert 'y' in err_data
    assert "is required" in err_data['y']

    with pytest.raises(t.DataError) as e:
        iv.check({'y': 2})
    err_data = e.value.as_dict()
    assert 'x' in err_data
    assert "is required" in err_data['x']

    with pytest.raises(t.DataError) as e:
        iv.check({'x': 1, 'y': 'string'})
    err_data = e.value.as_dict()
    assert 'y' in err_data
    assert "can't be converted to int" in err_data['y']

    with pytest.raises(t.DataError) as e:
        iv.check({'x': 1, 'Y': 'string'})
    err_data = e.value.as_dict()
    assert 'Y' in err_data
    assert "can't be converted to int" in err_data['Y']

    iv = t.Dict({
        t.Key('x', default=0): t.Int,
        tx.AliasedKey(['y', 'Y'], default=1): t.Int,
    })
    assert iv.check({'x': 5, 'Y': 6}) == {'x': 5, 'y': 6}
    assert iv.check({'x': 5, 'y': 6}) == {'x': 5, 'y': 6}
    assert iv.check({'y': 3}) == {'x': 0, 'y': 3}
    assert iv.check({'Y': 3}) == {'x': 0, 'y': 3}
    assert iv.check({'x': 3}) == {'x': 3, 'y': 1}
    assert iv.check({}) == {'x': 0, 'y': 1}

    with pytest.raises(t.DataError) as e:
        iv.check({'z': 99})
    err_data = e.value.as_dict()
    assert 'z' in err_data
    assert "not allowed key" in err_data['z']


def test_multikey():
    iv = t.Dict({
        tx.MultiKey('x'): t.List(t.Int),
        t.Key('y'): t.Int,
    })

    data = multidict.MultiDict()
    data.add('x', 1)
    data.add('x', 2)
    data.add('y', 3)
    result = iv.check(data)
    assert result['x'] == [1, 2]
    assert result['y'] == 3

    data = multidict.MultiDict()
    data.add('x', 1)
    data.add('y', 3)
    result = iv.check(data)
    assert result['x'] == [1]
    assert result['y'] == 3

    plain_data = {
        'x': [10, 20],
        'y': 30,
    }
    result = iv.check(plain_data)
    assert result['x'] == [10, 20]
    assert result['y'] == 30

    plain_data_nolist = {
        'x': 10,
        'y': 30,
    }
    result = iv.check(plain_data_nolist)
    assert result['x'] == [10]
    assert result['y'] == 30


def test_binary_size():
    iv = tx.BinarySize()
    assert iv.check('10M') == 10 * (2 ** 20)
    assert iv.check('1K') == 1024
    assert iv.check(1058476) == 1058476

    with pytest.raises(t.DataError):
        iv.check('XX')


def test_path():
    # TODO: write tests
    pass


def test_host_port_pair():
    iv = tx.HostPortPair()

    p = iv.check(('127.0.0.1', 80))
    assert isinstance(p, tx._HostPortPair)
    assert p.host == IPv4Address('127.0.0.1')
    assert p.port == 80

    p = iv.check('127.0.0.1:80')
    assert isinstance(p, tx._HostPortPair)
    assert p.host == IPv4Address('127.0.0.1')
    assert p.port == 80

    p = iv.check({'host': '127.0.0.1', 'port': 80})
    assert isinstance(p, tx._HostPortPair)
    assert p.host == IPv4Address('127.0.0.1')
    assert p.port == 80

    p = iv.check({'host': '127.0.0.1', 'port': '80'})
    assert isinstance(p, tx._HostPortPair)
    assert p.host == IPv4Address('127.0.0.1')
    assert p.port == 80

    p = iv.check(('mydomain.com', 443))
    assert isinstance(p, tx._HostPortPair)
    assert p.host == 'mydomain.com'
    assert p.port == 443

    p = iv.check('mydomain.com:443')
    assert isinstance(p, tx._HostPortPair)
    assert p.host == 'mydomain.com'
    assert p.port == 443

    p = iv.check({'host': 'mydomain.com', 'port': 443})
    assert isinstance(p, tx._HostPortPair)
    assert p.host == 'mydomain.com'
    assert p.port == 443

    p = iv.check({'host': 'mydomain.com', 'port': '443'})
    assert isinstance(p, tx._HostPortPair)
    assert p.host == 'mydomain.com'
    assert p.port == 443

    with pytest.raises(t.DataError):
        p = iv.check(('127.0.0.1', -1))
    with pytest.raises(t.DataError):
        p = iv.check(('127.0.0.1', 0))
    with pytest.raises(t.DataError):
        p = iv.check(('127.0.0.1', 65536))
    with pytest.raises(t.DataError):
        p = iv.check('127.0.0.1:65536')
    with pytest.raises(t.DataError):
        p = iv.check(('', 80))
    with pytest.raises(t.DataError):
        p = iv.check(':80')
    with pytest.raises(t.DataError):
        p = iv.check({})
    with pytest.raises(t.DataError):
        p = iv.check({'host': 'x'})
    with pytest.raises(t.DataError):
        p = iv.check({'port': 80})
    with pytest.raises(t.DataError):
        p = iv.check({'host': '', 'port': 80})


def test_port_range():
    iv = tx.PortRange()

    r = iv.check('1000-2000')
    assert isinstance(r, tuple)
    assert len(r) == 2
    assert r[0] == 1000
    assert r[1] == 2000

    r = iv.check([1000, 2000])
    assert isinstance(r, tuple)
    assert len(r) == 2
    assert r[0] == 1000
    assert r[1] == 2000

    r = iv.check((1000, 2000))
    assert isinstance(r, tuple)
    assert len(r) == 2
    assert r[0] == 1000
    assert r[1] == 2000

    with pytest.raises(t.DataError):
        r = iv.check([0, 1000])
    with pytest.raises(t.DataError):
        r = iv.check([1000, 65536])
    with pytest.raises(t.DataError):
        r = iv.check([2000, 1000])
    with pytest.raises(t.DataError):
        r = iv.check('x-y')


def test_uid():
    # TODO: write tests
    pass


def test_slug():
    iv = tx.Slug()
    assert iv.check('a') == 'a'
    assert iv.check('0Z') == '0Z'
    assert iv.check('abc') == 'abc'
    assert iv.check('a-b') == 'a-b'
    assert iv.check('a_b') == 'a_b'

    with pytest.raises(t.DataError):
        iv.check('_')
    with pytest.raises(t.DataError):
        iv.check('')

    iv = tx.Slug(allow_dot=True)
    assert iv.check('.a') == '.a'
    assert iv.check('a') == 'a'
    with pytest.raises(t.DataError):
        iv.check('..a')

    iv = tx.Slug[:4]
    assert iv.check('abc') == 'abc'
    assert iv.check('abcd') == 'abcd'
    with pytest.raises(t.DataError):
        iv.check('abcde')

    iv = tx.Slug[4:]
    with pytest.raises(t.DataError):
        iv.check('abc')
    assert iv.check('abcd') == 'abcd'
    assert iv.check('abcde') == 'abcde'

    iv = tx.Slug[2:4]
    with pytest.raises(t.DataError):
        iv.check('a')
    assert iv.check('ab') == 'ab'
    assert iv.check('abcd') == 'abcd'
    with pytest.raises(t.DataError):
        iv.check('abcde')

    iv = tx.Slug[2:2]
    with pytest.raises(t.DataError):
        iv.check('a')
    assert iv.check('ab') == 'ab'
    with pytest.raises(t.DataError):
        iv.check('abc')

    with pytest.raises(TypeError):
        tx.Slug[2:1]
    with pytest.raises(TypeError):
        tx.Slug[-1:]
    with pytest.raises(TypeError):
        tx.Slug[:-1]
