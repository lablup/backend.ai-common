from ipaddress import IPv4Address

from datetime import timedelta
import enum
import multidict
import pickle

import pytest
import trafaret as t

from ai.backend.common import validators as tx


def test_trafaret_dataerror_pickling():

    with pytest.raises(t.DataError):
        iv = t.Int()
        iv.check('x')

    # Remove the already installed monkey-patch.
    # (e.g., when running the whole test suite)
    try:
        if hasattr(t.DataError, '__reduce__'):
            delattr(t.DataError, '__reduce__')
    except AttributeError:
        pass

    with pytest.raises(RuntimeError):
        try:
            iv = t.Int()
            iv.check('x')
        except t.DataError as e:
            bindata = pickle.dumps(e)
            pickle.loads(bindata)

    tx.fix_trafaret_pickle_support()

    try:
        iv = t.Int()
        iv.check('x')
    except t.DataError as e:
        bindata = pickle.dumps(e)
        unpacked = pickle.loads(bindata)
        assert unpacked.error == e.error
        assert unpacked.name == e.name
        assert unpacked.value == e.value


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


def test_multikey_string():
    iv = t.Dict({
        tx.MultiKey('x'): t.List(t.String),
        t.Key('y'): t.String,
    })

    plain_data = {
        'x': ['abc'],
        'y': 'def',
    }
    result = iv.check(plain_data)
    assert result['x'] == ['abc']
    assert result['y'] == 'def'

    plain_data_nolist = {
        'x': 'abc',
        'y': 'def',
    }
    result = iv.check(plain_data_nolist)
    assert result['x'] == ['abc']
    assert result['y'] == 'def'


def test_binary_size():
    iv = tx.BinarySize()
    assert iv.check('10M') == 10 * (2 ** 20)
    assert iv.check('1K') == 1024
    assert iv.check(1058476) == 1058476

    with pytest.raises(t.DataError):
        iv.check('XX')


def test_binary_size_commutative_with_null():
    iv1 = t.Null | tx.BinarySize()
    iv2 = tx.BinarySize() | t.Null

    iv1.check(None)
    iv2.check(None)

    with pytest.raises(t.DataError):
        iv1.check('xxxxx')
    with pytest.raises(t.DataError):
        iv2.check('xxxxx')


def test_enum():

    class MyTypes(enum.Enum):
        TYPE1 = 1
        TYPE2 = 2

    iv = tx.Enum(MyTypes)
    assert iv.check(1) == MyTypes.TYPE1
    assert iv.check(2) == MyTypes.TYPE2
    with pytest.raises(t.DataError):
        iv.check(3)
    with pytest.raises(t.DataError):
        iv.check('STRING')

    iv = tx.Enum(MyTypes, use_name=True)
    assert iv.check('TYPE1') == MyTypes.TYPE1
    assert iv.check('TYPE2') == MyTypes.TYPE2
    with pytest.raises(t.DataError):
        iv.check('TYPE3')
    with pytest.raises(t.DataError):
        iv.check(0)


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


    # updates: '_' and empty string are allowed
    # with pytest.raises(t.DataError):
    #     iv.check('_')
    # with pytest.raises(t.DataError):
    #     iv.check('')

    iv = tx.Slug(allow_dot=True, ascii_only=False)
    assert iv.check('.a') == '.a'
    assert iv.check('a') == 'a'
    assert iv.check('.ㄱ') == '.ㄱ'
    assert iv.check('ㄱ') == 'ㄱ'
    assert iv.check('.Ç') == '.Ç'
    assert iv.check('Ç') == 'Ç'
    assert iv.check('.á') == '.á'
    assert iv.check('á') == 'á'
    assert iv.check('.あ') == '.あ'
    assert iv.check('あ') == 'あ'
    assert iv.check('.字') == '.字'
    assert iv.check('字') == '字'


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

    # ascii only
    iv = tx.Slug(allow_dot=True, ascii_only=True)
    assert iv.check('.a') == '.a'
    assert iv.check('a') == 'a'

    with pytest.raises(t.DataError):
        iv.check('.ㄱ')


def test_json_string():
    iv = tx.JSONString()
    iv.check('{}') == {}
    iv.check('{"a":123}') == {'a': 123}
    iv.check('[]') == []
    with pytest.raises(ValueError):
        iv.check('x')


def test_time_duration():
    iv = tx.TimeDuration()
    with pytest.raises(t.DataError):
        iv.check('')
    iv.check('1w') == timedelta(weeks=1)
    iv.check('1d') == timedelta(days=1)
    iv.check('0.5d') == timedelta(hours=12)
    iv.check('1h') == timedelta(hours=1)
    iv.check('1m') == timedelta(minutes=1)
    iv.check('1') == timedelta(seconds=1)
    iv.check('0.5h') == timedelta(minutes=30)
    iv.check('0.001') == timedelta(milliseconds=1)
    with pytest.raises(t.DataError):
        iv.check('-1')
    with pytest.raises(t.DataError):
        iv.check('a')
    with pytest.raises(t.DataError):
        iv.check('xxh')


def test_time_duration_negative():
    iv = tx.TimeDuration(allow_negative=True)
    with pytest.raises(t.DataError):
        iv.check('')
    iv.check('0.5h') == timedelta(minutes=30)
    iv.check('0.001') == timedelta(milliseconds=1)
    iv.check('-1') == timedelta(seconds=-1)
    iv.check('-3d') == timedelta(days=-3)
    with pytest.raises(t.DataError):
        iv.check('-a')
    with pytest.raises(t.DataError):
        iv.check('-xxh')
