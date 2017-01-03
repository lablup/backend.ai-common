from collections import OrderedDict
from unittest import mock

import aiohttp
import asyncio
import asynctest
import pytest

from sorna.utils import (
    odict, dict2kvlist, generate_uuid, nmget, readable_size_to_bytes,
    curl, get_instance_id, get_instance_ip, get_instance_type, AsyncBarrier
)


def mock_coroftn(return_value):
    """
    Return mock coroutine function.

    Python's default mock module does not support coroutines.
    """
    async def mock_coroftn(*args, **kargs):
        return return_value
    return mock.Mock(wraps=mock_coroftn)


async def mock_awaitable(**kwargs):
    """
    Mock awaitable.

    An awaitable can be a native coroutine object "returned from" a native
    coroutine function.
    """
    return asynctest.CoroutineMock(**kwargs)


class AsyncContextManagerMock:
    """
    Mock async context manager.

    Can be used to get around `async with` statement for testing.
    Must implement `__aenter__` and `__aexit__` which returns awaitable.
    Attributes of the awaitable (and self for convenience) can be set by
    passing `kwargs`.
    """
    def __init__(self, *args, **kwargs):
        self.context = kwargs
        for k, v in kwargs.items():
            setattr(self, k, v)

    async def __aenter__(self):
        return asynctest.CoroutineMock(**self.context)

    async def __aexit__(self, exc_type, exc_value, exc_tb):
        pass


def test_odict():
    assert odict(('a', 1), ('b', 2)) == OrderedDict([('a', 1), ('b', 2)])


def test_dict2kvlist():
    ret = list(dict2kvlist({'a': 1, 'b': 2}))
    assert set(ret) == {'a', 1, 'b', 2}


def test_generate_uuid():
    u = generate_uuid()
    assert len(u) == 22
    assert isinstance(u, str)


def test_nmget():
    o = {'a': {'b': 1}, 'x': None}
    assert nmget(o, 'a', 0) == {'b': 1}
    assert nmget(o, 'a.b', 0) == 1
    assert nmget(o, 'a/b', 0, '/') == 1
    assert nmget(o, 'a.c', 0) == 0
    assert nmget(o, 'a.c', 100) == 100
    assert nmget(o, 'x', 0) == 0
    assert nmget(o, 'x', 0, null_as_default=False) is None


def test_readable_size_to_bytes():
    assert readable_size_to_bytes(2) == 2
    assert readable_size_to_bytes('2') == 2
    assert readable_size_to_bytes('2K') == 2 * 1024
    assert readable_size_to_bytes('2k') == 2 * 1024
    assert readable_size_to_bytes('2M') == 2 * 1024 * 1024
    assert readable_size_to_bytes('2m') == 2 * 1024 * 1024
    assert readable_size_to_bytes('2G') == 2 * 1024 * 1024 * 1024
    assert readable_size_to_bytes('2g') == 2 * 1024 * 1024 * 1024
    assert readable_size_to_bytes('2T') == 2 * 1024 * 1024 * 1024 * 1024
    assert readable_size_to_bytes('2t') == 2 * 1024 * 1024 * 1024 * 1024
    with pytest.raises(KeyError):
        readable_size_to_bytes('3A')
    with pytest.raises(ValueError):
        readable_size_to_bytes('TT')


@pytest.mark.asyncio
async def test_curl_returns_stripped_body(mocker):
    mock_get = mocker.patch.object(aiohttp.ClientSession, 'get')
    mock_resp = {'status': 200, 'text': mock_coroftn(b'success  ')}
    mock_get.return_value = AsyncContextManagerMock(**mock_resp)

    resp = await curl('/test/url')

    body = await mock_resp['text']()
    assert resp == body.strip()


@pytest.mark.asyncio
async def test_curl_returns_default_value_if_not_success(mocker):
    mock_get = mocker.patch.object(aiohttp.ClientSession, 'get')
    mock_resp = {'status': 400, 'text': mock_coroftn(b'bad request')}
    mock_get.return_value = AsyncContextManagerMock(**mock_resp)

    # Value.
    resp = await curl('/test/url', default_value='default')
    assert resp == 'default'

    # Callable.
    resp = await curl('/test/url', default_value=lambda: 'default')
    assert resp == 'default'


@pytest.mark.asyncio
async def test_get_instance_id(mocker):
    mock_curl = mocker.patch('sorna.utils.curl')
    mock_curl.return_value = mock_awaitable()

    mock_curl.assert_not_called()
    await get_instance_id()
    args_lst = mock_curl.call_args[0]

    assert 'instance-id' in args_lst[0]
    assert callable(args_lst[1])


@pytest.mark.asyncio
async def test_get_instance_ip(mocker):
    mock_curl = mocker.patch('sorna.utils.curl')
    mock_curl.return_value = mock_awaitable()

    mock_curl.assert_not_called()
    await get_instance_ip()
    args_lst = mock_curl.call_args[0]

    assert 'local-ipv' in args_lst[0]


@pytest.mark.asyncio
async def test_get_instance_type(mocker):
    mock_curl = mocker.patch('sorna.utils.curl')
    mock_curl.return_value = mock_awaitable()

    mock_curl.assert_not_called()
    await get_instance_type()
    args_lst = mock_curl.call_args[0]

    assert 'instance-type' in args_lst[0]


class TestAsyncBarrier:
    def test_async_barrier_initialization(self):
        barrier = AsyncBarrier(num_parties=5)

        assert barrier.num_parties == 5
        assert barrier.loop is not None  # default event loop
        assert barrier.cond is not None  # default condition

    @pytest.mark.asyncio
    async def test_wait_notify_all_if_cound_eq_num_parties(self, mocker):
        mock_cond = mocker.patch.object(asyncio, 'Condition')
        mock_resp = {
            'notify_all': mock.Mock(),
            'wait': await mock_awaitable()
        }
        mock_cond.return_value = AsyncContextManagerMock(**mock_resp)

        barrier = AsyncBarrier(num_parties=1)
        assert barrier.count == 0

        await barrier.wait()

        assert barrier.count == 1
        mock_cond.return_value.notify_all.assert_called_once_with()
        mock_cond.return_value.wait.assert_not_called()

    def test_async_barrier_reset(self):
        barrier = AsyncBarrier(num_parties=5)
        barrier.count = 5

        assert barrier.count == 5
        barrier.reset()
        assert barrier.count == 0

