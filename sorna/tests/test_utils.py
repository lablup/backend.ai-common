from collections import OrderedDict
from unittest import mock

import pytest

from sorna.utils import (
    odict, dict2kvlist, generate_uuid, nmget, readable_size_to_bytes,
    curl, get_instance_id, get_instance_ip, get_instance_type, AsyncBarrier
)


class AsyncMock(mock.Mock):
    """
    Asynchronous Mock for testing.
    """
    def __call__(self, *args, **kwargs):
        sup = super(AsyncMock, self)
        async def coro():
            return sup.__call__(*args, **kwargs)
        return coro()

    def __await__(self):
        return self().__await__()

    def __aenter__(self):
        pass

    def __aexit__(self):
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


@pytest.mark.skip('not implemented yet')
@pytest.mark.asyncio
async def test_curl_fake(mocker):
    pass
    # TODO: I need to know asynchronous programming, and how to test
    #       with them.
    # import aiohttp
    # mock_get = mocker.patch.object(aiohttp.ClientSession, 'get')
    # mock_get.return_value = AsyncMock()
    #
    # await curl('/test/url')


@pytest.mark.asyncio
async def test_get_instance_id(mocker):
    mock_curl = mocker.patch('sorna.utils.curl')
    mock_curl.return_value = AsyncMock()

    mock_curl.assert_not_called()
    await get_instance_id()
    args_lst = mock_curl.call_args[0]

    assert 'instance-id' in args_lst[0]
    assert callable(args_lst[1])


@pytest.mark.asyncio
async def test_get_instance_ip(mocker):
    mock_curl = mocker.patch('sorna.utils.curl')
    mock_curl.return_value = AsyncMock()

    mock_curl.assert_not_called()
    await get_instance_ip()
    args_lst = mock_curl.call_args[0]

    assert 'local-ipv' in args_lst[0]


@pytest.mark.asyncio
async def test_get_instance_type(mocker):
    mock_curl = mocker.patch('sorna.utils.curl')
    mock_curl.return_value = AsyncMock()

    mock_curl.assert_not_called()
    await get_instance_type()
    args_lst = mock_curl.call_args[0]

    assert 'instance-type' in args_lst[0]


@pytest.mark.skip('not implemented yet')
@pytest.mark.asyncio
async def test_async_barrier():
    pass

