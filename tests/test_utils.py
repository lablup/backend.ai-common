import asyncio
from collections import OrderedDict
from unittest import mock

import aiohttp
import pytest

from ai.backend.common.utils import (
    odict, dict2kvlist, generate_uuid, get_random_seq, nmget, readable_size_to_bytes,
    curl, StringSetFlag, AsyncBarrier
)
from ai.backend.common.testutils import (
    mock_corofunc, mock_awaitable, AsyncContextManagerMock
)


def test_odict():
    assert odict(('a', 1), ('b', 2)) == OrderedDict([('a', 1), ('b', 2)])


def test_dict2kvlist():
    ret = list(dict2kvlist({'a': 1, 'b': 2}))
    assert set(ret) == {'a', 1, 'b', 2}


def test_generate_uuid():
    u = generate_uuid()
    assert len(u) == 22
    assert isinstance(u, str)


def test_random_seq():
    assert [*get_random_seq(10, 11, 1)] == [0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
    assert [*get_random_seq(10, 6, 2)] == [0, 2, 4, 6, 8, 10]
    with pytest.raises(AssertionError):
        [*get_random_seq(10, 12, 1)]
    with pytest.raises(AssertionError):
        [*get_random_seq(10, 7, 2)]
    for _ in range(30):
        result = [*get_random_seq(10, 9, 1)]
        assert result[0] >= 0
        assert result[-1] <= 10
        last_x = result[0]
        for x in result[1:]:
            assert x > last_x + 1


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
    assert readable_size_to_bytes('2K') == 2 * (2 ** 10)
    assert readable_size_to_bytes('2k') == 2 * (2 ** 10)
    assert readable_size_to_bytes('2M') == 2 * (2 ** 20)
    assert readable_size_to_bytes('2m') == 2 * (2 ** 20)
    assert readable_size_to_bytes('2G') == 2 * (2 ** 30)
    assert readable_size_to_bytes('2g') == 2 * (2 ** 30)
    assert readable_size_to_bytes('2T') == 2 * (2 ** 40)
    assert readable_size_to_bytes('2t') == 2 * (2 ** 40)
    assert readable_size_to_bytes('2P') == 2 * (2 ** 50)
    assert readable_size_to_bytes('2p') == 2 * (2 ** 50)
    assert readable_size_to_bytes('2E') == 2 * (2 ** 60)
    assert readable_size_to_bytes('2e') == 2 * (2 ** 60)
    assert readable_size_to_bytes('2Z') == 2 * (2 ** 70)
    assert readable_size_to_bytes('2z') == 2 * (2 ** 70)
    assert readable_size_to_bytes('2Y') == 2 * (2 ** 80)
    assert readable_size_to_bytes('2y') == 2 * (2 ** 80)
    with pytest.raises(ValueError):
        readable_size_to_bytes('3A')
    with pytest.raises(ValueError):
        readable_size_to_bytes('TT')


@pytest.mark.asyncio
async def test_curl_returns_stripped_body(mocker):
    mock_get = mocker.patch.object(aiohttp.ClientSession, 'get')
    mock_resp = {'status': 200, 'text': mock_corofunc(b'success  ')}
    mock_get.return_value = AsyncContextManagerMock(**mock_resp)

    resp = await curl('/test/url')

    body = await mock_resp['text']()
    assert resp == body.strip()


@pytest.mark.asyncio
async def test_curl_returns_default_value_if_not_success(mocker):
    mock_get = mocker.patch.object(aiohttp.ClientSession, 'get')
    mock_resp = {'status': 400, 'text': mock_corofunc(b'bad request')}
    mock_get.return_value = AsyncContextManagerMock(**mock_resp)

    # Value.
    resp = await curl('/test/url', default_value='default')
    assert resp == 'default'

    # Callable.
    resp = await curl('/test/url', default_value=lambda: 'default')
    assert resp == 'default'


def test_string_set_flag():

    class MyFlags(StringSetFlag):
        A = 'a'
        B = 'b'

    assert MyFlags.A in {'a', 'c'}
    assert MyFlags.B not in {'a', 'c'}

    assert MyFlags.A == 'a'
    assert MyFlags.A != 'b'
    assert 'a' == MyFlags.A
    assert 'b' != MyFlags.A

    assert {'a', 'b'} == MyFlags.A | MyFlags.B
    assert {'a', 'b'} == MyFlags.A | 'b'
    assert {'a', 'b'} == 'a' | MyFlags.B
    assert {'a', 'b', 'c'} == {'b', 'c'} | MyFlags.A
    assert {'a', 'b', 'c'} == MyFlags.A | {'b', 'c'}

    assert {'b', 'c'} == {'a', 'b', 'c'} ^ MyFlags.A
    assert {'a', 'b', 'c'} == {'b', 'c'} ^ MyFlags.A
    assert set() == MyFlags.A ^ 'a'
    assert {'b', } == MyFlags.A ^ {'a', 'b'}
    assert {'a', 'b', 'c'} == MyFlags.A ^ {'b', 'c'}
    with pytest.raises(TypeError):
        123 & MyFlags.A

    assert {'a', 'c'} & MyFlags.A
    assert not {'a', 'c'} & MyFlags.B
    assert 'a' & MyFlags.A
    assert not 'a' & MyFlags.B
    assert MyFlags.A & 'a'
    assert not MyFlags.A & 'b'
    assert MyFlags.A & {'a', 'b'}
    assert not MyFlags.A & {'b', 'c'}


class TestAsyncBarrier:
    def test_async_barrier_initialization(self):
        barrier = AsyncBarrier(num_parties=5)

        assert barrier.num_parties == 5
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
