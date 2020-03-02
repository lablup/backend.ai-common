import asyncio
try:
    from unittest import AsyncMock  # type: ignore
except ImportError:
    from asynctest import CoroutineMock as AsyncMock
import time

import aioredis
import pytest

from ai.backend.common import redis


@pytest.fixture
def mock_time(mocker):
    total_delay = 0.0
    call_count = 0
    base_time = time.monotonic()
    accum_time = 0.0

    async def _mock_async_sleep(delay) -> None:
        nonlocal total_delay, call_count, accum_time
        call_count += 1
        accum_time += delay
        total_delay += delay

    def _reset() -> None:
        nonlocal total_delay, call_count
        total_delay = 0.0
        call_count = 0

    def _get_total_delay() -> float:
        nonlocal total_delay
        return total_delay

    def _get_call_count() -> int:
        nonlocal call_count
        return call_count

    def _mock_time_monotonic() -> float:
        nonlocal accum_time
        return base_time + accum_time

    _mock_async_sleep.reset = _reset
    _mock_async_sleep.get_total_delay = _get_total_delay
    _mock_async_sleep.get_call_count = _get_call_count
    yield _mock_async_sleep, _mock_time_monotonic


@pytest.mark.asyncio
async def test_connect_with_retries(mocker, mock_time):
    mock_async_sleep, mock_time_monotonic = mock_time
    mock_create_pool = AsyncMock(side_effect=ConnectionRefusedError)
    mocker.patch('ai.backend.common.redis.aioredis.create_redis_pool', mock_create_pool)
    mocker.patch('ai.backend.common.redis.asyncio.sleep', mock_async_sleep)
    mocker.patch('ai.backend.common.redis.time.monotonic', mock_time_monotonic)
    with pytest.raises(asyncio.TimeoutError):
        await redis.connect_with_retries(retry_delay=0.5, max_retry_delay=60.0)
    assert mock_async_sleep.get_call_count() == 120
    assert mock_async_sleep.get_total_delay() == 60.0


@pytest.mark.asyncio
async def test_execute_with_retries(mocker, mock_time):
    mock_async_sleep, mock_time_monotonic = mock_time

    async def mock_work_connreset():
        raise ConnectionResetError

    async def mock_work_cancelled():
        raise asyncio.CancelledError

    async def mock_work_forceclosed():
        raise aioredis.errors.ConnectionForcedCloseError

    mocker.patch('ai.backend.common.redis.asyncio.sleep', mock_async_sleep)
    mocker.patch('ai.backend.common.redis.time.monotonic', mock_time_monotonic)
    with pytest.raises(asyncio.TimeoutError):
        await redis.execute_with_retries(mock_work_connreset, retry_delay=0.5, max_retry_delay=60.0)
    assert mock_async_sleep.get_call_count() == 120
    assert mock_async_sleep.get_total_delay() == 60.0

    mock_async_sleep.reset()

    with pytest.raises(asyncio.TimeoutError):
        await redis.execute_with_retries(mock_work_connreset, retry_delay=2, max_retry_delay=0, max_retries=1)
    assert mock_async_sleep.get_call_count() == 1
    assert mock_async_sleep.get_total_delay() == 2.0

    mock_async_sleep.reset()

    with pytest.raises(asyncio.TimeoutError):
        await redis.execute_with_retries(mock_work_connreset, retry_delay=2, max_retry_delay=0, max_retries=10)
    assert mock_async_sleep.get_call_count() == 10
    assert mock_async_sleep.get_total_delay() == 20.0

    mock_async_sleep.reset()

    with pytest.raises(asyncio.CancelledError):
        await redis.execute_with_retries(mock_work_cancelled, retry_delay=2, max_retry_delay=0, max_retries=10)
    assert mock_async_sleep.get_call_count() == 0
    assert mock_async_sleep.get_total_delay() == 0.0

    mock_async_sleep.reset()

    with pytest.raises(aioredis.errors.ConnectionForcedCloseError):
        await redis.execute_with_retries(mock_work_forceclosed, retry_delay=2, max_retry_delay=0, max_retries=10,
                                         suppress_force_closed=False)
    assert mock_async_sleep.get_call_count() == 0
    assert mock_async_sleep.get_total_delay() == 0.0

    mock_async_sleep.reset()

    await redis.execute_with_retries(mock_work_forceclosed, retry_delay=2, max_retry_delay=0, max_retries=1,
                                     suppress_force_closed=True)
    assert mock_async_sleep.get_call_count() == 0
    assert mock_async_sleep.get_total_delay() == 0.0
