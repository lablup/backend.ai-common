import asyncio
try:
    from unittest.mock import AsyncMock  # type: ignore
except ImportError:
    from asynctest import CoroutineMock as AsyncMock  # type: ignore

import aioredis
import pytest

from ai.backend.common import redis


@pytest.mark.asyncio
async def test_connect_with_retries(mocker, mock_time):
    mock_async_sleep, mock_time_monotonic = mock_time
    mock_create_pool = AsyncMock(side_effect=ConnectionRefusedError)
    mocker.patch('ai.backend.common.redis.aioredis.create_redis_pool', mock_create_pool)
    mocker.patch('ai.backend.common.redis.asyncio.sleep', mock_async_sleep)
    mocker.patch('ai.backend.common.redis.time.monotonic', mock_time_monotonic)
    with pytest.raises(asyncio.TimeoutError):
        await redis.connect_with_retries(retry_delay=0.5, retry_timeout=60.0)
    assert mock_async_sleep.get_call_count() in (7, 8)
    assert 61.5 <= mock_async_sleep.get_total_delay() <= 92.0


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
        await redis.execute_with_retries(mock_work_connreset, retry_delay=0.5, retry_timeout=60.0)
    # 0.5 + 1.0 + 2.0 + 4.0 + 8.0 + 16.0 + 30.0 (+ 30.0) == 61.5 or 91.5
    assert 61.0 <= mock_async_sleep.get_total_delay() <= 92.0

    mock_async_sleep.reset()

    with pytest.raises(asyncio.TimeoutError):
        await redis.execute_with_retries(mock_work_connreset, retry_delay=0.5, retry_timeout=60.0, exponential_backoff=False)
    assert 59.9 <= mock_async_sleep.get_total_delay() <= 60.9

    mock_async_sleep.reset()

    with pytest.raises(asyncio.TimeoutError):
        await redis.execute_with_retries(mock_work_connreset, retry_delay=2, retry_timeout=0, max_retries=1)
    assert mock_async_sleep.get_total_delay() == 2.0

    mock_async_sleep.reset()

    with pytest.raises(asyncio.TimeoutError):
        await redis.execute_with_retries(mock_work_connreset, retry_delay=2, retry_timeout=0, max_retries=10)
    # 2 + 4 + 6 + 8 + 16 + 30 + 30 + 30 + 30 + 30 (+ 30) == 186 or 216
    assert 180.0 <= mock_async_sleep.get_total_delay() <= 220.0

    mock_async_sleep.reset()

    with pytest.raises(asyncio.CancelledError):
        await redis.execute_with_retries(mock_work_cancelled, retry_delay=2, retry_timeout=0, max_retries=10)
    assert mock_async_sleep.get_total_delay() == 0.0

    mock_async_sleep.reset()

    with pytest.raises(aioredis.errors.ConnectionForcedCloseError):
        await redis.execute_with_retries(mock_work_forceclosed,
                                         retry_delay=2, retry_timeout=0, max_retries=10,
                                         suppress_force_closed=False)
    assert mock_async_sleep.get_total_delay() == 0.0

    mock_async_sleep.reset()

    await redis.execute_with_retries(mock_work_forceclosed,
                                     retry_delay=2, retry_timeout=0, max_retries=1,
                                     suppress_force_closed=True)
    assert mock_async_sleep.get_total_delay() == 0.0
