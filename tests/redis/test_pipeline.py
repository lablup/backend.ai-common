from __future__ import annotations

from typing import cast
from unittest import mock

import aioredis
import aioredis.client
import aioredis.sentinel
import pytest

from ai.backend.common.redis import execute
from ai.backend.common.types import RedisConnectionInfo

from .types import RedisClusterInfo


@pytest.mark.asyncio
async def test_pipeline_single_instance(redis_container: str) -> None:
    rconn = RedisConnectionInfo(
        aioredis.from_url(url='redis://localhost:9379', socket_timeout=0.5),
        service_name=None,
    )

    def _build_pipeline(r: aioredis.Redis) -> aioredis.client.Pipeline:
        pipe = r.pipeline(transaction=False)
        pipe.set("xyz", "123")
        pipe.incr("xyz")
        return pipe

    results = await execute(rconn, _build_pipeline)
    assert results[0] is True
    assert str(results[1]) == "124"

    actual_value = await execute(rconn, lambda r: r.get("xyz"))
    assert actual_value == b"124"

    async def _build_pipeline_async(r: aioredis.Redis) -> aioredis.client.Pipeline:
        pipe = r.pipeline(transaction=False)
        pipe.set("abc", "123")
        pipe.incr("abc")
        return pipe

    results = await execute(rconn, _build_pipeline_async)
    assert results[0] is True
    assert str(results[1]) == "124"

    actual_value = await execute(rconn, lambda r: r.get("abc"))
    assert actual_value == b"124"


@pytest.mark.asyncio
async def test_pipeline_single_instance_retries(redis_container: str) -> None:
    rconn = RedisConnectionInfo(
        aioredis.from_url(url='redis://localhost:9379', socket_timeout=0.5),
        service_name=None,
    )

    build_count = 0

    class FailingPipeline(mock.MagicMock):

        def _set_pipeline(self, pipeline):
            self._pipeline = pipeline
            self._fail_count = 0

        async def execute(self):
            self._fail_count += 1
            if self._fail_count == 3:
                return await self._pipeline.execute()
            else:
                raise ConnectionResetError

    class MockedPipeline(aioredis.client.Pipeline):

        def __new__(cls, *args, **kwargs):
            # A trick to replace implementation without changing the subclassing hierarchy.
            return FailingPipeline(spec=cls)

    def _build_pipeline(r: aioredis.Redis) -> aioredis.client.Pipeline:
        nonlocal build_count
        build_count += 1
        pipe = r.pipeline(transaction=False)
        pipe.set("xyz", "123")
        pipe.incr("xyz")
        mpipe = MockedPipeline()
        mpipe._set_pipeline(pipe)  # type: ignore
        return mpipe

    results = await execute(rconn, _build_pipeline, reconnect_poll_interval=0.01)
    assert build_count == 1
    assert results[0] is True
    assert str(results[1]) == "124"

    actual_value = await execute(rconn, lambda r: r.get("xyz"))
    assert actual_value == b"124"

    build_count = 0

    async def _build_pipeline_async(r: aioredis.Redis) -> aioredis.client.Pipeline:
        nonlocal build_count
        build_count += 1
        pipe = r.pipeline(transaction=False)
        pipe.set("abc", "123")
        pipe.incr("abc")
        mpipe = MockedPipeline()
        mpipe._set_pipeline(pipe)  # type: ignore
        return mpipe

    results = await execute(rconn, _build_pipeline_async, reconnect_poll_interval=0.01)
    assert build_count == 1
    assert results[0] is True
    assert str(results[1]) == "124"

    actual_value = await execute(rconn, lambda r: r.get("abc"))
    assert actual_value == b"124"


@pytest.mark.asyncio
async def test_pipeline_sentinel_cluster(redis_cluster: RedisClusterInfo) -> None:
    rconn = RedisConnectionInfo(
        aioredis.sentinel.Sentinel(
            redis_cluster.sentinel_addrs,
            password='develove',
            socket_timeout=0.5,
        ),
        service_name='mymaster',
    )

    def _build_pipeline(r: aioredis.Redis) -> aioredis.client.Pipeline:
        pipe = r.pipeline(transaction=False)
        pipe.set("xyz", "123")
        pipe.incr("xyz")
        return pipe

    results = await execute(rconn, _build_pipeline)
    assert results[0] is True
    assert str(results[1]) == "124"
