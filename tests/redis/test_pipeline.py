from __future__ import annotations

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
