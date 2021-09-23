from __future__ import annotations

import asyncio

import aioredis
import aioredis.client
import aioredis.exceptions
import aioredis.sentinel
import pytest

from .types import RedisClusterInfo
from .utils import simple_run_cmd


@pytest.mark.asyncio
async def test_connect(redis_container: str) -> None:
    r = aioredis.from_url(url='redis://localhost:9379')
    await r.ping()


@pytest.mark.asyncio
async def test_connect_cluster_haproxy(redis_cluster: RedisClusterInfo) -> None:
    with pytest.raises(aioredis.exceptions.AuthenticationError):
        r = aioredis.from_url(
            url=f'redis://localhost:{redis_cluster.haproxy_addr[1]}',
            # without password
        )
        await r.ping()
    r = aioredis.from_url(
        url=f'redis://localhost:{redis_cluster.haproxy_addr[1]}',
        password='develove',
    )
    await r.ping()


@pytest.mark.asyncio
async def test_connect_cluster_sentinel(redis_cluster: RedisClusterInfo) -> None:

    async def interrupt() -> None:
        await asyncio.sleep(5)
        await simple_run_cmd(['docker', 'stop', redis_cluster.worker_containers[0]])
        print("STOPPED node01")
        await asyncio.sleep(5)
        await simple_run_cmd(['docker', 'start', redis_cluster.worker_containers[0]])
        print("STARTED node01")

    s = aioredis.sentinel.Sentinel(
        redis_cluster.sentinel_addrs,
        password='develove',
        socket_timeout=0.5,
    )
    interrupt_task = asyncio.create_task(interrupt())
    await asyncio.sleep(0)

    # master_addr = await s.discover_master('mymaster')
    # assert master_addr[1] == 16379
    for _ in range(30):
        # master = s.master_for('mymaster', db=9)
        # await master.ping()
        try:
            master_addr = await s.discover_master('mymaster')
            print("MASTER", master_addr)
        except aioredis.sentinel.MasterNotFoundError:
            print("MASTER (not found)")
        slave_addrs = await s.discover_slaves('mymaster')
        print("SLAVE", slave_addrs)
        slave = s.slave_for('mymaster', db=9)
        await slave.ping()
        await asyncio.sleep(1)

    await interrupt_task
