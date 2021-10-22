from __future__ import annotations

import asyncio

import aioredis
import aioredis.client
import aioredis.exceptions
import aioredis.sentinel
import aiotools
import pytest

from .types import RedisClusterInfo
from .utils import simple_run_cmd, interrupt, with_timeout


@pytest.mark.asyncio
async def test_connect(redis_container: str) -> None:
    r = aioredis.from_url(url='redis://localhost:9379')
    await r.ping()


@pytest.mark.asyncio
@with_timeout(30.0)
async def test_connect_cluster_sentinel(redis_cluster: RedisClusterInfo) -> None:
    do_pause = asyncio.Event()
    paused = asyncio.Event()
    do_unpause = asyncio.Event()
    unpaused = asyncio.Event()

    async def control_interrupt() -> None:
        await asyncio.sleep(1)
        do_pause.set()
        await paused.wait()
        await asyncio.sleep(2)
        do_unpause.set()
        await unpaused.wait()

    s = aioredis.sentinel.Sentinel(
        redis_cluster.sentinel_addrs,
        password='develove',
        socket_timeout=0.5,
    )
    async with aiotools.TaskGroup() as tg:
        tg.create_task(control_interrupt())
        tg.create_task(interrupt(
            'stop',
            redis_cluster.worker_containers[0],
            redis_cluster.worker_addrs[0],
            do_pause=do_pause,
            do_unpause=do_unpause,
            paused=paused,
            unpaused=unpaused,
            redis_password='develove',
        ))
        await asyncio.sleep(0)
        await simple_run_cmd(["docker", "ps"])

        for _ in range(5):
            print(f"CONNECT REPEAT {_}")
            try:
                master_addr = await s.discover_master('mymaster')
                print("MASTER", master_addr)
            except aioredis.sentinel.MasterNotFoundError:
                print("MASTER (not found)")
            try:
                slave_addrs = await s.discover_slaves('mymaster')
                print("SLAVE", slave_addrs)
                slave = s.slave_for('mymaster', db=9)
                await slave.ping()
            except aioredis.sentinel.SlaveNotFoundError:
                print("SLAVE (not found)")
            await asyncio.sleep(1)
