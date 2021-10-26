from __future__ import annotations

import asyncio
import sys
from typing import (
    AsyncIterator,
)
from aioredis import sentinel

import pytest

from .types import RedisClusterInfo
from .docker import DockerComposeRedisSentinelCluster
from .native import NativeRedisSentinelCluster
from .utils import simple_run_cmd, wait_redis_ready


@pytest.fixture
async def redis_container(test_ns, test_case_ns) -> AsyncIterator[str]:
    p = await asyncio.create_subprocess_exec(*[
        'docker', 'run',
        '-d',
        '--name', f'bai-common.{test_ns}.{test_case_ns}',
        '-p', '9379:6379',
        'redis:6-alpine',
    ], stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    assert p.stdout is not None
    stdout = await p.stdout.read()
    await p.wait()
    cid = stdout.decode().strip()
    try:
        yield cid
    finally:
        await asyncio.sleep(0.2)
        await simple_run_cmd(['docker', 'rm', '-f', cid])
        await asyncio.sleep(0.2)


@pytest.fixture
async def redis_cluster(test_ns, test_case_ns) -> AsyncIterator[RedisClusterInfo]:
    if sys.platform.startswith("darwin"):
        impl = NativeRedisSentinelCluster
    else:
        impl = DockerComposeRedisSentinelCluster
    cluster = impl(test_ns, test_case_ns, password="develove", service_name="mymaster")
    async with cluster.make_cluster() as info:
        node_wait_tasks = [
            wait_redis_ready(host, port, "develove")
            for host, port in info.node_addrs
        ]
        sentinel_wait_tasks = [
            wait_redis_ready(host, port, None)
            for host, port in info.sentinel_addrs
        ]
        await asyncio.gather(*node_wait_tasks, *sentinel_wait_tasks)
        yield info
