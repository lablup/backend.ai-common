import asyncio
from pathlib import Path
from typing import (
    AsyncIterator,
    Sequence,
    Tuple,
)

import aioredis
import aioredis.sentinel
import pytest

from ai.backend.common import redis


async def simple_run_cmd(cmdargs: Sequence[str], **kwargs) -> asyncio.subprocess.Process:
    p = await asyncio.create_subprocess_exec(*cmdargs, **kwargs)
    await p.wait()
    return p


@pytest.fixture
async def redis_container(test_ns) -> AsyncIterator[str]:
    p = await asyncio.create_subprocess_exec(*[
        'docker', 'run',
        '-d',
        '--name', f'bai-common.{test_ns}',
        '-p', '9379:6379',
        'redis:6-alpine',
    ], stdout=asyncio.subprocess.PIPE)
    assert p.stdout is not None
    stdout = await p.stdout.read()
    await p.wait()
    cid = stdout.decode().strip()
    try:
        yield cid
    finally:
        await simple_run_cmd(['docker', 'rm', '-f', cid])


@pytest.fixture
async def redis_cluster(test_ns) -> AsyncIterator[Sequence[Tuple[str, int]]]:
    cfg_dir = Path(__file__).parent / 'redis'
    await simple_run_cmd([
        'docker-compose',
        '-p', test_ns,
        '-f', str(cfg_dir / 'redis-cluster.yml'),
        'up', '-d',
    ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    print()
    await asyncio.sleep(0.2)
    await simple_run_cmd([
        'docker-compose',
        '-p', test_ns,
        '-f', str(cfg_dir / 'redis-cluster.yml'),
        'ps',
    ])
    await simple_run_cmd(['docker', 'logs', f'{test_ns}_backendai-half-redis-sentinel01_1'])
    print()
    try:
        yield [
            ('127.0.0.1', 26379),
            ('127.0.0.1', 26380),
            ('127.0.0.1', 26381),
        ]
    finally:
        await simple_run_cmd([
            'docker-compose',
            '-p', test_ns,
            '-f', str(cfg_dir / 'redis-cluster.yml'),
            'down',
        ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)


@pytest.mark.asyncio
async def test_connect(redis_container):
    r = await redis.connect_with_retries(url='redis://localhost:9379')
    await r.ping()


@pytest.mark.asyncio
async def test_pubsub(redis_container):
    pass


@pytest.mark.asyncio
async def test_blist(redis_container):
    pass


@pytest.mark.asyncio
async def test_connect_cluster_haproxy(redis_cluster):
    try:
        await redis.connect_with_retries(
            url='redis://localhost:9379',
            password='develove',
        )
    except aioredis.exceptions.AuthenticationError:
        pass
    r = await redis.connect_with_retries(
        url='redis://localhost:9379',
        password='develove',
    )
    await r.ping()


# @pytest.mark.asyncio
# async def test_pubsub_cluster_haproxy(redis_cluster):
#     pass
#
#
# @pytest.mark.asyncio
# async def test_blist_cluster_haproxy(redis_cluster):
#     pass


@pytest.mark.asyncio
async def test_connect_cluster_sentinel(redis_cluster):
    s = aioredis.sentinel.Sentinel(
        redis_cluster,
        password='develove',
        socket_timeout=0.5,
    )
    master = s.master_for('mymaster', db=9)
    await master.ping()
    slave = s.slave_for('mymaster', db=9)
    await slave.ping()


# @pytest.mark.asyncio
# async def test_pubsub_cluster_sentinel(redis_cluster):
#     pass
#
#
# @pytest.mark.asyncio
# async def test_blist_cluster_sentinel(redis_cluster):
#     pass
