import asyncio
from pathlib import Path
from typing import (
    Sequence,
    AsyncIterator,
)

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
async def redis_cluster(test_ns) -> AsyncIterator[None]:
    cfg_dir = Path(__file__).parent / 'redis'
    await simple_run_cmd([
        'docker-compose',
        '-p', test_ns,
        '-f', str(cfg_dir / 'redis-cluster.yml'),
        'up', '-d',
    ])
    await asyncio.sleep(0.2)
    try:
        yield
    finally:
        await simple_run_cmd([
            'docker-compose',
            '-p', test_ns,
            '-f', str(cfg_dir / 'redis-cluster.yml'),
            'down',
        ])


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
async def test_pubsub_cluster(redis_cluster):
    pass


@pytest.mark.asyncio
async def test_blist_cluster(redis_cluster):
    pass
