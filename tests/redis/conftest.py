from __future__ import annotations

import asyncio
import json
from pathlib import Path
import re
import shutil
import sys
from typing import (
    AsyncIterator,
)

import pytest

from .types import RedisClusterInfo
from .utils import simple_run_cmd


@pytest.fixture
async def redis_container(test_ns) -> AsyncIterator[str]:
    p = await asyncio.create_subprocess_exec(*[
        'docker', 'run',
        '-d',
        '--name', f'bai-common.{test_ns}',
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
        await simple_run_cmd(['docker', 'rm', '-f', cid])


r"""
@pytest.fixture
async def redis_cluster() -> AsyncIterator[RedisClusterInfo]:
    yield RedisClusterInfo(
        worker_addrs=[
            ('127.0.0.1', 16379),
            ('127.0.0.1', 16380),
            ('127.0.0.1', 16381),
        ],
        worker_containers=[
            'testing_backendai-half-redis-node01_1',
            'testing_backendai-half-redis-node02_1',
            'testing_backendai-half-redis-node03_1',
        ],
        sentinel_addrs=[
            ('127.0.0.1', 26379),
            ('127.0.0.1', 26380),
            ('127.0.0.1', 26381),
        ],
        sentinel_containers=[
            'testing_backendai-half-redis-sentinel01_1',
            'testing_backendai-half-redis-sentinel02_1',
            'testing_backendai-half-redis-sentinel03_1',
        ],
    )
"""


@pytest.fixture
async def redis_cluster(test_ns) -> AsyncIterator[RedisClusterInfo]:
    cfg_dir = Path(__file__).parent
    if sys.platform.startswith('darwin'):
        # docker for mac
        pass
    else:
        compose_cfg = cfg_dir / 'redis-cluster.yml'
        shutil.copy(compose_cfg, compose_cfg.with_name(f'{compose_cfg.name}.bak'))
        t = compose_cfg.read_bytes()
        t = t.replace(b'host.docker.internal', b'127.0.0.1')
        t = re.sub(br'ports:\n      - \d+:\d+', b'network_mode: host', t, flags=re.M)
        compose_cfg.write_bytes(t)
    await simple_run_cmd([
        'docker', 'compose',
        '-p', test_ns,
        '-f', str(cfg_dir / 'redis-cluster.yml'),
        'up', '-d',
    ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await asyncio.sleep(0.2)
    try:
        p = await asyncio.create_subprocess_exec(*[
            'docker', 'compose',
            '-p', test_ns,
            '-f', str(cfg_dir / 'redis-cluster.yml'),
            'ps',
            '--format', 'json',
        ], stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        assert p.stdout is not None
        ps_output = json.loads(await p.stdout.read())
        await p.wait()
        workers = {}
        sentinels = {}

        def find_port_node(item):
            if m := re.search(r"--port (\d+) ", item['Command']):
                return int(m.group(1))
            return None

        def find_port_sentinel(item):
            if m := re.search(r"redis-sentinel(\d+)_", item['Name']):
                return 26379 + (int(m.group(1)) - 1)
            return None

        for item in ps_output:
            if 'redis-node' in item['Name']:
                port = find_port_node(item)
                workers[port] = item['ID']
            elif 'redis-sentinel' in item['Name']:
                port = find_port_sentinel(item)
                sentinels[port] = item['ID']

        yield RedisClusterInfo(
            worker_addrs=[
                ('127.0.0.1', 16379),
                ('127.0.0.1', 16380),
                ('127.0.0.1', 16381),
            ],
            worker_containers=[
                workers[16379],
                workers[16380],
                workers[16381],
            ],
            sentinel_addrs=[
                ('127.0.0.1', 26379),
                ('127.0.0.1', 26380),
                ('127.0.0.1', 26381),
            ],
            sentinel_containers=[
                sentinels[26379],
                sentinels[26380],
                sentinels[26381],
            ],
        )
    finally:
        await simple_run_cmd([
            'docker', 'compose',
            '-p', test_ns,
            '-f', str(cfg_dir / 'redis-cluster.yml'),
            'down',
        ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        if sys.platform.startswith('darwin'):
            # docker for mac
            pass
        else:
            shutil.copy(compose_cfg.with_name(f'{compose_cfg.name}.bak'), compose_cfg)
