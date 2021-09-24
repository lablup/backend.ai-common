from __future__ import annotations

import asyncio
import json
import os
from pathlib import Path
import re
import sys
import tempfile
from typing import (
    AsyncIterator,
)

import async_timeout
import pytest

from .types import RedisClusterInfo
from .utils import simple_run_cmd


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
async def redis_cluster(test_ns, test_case_ns) -> AsyncIterator[RedisClusterInfo]:
    cfg_dir = Path(__file__).parent
    if sys.platform.startswith('darwin'):
        # docker for mac
        modified_compose_cfg = cfg_dir / 'redis-cluster.yml'
    else:
        orig_compose_cfg = cfg_dir / 'redis-cluster.yml'
        modified_compose_cfg = Path(tempfile.gettempdir()) / f'redis-cluster.{test_ns}.{test_case_ns}.yml'
        t = orig_compose_cfg.read_bytes()
        t = t.replace(b'host.docker.internal', b'127.0.0.1')
        t = t.replace(b'context: .', os.fsencode(f'context: {cfg_dir}'))
        t = re.sub(br'ports:\n      - \d+:\d+', b'network_mode: host', t, flags=re.M)
        modified_compose_cfg.write_bytes(t)

    with async_timeout.timeout(30.0):
        p = await simple_run_cmd([
            'docker', 'compose',
            '-p', f"{test_ns}.{test_case_ns}",
            '-f', os.fsencode(modified_compose_cfg),
            'up', '-d', '--build',
        ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        assert p.returncode == 0, "Compose cluster creation has failed."

    await asyncio.sleep(0.2)
    try:
        p = await asyncio.create_subprocess_exec(*[
            'docker', 'compose',
            '-p', f"{test_ns}.{test_case_ns}",
            '-f', os.fsencode(modified_compose_cfg),
            'ps',
            '--format', 'json',
        ], stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        assert p.stdout is not None
        try:
            ps_output = json.loads(await p.stdout.read())
        except json.JSONDecodeError:
            pytest.fail("Cannot parse \"docker compose ... ps --format json\" output. "
                        "You may need to upgrade to docker-compose v2.0.0.rc.3 or later")
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

        if not ps_output:
            pytest.fail("Cannot detect the temporary Redis cluster running as docker compose containers")
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
        await asyncio.sleep(0.2)
        with async_timeout.timeout(30.0):
            await simple_run_cmd([
                'docker', 'compose',
                '-p', f"{test_ns}.{test_case_ns}",
                '-f', os.fsencode(modified_compose_cfg),
                'down',
            ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        await asyncio.sleep(0.2)
