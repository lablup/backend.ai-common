import asyncio
from decimal import Decimal
import os
import secrets
import time
from typing import (
    AsyncIterator,
)

from etcetra.types import HostPortPair

from ai.backend.common.etcd import AsyncEtcd, ConfigScopes

import pytest

from .redis.utils import simple_run_cmd, wait_redis_ready


@pytest.fixture
def etcd_addr():
    env_addr = os.environ.get('BACKEND_ETCD_ADDR')
    if env_addr is not None:
        return HostPortPair.parse(env_addr)
    return HostPortPair.parse('localhost:2379')


@pytest.fixture(scope="session", autouse=True)
def event_loop():
    # uvloop.install()
    loop = asyncio.new_event_loop()
    # setup_child_watcher()
    yield loop
    loop.close()


@pytest.fixture(scope="session", autouse=True)
def test_ns():
    return f'test-{secrets.token_hex(8)}'


@pytest.fixture
def test_case_ns():
    return secrets.token_hex(8)


@pytest.fixture
async def etcd(etcd_addr, test_ns):
    etcd = AsyncEtcd(addr=etcd_addr, namespace=test_ns, scope_prefix_map={
        ConfigScopes.GLOBAL: 'global',
        ConfigScopes.SGROUP: 'sgroup/testing',
        ConfigScopes.NODE: 'node/i-test',
    })
    try:
        await etcd.delete_prefix('', scope=ConfigScopes.GLOBAL)
        await etcd.delete_prefix('', scope=ConfigScopes.SGROUP)
        await etcd.delete_prefix('', scope=ConfigScopes.NODE)
        yield etcd
    finally:
        await etcd.delete_prefix('', scope=ConfigScopes.GLOBAL)
        await etcd.delete_prefix('', scope=ConfigScopes.SGROUP)
        await etcd.delete_prefix('', scope=ConfigScopes.NODE)
        await etcd.close()
        del etcd


@pytest.fixture
async def gateway_etcd(etcd_addr, test_ns):
    etcd = AsyncEtcd(addr=etcd_addr, namespace=test_ns, scope_prefix_map={
        ConfigScopes.GLOBAL: '',
    })
    try:
        await etcd.delete_prefix('', scope=ConfigScopes.GLOBAL)
        yield etcd
    finally:
        await etcd.delete_prefix('', scope=ConfigScopes.GLOBAL)
        del etcd


@pytest.fixture
async def chaos_generator():

    async def _chaos():
        try:
            while True:
                await asyncio.sleep(0.001)
        except asyncio.CancelledError:
            return

    tasks = []
    for i in range(20):
        tasks.append(asyncio.create_task(_chaos()))
    yield
    for i in range(20):
        tasks[i].cancel()
    await asyncio.gather(*tasks, return_exceptions=True)


@pytest.fixture
def mock_time(mocker):
    total_delay = Decimal(0)
    call_count = 0
    base_time = time.monotonic()
    accum_time = Decimal(0)
    q = Decimal('.000000')

    async def _mock_async_sleep(delay: float) -> None:
        nonlocal total_delay, call_count, accum_time, q
        call_count += 1
        quantized_delay = Decimal(delay).quantize(q)
        accum_time += quantized_delay
        total_delay += quantized_delay

    def _reset() -> None:
        nonlocal total_delay, call_count
        total_delay = Decimal(0)
        call_count = 0

    def _get_total_delay() -> float:
        nonlocal total_delay
        return float(total_delay)

    def _get_call_count() -> int:
        nonlocal call_count
        return call_count

    def _mock_time_monotonic() -> float:
        nonlocal accum_time
        return base_time + float(accum_time)

    _mock_async_sleep.reset = _reset
    _mock_async_sleep.get_total_delay = _get_total_delay
    _mock_async_sleep.get_call_count = _get_call_count
    yield _mock_async_sleep, _mock_time_monotonic


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
    await wait_redis_ready('127.0.0.1', 9379)
    try:
        yield cid
    finally:
        await asyncio.sleep(0.2)
        await simple_run_cmd(['docker', 'rm', '-f', cid])
        await asyncio.sleep(0.2)
