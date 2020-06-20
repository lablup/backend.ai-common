from decimal import Decimal
import os
import secrets
import time

from ai.backend.common.argparse import host_port_pair
from ai.backend.common.etcd import AsyncEtcd, ConfigScopes

import pytest


@pytest.fixture
def etcd_addr():
    env_addr = os.environ.get('BACKEND_ETCD_ADDR')
    if env_addr is not None:
        return host_port_pair(env_addr)
    return host_port_pair('localhost:2379')


@pytest.fixture
def test_ns():
    return f'test-{secrets.token_hex(8)}'


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
