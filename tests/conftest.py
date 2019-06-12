import os
import secrets

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
    etcd = AsyncEtcd(addr=etcd_addr, namespace=test_ns, scope_prefixes={
        ConfigScopes.GLOBAL: 'global',
        ConfigScopes.SGROUP: 'sgroup/testing',
        ConfigScopes.NODE: 'node/i-test',
    })
    try:
        await etcd.delete_prefix('', ConfigScopes.GLOBAL)
        await etcd.delete_prefix('', ConfigScopes.SGROUP)
        await etcd.delete_prefix('', ConfigScopes.NODE)
        yield etcd
    finally:
        await etcd.delete_prefix('', ConfigScopes.GLOBAL)
        await etcd.delete_prefix('', ConfigScopes.SGROUP)
        await etcd.delete_prefix('', ConfigScopes.NODE)
        del etcd
