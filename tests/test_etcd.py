import asyncio
import os

import pytest

from ai.backend.common.etcd import AsyncEtcd
from ai.backend.common.argparse import host_port_pair


@pytest.fixture
def etcd_addr():
    env_addr = os.environ.get('BACKEND_ETCD_ADDR')
    if env_addr is not None:
        return host_port_pair(env_addr)
    return host_port_pair('localhost:2379')


@pytest.mark.asyncio
async def test_basic_crud(etcd_addr):

    etcd = AsyncEtcd(addr=etcd_addr, namespace='local')

    await etcd.put('wow', 'abc')

    v = await etcd.get('wow')
    assert v == 'abc'
    v = list(await etcd.get_prefix('wow'))
    assert len(v) == 1
    assert v[0] == ('wow', 'abc')

    r = await etcd.replace('wow', 'aaa', 'ccc')
    assert r is False
    r = await etcd.replace('wow', 'abc', 'def')
    assert r is True
    v = await etcd.get('wow')
    assert v == 'def'

    await etcd.delete('wow')

    v = await etcd.get('wow')
    assert v is None
    v = list(await etcd.get_prefix('wow'))
    assert len(v) == 0


@pytest.mark.asyncio
async def test_multi(etcd_addr):

    etcd = AsyncEtcd(addr=etcd_addr, namespace='local')
    v = await etcd.get('foo')
    assert v is None
    v = await etcd.get('bar')
    assert v is None

    await etcd.put_multi(['foo', 'bar'], ['x', 'y'])
    v = await etcd.get('foo')
    assert v == 'x'
    v = await etcd.get('bar')
    assert v == 'y'

    await etcd.delete_multi(['foo', 'bar'])
    v = await etcd.get('foo')
    assert v is None
    v = await etcd.get('bar')
    assert v is None


@pytest.mark.asyncio
async def test_watch(etcd_addr, event_loop):

    etcd = AsyncEtcd(addr=etcd_addr, namespace='local')

    records = []
    records_prefix = []

    async def _record():
        try:
            async for ev in etcd.watch('wow'):
                records.append(ev)
        except asyncio.CancelledError:
            pass

    async def _record_prefix():
        try:
            async for ev in etcd.watch_prefix('wow'):
                records_prefix.append(ev)
        except asyncio.CancelledError:
            pass

    t1 = event_loop.create_task(_record())
    t2 = event_loop.create_task(_record_prefix())
    await asyncio.sleep(0)

    await etcd.put('wow', '123')
    await etcd.get('wow')
    await etcd.put('wow/child', 'hello')
    await etcd.delete_prefix('wow')

    await asyncio.sleep(0.2)
    t1.cancel()
    t2.cancel()
    await t1
    await t2

    assert len(records) == 2
    assert records[0].key == 'wow'
    assert records[0].event == 'put'
    assert records[0].value == '123'
    assert records[1].key == 'wow'
    assert records[1].event == 'delete'
    assert records[1].value == ''

    assert len(records_prefix) == 4
    assert records_prefix[0].key == 'wow'
    assert records_prefix[0].event == 'put'
    assert records_prefix[0].value == '123'
    assert records_prefix[1].key == 'wow/child'
    assert records_prefix[1].event == 'put'
    assert records_prefix[1].value == 'hello'
    assert records_prefix[2].key == 'wow'
    assert records_prefix[2].event == 'delete'
    assert records_prefix[2].value == ''
    assert records_prefix[3].key == 'wow/child'
    assert records_prefix[3].event == 'delete'
    assert records_prefix[3].value == ''


@pytest.mark.asyncio
async def test_watch_once(etcd_addr, event_loop):

    etcd = AsyncEtcd(addr=etcd_addr, namespace='test')

    records = []
    records_prefix = []

    async def _record():
        try:
            async for ev in etcd.watch('wow', once=True):
                records.append(ev)
        except asyncio.CancelledError:
            pass

    async def _record_prefix():
        try:
            async for ev in etcd.watch_prefix('wow', once=True):
                records_prefix.append(ev)
        except asyncio.CancelledError:
            pass

    t1 = event_loop.create_task(_record())
    t2 = event_loop.create_task(_record_prefix())
    await asyncio.sleep(0)

    await etcd.put('wow/city1', 'seoul')
    await etcd.put('wow/city2', 'daejeon')
    await etcd.put('wow', 'korea')
    await etcd.delete_prefix('wow')

    await asyncio.sleep(0.2)
    t1.cancel()
    t2.cancel()
    await t1
    await t2

    assert len(records) == 1
    assert records[0].key == 'wow'
    assert records[0].event == 'put'
    assert records[0].value == 'korea'

    assert len(records_prefix) == 1
    assert records_prefix[0].key == 'wow/city1'
    assert records_prefix[0].event == 'put'
    assert records_prefix[0].value == 'seoul'
