import asyncio

import pytest

from sorna.etcd import AsyncEtcd, Event
from sorna.argparse import host_port_pair


@pytest.mark.asyncio
async def test_basic_crud():

    etcd = AsyncEtcd(addr=host_port_pair('localhost:2379'), namespace='local')

    await etcd.put('wow', 'abc')

    v = await etcd.get('wow')
    assert v == 'abc'
    v = list(await etcd.get_prefix('wow'))
    assert len(v) == 1
    assert v[0] == 'abc'

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
async def test_watch(event_loop):

    etcd = AsyncEtcd(addr=host_port_pair('localhost:2379'), namespace='local')

    records = []
    records_prefix = []
    async def _record():
        async for ev in etcd.watch('wow'):
            records.append(ev)
    async def _record_prefix():
        async for ev in etcd.watch_prefix('wow'):
            records_prefix.append(ev)

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
async def test_watch_once(event_loop):

    etcd = AsyncEtcd(addr=host_port_pair('localhost:2379'), namespace='test')

    records = []
    records_prefix = []
    async def _record():
        async for ev in etcd.watch('wow', once=True):
            records.append(ev)
    async def _record_prefix():
        async for ev in etcd.watch_prefix('wow', once=True):
            records_prefix.append(ev)

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
