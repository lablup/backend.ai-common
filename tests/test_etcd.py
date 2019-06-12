import asyncio

import pytest

from ai.backend.common.etcd import ConfigScopes


@pytest.mark.asyncio
async def test_basic_crud(etcd):

    await etcd.put('wow', 'abc', ConfigScopes.GLOBAL)

    v = await etcd.get('wow')
    assert v == 'abc'
    v = await etcd.get_prefix('wow')
    assert len(v) == 1
    assert v == {'': 'abc'}

    r = await etcd.replace('wow', 'aaa', 'ccc', ConfigScopes.GLOBAL)
    assert r is False
    r = await etcd.replace('wow', 'abc', 'def', ConfigScopes.GLOBAL)
    assert r is True
    v = await etcd.get('wow')
    assert v == 'def'

    await etcd.delete('wow', ConfigScopes.GLOBAL)

    v = await etcd.get('wow')
    assert v is None
    v = await etcd.get_prefix('wow')
    assert len(v) == 0


@pytest.mark.asyncio
async def test_scope(etcd):
    await etcd.put('wow', 'abc', ConfigScopes.GLOBAL)
    await etcd.put('wow', 'def', ConfigScopes.SGROUP)
    await etcd.put('wow', 'ghi', ConfigScopes.NODE)
    v = await etcd.get('wow')
    assert v == 'ghi'

    await etcd.delete('wow', ConfigScopes.NODE)
    v = await etcd.get('wow')
    assert v == 'def'

    await etcd.delete('wow', ConfigScopes.SGROUP)
    v = await etcd.get('wow')
    assert v == 'abc'

    await etcd.delete('wow', ConfigScopes.GLOBAL)
    v = await etcd.get('wow')
    assert v is None

    await etcd.put('wow', '000', ConfigScopes.NODE)
    v = await etcd.get('wow')
    assert v == '000'


@pytest.mark.asyncio
async def test_scope_dict(etcd):
    await etcd.put_dict({'point/x': '1', 'point/y': '2'}, ConfigScopes.GLOBAL)
    await etcd.put_dict({'point/y': '3'}, ConfigScopes.SGROUP)
    await etcd.put_dict({'point/x': '4'}, ConfigScopes.NODE)
    v = await etcd.get_prefix('point')
    assert v == {'x': '4', 'y': '3'}

    await etcd.delete_prefix('point', ConfigScopes.NODE)
    v = await etcd.get_prefix('point')
    assert v == {'x': '1', 'y': '3'}

    await etcd.delete_prefix('point', ConfigScopes.SGROUP)
    v = await etcd.get_prefix('point')
    assert v == {'x': '1', 'y': '2'}

    await etcd.delete_prefix('point', ConfigScopes.GLOBAL)
    v = await etcd.get_prefix('point')
    assert len(v) == 0


@pytest.mark.asyncio
async def test_multi(etcd):

    v = await etcd.get('foo')
    assert v is None
    v = await etcd.get('bar')
    assert v is None

    await etcd.put_dict({'foo': 'x', 'bar': 'y'}, ConfigScopes.GLOBAL)
    v = await etcd.get('foo')
    assert v == 'x'
    v = await etcd.get('bar')
    assert v == 'y'

    await etcd.delete_multi(['foo', 'bar'], ConfigScopes.GLOBAL)
    v = await etcd.get('foo')
    assert v is None
    v = await etcd.get('bar')
    assert v is None


@pytest.mark.asyncio
async def test_watch(etcd, event_loop):

    records = []
    records_prefix = []
    r_ready = asyncio.Event()
    rp_ready = asyncio.Event()

    async def _record():
        try:
            async for ev in etcd.watch('wow', ConfigScopes.GLOBAL, ready_event=r_ready):
                records.append(ev)
        except asyncio.CancelledError:
            pass

    async def _record_prefix():
        try:
            async for ev in etcd.watch_prefix('wow', ConfigScopes.GLOBAL, ready_event=rp_ready):
                records_prefix.append(ev)
        except asyncio.CancelledError:
            pass

    t1 = event_loop.create_task(_record())
    t2 = event_loop.create_task(_record_prefix())

    await r_ready.wait()
    await rp_ready.wait()

    await etcd.put('wow', '123', ConfigScopes.GLOBAL)
    await etcd.delete('wow', ConfigScopes.GLOBAL)
    await etcd.put('wow/child', 'hello', ConfigScopes.GLOBAL)
    await etcd.delete_prefix('wow', ConfigScopes.GLOBAL)

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
    assert records_prefix[1].key == 'wow'
    assert records_prefix[1].event == 'delete'
    assert records_prefix[1].value == ''
    assert records_prefix[2].key == 'wow/child'
    assert records_prefix[2].event == 'put'
    assert records_prefix[2].value == 'hello'
    assert records_prefix[3].key == 'wow/child'
    assert records_prefix[3].event == 'delete'
    assert records_prefix[3].value == ''


@pytest.mark.asyncio
async def test_watch_once(etcd, event_loop):

    records = []
    records_prefix = []
    r_ready = asyncio.Event()
    rp_ready = asyncio.Event()

    async def _record():
        try:
            async for ev in etcd.watch('wow', ConfigScopes.GLOBAL, once=True, ready_event=r_ready):
                records.append(ev)
        except asyncio.CancelledError:
            pass

    async def _record_prefix():
        try:
            async for ev in etcd.watch_prefix('wow/city', ConfigScopes.GLOBAL, once=True, ready_event=rp_ready):
                records_prefix.append(ev)
        except asyncio.CancelledError:
            pass

    t1 = event_loop.create_task(_record())
    t2 = event_loop.create_task(_record_prefix())
    await r_ready.wait()
    await rp_ready.wait()

    await etcd.put('wow/city1', 'seoul', ConfigScopes.GLOBAL)
    await etcd.put('wow/city2', 'daejeon', ConfigScopes.GLOBAL)
    await etcd.put('wow', 'korea', ConfigScopes.GLOBAL)
    await etcd.delete_prefix('wow', ConfigScopes.GLOBAL)

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
