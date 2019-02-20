'''
An asynchronous client wrapper for etcd v3 API.

This is a working version with aioetcd3, but still watcher clean up is not
supported by the library yet.
'''

import asyncio
import collections
import logging
from typing import Any, Mapping

from aioetcd3.client import client
from aioetcd3.help import range_prefix
from aioetcd3.kv import KV
from aioetcd3.transaction import Value
from aioetcd3.watch import EVENT_TYPE_CREATE, EVENT_TYPE_DELETE, EVENT_TYPE_MODIFY

Event = collections.namedtuple('Event', 'key event value')
log = logging.getLogger(__name__)

event_map = {
    EVENT_TYPE_CREATE: 'put',  # for compatibility
    EVENT_TYPE_DELETE: 'delete',
    EVENT_TYPE_MODIFY: 'put',
}


class AsyncEtcd:

    def __init__(self, addr, namespace, *, credentials=None, encoding='utf8', loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.etcd = client(str(addr))
        # TODO: apply credentials if given
        #       (2019.02: aioetcd3 seems not have implemented user/password auth)
        self.ns = namespace
        log.info(f'using etcd cluster from {addr} with namespace "{namespace}"')
        self.encoding = encoding

    def _mangle_key(self, k):
        if k.startswith('/'):
            k = k[1:]
        return f'/sorna/{self.ns}/{k}'.encode(self.encoding)

    def _demangle_key(self, k):
        if isinstance(k, bytes):
            k = k.decode(self.encoding)
        prefix = f'/sorna/{self.ns}/'
        if k.startswith(prefix):
            k = k[len(prefix):]
        return k

    async def put(self, key, val):
        key = self._mangle_key(key)
        await self.etcd.put(key, str(val).encode(self.encoding))

    async def put_multi(self, keys, values):
        success, responses = await self.etcd.txn(
            compare=[],
            success=[KV.put.txn(self._mangle_key(k),
                                str(v).encode(self.encoding))
                     for k, v in zip(keys, values)],
            fail=[])
        return success

    async def put_dict(self, dict_obj: Mapping[str, Any]):
        success, responses = await self.etcd.txn(
            compare=[],
            success=[KV.put.txn(self._mangle_key(k),
                                str(v).encode(self.encoding))
                     for k, v in dict_obj.items()],
            fail=[])
        return success

    async def get(self, key):
        key = self._mangle_key(key)
        val, metadata = await self.etcd.get(key)
        if val is None:
            return None
        return val.decode(self.encoding)

    async def get_prefix(self, key_prefix):
        key_prefix = self._mangle_key(key_prefix)
        results = await self.etcd.range(range_prefix(key_prefix))
        # results is a list of tuples (key, val, kvmeta)
        return ((self._demangle_key(t[0]),
                 t[1].decode(self.encoding))
                for t in results)

    async def get_prefix_dict(self, key_prefix, path_sep='/'):
        raise NotImplementedError

    async def replace(self, key, initial_val, new_val):
        key = self._mangle_key(key)
        success, _ = await self.etcd.txn(
            [Value(key) == initial_val],
            [self.etcd.put.txn(key, new_val)],
            [],
        )
        return success

    async def delete(self, key):
        key = self._mangle_key(key)
        await self.etcd.delete(key)

    async def delete_multi(self, keys):
        is_success, responses = await self.etcd.txn(
            compare=[],
            success=[KV.delete.txn(self._mangle_key(k))
                     for k in keys],
            fail=[])
        return is_success

    async def delete_prefix(self, key_prefix):
        key_prefix = self._mangle_key(key_prefix)
        await self.etcd.delete(range_prefix(key_prefix))

    # NOTE: aioetcd3's watch API is not finalized yet as of 2017 October.

    async def watch(self, key, *, once=False, ready_event=None):
        key = self._mangle_key(key)
        try:
            if ready_event is not None:
                ready_event.set()
            async for event in self.etcd.watch(key, prev_kv=False):
                yield Event(
                    self._demangle_key(event.key),
                    event_map[event.type],
                    event.value.decode(self.encoding))
                if once:
                    break
        finally:
            # TODO: deregister/clean-up watcher
            pass

    async def watch_prefix(self, key_prefix, *, once=False, ready_event=None):
        key_prefix = range_prefix(self._mangle_key(key_prefix))
        try:
            async with self.etcd.watch_scope(key_prefix, prev_kv=False) as resp:
                if ready_event is not None:
                    ready_event.set()
                async for event in resp:
                    yield Event(
                        self._demangle_key(event.key),
                        event_map[event.type],
                        event.value.decode(self.encoding))
                    if once:
                        break
        finally:
            # TODO: deregister/clean-up watcher
            pass
