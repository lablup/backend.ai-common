'''
An asynchronous client wrapper for etcd v3 API.

It uses the etcd3 library using a thread pool executor.
We plan to migrate to aioetcd3 library but it requires more work to get maturity.
Fortunately, etcd3's watchers are not blocking because they are implemented
using callbacks in separate threads.
'''

import asyncio
import collections
from concurrent.futures import ThreadPoolExecutor
import functools
import logging

# from aioetcd3.client import client
# from aioetcd3.help import range_prefix
# from aioetcd3.transaction import Value
# from aioetcd3.watch import EVENT_TYPE_CREATE, EVENT_TYPE_DELETE, EVENT_TYPE_MODIFY
import etcd3

Event = collections.namedtuple('Event', 'key event value')
log = logging.getLogger(__name__)

# event_map = {
#     EVENT_TYPE_CREATE: 'put',  # for compatibility
#     EVENT_TYPE_DELETE: 'delete',
#     EVENT_TYPE_MODIFY: 'put',
# }


class AsyncEtcd:

    def __init__(self, addr, namespace, *, encoding='utf8', loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        # self.etcd = client(str(addr))
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix='etcd')
        self.etcd_sync = etcd3.client(host=str(addr.host), port=addr.port)
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
        # await self.etcd.put(key, str(val).encode(self.encoding))
        return await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.put, key, str(val).encode(self.encoding))

    async def put_multi(self, keys, values):
        return await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.transaction,
            [],
            [self.etcd_sync.transactions.put(
                self._mangle_key(k), str(v).encode(self.encoding))
             for k, v in zip(keys, values)],
            [])

    async def get(self, key):
        key = self._mangle_key(key)
        # val, metadata = await self.etcd.get(key)
        # if val is None:
        #     return None
        # return val.decode(self.encoding)
        val, _ = await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.get, key)
        return val.decode(self.encoding) if val is not None else None

    async def get_prefix(self, key_prefix):
        key_prefix = self._mangle_key(key_prefix)
        # results = await self.etcd.range(range_prefix(key_prefix))
        # # results is a list of tuples (key, val, kvmeta)
        # return ((self._demangle_key(t[0]),
        #          t[1].decode(self.encoding))
        #         for t in results)
        results = await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.get_prefix, key_prefix)
        return ((self._demangle_key(t[1].key),
                 t[0].decode(self.encoding))
                for t in results)

    async def replace(self, key, initial_val, new_val):
        key = self._mangle_key(key)
        # success, _ = await self.etcd.txn(
        #     [Value(key) == initial_val],
        #     [self.etcd.put.txn(key, new_val)],
        #     [],
        # )
        success = await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.replace, key, initial_val, new_val)
        return success

    async def delete(self, key):
        key = self._mangle_key(key)
        # await self.etcd.delete(key)
        return await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.delete, key)

    async def delete_multi(self, keys):
        return await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.transaction,
            [],
            [self.etcd_sync.transactions.delete(self._mangle_key(k))
             for k in keys],
            [])

    async def delete_prefix(self, key_prefix):
        key_prefix = self._mangle_key(key_prefix)
        # await self.etcd.delete(range_prefix(key_prefix))
        return await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.delete_prefix, key_prefix)

# NOTE: aioetcd3's watch API is not finalized yet as of 2017 October.

#    def stop_task(self):
#        self.etcd.stop_task()
#
#    async def watch(self, key, once=False):
#        key = self._mangle_key(key)
#        try:
#            async for event, meta in self.etcd.watch(key, prev_kv=True):
#                print(event)
#                yield Event(
#                    self._demangle_key(event.key),
#                    event_map[event.type],
#                    event.value.decode(self.encoding))
#                if once:
#                    break
#        finally:
#            self.etcd.stop_task()
#
#    async def watch_prefix(self, key_prefix, once=False):
#        key_prefix = self._mangle_key(key_prefix)
#        try:
#            async with self.etcd.watch_scope(key_prefix, prev_kv=True) as resp:
#                async for event, meta in resp:
#                    yield Event(
#                        self._demangle_key(event.key),
#                        event_map[event.type],
#                        event.value.decode(self.encoding))
#                    if once:
#                        break
#        finally:
#            self.etcd.stop_task()

    def _watch_cb(self, queue, ev):
        if isinstance(ev, etcd3.events.PutEvent):
            ev_type = 'put'
        elif isinstance(ev, etcd3.events.DeleteEvent):
            ev_type = 'delete'
        else:
            assert False, 'Not recognized etcd event type.'
        # etcd3 library uses a separate thread for its watchers.
        event = Event(
            self._demangle_key(ev.key),
            ev_type,
            ev.value.decode(self.encoding),
        )
        self.loop.call_soon_threadsafe(queue.put_nowait, event)

    async def _watch_impl(self, raw_key, **kwargs):
        queue = asyncio.Queue(loop=self.loop)
        cb = functools.partial(self._watch_cb, queue)
        watch_id = self.etcd_sync.add_watch_callback(raw_key, cb, **kwargs)
        try:
            while True:
                ev = await queue.get()
                yield ev
                queue.task_done()
        except asyncio.CancelledError:
            pass
        finally:
            self.etcd_sync.cancel_watch(watch_id)
            del queue

    async def watch(self, key, once=False):
        key = self._mangle_key(key)
        # yield from in async-generator is not supported.
        async for ev in self._watch_impl(key):
            yield ev
            if once:
                break

    async def watch_prefix(self, key_prefix, once=False):
        key_prefix = self._mangle_key(key_prefix)
        range_end = etcd3.utils.increment_last_byte(etcd3.utils.to_bytes(key_prefix))
        async for ev in self._watch_impl(key_prefix, range_end=range_end):
            yield ev
            if once:
                break
