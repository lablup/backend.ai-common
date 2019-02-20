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
from typing import Any, Mapping
from urllib.parse import quote as _quote, unquote

import etcd3

__all__ = (
    'quote', 'unquote',
    'AsyncEtcd',
)

Event = collections.namedtuple('Event', 'key event value')
log = logging.getLogger(__name__)

quote = functools.partial(_quote, safe='')


def make_dict_from_pairs(key_prefix, pairs, path_sep='/'):
    result = {}
    len_prefix = len(key_prefix)
    if isinstance(pairs, dict):
        iterator = pairs.items()
    else:
        iterator = pairs
    for k, v in iterator:
        if not k.startswith(key_prefix):
            continue
        subkey = k[len_prefix:]
        if subkey.startswith(path_sep):
            subkey = subkey[1:]
        path_components = subkey.split('/')
        parent = result
        for p in path_components[:-1]:
            if p not in parent:
                parent[p] = {}
            if p in parent and not isinstance(parent[p], dict):
                root = parent[p]
                parent[p] = {'': root}
            parent = parent[p]
        parent[path_components[-1]] = v
    return result


class AsyncEtcd:

    def __init__(self, addr, namespace, *, credentials=None, encoding='utf8', loop=None):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix='etcd')
        self.etcd_sync = etcd3.client(
            host=str(addr.host), port=addr.port,
            user=credentials.get('user') if credentials else None,
            password=credentials.get('password') if credentials else None)
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

    async def put_dict(self, dict_obj: Mapping[str, Any]):
        return await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.transaction,
            [],
            [self.etcd_sync.transactions.put(
                self._mangle_key(k), str(v).encode(self.encoding))
             for k, v in dict_obj.items()],
            [])

    async def get(self, key):
        key = self._mangle_key(key)
        val, _ = await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.get, key)
        return val.decode(self.encoding) if val is not None else None

    async def get_prefix(self, key_prefix):
        key_prefix = self._mangle_key(key_prefix)
        results = await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.get_prefix, key_prefix)
        return ((self._demangle_key(t[1].key),
                 t[0].decode(self.encoding))
                for t in results)

    async def get_prefix_dict(self, key_prefix, path_sep='/'):
        pairs = await self.get_prefix(key_prefix)
        return make_dict_from_pairs(key_prefix, pairs, path_sep)

    async def replace(self, key, initial_val, new_val):
        key = self._mangle_key(key)
        success = await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.replace, key, initial_val, new_val)
        return success

    async def delete(self, key):
        key = self._mangle_key(key)
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
        return await self.loop.run_in_executor(
            self.executor,
            self.etcd_sync.delete_prefix, key_prefix)

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

    async def watch(self, key, *, once=False, ready_event=None):
        key = self._mangle_key(key)
        if ready_event:
            ready_event.set()
        # yield from in async-generator is not supported.
        async for ev in self._watch_impl(key):
            yield ev
            if once:
                break

    async def watch_prefix(self, key_prefix, *, once=False, ready_event=True):
        key_prefix = self._mangle_key(key_prefix)
        range_end = etcd3.utils.increment_last_byte(etcd3.utils.to_bytes(key_prefix))
        if ready_event:
            ready_event.set()
        async for ev in self._watch_impl(key_prefix, range_end=range_end):
            yield ev
            if once:
                break
