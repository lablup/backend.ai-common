'''
Implements a asynchronous wrapper of etcd client.

In the future, we may replace the underlying etcd3 library with an asyncio-version
of etcd3 client libraries, hopefully, when gRPC ships asyncio stub generation.
'''

import asyncio
import collections
import functools
import logging

import etcd3

Event = collections.namedtuple('Event', 'key event value')
log = logging.getLogger(__name__)


class AsyncEtcd:

    def __init__(self, addr, namespace, encoding='utf8', loop=None, **kwargs):
        self.loop = loop if loop else asyncio.get_event_loop()
        self.etcd = etcd3.client(host=str(addr.host), port=addr.port, **kwargs)
        self.ns = namespace
        log.info(f'using etcd cluster from {addr} with namespace "{namespace}"')
        self.encoding = encoding
        self.watch_queues = dict()

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
        self.etcd.put(key, str(val).encode(self.encoding))

    async def get(self, key):
        key = self._mangle_key(key)
        val, metadata = self.etcd.get(key)
        if val is None:
            return None
        return val.decode(self.encoding)

    async def get_prefix(self, key_prefix):
        key_prefix = self._mangle_key(key_prefix)
        results = self.etcd.get_prefix(key_prefix)
        return (t[0].decode(self.encoding) for t in results)

    async def replace(self, key, initial_val, new_val):
        key = self._mangle_key(key)
        success = self.etcd.replace(key, initial_val, new_val)
        return success

    async def delete(self, key):
        key = self._mangle_key(key)
        self.etcd.delete(key)

    async def delete_prefix(self, key_prefix):
        key_prefix = self._mangle_key(key_prefix)
        self.etcd.delete_prefix(key_prefix)

    def _watch_cb(self, queue, ev):
        if isinstance(ev, etcd3.events.PutEvent):
            ev_type = 'put'
        elif isinstance(ev, etcd3.events.DeleteEvent):
            ev_type = 'delete'
        else:
            assert False, 'Not recognized etcd event type.'
        # etcd3 library uses a separate thread for its watchers.
        self.loop.call_soon_threadsafe(
            queue.put_nowait,
            Event(self._demangle_key(ev.key), ev_type, ev.value.decode(self.encoding))
        )

    async def _watch_impl(self, raw_key, **kwargs):
        queue = asyncio.Queue(loop=self.loop)
        cb = functools.partial(self._watch_cb, queue)
        watch_id = self.etcd.add_watch_callback(raw_key, cb, **kwargs)
        try:
            while True:
                ev = await queue.get()
                yield ev
                queue.task_done()
        except asyncio.CancelledError:
            pass
        finally:
            self.etcd.cancel_watch(watch_id)
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
