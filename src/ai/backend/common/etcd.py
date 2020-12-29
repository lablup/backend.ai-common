'''
An asynchronous client wrapper for etcd v3 API.

It uses the etcd3 library using a thread pool executor.
We plan to migrate to aioetcd3 library but it requires more work to get maturity.
Fortunately, etcd3's watchers are not blocking because they are implemented
using callbacks in separate threads.
'''

import asyncio
from collections import namedtuple, ChainMap
from concurrent.futures import ThreadPoolExecutor
import enum
import functools
import logging
import time
from typing import (
    Any, Awaitable, Callable, Iterable, Optional, Union,
    AsyncGenerator,
    Dict, Mapping,
    Tuple,
)
from urllib.parse import quote as _quote, unquote

from aiotools import aclosing
from async_timeout import timeout as _timeout
import etcd3
from etcd3 import etcdrpc
from etcd3.client import EtcdTokenCallCredentials
import grpc
import trafaret as t

from .logging_utils import BraceStyleAdapter
from .types import HostPortPair, QueueSentinel

__all__ = (
    'quote', 'unquote',
    'AsyncEtcd',
)

Event = namedtuple('Event', 'key event value')

log = BraceStyleAdapter(logging.getLogger(__name__))


class ConfigScopes(enum.Enum):
    MERGED = 0
    GLOBAL = 1
    SGROUP = 2
    NODE = 3


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
            p = unquote(p)
            if p not in parent:
                parent[p] = {}
            if p in parent and not isinstance(parent[p], dict):
                root = parent[p]
                parent[p] = {'': root}
            parent = parent[p]
        parent[unquote(path_components[-1])] = v
    return result


def _slash(v: str):
    return v.rstrip('/') + '/' if len(v) > 0 else ''


async def reauthenticate(etcd_sync, creds, executor):
    # This code is taken from the constructor of etcd3.client.Etcd3Client class.
    # Related issue: kragniz/python-etcd3#580
    etcd_sync.auth_stub = etcdrpc.AuthStub(etcd_sync.channel)
    auth_request = etcdrpc.AuthenticateRequest(
        name=creds['user'],
        password=creds['password'],
    )
    loop = asyncio.get_event_loop()
    resp = await loop.run_in_executor(
        executor,
        lambda: etcd_sync.auth_stub.Authenticate(auth_request, etcd_sync.timeout))
    etcd_sync.metadata = (('token', resp.token),)
    etcd_sync.call_credentials = grpc.metadata_call_credentials(
        EtcdTokenCallCredentials(resp.token))


def reconn_reauth_adaptor(meth: Callable[..., Awaitable[Any]]):
    @functools.wraps(meth)
    async def wrapped(self, *args, **kwargs):
        num_reauth_tries = 0
        num_reconn_tries = 0
        while True:
            try:
                return await meth(self, *args, **kwargs)
            except etcd3.exceptions.ConnectionFailedError:
                if num_reconn_tries >= 30:
                    log.warning('etcd3 connection failed more than %d times. retrying after 1 sec...',
                                num_reconn_tries)
                else:
                    log.debug('etcd3 connection failed. retrying after 1 sec...')
                await asyncio.sleep(1.0)
                num_reconn_tries += 1
                continue
            except grpc.RpcError as e:
                if (
                    e.code() == grpc.StatusCode.UNAUTHENTICATED or
                    (e.code() == grpc.StatusCode.UNKNOWN and "invalid auth token" in e.details())
                ) and self._creds:
                    if num_reauth_tries > 0:
                        raise
                    await reauthenticate(self.etcd_sync, self._creds, self.executor)
                    log.debug('etcd3 reauthenticated due to auth token expiration.')
                    num_reauth_tries += 1
                    continue
                else:
                    raise
    return wrapped


class AsyncEtcd:

    def __init__(self, addr: HostPortPair, namespace: str,
                 scope_prefix_map: Mapping[ConfigScopes, str], *,
                 credentials=None, encoding='utf8'):
        self.scope_prefix_map = t.Dict({
            t.Key(ConfigScopes.GLOBAL): t.String(allow_blank=True),
            t.Key(ConfigScopes.SGROUP, optional=True): t.String,
            t.Key(ConfigScopes.NODE, optional=True): t.String,
        }).check(scope_prefix_map)
        self.loop = asyncio.get_running_loop()
        self.executor = ThreadPoolExecutor(max_workers=5, thread_name_prefix='etcd')
        self._creds = credentials
        while True:
            try:
                self.etcd_sync = etcd3.client(
                    host=str(addr.host), port=addr.port,
                    user=credentials.get('user') if credentials else None,
                    password=credentials.get('password') if credentials else None)
                break
            except grpc.RpcError as e:
                if e.code() in (grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.UNKNOWN):
                    log.debug('etcd3 connection failed. retrying after 1 sec...')
                    time.sleep(1)
                    continue
                raise
        self.ns = namespace
        log.info('using etcd cluster from {} with namespace "{}"', addr, namespace)
        self.encoding = encoding

    async def close(self):
        ret = await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.close())
        # ref: https://github.com/kragniz/python-etcd3/issues/997
        # Currently there is no public API to control this... :(
        if self.etcd_sync.watcher._callback_thread:
            self.etcd_sync.watcher._callback_thread.join()
        return ret

    def _mangle_key(self, k: str) -> bytes:
        if k.startswith('/'):
            k = k[1:]
        return f'/sorna/{self.ns}/{k}'.encode(self.encoding)

    def _demangle_key(self, k: Union[bytes, str]) -> str:
        if isinstance(k, bytes):
            k = k.decode(self.encoding)
        prefix = f'/sorna/{self.ns}/'
        if k.startswith(prefix):
            k = k[len(prefix):]
        return k

    @reconn_reauth_adaptor
    async def put(self, key: str, val: str, *,
                  scope: ConfigScopes = ConfigScopes.GLOBAL,
                  scope_prefix_map: Mapping[ConfigScopes, str] = None):
        """
        Put a single key-value pair to the etcd.

        :param key: The key. This must be quoted by the caller as needed.
        :param val: The value.
        :param scope: The config scope for putting the values.
        :param scope_prefix_map: The scope map used to mangle the prefix for the config scope.
        :return:
        """
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        mangled_key = self._mangle_key(f'{_slash(scope_prefix)}{key}')
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.put(mangled_key, str(val).encode(self.encoding)))

    @reconn_reauth_adaptor
    async def put_prefix(self, key: str, dict_obj: Mapping[str, str], *,
                         scope: ConfigScopes = ConfigScopes.GLOBAL,
                         scope_prefix_map: Mapping[ConfigScopes, str] = None):
        """
        Put a nested dict object under the given key prefix.
        All keys in the dict object are automatically quoted to avoid conflicts with the path separator.

        :param key: Prefix to put the given data. This must be quoted by the caller as needed.
        :param dict_obj: Nested dictionary representing the data.
        :param scope: The config scope for putting the values.
        :param scope_prefix_map: The scope map used to mangle the prefix for the config scope.
        :return:
        """
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        flattened_dict: Dict[str, str] = {}

        def _flatten(prefix: str, inner_dict: Mapping[str, str]) -> None:
            for k, v in inner_dict.items():
                if k == '':
                    flattened_key = prefix
                else:
                    flattened_key = prefix + '/' + quote(k)
                if isinstance(v, dict):
                    _flatten(flattened_key, v)
                else:
                    flattened_dict[flattened_key] = v

        _flatten(key, dict_obj)

        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.transaction(
                [],
                [self.etcd_sync.transactions.put(
                    self._mangle_key(f'{_slash(scope_prefix)}{k}'), str(v).encode(self.encoding))
                    for k, v in flattened_dict.items()],
                []
            ))

    @reconn_reauth_adaptor
    async def put_dict(self, dict_obj: Mapping[str, str], *,
                       scope: ConfigScopes = ConfigScopes.GLOBAL,
                       scope_prefix_map: Mapping[ConfigScopes, str] = None):
        """
        Put a flattened key-value pairs into the etcd.
        Since the given dict must be a flattened one, its keys must be quoted as needed by the caller.
        For new codes, ``put_prefix()`` is recommended.

        :param dict_obj: Flattened key-value pairs to put.
        :param scope: The config scope for putting the values.
        :param scope_prefix_map: The scope map used to mangle the prefix for the config scope.
        :return:
        """
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.transaction(
                [],
                [self.etcd_sync.transactions.put(
                    self._mangle_key(f'{_slash(scope_prefix)}{k}'), str(v).encode(self.encoding))
                 for k, v in dict_obj.items()],
                []
            ))

    @reconn_reauth_adaptor
    async def get(self, key: str, *,
                  scope: ConfigScopes = ConfigScopes.MERGED,
                  scope_prefix_map: Mapping[ConfigScopes, str] = None) \
                  -> Optional[str]:
        """
        Get a single key from the etcd.
        Returns ``None`` if the key does not exist.
        The returned value may be an empty string if the value is a zero-length string.

        :param key: The key. This must be quoted by the caller as needed.
        :param scope: The config scope to get the value.
        :param scope_prefix_map: The scope map used to mangle the prefix for the config scope.
        :return:
        """

        async def get_impl(key: str) -> Optional[str]:
            mangled_key = self._mangle_key(key)
            val, _ = await self.loop.run_in_executor(
                self.executor,
                lambda: self.etcd_sync.get(mangled_key))
            return val.decode(self.encoding) if val is not None else None

        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        if scope == ConfigScopes.MERGED or scope == ConfigScopes.NODE:
            scope_prefixes = [scope_prefix_map[ConfigScopes.GLOBAL]]
            p = scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
            p = scope_prefix_map.get(ConfigScopes.NODE)
            if p is not None:
                scope_prefixes.insert(0, p)
        elif scope == ConfigScopes.SGROUP:
            scope_prefixes = [scope_prefix_map[ConfigScopes.GLOBAL]]
            p = scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
        elif scope == ConfigScopes.GLOBAL:
            scope_prefixes = [scope_prefix_map[ConfigScopes.GLOBAL]]
        else:
            raise ValueError('Invalid scope prefix value')
        values = await asyncio.gather(*[
            get_impl(f'{_slash(scope_prefix)}{key}')
            for scope_prefix in scope_prefixes
        ])
        for value in values:
            if value is not None:
                break
        else:
            value = None
        return value

    @reconn_reauth_adaptor
    async def get_prefix(self, key_prefix: str,
                         scope: ConfigScopes = ConfigScopes.MERGED,
                         scope_prefix_map: Mapping[ConfigScopes, str] = None) \
                         -> Mapping[str, Optional[str]]:
        """
        Retrieves all key-value pairs under the given key prefix as a nested dictionary.
        All dictionary keys are automatically unquoted.
        If a key has a value while it is also used as path prefix for other keys,
        the value directly referenced by the key itself is included as a value in a dictionary
        with the empty-string key.

        For instance, when the etcd database has the following key-value pairs:

        .. code-block::

           myprefix/mydata = abc
           myprefix/mydata/x = 1
           myprefix/mydata/y = 2
           myprefix/mykey = def

        ``get_prefix("myprefix")`` returns the following dictionary:

        .. code-block::

           {
             "mydata": {
               "": "abc",
               "x": "1",
               "y": "2",
             },
             "mykey": "def",
           }

        :param key_prefix: The key. This must be quoted by the caller as needed.
        :param scope: The config scope to get the value.
        :param scope_prefix_map: The scope map used to mangle the prefix for the config scope.
        :return:
        """

        async def get_prefix_impl(key_prefix: str) -> Iterable[Tuple[str, str]]:
            mangled_key_prefix = self._mangle_key(key_prefix)
            results = await self.loop.run_in_executor(
                self.executor,
                lambda: self.etcd_sync.get_prefix(mangled_key_prefix))
            return ((self._demangle_key(t[1].key),
                     t[0].decode(self.encoding))
                    for t in results)

        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        if scope == ConfigScopes.MERGED or scope == ConfigScopes.NODE:
            scope_prefixes = [scope_prefix_map[ConfigScopes.GLOBAL]]
            p = scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
            p = scope_prefix_map.get(ConfigScopes.NODE)
            if p is not None:
                scope_prefixes.insert(0, p)
        elif scope == ConfigScopes.SGROUP:
            scope_prefixes = [scope_prefix_map[ConfigScopes.GLOBAL]]
            p = scope_prefix_map.get(ConfigScopes.SGROUP)
            if p is not None:
                scope_prefixes.insert(0, p)
        elif scope == ConfigScopes.GLOBAL:
            scope_prefixes = [scope_prefix_map[ConfigScopes.GLOBAL]]
        else:
            raise ValueError('Invalid scope prefix value')
        pair_sets = await asyncio.gather(*[
            get_prefix_impl(f'{_slash(scope_prefix)}{key_prefix}')
            for scope_prefix in scope_prefixes
        ])
        configs = [
            make_dict_from_pairs(f'{_slash(scope_prefix)}{key_prefix}', pairs, '/')
            for scope_prefix, pairs in zip(scope_prefixes, pair_sets)
        ]
        return ChainMap(*configs)

    # for legacy
    get_prefix_dict = get_prefix

    @reconn_reauth_adaptor
    async def replace(self, key: str, initial_val: str, new_val: str, *,
                      scope: ConfigScopes = ConfigScopes.GLOBAL,
                      scope_prefix_map: Mapping[ConfigScopes, str] = None) -> bool:
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        mangled_key = self._mangle_key(f'{_slash(scope_prefix)}{key}')
        success = await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.replace(mangled_key, initial_val, new_val))
        return success

    @reconn_reauth_adaptor
    async def delete(self, key: str, *,
                     scope: ConfigScopes = ConfigScopes.GLOBAL,
                     scope_prefix_map: Mapping[ConfigScopes, str] = None):
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        mangled_key = self._mangle_key(f'{_slash(scope_prefix)}{key}')
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.delete(mangled_key))

    @reconn_reauth_adaptor
    async def delete_multi(self, keys: Iterable[str], *,
                           scope: ConfigScopes = ConfigScopes.GLOBAL,
                           scope_prefix_map: Mapping[ConfigScopes, str] = None):
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.transaction(
                [],
                [self.etcd_sync.transactions.delete(self._mangle_key(f'{_slash(scope_prefix)}{k}'))
                 for k in keys],
                []
            ))

    @reconn_reauth_adaptor
    async def delete_prefix(self, key_prefix: str, *,
                            scope: ConfigScopes = ConfigScopes.GLOBAL,
                            scope_prefix_map: Mapping[ConfigScopes, str] = None):
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        mangled_key_prefix = self._mangle_key(f'{_slash(scope_prefix)}{key_prefix}')
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.delete_prefix(mangled_key_prefix))

    def _watch_cb(self, queue: asyncio.Queue, resp: etcd3.watch.WatchResponse) -> None:
        if isinstance(resp, grpc.RpcError):
            if (
                resp.code() == grpc.StatusCode.UNAVAILABLE or
                (resp.code() == grpc.StatusCode.UNKNOWN and "invalid auth token" not in resp.details())
            ):
                # server restarting or terminated
                self.loop.call_soon_threadsafe(queue.put_nowait, QueueSentinel.CLOSED)
                return
            else:
                raise RuntimeError(f'Unexpected RPC Error: {resp}')
        for ev in resp.events:
            if isinstance(ev, etcd3.events.PutEvent):
                ev_type = 'put'
            elif isinstance(ev, etcd3.events.DeleteEvent):
                ev_type = 'delete'
            else:
                raise TypeError('Not recognized etcd event type.')
            # etcd3 library uses a separate thread for its watchers.
            event = Event(
                self._demangle_key(ev.key),
                ev_type,
                ev.value.decode(self.encoding),
            )
            self.loop.call_soon_threadsafe(queue.put_nowait, event)

    @reconn_reauth_adaptor
    async def _add_watch_callback(self, raw_key: bytes, cb, **kwargs):
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.add_watch_callback(raw_key, cb, **kwargs))

    @reconn_reauth_adaptor
    async def _add_watch_prefix_callback(self, raw_key: bytes, cb, **kwargs):
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.add_watch_prefix_callback(raw_key, cb, **kwargs))

    @reconn_reauth_adaptor
    async def _cancel_watch(self, watch_id):
        return await self.loop.run_in_executor(
            self.executor,
            lambda: self.etcd_sync.cancel_watch(watch_id))

    async def _watch_impl(
        self,
        raw_key: bytes,
        ready_event: asyncio.Event = None,
        cleanup_event: asyncio.Event = None,
        prefix: bool = False,
        timeout: float = None,
        **kwargs,
    ) -> AsyncGenerator[Union[QueueSentinel, Event], None]:
        queue: asyncio.Queue[Union[QueueSentinel, Event]] = asyncio.Queue()
        cb = functools.partial(self._watch_cb, queue)
        if prefix:
            watch_id = await self._add_watch_prefix_callback(raw_key, cb, **kwargs)
        else:
            watch_id = await self._add_watch_callback(raw_key, cb, **kwargs)
        if ready_event is not None:
            ready_event.set()
        try:
            while True:
                try:
                    async with _timeout(timeout):
                        ev = await queue.get()
                except asyncio.TimeoutError:
                    yield QueueSentinel.TIMEOUT
                else:
                    yield ev
                    queue.task_done()
        except asyncio.CancelledError:
            raise
        finally:
            await self._cancel_watch(watch_id)
            if cleanup_event is not None:
                cleanup_event.set()

    async def watch(
        self, key: str, *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
        once: bool = False,
        ready_event: asyncio.Event = None,
        cleanup_event: asyncio.Event = None,
        wait_timeout: float = None,
    ) -> AsyncGenerator[Union[QueueSentinel, Event], None]:
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        scope_prefix_len = len(f'{_slash(scope_prefix)}')
        mangled_key = self._mangle_key(f'{_slash(scope_prefix)}{key}')
        # NOTE: yield from in async-generator is not supported.
        while True:
            try:
                async with aclosing(
                    self._watch_impl(mangled_key,
                                     ready_event,
                                     cleanup_event,
                                     timeout=wait_timeout)
                ) as agen:
                    async for ev in agen:
                        if ev is QueueSentinel.CLOSED:
                            log.debug('watch(): etcd connection closed, restarting watch')
                            break
                        elif ev is QueueSentinel.TIMEOUT:
                            yield ev
                        else:
                            yield Event(ev.key[scope_prefix_len:], ev.event, ev.value)
                            if once:
                                return
            except asyncio.CancelledError:
                break
        if cleanup_event:
            cleanup_event.set()

    async def watch_prefix(
        self, key_prefix: str, *,
        scope: ConfigScopes = ConfigScopes.GLOBAL,
        scope_prefix_map: Mapping[ConfigScopes, str] = None,
        once: bool = False,
        ready_event: asyncio.Event = None,
        cleanup_event: asyncio.Event = None,
        wait_timeout: float = None,
    ) -> AsyncGenerator[Union[QueueSentinel, Event], None]:
        scope_prefix_map = ChainMap(scope_prefix_map or {}, self.scope_prefix_map)
        scope_prefix = scope_prefix_map[scope]
        scope_prefix_len = len(f'{_slash(scope_prefix)}')
        mangled_key_prefix = self._mangle_key(f'{_slash(scope_prefix)}{key_prefix}')
        while True:
            try:
                async with aclosing(
                    self._watch_impl(mangled_key_prefix,
                                     ready_event,
                                     cleanup_event,
                                     prefix=True,
                                     timeout=wait_timeout)
                ) as agen:
                    async for ev in agen:
                        if ev is QueueSentinel.CLOSED:
                            log.debug('watch_prefix(): etcd connection closed, restarting watch')
                            break
                        elif ev is QueueSentinel.TIMEOUT:
                            yield ev
                        else:
                            yield Event(ev.key[scope_prefix_len:], ev.event, ev.value)
                            if once:
                                return
            except asyncio.CancelledError:
                break
        if cleanup_event:
            cleanup_event.set()
