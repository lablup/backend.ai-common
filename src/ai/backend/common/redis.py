from __future__ import annotations

import asyncio
import contextvars
import inspect
import socket
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Dict,
    Mapping,
    MutableMapping,
    NoReturn,
    Optional,
    Sequence,
    Tuple,
    Union,
    TYPE_CHECKING,
)
import uuid

import aioredis
import aioredis.client
import aioredis.sentinel
import aioredis.exceptions
from aioredis.exceptions import LockError, LockNotOwnedError
import yarl

from .types import EtcdRedisConfig, RedisConnectionInfo
from .validators import DelimiterSeperatedList, HostPortPair

if TYPE_CHECKING:
    from aioredis import Redis

__all__ = (
    'execute',
    'subscribe',
    'blpop',
    'read_stream',
    'read_stream_by_group',
    'get_redis_object',
)

_keepalive_options: MutableMapping[int, int] = {}

# macOS does not support several TCP_ options
# so check if socket package includes TCP options before adding it
if hasattr(socket, 'TCP_KEEPIDLE'):
    _keepalive_options[socket.TCP_KEEPIDLE] = 20

if hasattr(socket, 'TCP_KEEPINTVL'):
    _keepalive_options[socket.TCP_KEEPINTVL] = 5

if hasattr(socket, 'TCP_KEEPCNT'):
    _keepalive_options[socket.TCP_KEEPCNT] = 3


_default_conn_opts: Mapping[str, Any] = {
    'socket_timeout': 3.0,
    'socket_connect_timeout': 0.3,
    'socket_keepalive': True,
    'socket_keepalive_options': _keepalive_options,
}


_scripts: Dict[str, str] = {}


class ConnectionNotAvailable(Exception):
    pass


def _calc_delay_exp_backoff(initial_delay: float, retry_count: float, time_limit: float) -> float:
    if time_limit > 0:
        return min(initial_delay * (2 ** retry_count), time_limit / 2)
    return min(initial_delay * (2 ** retry_count), 30.0)


def _parse_stream_msg_id(msg_id: bytes) -> Tuple[int, int]:
    timestamp, _, sequence = msg_id.partition(b'-')
    return int(timestamp), int(sequence)


async def subscribe(
    channel: aioredis.client.PubSub,
    *,
    reconnect_poll_interval: float = 0.3,
) -> AsyncIterator[Any]:
    """
    An async-generator wrapper for pub-sub channel subscription.
    It automatically recovers from server shutdowns until explicitly cancelled.
    """
    async def _reset_chan():
        channel.connection = None
        try:
            await channel.ping()
        except aioredis.exceptions.ConnectionError:
            pass
        else:
            assert channel.connection is not None
            await channel.on_connect(channel.connection)

    while True:
        try:
            if not channel.connection:
                raise ConnectionNotAvailable
            message = await channel.get_message(ignore_subscribe_messages=True, timeout=10.0)
            if message is not None:
                yield message["data"]
        except (
            aioredis.exceptions.ConnectionError,
            aioredis.sentinel.MasterNotFoundError,
            aioredis.sentinel.SlaveNotFoundError,
            aioredis.exceptions.ReadOnlyError,
            aioredis.exceptions.ResponseError,
            ConnectionResetError,
            ConnectionNotAvailable,
        ):
            await asyncio.sleep(reconnect_poll_interval)
            await _reset_chan()
            continue
        except aioredis.exceptions.ResponseError as e:
            if e.args[0].startswith("NOREPLICAS "):
                await asyncio.sleep(reconnect_poll_interval)
                await _reset_chan()
                continue
            raise
        except (TimeoutError, asyncio.TimeoutError):
            continue
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.sleep(0)


async def blpop(
    redis: RedisConnectionInfo | aioredis.Redis | aioredis.sentinel.Sentinel,
    key: str,
    *,
    service_name: str = None,
    reconnect_poll_interval: float = 0.3,
) -> AsyncIterator[Any]:
    """
    An async-generator wrapper for blpop (blocking left pop).
    It automatically recovers from server shutdowns until explicitly cancelled.
    """
    _conn_opts = {
        **_default_conn_opts,
        'socket_connect_timeout': reconnect_poll_interval,
    }
    if isinstance(redis, RedisConnectionInfo):
        redis_client = redis.client
        service_name = service_name or redis.service_name
    else:
        redis_client = redis

    if isinstance(redis_client, aioredis.sentinel.Sentinel):
        assert service_name is not None
        r = redis_client.master_for(
            service_name,
            redis_class=aioredis.Redis,
            connection_pool_class=aioredis.sentinel.SentinelConnectionPool,
            **_conn_opts,
        )
    else:
        r = redis_client
    while True:
        try:
            raw_msg = await r.blpop(key, timeout=10.0)
            if not raw_msg:
                continue
            yield raw_msg[1]
        except (
            aioredis.exceptions.ConnectionError,
            aioredis.sentinel.MasterNotFoundError,
            aioredis.exceptions.ReadOnlyError,
            aioredis.exceptions.ResponseError,
            ConnectionResetError,
        ):
            await asyncio.sleep(reconnect_poll_interval)
            continue
        except aioredis.exceptions.ResponseError as e:
            if e.args[0].startswith("NOREPLICAS "):
                await asyncio.sleep(reconnect_poll_interval)
                continue
            raise
        except (TimeoutError, asyncio.TimeoutError):
            continue
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.sleep(0)


async def execute(
    redis: RedisConnectionInfo | aioredis.Redis | aioredis.sentinel.Sentinel,
    func: Callable[[aioredis.Redis], Awaitable[Any]],
    *,
    service_name: str = None,
    read_only: bool = False,
    reconnect_poll_interval: float = 0.3,
    encoding: Optional[str] = None,
) -> Any:
    _conn_opts = {
        **_default_conn_opts,
        'socket_connect_timeout': reconnect_poll_interval,
    }
    if isinstance(redis, RedisConnectionInfo):
        redis_client = redis.client
        service_name = service_name or redis.service_name
    else:
        redis_client = redis

    if isinstance(redis_client, aioredis.sentinel.Sentinel):
        assert service_name is not None
        if read_only:
            r = redis_client.slave_for(
                service_name,
                redis_class=aioredis.Redis,
                connection_pool_class=aioredis.sentinel.SentinelConnectionPool,
                **_conn_opts,
            )
        else:
            r = redis_client.master_for(
                service_name,
                redis_class=aioredis.Redis,
                connection_pool_class=aioredis.sentinel.SentinelConnectionPool,
                **_conn_opts,
            )
    else:
        r = redis_client
    while True:
        try:
            async with r:
                if callable(func):
                    aw_or_pipe = func(r)
                else:
                    raise TypeError('The func must be a function or a coroutinefunction '
                                    'with no arguments.')
                if isinstance(aw_or_pipe, aioredis.client.Pipeline):
                    result = await aw_or_pipe.execute()
                elif inspect.isawaitable(aw_or_pipe):
                    result = await aw_or_pipe
                else:
                    raise TypeError('The return value must be an awaitable'
                                    'or aioredis.commands.Pipeline object')
                if encoding:
                    if isinstance(result, bytes):
                        return result.decode(encoding)
                    elif isinstance(result, dict):
                        newdict = {}
                        for k, v in result.items():
                            newdict[k.decode(encoding)] = v.decode(encoding)
                        return newdict
                else:
                    return result
        except (
            aioredis.exceptions.ConnectionError,
            aioredis.sentinel.MasterNotFoundError,
            aioredis.sentinel.SlaveNotFoundError,
            aioredis.exceptions.ReadOnlyError,
            ConnectionResetError,
        ):
            await asyncio.sleep(reconnect_poll_interval)
            continue
        except aioredis.exceptions.ResponseError as e:
            if e.args[0].startswith("NOREPLICAS "):
                await asyncio.sleep(reconnect_poll_interval)
                continue
            raise
        except (TimeoutError, asyncio.TimeoutError):
            continue
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.sleep(0)


async def execute_script(
    redis: RedisConnectionInfo | aioredis.Redis | aioredis.sentinel.Sentinel,
    script_id: str,
    script: str,
    keys: Sequence[str],
    args: Sequence[str],
) -> Any:
    """
    Auto-load and execute the given script.
    It uses the hash keys for scripts so that it does not send the whole
    script every time but only at the first time.

    Args:
        conn: A Redis connection or pool with the commands mixin.
        script_id: A human-readable identifier for the script.
            This can be arbitrary string but must be unique for each script.
        script: The script content.
        keys: The Redis keys that will be passed to the script.
        args: The arguments that will be passed to the script.
    """
    script_hash = _scripts.get(script_id, 'x')
    while True:
        try:
            ret = await execute(redis, lambda r: r.evalsha(
                script_hash,
                len(keys),
                *keys, *args,
            ))
            break
        except aioredis.exceptions.NoScriptError:
            # Redis may have been restarted.
            script_hash = await execute(redis, lambda r: r.script_load(script))
            _scripts[script_id] = script_hash
        except aioredis.exceptions.ResponseError as e:
            if 'NOSCRIPT' in e.args[0]:
                # Redis may have been restarted.
                script_hash = await execute(redis, lambda r: r.script_load(script))
                _scripts[script_id] = script_hash
            else:
                raise
            continue
    return ret


async def read_stream(
    r: RedisConnectionInfo,
    stream_key: str,
    *,
    block_timeout: int = 10_000,  # in msec
) -> AsyncIterator[Tuple[bytes, bytes]]:
    """
    A high-level wrapper for the XREAD command.
    """
    last_id = b'0-0'
    while True:
        try:
            reply = await execute(
                r,
                lambda r: r.xread(
                    {stream_key: last_id},
                    block=block_timeout,
                ),
            )
            if reply is None:
                continue
            for msg_id, msg_data in reply[0][1]:
                try:
                    yield msg_id, msg_data
                finally:
                    last_id = msg_id
        except asyncio.CancelledError:
            raise


async def read_stream_by_group(
    r: RedisConnectionInfo,
    stream_key: str,
    group_name: str,
    consumer_id: str,
    *,
    autoclaim_idle_timeout: int = 1_000,  # in msec
    block_timeout: int = 10_000,  # in msec
) -> AsyncIterator[Tuple[bytes, bytes]]:
    """
    A high-level wrapper for the XREADGROUP command
    combined with XAUTOCLAIM and XGROUP_CREATE.
    """
    last_ack = b"0-0"
    while True:
        try:
            messages = []
            autoclaim_start_id = b'0-0'
            while True:
                reply = await execute(
                    r,
                    lambda r: r.execute_command(
                        "XAUTOCLAIM",
                        stream_key,
                        group_name,
                        consumer_id,
                        str(autoclaim_idle_timeout),
                        autoclaim_start_id,
                    ),
                )
                for msg_id, msg_data in aioredis.client.parse_stream_list(reply[1]):
                    messages.append((msg_id, msg_data))
                if reply[0] == b'0-0':
                    break
                autoclaim_start_id = reply[0]
            reply = await execute(
                r,
                lambda r: r.xreadgroup(
                    group_name,
                    consumer_id,
                    {stream_key: b">"},  # fetch messages not seen by other consumers
                    block=block_timeout,
                ),
            )
            assert reply[0][0].decode() == stream_key
            for msg_id, msg_data in reply[0][1]:
                messages.append((msg_id, msg_data))
            for msg_id, msg_data in messages:
                try:
                    yield msg_id, msg_data
                finally:
                    if _parse_stream_msg_id(last_ack) < _parse_stream_msg_id(msg_id):
                        last_ack = msg_id
                    await execute(
                        r,
                        lambda r: r.xack(
                            stream_key,
                            group_name,
                            msg_id,
                        ),
                    )
        except asyncio.CancelledError:
            raise
        except aioredis.exceptions.ResponseError as e:
            if e.args[0].startswith("NOGROUP "):
                try:
                    await execute(
                        r,
                        lambda r: r.xgroup_create(
                            stream_key,
                            group_name,
                            b"$",
                            mkstream=True,
                        ),
                    )
                except aioredis.exceptions.ResponseError as e:
                    if e.args[0].startswith("BUSYGROUP "):
                        pass
                    else:
                        raise
                continue
            raise


def get_redis_object(
    redis_config: EtcdRedisConfig,
    db: int = 0,
    **kwargs,
) -> RedisConnectionInfo:
    if _sentinel_addresses := redis_config.get('sentinel'):
        sentinel_addresses: Any = None
        if isinstance(_sentinel_addresses, str):
            sentinel_addresses = DelimiterSeperatedList(HostPortPair).check_and_return(_sentinel_addresses)
        else:
            sentinel_addresses = _sentinel_addresses

        assert redis_config.get('service_name') is not None
        sentinel = aioredis.sentinel.Sentinel(
            [(str(host), port) for host, port in sentinel_addresses],
            password=redis_config.get('password'),
            db=str(db),
            sentinel_kwargs={
                **kwargs,
            },
        )
        return RedisConnectionInfo(
            client=sentinel,
            service_name=redis_config.get('service_name'),
        )
    else:
        redis_url = redis_config.get('addr')
        assert redis_url is not None
        url = (
            yarl.URL('redis://host')
            .with_host(str(redis_url[0]))
            .with_port(redis_url[1])
            .with_password(redis_config.get('password')) / str(db)
        )
        return RedisConnectionInfo(
            client=aioredis.Redis.from_url(str(url), **kwargs),
            service_name=None,
        )


_local_token: contextvars.ContextVar[bytes] = contextvars.ContextVar('_local_token', default=None)


class Lock:
    """
    A shared, distributed Lock. Using Redis for locking allows the Lock
    to be shared across processes and/or machines.

    It's left to the user to resolve deadlock issues and make sure
    multiple clients play nicely together.

    The code is temporarily taken from aioredis v2 to workaround
    its thread-local storage limitation with contextvars.
    """

    lua_release = None
    lua_extend = None
    lua_reacquire = None

    # KEYS[1] - lock name
    # ARGV[1] - token
    # return 1 if the lock was released, otherwise 0
    LUA_RELEASE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('del', KEYS[1])
        return 1
    """

    # KEYS[1] - lock name
    # ARGV[1] - token
    # ARGV[2] - additional milliseconds
    # ARGV[3] - "0" if the additional time should be added to the lock's
    #           existing ttl or "1" if the existing ttl should be replaced
    # return 1 if the locks time was extended, otherwise 0
    LUA_EXTEND_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        local expiration = redis.call('pttl', KEYS[1])
        if not expiration then
            expiration = 0
        end
        if expiration < 0 then
            return 0
        end

        local newttl = ARGV[2]
        if ARGV[3] == "0" then
            newttl = ARGV[2] + expiration
        end
        redis.call('pexpire', KEYS[1], newttl)
        return 1
    """

    # KEYS[1] - lock name
    # ARGV[1] - token
    # ARGV[2] - milliseconds
    # return 1 if the locks time was reacquired, otherwise 0
    LUA_REACQUIRE_SCRIPT = """
        local token = redis.call('get', KEYS[1])
        if not token or token ~= ARGV[1] then
            return 0
        end
        redis.call('pexpire', KEYS[1], ARGV[2])
        return 1
    """

    def __init__(
        self,
        redis: "Redis",
        name: Union[str, bytes, memoryview],
        timeout: float = None,
        sleep: float = 0.1,
        blocking: bool = True,
        blocking_timeout: float = None,
        thread_local: bool = True,  # for compatibility; unused
    ):
        """
        Create a new Lock instance named ``name`` using the Redis client
        supplied by ``redis``.

        ``timeout`` indicates a maximum life for the lock.
        By default, it will remain locked until release() is called.
        ``timeout`` can be specified as a float or integer, both representing
        the number of seconds to wait.

        ``sleep`` indicates the amount of time to sleep per loop iteration
        when the lock is in blocking mode and another client is currently
        holding the lock.

        ``blocking`` indicates whether calling ``acquire`` should block until
        the lock has been acquired or to fail immediately, causing ``acquire``
        to return False and the lock not being acquired. Defaults to True.
        Note this value can be overridden by passing a ``blocking``
        argument to ``acquire``.

        ``blocking_timeout`` indicates the maximum amount of time in seconds to
        spend trying to acquire the lock. A value of ``None`` indicates
        continue trying forever. ``blocking_timeout`` can be specified as a
        float or integer, both representing the number of seconds to wait.

        ``thread_local`` indicates whether the lock token is placed in
        thread-local storage. By default, the token is placed in thread local
        storage so that a thread only sees its token, not a token set by
        another thread. Consider the following timeline:

            time: 0, thread-1 acquires `my-lock`, with a timeout of 5 seconds.
                     thread-1 sets the token to "abc"
            time: 1, thread-2 blocks trying to acquire `my-lock` using the
                     Lock instance.
            time: 5, thread-1 has not yet completed. redis expires the lock
                     key.
            time: 5, thread-2 acquired `my-lock` now that it's available.
                     thread-2 sets the token to "xyz"
            time: 6, thread-1 finishes its work and calls release(). if the
                     token is *not* stored in thread local storage, then
                     thread-1 would see the token value as "xyz" and would be
                     able to successfully release the thread-2's lock.

        In some use cases it's necessary to disable thread local storage. For
        example, if you have code where one thread acquires a lock and passes
        that lock instance to a worker thread to release later. If thread
        local storage isn't disabled in this case, the worker thread won't see
        the token set by the thread that acquired the lock. Our assumption
        is that these cases aren't common and as such default to using
        thread local storage.
        """
        self.redis = redis
        self.name = name
        self.timeout = timeout
        self.sleep = sleep
        self.blocking = blocking
        self.blocking_timeout = blocking_timeout
        self._reset_token = None
        self.register_scripts()

    def register_scripts(self):
        cls = self.__class__
        client = self.redis
        if cls.lua_release is None:
            cls.lua_release = client.register_script(cls.LUA_RELEASE_SCRIPT)
        if cls.lua_extend is None:
            cls.lua_extend = client.register_script(cls.LUA_EXTEND_SCRIPT)
        if cls.lua_reacquire is None:
            cls.lua_reacquire = client.register_script(cls.LUA_REACQUIRE_SCRIPT)

    async def __aenter__(self):
        if await self.acquire():
            return self
        raise LockError("Unable to acquire lock within the time specified")

    async def __aexit__(self, exc_type, exc_value, traceback):
        await self.release()

    async def acquire(
        self,
        blocking: bool = None,
        blocking_timeout: float = None,
        token: Union[str, bytes] = None,
    ):
        """
        Use Redis to hold a shared, distributed lock named ``name``.
        Returns True once the lock is acquired.

        If ``blocking`` is False, always return immediately. If the lock
        was acquired, return True, otherwise return False.

        ``blocking_timeout`` specifies the maximum number of seconds to
        wait trying to acquire the lock.

        ``token`` specifies the token value to be used. If provided, token
        must be a bytes object or a string that can be encoded to a bytes
        object with the default encoding. If a token isn't specified, a UUID
        will be generated.
        """
        loop = asyncio.get_event_loop()
        sleep = self.sleep
        if token is None:
            token = uuid.uuid1().hex.encode()
        else:
            encoder = self.redis.connection_pool.get_encoder()
            token = encoder.encode(token)
        if blocking is None:
            blocking = self.blocking
        if blocking_timeout is None:
            blocking_timeout = self.blocking_timeout
        stop_trying_at = None
        if blocking_timeout is not None:
            stop_trying_at = loop.time() + blocking_timeout
        while True:
            if await self.do_acquire(token):
                self._reset_token = _local_token.set(token)
                return True
            if not blocking:
                return False
            next_try_at = loop.time() + sleep
            if stop_trying_at is not None and next_try_at > stop_trying_at:
                return False
            await asyncio.sleep(sleep)

    async def do_acquire(self, token: Union[str, bytes]) -> bool:
        if self.timeout:
            # convert to milliseconds
            timeout = int(self.timeout * 1000)
        else:
            timeout = None
        if await self.redis.set(self.name, token, nx=True, px=timeout):
            return True
        return False

    async def locked(self) -> bool:
        """
        Returns True if this key is locked by any process, otherwise False.
        """
        return await self.redis.get(self.name) is not None

    async def owned(self) -> bool:
        """
        Returns True if this key is locked by this lock, otherwise False.
        """
        stored_token = await self.redis.get(self.name)
        # need to always compare bytes to bytes
        # TODO: this can be simplified when the context manager is finished
        if stored_token and not isinstance(stored_token, bytes):
            encoder = self.redis.connection_pool.get_encoder()
            stored_token = encoder.encode(stored_token)
        local_token = _local_token.get()
        return local_token is not None and stored_token == local_token

    def release(self) -> Awaitable[NoReturn]:
        """Releases the already acquired lock"""
        expected_token = _local_token.get()
        if expected_token is None:
            raise LockError("Cannot release an unlocked lock")
        _local_token.reset(self._reset_token)
        return self.do_release(expected_token)

    async def do_release(self, expected_token: bytes):
        if not bool(
            await self.lua_release(
                keys=[self.name], args=[expected_token], client=self.redis
            )
        ):
            raise LockNotOwnedError("Cannot release a lock" " that's no longer owned")

    def extend(
        self, additional_time: float, replace_ttl: bool = False
    ) -> Awaitable[bool]:
        """
        Adds more time to an already acquired lock.

        ``additional_time`` can be specified as an integer or a float, both
        representing the number of seconds to add.

        ``replace_ttl`` if False (the default), add `additional_time` to
        the lock's existing ttl. If True, replace the lock's ttl with
        `additional_time`.
        """
        if _local_token.get() is None:
            raise LockError("Cannot extend an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot extend a lock with no timeout")
        return self.do_extend(additional_time, replace_ttl)

    async def do_extend(self, additional_time, replace_ttl) -> bool:
        additional_time = int(additional_time * 1000)
        if not bool(
            await self.lua_extend(
                keys=[self.name],
                args=[_local_token.get(), additional_time, replace_ttl and "1" or "0"],
                client=self.redis,
            )
        ):
            raise LockNotOwnedError("Cannot extend a lock that's" " no longer owned")
        return True

    def reacquire(self) -> Awaitable[bool]:
        """
        Resets a TTL of an already acquired lock back to a timeout value.
        """
        if _local_token.get() is None:
            raise LockError("Cannot reacquire an unlocked lock")
        if self.timeout is None:
            raise LockError("Cannot reacquire a lock with no timeout")
        return self.do_reacquire()

    async def do_reacquire(self) -> bool:
        timeout = int(self.timeout * 1000)
        if not bool(
            await self.lua_reacquire(
                keys=[self.name], args=[_local_token.get(), timeout], client=self.redis
            )
        ):
            raise LockNotOwnedError("Cannot reacquire a lock that's" " no longer owned")
        return True
