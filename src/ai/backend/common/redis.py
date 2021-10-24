from __future__ import annotations

import asyncio
import inspect
import socket
import sys
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Mapping,
    MutableMapping,
    Optional,
    Sequence,
    Dict,
)

import aioredis
import aioredis.client
import aioredis.sentinel
import aioredis.exceptions
import yarl

from .types import EtcdRedisConfig, RedisConnectionInfo

__all__ = (
    'execute',
    'subscribe',
    'blpop',
    'get_redis_object',
)

_keepalive_options: MutableMapping[int, int] = {}

# macOS does not support several TCP_ options
# so check if socket package includes TCP options before adding it
if hasattr(socket, 'TCP_KEEPIDLE'):
    _keepalive_options[socket.TCP_KEEPIDLE] = 20

if hasattr(socket, 'TCP_KEEPINTVL'):
    _keepalive_options[socket.TCP_KEEPINTVL] = 20

if hasattr(socket, 'TCP_KEEPCNT'):
    _keepalive_options[socket.TCP_KEEPCNT] = 20


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
        except asyncio.TimeoutError:
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
        'socket_timeout': reconnect_poll_interval,
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
        except asyncio.TimeoutError:
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
        'socket_timeout': reconnect_poll_interval,
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
                        print("EXECUTE-RETRY: Done", file=sys.stderr)
                        return result.decode(encoding)
                    elif isinstance(result, dict):
                        newdict = {}
                        for k, v in result.items():
                            newdict[k.decode(encoding)] = v.decode(encoding)
                        print("EXECUTE-RETRY: Done", file=sys.stderr)
                        return newdict
                else:
                    print("EXECUTE-RETRY: Done", file=sys.stderr)
                    return result
        except (
            aioredis.exceptions.ConnectionError,
            aioredis.sentinel.MasterNotFoundError,
            aioredis.sentinel.SlaveNotFoundError,
            aioredis.exceptions.ReadOnlyError,
            ConnectionResetError,
        ) as e:
            print("EXECUTE-RETRY", repr(func), repr(e), file=sys.stderr)
            await asyncio.sleep(reconnect_poll_interval)
            continue
        except aioredis.exceptions.ResponseError as e:
            if e.args[0].startswith("NOREPLICAS "):
                print("EXECUTE-RETRY", repr(func), repr(e))
                await asyncio.sleep(reconnect_poll_interval)
                continue
            print("EXECUTE-RETRY: Unexpected ResponseError", repr(e), file=sys.stderr)
            raise
        except asyncio.TimeoutError:
            print("EXECUTE-RETRY", repr(func), "timeout", file=sys.stderr)
            return
        except asyncio.CancelledError:
            raise
        except Exception as e:
            print("EXECUTE-RETRY: Unexpected generic exception", repr(e), file=sys.stderr)
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


def get_redis_object(
    redis_config: EtcdRedisConfig,
    db: int = 0,
    **kwargs,
) -> RedisConnectionInfo:
    if sentinel_addresses := redis_config.get('sentinel'):
        assert redis_config.get('service_name') is not None
        sentinel = aioredis.sentinel.Sentinel(
            sentinel_addresses,
            sentinel_kwargs={
                'password': redis_config.get('password'),
                'db': str(db),
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
