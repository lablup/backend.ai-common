from __future__ import annotations

import asyncio
import inspect
from typing import (
    Any,
    AsyncIterator,
    Awaitable,
    Callable,
    Sequence,
    Dict,
)

import aioredis
import aioredis.client
import aioredis.sentinel
import aioredis.exceptions

__all__ = (
    'execute',
    'subscribe',
    'blpop',
)

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
            channel.connection = None
            try:
                await channel.ping()
            except aioredis.exceptions.ConnectionError:
                pass
            else:
                assert channel.connection is not None
                await channel.on_connect(channel.connection)
            continue
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.sleep(0)


async def blpop(
    redis: aioredis.Redis | aioredis.sentinel.Sentinel,
    key: str,
    *,
    service_name: str = None,
    reconnect_poll_interval: float = 0.3,
) -> AsyncIterator[Any]:
    """
    An async-generator wrapper for blpop (blocking left pop).
    It automatically recovers from server shutdowns until explicitly cancelled.
    """
    if isinstance(redis, aioredis.sentinel.Sentinel):
        assert service_name is not None
        r = redis.master_for(service_name, socket_timeout=reconnect_poll_interval)
    else:
        r = redis
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
        except asyncio.TimeoutError:
            continue
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.sleep(0)


async def execute(
    redis: aioredis.Redis | aioredis.sentinel.Sentinel,
    func: Callable[[aioredis.Redis], Awaitable[Any]],
    *,
    service_name: str = None,
    read_only: bool = False,
    reconnect_poll_interval: float = 0.3,
) -> Any:
    if isinstance(redis, aioredis.sentinel.Sentinel):
        assert service_name is not None
        if read_only:
            r = redis.slave_for(service_name, socket_timeout=reconnect_poll_interval)
        else:
            r = redis.master_for(service_name, socket_timeout=reconnect_poll_interval)
    else:
        r = redis
    while True:
        try:
            async with r:
                if callable(func):
                    aw_or_pipe = func(r)
                else:
                    raise TypeError('The func must be a function or a coroutinefunction '
                                    'with no arguments.')
                if isinstance(aw_or_pipe, aioredis.client.Pipeline):
                    return await aw_or_pipe.execute()
                elif inspect.isawaitable(aw_or_pipe):
                    return await aw_or_pipe
                else:
                    raise TypeError('The return value must be an awaitable'
                                    'or aioredis.commands.Pipeline object')
        except (
            aioredis.exceptions.ConnectionError,
            aioredis.sentinel.MasterNotFoundError,
            aioredis.sentinel.SlaveNotFoundError,
            aioredis.exceptions.ReadOnlyError,
            aioredis.exceptions.ResponseError,
            ConnectionResetError,
        ) as e:
            print("EXECUTE-RETRY", repr(func), repr(e))
            await asyncio.sleep(reconnect_poll_interval)
            continue
        except asyncio.TimeoutError:
            print("EXECUTE-RETRY", repr(func), "timeout")
            return
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.sleep(0)


async def execute_script(
    conn: aioredis.Redis,
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
            ret = await execute_with_retries(lambda: conn.evalsha(
                script_hash,
                keys=keys,
                args=args,
            ))
            break
        except aioredis.errors.ReplyError as e:
            if 'NOSCRIPT' in e.args[0]:
                # Redis may have been restarted.
                script_hash = await conn.script_load(script)
                _scripts[script_id] = script_hash
            else:
                raise
            continue
    return ret
