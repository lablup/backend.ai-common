import asyncio
import inspect
import time
from typing import (
    Any,
    Sequence,
    Dict,
)

import aioredis

__all__ = (
    'connect_with_retries',
    'execute_with_retries',
)

_scripts: Dict[str, str] = {}


def _calc_delay_exp_backoff(initial_delay: float, retry_count: float, time_limit: float) -> float:
    if time_limit > 0:
        return min(initial_delay * (2 ** retry_count), time_limit / 2)
    return min(initial_delay * (2 ** retry_count), 30.0)


async def connect_with_retries(
    *args,
    retry_delay: float = 0.5,
    retry_timeout: float = 60.0,
    max_retries: int = 0,
    exponential_backoff: bool = True,
    **kwargs,
) -> aioredis.abc.AbcPool:
    '''
    Create a Redis connection pool with multiple retries.
    '''
    begin = time.monotonic()
    num_retries = 0
    while True:
        try:
            return await aioredis.create_redis_pool(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except ConnectionRefusedError:
            if max_retries > 0 and num_retries >= max_retries:
                raise asyncio.TimeoutError('Exceeded the maximum retry count')
            if retry_timeout > 0 and time.monotonic() - begin >= retry_timeout:
                raise asyncio.TimeoutError('Too much delayed for retries')
            delay = _calc_delay_exp_backoff(
                retry_delay, num_retries, retry_timeout,
            ) if exponential_backoff else retry_delay
            await asyncio.sleep(delay)
            num_retries += 1
            continue


async def execute_with_retries(
    func: Any,
    retry_delay: float = 0.5,
    retry_timeout: float = 60.0,
    max_retries: int = 0,
    exponential_backoff: bool = True,
    suppress_force_closed: bool = True,
) -> Any:
    '''
    Execute the given Redis commands with multiple retries.
    The Redis commands must be generated as a ``aioredis.commands.Pipeline`` object
    or as a coroutine to execute single-shot aioredis commands by *func*.
    '''
    begin = time.monotonic()
    num_retries = 0
    await asyncio.sleep(0)
    while True:
        try:
            if inspect.iscoroutinefunction(func):
                aw_or_pipe = await func()
            elif callable(func):
                aw_or_pipe = func()
            else:
                raise TypeError('The func must be a function or a coroutinefunction '
                                'with no arguments.')
            if isinstance(aw_or_pipe, aioredis.commands.Pipeline):
                return await aw_or_pipe.execute()
            elif inspect.isawaitable(aw_or_pipe):
                return await aw_or_pipe
            else:
                raise TypeError('The return value must be an awaitable'
                                'or aioredis.commands.Pipeline object')
        except asyncio.CancelledError:
            raise
        except (aioredis.errors.PoolClosedError,
                aioredis.errors.ConnectionForcedCloseError):
            # This happens when we shut down the connection/pool.
            if suppress_force_closed:
                return None
            else:
                raise
        except (ConnectionResetError,
                ConnectionRefusedError,
                aioredis.errors.ConnectionClosedError,
                aioredis.errors.PipelineError):
            # Other cases mean server disconnection.
            if max_retries > 0 and num_retries >= max_retries:
                raise asyncio.TimeoutError('Exceeded the maximum retry count')
            if retry_timeout > 0 and time.monotonic() - begin >= retry_timeout:
                raise asyncio.TimeoutError('Too much delayed for retries')
            delay = _calc_delay_exp_backoff(
                retry_delay, num_retries, retry_timeout,
            ) if exponential_backoff else retry_delay
            await asyncio.sleep(delay)
            num_retries += 1
            continue
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
