import asyncio
import inspect
import time
from typing import Any

import aioredis

__all__ = (
    'execute_with_retries',
)


async def connect_with_retries(
        *args,
        retry_delay: float = 0.5,
        max_retry_delay: float = 60.0,
        **kwargs) -> aioredis.abc.AbcPool:
    '''
    Create a Redis connection pool with multiple retries.
    '''
    begin = time.monotonic()
    while True:
        try:
            return await aioredis.create_redis_pool(*args, **kwargs)
        except asyncio.CancelledError:
            raise
        except ConnectionRefusedError:
            if max_retry_delay > 0 and time.monotonic() - begin >= max_retry_delay:
                raise asyncio.TimeoutError('Too much delayed for making connection')
            await asyncio.sleep(retry_delay)
            continue


async def execute_with_retries(
    func: Any,
    retry_delay: float = 0.5,
    max_retry_delay: float = 60.0,
    max_retries: int = 0,
    suppress_force_closed: bool = True,
) -> Any:
    '''
    Execute the given Redis commands with multiple retries.
    The Redis commands must be generated as a ``aioredis.commands.Pipeline`` object
    or as a coroutine to execute single-shot aioredis commands by *func*.
    '''
    begin = time.monotonic()
    num_retries = 0
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
        except aioredis.errors.ConnectionForcedCloseError:
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
            if max_retry_delay > 0 and time.monotonic() - begin >= max_retry_delay:
                raise asyncio.TimeoutError('Too much delayed for retries')
            await asyncio.sleep(retry_delay)
            num_retries += 1
            continue
