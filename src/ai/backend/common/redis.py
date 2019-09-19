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
    Execute the given coroutine with multiple retries, assuming
    that the coro is a connection to a Redis server.
    The coro must be a call to ``aioredis.create_redis()`` or
    ``aioredis.create_redis_pool()``
    '''
    begin = time.monotonic()
    while True:
        try:
            return await aioredis.create_redis_pool(*args, **kwargs)
        except ConnectionRefusedError:
            if time.monotonic() - begin >= max_retry_delay:
                raise
            await asyncio.sleep(retry_delay)
            continue


async def execute_with_retries(
        func: Any,
        retry_delay: float = 0.5,
        max_retry_delay: float = 60.0) -> Any:
    '''
    Execute the given Redis commands with multiple retries.
    The Redis commands must be generated as a ``aioredis.commands.Pipeline`` object
    or as a coroutine to execute single-shot aioredis commands by *func*.
    '''
    begin = time.monotonic()
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
        except aioredis.errors.ConnectionForcedCloseError:
            # This happens when we shut down the connection/pool.
            raise
        except (ConnectionResetError,
                ConnectionRefusedError,
                aioredis.errors.ConnectionClosedError,
                aioredis.errors.PipelineError):
            # Other cases mean server disconnection.
            if time.monotonic() - begin >= max_retry_delay:
                raise
            await asyncio.sleep(retry_delay)
            continue
