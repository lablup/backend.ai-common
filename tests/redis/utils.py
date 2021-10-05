from __future__ import annotations

import aioredis
import aioredis.exceptions
import async_timeout
import asyncio
import functools
from typing import (
    Awaitable,
    Callable,
    Final,
    Sequence,
    TypeVar,
    Tuple,
    Union,
)
from typing_extensions import (
    ParamSpec,
)


disruptions: Final = {
    'stop': {
        'begin': 'stop',
        'end': 'start',
    },
    'pause': {
        'begin': 'pause',
        'end': 'unpause',
    },
}


async def simple_run_cmd(cmdargs: Sequence[Union[str, bytes]], **kwargs) -> asyncio.subprocess.Process:
    p = await asyncio.create_subprocess_exec(*cmdargs, **kwargs)
    await p.wait()
    return p


async def wait_redis_ready(host: str, port: int, password: str = None) -> None:
    r = aioredis.from_url(f"redis://{host}:{port}", password=password, socket_timeout=0.2)
    while True:
        try:
            print("PING")
            await r.ping()
            print("PONG")
        except aioredis.exceptions.AuthenticationError:
            raise
        except (
            ConnectionResetError,
            aioredis.exceptions.ConnectionError,
        ):
            await asyncio.sleep(0.1)
        except aioredis.exceptions.TimeoutError:
            pass
        else:
            break


async def interrupt(
    disruption_method: str,
    container_id: str,
    container_addr: Tuple[str, int],
    *,
    do_pause: asyncio.Event,
    do_unpause: asyncio.Event,
    paused: asyncio.Event,
    unpaused: asyncio.Event,
    redis_password: str = None,
) -> None:
    await do_pause.wait()
    await simple_run_cmd(
        ['docker', disruptions[disruption_method]['begin'], container_id],
        # stdout=asyncio.subprocess.DEVNULL,
        # stderr=asyncio.subprocess.DEVNULL,
    )
    print(f"STOPPED {container_id[:12]}")
    paused.set()
    await do_unpause.wait()
    await simple_run_cmd(
        ['docker', disruptions[disruption_method]['end'], container_id],
        # stdout=asyncio.subprocess.DEVNULL,
        # stderr=asyncio.subprocess.DEVNULL,
    )
    await wait_redis_ready(*container_addr, password=redis_password)
    await asyncio.sleep(0.6)
    print(f"STARTED {container_id[:12]}")
    unpaused.set()


_TReturn = TypeVar('_TReturn')
_PInner = ParamSpec('_PInner')


# FIXME: mypy 0.910 does not support PEP-612 (ParamSpec) yet...

def with_timeout(t: float) -> Callable[        # type: ignore
    [Callable[_PInner, Awaitable[_TReturn]]],
    Callable[_PInner, Awaitable[_TReturn]]
]:
    def wrapper(
        corofunc: Callable[_PInner, Awaitable[_TReturn]],  # type: ignore
    ) -> Callable[_PInner, Awaitable[_TReturn]]:           # type: ignore
        @functools.wraps(corofunc)
        async def run(*args: _PInner.args, **kwargs: _PInner.kwargs) -> _TReturn:  # type: ignore
            with async_timeout.timeout(t):
                return await corofunc(*args, **kwargs)
        return run
    return wrapper
