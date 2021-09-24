import asyncio
import traceback
from typing import (
    Final,
    Sequence,
    Tuple,
)

import aioredis
import aioredis.exceptions


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


async def simple_run_cmd(cmdargs: Sequence[str], **kwargs) -> asyncio.subprocess.Process:
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
        #stdout=asyncio.subprocess.DEVNULL,
        #stderr=asyncio.subprocess.DEVNULL,
    )
    print(f"STOPPED {container_id[:12]}")
    paused.set()
    await do_unpause.wait()
    await simple_run_cmd(
        ['docker', disruptions[disruption_method]['end'], container_id],
        #stdout=asyncio.subprocess.DEVNULL,
        #stderr=asyncio.subprocess.DEVNULL,
    )
    await wait_redis_ready(*container_addr, password=redis_password)
    print(f"STARTED {container_id[:12]}")
    unpaused.set()
