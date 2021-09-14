import asyncio
from asyncio.exceptions import CancelledError
from pathlib import Path
import re
import shutil
import sys
import time
from typing import (
    AsyncIterator,
    List,
    Sequence,
    Tuple,
)

import aioredis
import aioredis.client
import aioredis.exceptions
import aioredis.sentinel
import aiotools
import pytest

from ai.backend.common import redis


async def simple_run_cmd(cmdargs: Sequence[str], **kwargs) -> asyncio.subprocess.Process:
    p = await asyncio.create_subprocess_exec(*cmdargs, **kwargs)
    await p.wait()
    return p


@pytest.fixture
async def redis_container(test_ns) -> AsyncIterator[str]:
    p = await asyncio.create_subprocess_exec(*[
        'docker', 'run',
        '-d',
        '--name', f'bai-common.{test_ns}',
        '-p', '9379:6379',
        'redis:6-alpine',
    ], stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
    assert p.stdout is not None
    stdout = await p.stdout.read()
    await p.wait()
    cid = stdout.decode().strip()
    try:
        yield cid
    finally:
        await simple_run_cmd(['docker', 'rm', '-f', cid])


@pytest.fixture
async def redis_cluster(test_ns) -> AsyncIterator[Sequence[Tuple[str, int]]]:
    cfg_dir = Path(__file__).parent / 'redis'
    if sys.platform.startswith('darwin'):
        # docker for mac
        haproxy_cfg = cfg_dir / 'haproxy.cfg'
        t = haproxy_cfg.read_bytes()
        t = t.replace(b'127.0.0.1', b'host.docker.internal')
        haproxy_cfg.write_bytes(t)
    else:
        compose_cfg = cfg_dir / 'redis-cluster.yml'
        shutil.copy(compose_cfg, compose_cfg.with_name(f'{compose_cfg.name}.bak'))
        t = compose_cfg.read_bytes()
        t = t.replace(b'host.docker.internal', b'127.0.0.1')
        t = re.sub(br'ports:\n      - \d+:\d+', b'network_mode: host', t, flags=re.M)
        compose_cfg.write_bytes(t)
    await simple_run_cmd([
        'docker-compose',
        '-p', test_ns,
        '-f', str(cfg_dir / 'redis-cluster.yml'),
        'up', '-d',
    ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await asyncio.sleep(0.2)
    await simple_run_cmd([
        'docker-compose',
        '-p', test_ns,
        '-f', str(cfg_dir / 'redis-cluster.yml'),
        'ps',
    ])
    await simple_run_cmd(['docker', 'logs', f'{test_ns}_backendai-half-redis-sentinel01_1'])
    try:
        yield [
            ('127.0.0.1', 26379),
            ('127.0.0.1', 26380),
            ('127.0.0.1', 26381),
        ]
    finally:
        await simple_run_cmd([
            'docker-compose',
            '-p', test_ns,
            '-f', str(cfg_dir / 'redis-cluster.yml'),
            'down',
        ], stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
        if sys.platform.startswith('darwin'):
            # docker for mac
            haproxy_cfg = cfg_dir / 'haproxy.cfg'
            t = haproxy_cfg.read_bytes()
            t = t.replace(b'host.docker.internal', b'127.0.0.1')
            haproxy_cfg.write_bytes(t)
        else:
            shutil.copy(compose_cfg.with_name(f'{compose_cfg.name}.bak'), compose_cfg)


@pytest.mark.asyncio
async def test_connect(redis_container: str) -> None:
    r = await redis.connect_with_retries(url='redis://localhost:9379')
    await r.ping()


@pytest.mark.asyncio
async def test_pubsub(redis_container: str) -> None:
    do_pause = asyncio.Event()
    paused = asyncio.Event()
    do_unpause = asyncio.Event()
    unpaused = asyncio.Event()
    received_messages: List[str] = []

    async def interrupt() -> None:
        await do_pause.wait()
        await simple_run_cmd(['docker', 'stop', redis_container])
        paused.set()
        await do_unpause.wait()
        await simple_run_cmd(['docker', 'start', redis_container])
        # The pub-sub channel may loose some messages while starting up.
        # Make a pause here to wait until the container actually begins to listen.
        await asyncio.sleep(0.5)
        unpaused.set()

    async def subscribe(pubsub: aioredis.client.PubSub) -> None:
        try:
            async with aiotools.aclosing(
                redis.subscribe(pubsub, reconnect_poll_interval=0.3)
            ) as agen:
                async for raw_msg in agen:
                    msg = raw_msg.decode()
                    received_messages.append(msg)
        except asyncio.CancelledError:
            pass

    r = await redis.connect_with_retries(url='redis://localhost:9379', socket_timeout=0.5)
    await r.delete("ch1")
    pubsub = r.pubsub()
    async with pubsub:
        await pubsub.subscribe("ch1")

        subscribe_task = asyncio.create_task(subscribe(pubsub))
        interrupt_task = asyncio.create_task(interrupt())
        await asyncio.sleep(0)

        for i in range(5):
            await r.publish("ch1", str(i))
            await asyncio.sleep(0.1)
        do_pause.set()
        await paused.wait()
        for i in range(5):
            # The Redis server is dead temporarily...
            with pytest.raises(aioredis.exceptions.ConnectionError):
                await r.publish("ch1", str(5 + i))
            await asyncio.sleep(0.1)
        do_unpause.set()
        await unpaused.wait()
        for i in range(5):
            await r.publish("ch1", str(10 + i))
            await asyncio.sleep(0.1)

        await interrupt_task
        subscribe_task.cancel()
        await subscribe_task
        assert subscribe_task.done()

    assert [*map(int, received_messages)] == [*range(0, 5), *range(10, 15)]


@pytest.mark.asyncio
async def test_pubsub_with_retrying_pub(redis_container: str) -> None:
    do_pause = asyncio.Event()
    paused = asyncio.Event()
    do_unpause = asyncio.Event()
    unpaused = asyncio.Event()
    received_messages: List[str] = []

    async def interrupt() -> None:
        await do_pause.wait()
        await simple_run_cmd(['docker', 'stop', redis_container])
        paused.set()
        await do_unpause.wait()
        await simple_run_cmd(['docker', 'start', redis_container])
        # The pub-sub channel may loose some messages while starting up.
        # Make a pause here to wait until the container actually begins to listen.
        await asyncio.sleep(0.5)
        unpaused.set()

    async def subscribe(pubsub: aioredis.client.PubSub) -> None:
        try:
            async with aiotools.aclosing(
                redis.subscribe(pubsub, reconnect_poll_interval=0.3)
            ) as agen:
                async for raw_msg in agen:
                    msg = raw_msg.decode()
                    received_messages.append(msg)
        except asyncio.CancelledError:
            pass

    r = await redis.connect_with_retries(url='redis://localhost:9379', socket_timeout=0.5)
    await r.delete("ch1")
    pubsub = r.pubsub()
    async with pubsub:
        await pubsub.subscribe("ch1")

        subscribe_task = asyncio.create_task(subscribe(pubsub))
        interrupt_task = asyncio.create_task(interrupt())
        await asyncio.sleep(0)

        for i in range(5):
            await redis.execute_with_retries(lambda: r.publish("ch1", str(i)))
            await asyncio.sleep(0.1)
        do_pause.set()
        await paused.wait()

        async def wakeup():
            await asyncio.sleep(0.3)
            do_unpause.set()

        wakeup_task = asyncio.create_task(wakeup())
        for i in range(5):
            await redis.execute_with_retries(lambda: r.publish("ch1", str(5 + i)))
            await asyncio.sleep(0.1)
        await wakeup_task

        await unpaused.wait()
        for i in range(5):
            await redis.execute_with_retries(lambda: r.publish("ch1", str(10 + i)))
            await asyncio.sleep(0.1)

        await interrupt_task
        subscribe_task.cancel()
        await subscribe_task
        assert subscribe_task.done()

    assert [*map(int, received_messages)] == [*range(0, 15)]


@pytest.mark.asyncio
async def test_blist(redis_container: str) -> None:
    do_pause = asyncio.Event()
    paused = asyncio.Event()
    do_unpause = asyncio.Event()
    unpaused = asyncio.Event()
    received_messages: List[str] = []

    async def interrupt() -> None:
        await do_pause.wait()
        await simple_run_cmd(['docker', 'stop', redis_container])
        paused.set()
        await do_unpause.wait()
        await simple_run_cmd(['docker', 'start', redis_container])
        # The pub-sub channel may loose some messages while starting up.
        # Make a pause here to wait until the container actually begins to listen.
        await asyncio.sleep(0.5)
        unpaused.set()

    async def pop(r: aioredis.Redis, key: str) -> None:
        try:
            async with aiotools.aclosing(
                redis.blpop(r, key, reconnect_poll_interval=0.3)
            ) as agen:
                async for raw_msg in agen:
                    msg = raw_msg.decode()
                    received_messages.append(msg)
        except asyncio.CancelledError:
            pass

    r = await redis.connect_with_retries(url='redis://localhost:9379', socket_timeout=0.5)
    await r.delete("bl1")

    pop_task = asyncio.create_task(pop(r, "bl1"))
    interrupt_task = asyncio.create_task(interrupt())
    await asyncio.sleep(0)

    for i in range(5):
        await r.rpush("bl1", str(i))
        await asyncio.sleep(0.1)
    do_pause.set()
    await paused.wait()
    for i in range(5):
        # The Redis server is dead temporarily...
        with pytest.raises(aioredis.exceptions.ConnectionError):
            await r.rpush("bl1", str(5 + i))
        await asyncio.sleep(0.1)
    do_unpause.set()
    await unpaused.wait()
    for i in range(5):
        await r.rpush("bl1", str(10 + i))
        await asyncio.sleep(0.1)

    await interrupt_task
    pop_task.cancel()
    await pop_task
    assert pop_task.done()

    assert [*map(int, received_messages)] == [*range(0, 5), *range(10, 15)]


@pytest.mark.asyncio
async def test_blist_with_retrying_rpush(redis_container: str) -> None:
    do_pause = asyncio.Event()
    paused = asyncio.Event()
    do_unpause = asyncio.Event()
    unpaused = asyncio.Event()
    received_messages: List[str] = []

    async def interrupt() -> None:
        await do_pause.wait()
        await simple_run_cmd(['docker', 'stop', redis_container])
        paused.set()
        await do_unpause.wait()
        await simple_run_cmd(['docker', 'start', redis_container])
        # The pub-sub channel may loose some messages while starting up.
        # Make a pause here to wait until the container actually begins to listen.
        await asyncio.sleep(0.5)
        unpaused.set()

    async def pop(r: aioredis.Redis, key: str) -> None:
        try:
            async with aiotools.aclosing(
                redis.blpop(r, key, reconnect_poll_interval=0.3)
            ) as agen:
                async for raw_msg in agen:
                    msg = raw_msg.decode()
                    received_messages.append(msg)
        except asyncio.CancelledError:
            pass

    r = await redis.connect_with_retries(url='redis://localhost:9379', socket_timeout=0.5)
    await r.delete("bl1")

    pop_task = asyncio.create_task(pop(r, "bl1"))
    interrupt_task = asyncio.create_task(interrupt())
    await asyncio.sleep(0)

    for i in range(5):
        await redis.execute_with_retries(lambda: r.rpush("bl1", str(i)))
        await asyncio.sleep(0.1)
    do_pause.set()
    await paused.wait()

    async def wakeup():
        await asyncio.sleep(0.3)
        do_unpause.set()

    wakeup_task = asyncio.create_task(wakeup())
    for i in range(5):
        await redis.execute_with_retries(lambda: r.rpush("bl1", str(5 + i)))
        await asyncio.sleep(0.1)
    await wakeup_task

    await unpaused.wait()
    for i in range(5):
        await redis.execute_with_retries(lambda: r.rpush("bl1", str(10 + i)))
        await asyncio.sleep(0.1)

    await interrupt_task
    pop_task.cancel()
    await pop_task
    assert pop_task.done()

    assert [*map(int, received_messages)] == [*range(0, 15)]


@pytest.mark.asyncio
async def test_connect_cluster_haproxy(redis_cluster) -> None:
    with pytest.raises(aioredis.exceptions.AuthenticationError):
        await redis.connect_with_retries(
            url='redis://localhost:9379',
            password='develove',
        )
    r = await redis.connect_with_retries(
        url='redis://localhost:9379',
        password='develove',
    )
    await r.ping()


# @pytest.mark.asyncio
# async def test_pubsub_cluster_haproxy(redis_cluster):
#     pass
#
#
# @pytest.mark.asyncio
# async def test_blist_cluster_haproxy(redis_cluster):
#     pass


@pytest.mark.asyncio
async def test_connect_cluster_sentinel(redis_cluster):
    s = aioredis.sentinel.Sentinel(
        redis_cluster,
        password='develove',
        socket_timeout=0.5,
    )
    master = s.master_for('mymaster', db=9)
    await master.ping()
    slave = s.slave_for('mymaster', db=9)
    await slave.ping()


# @pytest.mark.asyncio
# async def test_pubsub_cluster_sentinel(redis_cluster):
#     pass
#
#
# @pytest.mark.asyncio
# async def test_blist_cluster_sentinel(redis_cluster):
#     pass
