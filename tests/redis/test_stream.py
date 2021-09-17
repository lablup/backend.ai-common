from __future__ import annotations

from typing import (
    Dict,
    List,
)

import aioredis
import aioredis.client
import aioredis.exceptions
import aioredis.sentinel
import aiotools
import pytest

from ai.backend.common import redis

from .types import RedisClusterInfo, disruptions
from .utils import simple_run_cmd


import asyncio


@pytest.mark.asyncio
@pytest.mark.parametrize("disruption_method", ['stop', 'pause'])
async def test_stream_fanout(redis_container: str, disruption_method: str) -> None:
    do_pause = asyncio.Event()
    paused = asyncio.Event()
    do_unpause = asyncio.Event()
    unpaused = asyncio.Event()
    received_messages: Dict[str, List[str]] = {
        "c1": [],
        "c2": [],
    }

    async def interrupt() -> None:
        await do_pause.wait()
        await simple_run_cmd(['docker', disruptions[disruption_method]['begin'], redis_container])
        paused.set()
        await do_unpause.wait()
        await simple_run_cmd(['docker', disruptions[disruption_method]['end'], redis_container])
        # The pub-sub channel may loose some messages while starting up.
        # Make a pause here to wait until the container actually begins to listen.
        await asyncio.sleep(0.5)
        unpaused.set()

    async def consume(
        consumer_id: str,
        r: aioredis.Redis | aioredis.sentinel.Sentinel,
        key: str,
        group: str,
    ) -> None:
        last_id = b'0-0'
        while True:
            try:
                reply = await redis.execute(
                    r,
                    lambda r: r.xread({key: last_id}, block=2000),
                )
                if reply is None:
                    continue
                assert reply[0][0].decode() == key
                for msg_id, msg_data in reply[0][1]:
                    print(f"XREAD[{group}:{consumer_id}]", msg_id, repr(msg_data))
                    received_messages[consumer_id].append(msg_data[b"idx"])
                    last_id = msg_id
            except asyncio.CancelledError:
                return

    r = aioredis.from_url(url='redis://localhost:9379', socket_timeout=0.5)
    await r.delete("stream1")

    consumer_tasks = [
        asyncio.create_task(consume("c1", r, "stream1", "group1")),
        asyncio.create_task(consume("c2", r, "stream1", "group1")),
    ]
    interrupt_task = asyncio.create_task(interrupt())
    await asyncio.sleep(0)

    for i in range(5):
        await r.xadd("stream1", {"idx": i})
        await asyncio.sleep(0.1)
    do_pause.set()
    await paused.wait()
    for i in range(5):
        # The Redis server is dead temporarily...
        if disruption_method == 'stop':
            with pytest.raises(aioredis.exceptions.ConnectionError):
                await r.xadd("stream1", {"idx": 5 + i})
        elif disruption_method == 'pause':
            with pytest.raises(asyncio.TimeoutError):
                await r.xadd("stream1", {"idx": 5 + i})
        else:
            raise RuntimeError("should not reach here")
        await asyncio.sleep(0.1)
    do_unpause.set()
    await unpaused.wait()
    for i in range(5):
        await r.xadd("stream1", {"idx": 10 + i})
        await asyncio.sleep(0.1)

    await interrupt_task
    for t in consumer_tasks:
        t.cancel()
        await t
    for t in consumer_tasks:
        assert t.done()

    if disruption_method == "stop":
        assert [*map(int, received_messages["c1"])] == [*range(0, 5), *range(10, 15)]
        assert [*map(int, received_messages["c2"])] == [*range(0, 5), *range(10, 15)]
    else:
        # pause keeps the TCP connection and the messages are delivered late.
        assert [*map(int, received_messages["c1"])] == [*range(0, 15)]
        assert [*map(int, received_messages["c2"])] == [*range(0, 15)]


@pytest.mark.asyncio
@pytest.mark.parametrize("disruption_method", ['stop', 'pause'])
async def test_stream_fanout_cluster(redis_cluster: RedisClusterInfo, disruption_method: str) -> None:
    do_pause = asyncio.Event()
    paused = asyncio.Event()
    do_unpause = asyncio.Event()
    unpaused = asyncio.Event()
    received_messages: Dict[str, List[str]] = {
        "c1": [],
        "c2": [],
    }

    async def interrupt() -> None:
        await do_pause.wait()
        await simple_run_cmd(['docker', disruptions[disruption_method]['begin'], redis_cluster.worker_containers[0]])
        paused.set()
        await do_unpause.wait()
        await simple_run_cmd(['docker', disruptions[disruption_method]['end'], redis_cluster.worker_containers[0]])
        # The pub-sub channel may loose some messages while starting up.
        # Make a pause here to wait until the container actually begins to listen.
        await asyncio.sleep(0.5)
        unpaused.set()

    async def consume(
        consumer_id: str,
        r: aioredis.Redis | aioredis.sentinel.Sentinel,
        key: str,
        group: str,
    ) -> None:
        last_id = b'0-0'
        while True:
            try:
                reply = await redis.execute(
                    r,
                    lambda r: r.xread({key: last_id}, block=2000),
                    service_name="mymaster",
                )
                if reply is None:
                    continue
                assert reply[0][0].decode() == key
                for msg_id, msg_data in reply[0][1]:
                    print(f"XREAD[{group}:{consumer_id}]", msg_id, repr(msg_data))
                    received_messages[consumer_id].append(msg_data[b"idx"])
                    last_id = msg_id
            except asyncio.CancelledError:
                return

    s = aioredis.sentinel.Sentinel(
        redis_cluster.sentinel_addrs,
        password='develove',
        socket_timeout=0.5,
    )
    await redis.execute(s, lambda r: r.delete("stream1"), service_name="mymaster")

    consumer_tasks = [
        asyncio.create_task(consume("c1", s, "stream1", "group1")),
        asyncio.create_task(consume("c2", s, "stream1", "group1")),
    ]
    interrupt_task = asyncio.create_task(interrupt())
    await asyncio.sleep(0)

    for i in range(5):
        await redis.execute(s, lambda r: r.xadd("stream1", {"idx": i}), service_name="mymaster")
        await asyncio.sleep(0.1)
    do_pause.set()
    await paused.wait()
    for i in range(5):
        # The Redis server is dead temporarily...
        if disruption_method == 'stop':
            await redis.execute(s, lambda r: r.xadd("stream1", {"idx": 5 + i}), service_name="mymaster")
        elif disruption_method == 'pause':
            await redis.execute(s, lambda r: r.xadd("stream1", {"idx": 5 + i}), service_name="mymaster")
        else:
            raise RuntimeError("should not reach here")
        await asyncio.sleep(0.1)
    do_unpause.set()
    await unpaused.wait()
    for i in range(5):
        await redis.execute(s, lambda r: r.xadd("stream1", {"idx": 10 + i}), service_name="mymaster")
        await asyncio.sleep(0.1)

    await interrupt_task
    for t in consumer_tasks:
        t.cancel()
        await t
    for t in consumer_tasks:
        assert t.done()

    assert [*map(int, received_messages["c1"])] == [*range(0, 15)]
    assert [*map(int, received_messages["c2"])] == [*range(0, 15)]
