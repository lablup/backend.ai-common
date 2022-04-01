from __future__ import annotations

import asyncio
import threading
import time
from decimal import Decimal
from pathlib import Path
from typing import (
    Any,
    Iterable,
    List,
)

import attr
import pytest

from .distributed import GlobalTimer
from .events import AbstractEvent, EventDispatcher, EventProducer
from .lock import FileLock
from .types import AgentId, EtcdRedisConfig, HostPortPair


def drange(start: Decimal, stop: Decimal, step: Decimal) -> Iterable[Decimal]:
    while start < stop:
        yield start
        start += step


def dslice(start: Decimal, stop: Decimal, num: int):
    """
    A simplified version of numpy.linspace with default options
    """
    delta = stop - start
    step = delta / (num - 1)
    yield from (start + step * Decimal(tick) for tick in range(0, num))


@attr.s(slots=True, frozen=True)
class NoopEvent(AbstractEvent):
    name = "_noop"

    test_id: str = attr.ib()

    def serialize(self) -> tuple:
        return (self.test_id, )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(value[0])


class TimerNode(threading.Thread):

    def __init__(
        self,
        interval: float,
        thread_idx: int,
        test_id: str,
        event_records: List[float],
    ) -> None:
        super().__init__()
        self.interval = interval
        self.thread_idx = thread_idx
        self.test_id = test_id
        self.event_records = event_records

    async def timer_node_async(self) -> None:
        self.loop = asyncio.get_running_loop()
        self.stop_event = asyncio.Event()

        async def _tick(context: Any, source: AgentId, event: NoopEvent) -> None:
            print("_tick")
            self.event_records.append(time.monotonic())

        redis_config = EtcdRedisConfig(addr=HostPortPair("127.0.0.1", 9379))
        event_dispatcher = await EventDispatcher.new(
            redis_config,
            node_id=self.test_id,
        )
        event_producer = await EventProducer.new(
            redis_config,
        )
        event_dispatcher.consume(NoopEvent, None, _tick)

        timer = GlobalTimer(
            FileLock(Path(f'/tmp/{self.test_id}.lock')),
            event_producer,
            lambda: NoopEvent(self.test_id),
            self.interval,
        )
        try:
            await timer.join()
            await self.stop_event.wait()
        finally:
            await timer.leave()
            await event_producer.close()
            await event_dispatcher.close()

    def run(self) -> None:
        asyncio.run(self.timer_node_async())


@pytest.mark.asyncio
async def test_global_timer(test_id) -> None:
    event_records: List[float] = []
    num_threads = 7
    num_records = 0
    delay = 3.0
    interval = 0.5
    target_count = (delay / interval)
    threads: List[TimerNode] = []
    for thread_idx in range(num_threads):
        timer_node = TimerNode(
            interval,
            thread_idx,
            test_id,
            event_records,
        )
        threads.append(timer_node)
        timer_node.start()
    print(f"spawned {num_threads} timers")
    print(threads)
    print("waiting")
    time.sleep(delay)
    print("stopping timers")
    for timer_node in threads:
        timer_node.loop.call_soon_threadsafe(timer_node.stop_event.set)
    print("joining timer threads")
    for timer_node in threads:
        timer_node.join()
    print("checking records")
    print(event_records)
    num_records = len(event_records)
    print(f"{num_records=}")
    assert target_count - 2 <= num_records <= target_count + 2


@pytest.mark.asyncio
async def test_global_timer_join_leave(test_id) -> None:

    event_records = []

    async def _tick(context: Any, source: AgentId, event: NoopEvent) -> None:
        print("_tick")
        event_records.append(time.monotonic())

    redis_config = EtcdRedisConfig(addr=HostPortPair("127.0.0.1", 9379))
    event_dispatcher = await EventDispatcher.new(
        redis_config,
        node_id=test_id,
    )
    event_producer = await EventProducer.new(
        redis_config,
    )
    event_dispatcher.consume(NoopEvent, None, _tick)

    for _ in range(10):
        timer = GlobalTimer(
            FileLock(Path(f'/tmp/{test_id}.lock')),
            event_producer,
            lambda: NoopEvent(test_id),
            0.01,
        )
        await timer.join()
        await timer.leave()

    await event_producer.close()
    await event_dispatcher.close()
