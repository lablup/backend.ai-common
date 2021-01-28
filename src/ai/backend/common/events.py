from __future__ import annotations

import abc
import asyncio
from collections import defaultdict
import functools
import logging
from typing import (
    Any,
    Awaitable,
    Generic,
    Optional,
    Protocol,
    Sequence,
    Type,
    TypeVar,
    cast,
)
import uuid
import weakref

import aioredis
import attr

from . import msgpack, redis
from .logging import BraceStyleAdapter
from .types import (
    aobject,
    AgentId,
    KernelId,
    SessionId,
)

__all__ = (
    'EventArgs',
    'EventCallback',
    'EventDispatcher',
    'EventHandler',
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.common.events'))


TArgs = TypeVar('TArgs', bound='EventArgs')


@attr.s(auto_attribs=True, slots=True)
class EventArgs(metaclass=abc.ABCMeta):

    # derivatives shoudld define the fields.

    @abc.abstractmethod
    def serialize(self) -> tuple:
        """
        Return a msgpack-serializable tuple.
        """
        pass

    @abc.abstractmethod
    @classmethod
    def deserialize(cls: Type[TArgs], value: tuple) -> TArgs:
        """
        Construct the event args from a tuple deserialized from msgpack.
        """
        pass


@attr.s(auto_attribs=True, slots=True)
class KernelCreationEventArgs(EventArgs):
    kernel_id: KernelId
    creation_id: str
    reason: str = ''

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            self.creation_id,
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple) -> KernelCreationEventArgs:
        return cls(
            kernel_id=KernelId(uuid.UUID(value[0])),
            creation_id=value[1],
            reason=value[2],
        )


@attr.s(auto_attribs=True, slots=True)
class KernelTerminationEventArgs(EventArgs):
    kernel_id: KernelId
    reason: str = ''
    exit_code: Optional[int] = None

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            self.reason,
            self.exit_code,
        )

    @classmethod
    def deserialize(cls, value: tuple) -> KernelTerminationEventArgs:
        return cls(
            KernelId(uuid.UUID(value[0])),
            value[1],
            value[2],
        )


@attr.s(auto_attribs=True, slots=True)
class SessionCreationEventArgs(EventArgs):
    session_id: SessionId
    creation_id: str
    reason: str = ''

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.creation_id,
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple) -> SessionCreationEventArgs:
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
            value[2],
        )


@attr.s(auto_attribs=True, slots=True)
class SessionTerminationEventArgs(EventArgs):
    session_id: SessionId
    reason: str = ''

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple) -> SessionTerminationEventArgs:
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
        )


@attr.s(auto_attribs=True, slots=True)
class KernelStatSyncEventArgs(EventArgs):
    kernel_ids: Sequence[KernelId]

    def serialize(self) -> tuple:
        return (
            [*map(str, self.kernel_ids)],
        )

    @classmethod
    def deserialize(cls, value: tuple) -> KernelStatSyncEventArgs:
        return cls(
            kernel_ids=tuple(
                KernelId(uuid.UUID(item)) for item in value[0]
            )
        )


class RedisConnectorFunc(Protocol):
    async def __call__(
        self,
    ) -> aioredis.abc.AbcPool:
        ...


C = TypeVar('C', bound=object, contravariant=True)
TA = TypeVar('TA', bound=EventArgs, contravariant=True)


class EventCallback(Protocol[C, TA]):
    async def __call__(
        self,
        context: C,
        agent_id: AgentId,
        event_name: str,
        args: TA,
    ) -> None:
        ...


@attr.s(auto_attribs=True, slots=True, frozen=True, eq=False, order=False)
class EventHandler(Generic[C, TArgs]):
    context: C
    callback: EventCallback[C, TArgs]
    argtype: Type[TArgs]


class EventDispatcher(aobject):
    '''
    We have two types of event handlers: consumer and subscriber.

    Consumers use the distribution pattern. Only one consumer among many manager worker processes
    receives the event.

    Consumer example: database updates upon specific events.

    Subscribers use the broadcast pattern. All subscribers in many manager worker processes
    receive the same event.

    Subscriber example: enqueuing events to the queues for event streaming API handlers
    '''

    consumers: defaultdict[str, set[EventHandler[Any, EventArgs]]]
    subscribers: defaultdict[str, set[EventHandler[Any, EventArgs]]]
    redis_producer: aioredis.Redis
    redis_consumer: aioredis.Redis
    redis_subscriber: aioredis.Redis
    consumer_loop_task: asyncio.Task
    subscriber_loop_task: asyncio.Task
    producer_lock: asyncio.Lock
    consumer_taskset: weakref.WeakSet[asyncio.Task]
    subscriber_taskset: weakref.WeakSet[asyncio.Task]

    def __init__(self, connector: RedisConnectorFunc, log_events: bool = False) -> None:
        self._connector = connector
        self._log_events = log_events
        self.consumers = defaultdict(set)
        self.subscribers = defaultdict(set)

    async def __ainit__(self) -> None:
        self.redis_producer = await self._connector()
        self.redis_consumer = await self._connector()
        self.redis_subscriber = await self._connector()
        self.consumer_loop_task = asyncio.create_task(self._consume_loop())
        self.subscriber_loop_task = asyncio.create_task(self._subscribe_loop())
        self.producer_lock = asyncio.Lock()
        self.consumer_taskset = weakref.WeakSet()
        self.subscriber_taskset = weakref.WeakSet()

    async def close(self) -> None:
        cancelled_tasks = []
        for task in self.consumer_taskset:
            if not task.done():
                task.cancel()
                cancelled_tasks.append(task)
        for task in self.subscriber_taskset:
            if not task.done():
                task.cancel()
                cancelled_tasks.append(task)
        self.consumer_loop_task.cancel()
        self.subscriber_loop_task.cancel()
        cancelled_tasks.append(self.consumer_loop_task)
        cancelled_tasks.append(self.subscriber_loop_task)
        await asyncio.gather(*cancelled_tasks, return_exceptions=True)
        self.redis_producer.close()
        self.redis_consumer.close()
        self.redis_subscriber.close()
        await self.redis_producer.wait_closed()
        await self.redis_consumer.wait_closed()
        await self.redis_subscriber.wait_closed()

    def consume(
        self,
        event_name: str,
        context: C,
        callback: EventCallback[C, TArgs],
        argtype: Type[TArgs],
    ) -> EventHandler[C, TArgs]:
        handler = EventHandler(context, callback, argtype)
        self.consumers[event_name].add(cast(EventHandler[Any, EventArgs], handler))
        return handler

    def unconsume(
        self,
        event_name: str,
        handler: EventHandler[Any, EventArgs],
    ) -> None:
        self.consumers[event_name].discard(handler)

    def subscribe(
        self,
        event_name: str,
        context: C,
        callback: EventCallback[C, TArgs],
        argtype: Type[TArgs],
    ) -> EventHandler[C, TArgs]:
        handler = EventHandler(context, callback, argtype)
        self.subscribers[event_name].add(cast(EventHandler[Any, EventArgs], handler))
        return handler

    def unsubscribe(
        self,
        event_name: str,
        handler: EventHandler[Any, EventArgs],
    ) -> None:
        self.subscribers[event_name].discard(handler)

    async def produce_event(
        self,
        event_name: str,
        args: EventArgs,
        *,
        agent_id: str = 'manager',
    ) -> None:
        raw_msg = msgpack.packb({
            'event_name': event_name,
            'agent_id': agent_id,
            'args': args.serialize(),
        })
        async with self.producer_lock:
            def _pipe_builder():
                pipe = self.redis_producer.pipeline()
                pipe.rpush('events.prodcons', raw_msg)
                pipe.publish('events.pubsub', raw_msg)
                return pipe
            await redis.execute_with_retries(_pipe_builder)

    async def dispatch_consumers(
        self,
        event_name: str,
        agent_id: AgentId,
        args: tuple,
    ) -> None:
        log_fmt = 'DISPATCH_CONSUMERS(ev:{}, ag:{})'
        log_args = (event_name, agent_id)
        if self._log_events:
            log.debug(log_fmt, *log_args)
        loop = asyncio.get_running_loop()
        for consumer in self.consumers[event_name]:
            cb = consumer.callback
            argtype = consumer.argtype
            if asyncio.iscoroutine(cb):
                self.consumer_taskset.add(asyncio.create_task(cast(Awaitable, cb)))
            elif asyncio.iscoroutinefunction(cb):
                self.consumer_taskset.add(asyncio.create_task(
                    cb(consumer.context, agent_id, event_name, argtype.deserialize(args))
                ))
            else:
                cb = functools.partial(
                    cb, consumer.context, agent_id, event_name, argtype.deserialize(args),
                )
                loop.call_soon(cb)

    async def dispatch_subscribers(
        self,
        event_name: str,
        agent_id: AgentId,
        args: tuple,
    ) -> None:
        log_fmt = 'DISPATCH_SUBSCRIBERS(ev:{}, ag:{})'
        log_args = (event_name, agent_id)
        if self._log_events:
            log.debug(log_fmt, *log_args)
        loop = asyncio.get_running_loop()
        for subscriber in self.subscribers[event_name]:
            cb = subscriber.callback
            argtype = subscriber.argtype
            if asyncio.iscoroutine(cb):
                self.subscriber_taskset.add(asyncio.create_task(cast(Awaitable, cb)))
            elif asyncio.iscoroutinefunction(cb):
                self.subscriber_taskset.add(asyncio.create_task(
                    cb(subscriber.context, agent_id, event_name, argtype.deserialize(args))
                ))
            else:
                cb = functools.partial(
                    cb, subscriber.context, agent_id, event_name, argtype.deserialize(args),
                )
                loop.call_soon(cb)

    async def _consume_loop(self) -> None:
        while True:
            try:
                key, raw_msg = await redis.execute_with_retries(
                    lambda: self.redis_consumer.blpop('events.prodcons'))
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch_consumers(msg['event_name'],
                                              msg['agent_id'],
                                              msg['args'])
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('EventDispatcher.consume(): unexpected-error')

    async def _subscribe_loop(self) -> None:

        async def _subscribe_impl():
            channels = await self.redis_subscriber.subscribe('events.pubsub')
            async for raw_msg in channels[0].iter():
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch_subscribers(msg['event_name'],
                                                msg['agent_id'],
                                                msg['args'])

        while True:
            try:
                await redis.execute_with_retries(lambda: _subscribe_impl())
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('EventDispatcher.subscribe(): unexpected-error')
