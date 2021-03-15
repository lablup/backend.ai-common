from __future__ import annotations

import abc
import asyncio
from collections import defaultdict
import functools
import logging
from typing import (
    Any,
    Callable,
    ClassVar,
    Coroutine,
    Generic,
    Mapping,
    Optional,
    Protocol,
    Sequence,
    Type,
    TypeVar,
    Union,
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
    LogSeverity,
)

__all__ = (
    'AbstractEvent',
    'EventCallback',
    'EventDispatcher',
    'EventHandler',
    'EventProducer',
)

log = BraceStyleAdapter(logging.getLogger('ai.backend.common.events'))


class AbstractEvent(metaclass=abc.ABCMeta):

    # derivatives shoudld define the fields.

    name: ClassVar[str] = "undefined"

    @abc.abstractmethod
    def serialize(self) -> tuple:
        """
        Return a msgpack-serializable tuple.
        """
        pass

    @classmethod
    @abc.abstractmethod
    def deserialize(cls, value: tuple):
        """
        Construct the event args from a tuple deserialized from msgpack.
        """
        pass


class EmptyEventArgs():

    def serialize(self) -> tuple:
        return tuple()

    @classmethod
    def deserialize(cls, value: tuple):
        return cls()


class DoScheduleEvent(EmptyEventArgs, AbstractEvent):
    name = "do_schedule"


class DoIdleCheckEvent(EmptyEventArgs, AbstractEvent):
    name = "do_idle_check"


@attr.s(slots=True, frozen=True)
class DoTerminateSessionEvent(AbstractEvent):
    name = "do_terminate_session"

    session_id: SessionId = attr.ib()
    reason: str = attr.ib()

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
        )


@attr.s(slots=True, frozen=True)
class GenericAgentEventArgs():

    reason: str = attr.ib(default='')

    def serialize(self) -> tuple:
        return (self.reason, )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(value[0])


class AgentStartedEvent(GenericAgentEventArgs, AbstractEvent):
    name = "agent_started"


class AgentTerminatedEvent(GenericAgentEventArgs, AbstractEvent):
    name = "agent_terminated"


@attr.s(slots=True, frozen=True)
class AgentErrorEvent(AbstractEvent):
    name = "agent_error"

    message: str = attr.ib()
    traceback: Optional[str] = attr.ib(default=None)
    user: Optional[Any] = attr.ib(default=None)
    context_env: Mapping[str, Any] = attr.ib(factory=dict)
    severity: LogSeverity = attr.ib(default=LogSeverity.ERROR)

    def serialize(self) -> tuple:
        return (
            self.message,
            self.traceback,
            self.user,
            self.context_env,
            str(self.severity),
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            value[0],
            value[1],
            value[2],
            value[3],
            LogSeverity(value[4]),
        )


@attr.s(slots=True, frozen=True)
class AgentHeartbeatEvent(AbstractEvent):
    name = "agent_heartbeat"

    agent_info: Mapping[str, Any] = attr.ib()

    def serialize(self) -> tuple:
        return (self.agent_info, )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(value[0])


@attr.s(slots=True, frozen=True)
class KernelCreationEventArgs():
    kernel_id: KernelId = attr.ib()
    creation_id: str = attr.ib()
    reason: str = attr.ib(default='')

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            self.creation_id,
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            kernel_id=KernelId(uuid.UUID(value[0])),
            creation_id=value[1],
            reason=value[2],
        )


class KernelEnqueuedEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_enqueued"


class KernelPreparingEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_preparing"


class KernelPullingEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_pulling"


class KernelCreatingEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_creating"


class KernelStartedEvent(KernelCreationEventArgs, AbstractEvent):
    name = "kernel_started"


@attr.s(slots=True, frozen=True)
class KernelTerminationEventArgs():
    kernel_id: KernelId = attr.ib()
    reason: str = attr.ib(default='')
    exit_code: int = attr.ib(default=-1)

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            self.reason,
            self.exit_code,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            KernelId(uuid.UUID(value[0])),
            value[1],
            value[2],
        )


class KernelCancelledEvent(KernelTerminationEventArgs, AbstractEvent):
    name = "kernel_cancelled"


class KernelTerminatingEvent(KernelTerminationEventArgs, AbstractEvent):
    name = "kernel_terminating"


class KernelTerminatedEvent(KernelTerminationEventArgs, AbstractEvent):
    name = "kernel_terminated"


@attr.s(slots=True, frozen=True)
class SessionCreationEventArgs():
    session_id: SessionId = attr.ib()
    creation_id: str = attr.ib()
    reason: str = attr.ib(default='')

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.creation_id,
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
            value[2],
        )


class SessionEnqueuedEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_enqueued"


class SessionScheduledEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_scheduled"


class SessionCancelledEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_cancelled"


class SessionStartedEvent(SessionCreationEventArgs, AbstractEvent):
    name = "session_started"


@attr.s(slots=True, frozen=True)
class SessionTerminationEventArgs():
    session_id: SessionId = attr.ib()
    reason: str = attr.ib(default='')

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.reason,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
        )


class SessionTerminatedEvent(SessionTerminationEventArgs, AbstractEvent):
    name = "session_terminated"


@attr.s(slots=True, frozen=True)
class SessionResultEventArgs():
    session_id: SessionId = attr.ib()
    reason: str = attr.ib(default='')
    exit_code: int = attr.ib(default=-1)

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
            self.reason,
            self.exit_code,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
            value[1],
            value[2],
        )


class SessionSuccessEvent(SessionResultEventArgs, AbstractEvent):
    name = "session_success"


class SessionFailureEvent(SessionResultEventArgs, AbstractEvent):
    name = "session_failure"


@attr.s(auto_attribs=True, slots=True)
class DoSyncKernelLogsEvent(AbstractEvent):
    name = "do_sync_kernel_logs"

    kernel_id: KernelId = attr.ib()
    container_id: str = attr.ib()

    def serialize(self) -> tuple:
        return (
            str(self.kernel_id),
            self.container_id,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            KernelId(uuid.UUID(value[0])),
            value[1],
        )


@attr.s(auto_attribs=True, slots=True)
class DoSyncKernelStatsEvent(AbstractEvent):
    name = "do_sync_kernel_stats"

    kernel_ids: Sequence[KernelId] = attr.ib()

    def serialize(self) -> tuple:
        return (
            [*map(str, self.kernel_ids)],
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            kernel_ids=tuple(
                KernelId(uuid.UUID(item)) for item in value[0]
            )
        )


@attr.s(auto_attribs=True, slots=True)
class GenericSessionEventArgs(AbstractEvent):
    session_id: SessionId = attr.ib()

    def serialize(self) -> tuple:
        return (
            str(self.session_id),
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            SessionId(uuid.UUID(value[0])),
        )


class ExecutionStartedEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_started"


class ExecutionFinishedEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_finished"


class ExecutionTimeoutEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_timeout"


class ExecutionCancelledEvent(GenericSessionEventArgs, AbstractEvent):
    name = "execution_cancelled"


@attr.s(auto_attribs=True, slots=True)
class BgtaskUpdatedEvent(AbstractEvent):
    name = "bgtask_updated"

    task_id: uuid.UUID = attr.ib()
    current_progress: float = attr.ib()
    total_progress: float = attr.ib()
    message: Optional[str] = attr.ib(default=None)

    def serialize(self) -> tuple:
        return (
            str(self.task_id),
            self.current_progress,
            self.total_progress,
            self.message,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            uuid.UUID(value[0]),
            value[1],
            value[2],
            value[3],
        )


@attr.s(auto_attribs=True, slots=True)
class BgtaskDoneEventArgs():
    task_id: uuid.UUID = attr.ib()
    message: Optional[str] = attr.ib(default=None)

    def serialize(self) -> tuple:
        return (
            str(self.task_id),
            self.message,
        )

    @classmethod
    def deserialize(cls, value: tuple):
        return cls(
            uuid.UUID(value[0]),
            value[1],
        )


class BgtaskDoneEvent(BgtaskDoneEventArgs, AbstractEvent):
    name = "bgtask_done"


class BgtaskCancelledEvent(BgtaskDoneEventArgs, AbstractEvent):
    name = "bgtask_cancelled"


class BgtaskFailedEvent(BgtaskDoneEventArgs, AbstractEvent):
    name = "bgtask_failed"


class RedisConnectorFunc(Protocol):
    async def __call__(
        self,
    ) -> aioredis.abc.AbcPool:
        ...


TEvent = TypeVar('TEvent', bound='AbstractEvent', contravariant=True)
TEventCov = TypeVar('TEventCov', bound='AbstractEvent')
TContext = TypeVar('TContext', contravariant=True)

EventCallback = Union[
    Callable[[TContext, AgentId, TEvent], Coroutine[Any, Any, None]],
    Callable[[TContext, AgentId, TEvent], None]
]


@attr.s(auto_attribs=True, slots=True, frozen=True, eq=False, order=False)
class EventHandler(Generic[TContext, TEvent]):
    event_cls: Type[TEvent]
    context: TContext
    callback: EventCallback[TContext, TEvent]


class EventDispatcher(aobject):
    """
    We have two types of event handlers: consumer and subscriber.

    Consumers use the distribution pattern. Only one consumer among many manager worker processes
    receives the event.

    Consumer example: database updates upon specific events.

    Subscribers use the broadcast pattern. All subscribers in many manager worker processes
    receive the same event.

    Subscriber example: enqueuing events to the queues for event streaming API handlers
    """

    consumers: defaultdict[str, set[EventHandler[Any, AbstractEvent]]]
    subscribers: defaultdict[str, set[EventHandler[Any, AbstractEvent]]]
    redis_consumer: aioredis.Redis
    redis_subscriber: aioredis.Redis
    consumer_loop_task: asyncio.Task
    subscriber_loop_task: asyncio.Task
    consumer_taskset: weakref.WeakSet[asyncio.Task]
    subscriber_taskset: weakref.WeakSet[asyncio.Task]

    _connector: RedisConnectorFunc
    _log_events: bool

    def __init__(self, connector: RedisConnectorFunc, log_events: bool = False) -> None:
        self._connector = connector
        self._log_events = log_events
        self.consumers = defaultdict(set)
        self.subscribers = defaultdict(set)

    async def __ainit__(self) -> None:
        self.redis_consumer = await self._connector()
        self.redis_subscriber = await self._connector()
        self.consumer_loop_task = asyncio.create_task(self._consume_loop())
        self.subscriber_loop_task = asyncio.create_task(self._subscribe_loop())
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
        self.redis_consumer.close()
        self.redis_subscriber.close()
        await self.redis_consumer.wait_closed()
        await self.redis_subscriber.wait_closed()

    def consume(
        self,
        event_cls: Type[TEvent],
        context: TContext,
        callback: EventCallback[TContext, TEvent],
    ) -> EventHandler[TContext, TEvent]:
        handler = EventHandler(event_cls, context, callback)
        self.consumers[event_cls.name].add(cast(EventHandler[Any, AbstractEvent], handler))
        return handler

    def unconsume(
        self,
        handler: EventHandler[TContext, TEvent],
    ) -> None:
        self.consumers[handler.event_cls.name].discard(cast(EventHandler[Any, AbstractEvent], handler))

    def subscribe(
        self,
        event_cls: Type[TEvent],
        context: TContext,
        callback: EventCallback[TContext, TEvent],
    ) -> EventHandler[TContext, TEvent]:
        handler = EventHandler(event_cls, context, callback)
        self.subscribers[event_cls.name].add(cast(EventHandler[Any, AbstractEvent], handler))
        return handler

    def unsubscribe(
        self,
        handler: EventHandler[TContext, TEvent],
    ) -> None:
        self.subscribers[handler.event_cls.name].discard(cast(EventHandler[Any, AbstractEvent], handler))

    async def dispatch_consumers(
        self,
        event_name: str,
        source: AgentId,
        args: tuple,
    ) -> None:
        if self._log_events:
            log_fmt = 'DISPATCH_CONSUMERS(ev:{}, ag:{})'
            log_args = (event_name, source)
            log.debug(log_fmt, *log_args)
        loop = asyncio.get_running_loop()
        for consumer in self.consumers[event_name]:
            cb = consumer.callback
            event_cls = consumer.event_cls
            if asyncio.iscoroutinefunction(cb):
                # mypy cannot catch the meaning of asyncio.iscoroutinefunction().
                self.consumer_taskset.add(asyncio.create_task(
                    cb(consumer.context, source, event_cls.deserialize(args))  # type: ignore
                ))
            else:
                cb = functools.partial(
                    cb, consumer.context, source, event_cls.deserialize(args),  # type: ignore
                )
                loop.call_soon(cb)

    async def dispatch_subscribers(
        self,
        event_name: str,
        source: AgentId,
        args: tuple,
    ) -> None:
        if self._log_events:
            log_fmt = 'DISPATCH_SUBSCRIBERS(ev:{}, ag:{})'
            log_args = (event_name, source)
            log.debug(log_fmt, *log_args)
        loop = asyncio.get_running_loop()
        for subscriber in self.subscribers[event_name]:
            cb = subscriber.callback
            event_cls = subscriber.event_cls
            if asyncio.iscoroutinefunction(cb):
                # mypy cannot catch the meaning of asyncio.iscoroutinefunction().
                self.subscriber_taskset.add(asyncio.create_task(
                    cb(subscriber.context, source, event_cls.deserialize(args))  # type: ignore
                ))
            else:
                cb = functools.partial(
                    cb, subscriber.context, source, event_cls.deserialize(args),  # type: ignore
                )
                loop.call_soon(cb)

    async def _consume_loop(self) -> None:
        while True:
            try:
                key, raw_msg = await redis.execute_with_retries(
                    lambda: self.redis_consumer.blpop('events.prodcons'))
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch_consumers(msg['name'],
                                              msg['source'],
                                              msg['args'])
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('EventDispatcher.consume(): unexpected-error')

    async def _subscribe_loop(self) -> None:

        async def _subscribe_impl() -> None:
            channels = await self.redis_subscriber.subscribe('events.pubsub')
            async for raw_msg in channels[0].iter():
                msg = msgpack.unpackb(raw_msg)
                await self.dispatch_subscribers(msg['name'],
                                                msg['source'],
                                                msg['args'])

        while True:
            try:
                await redis.execute_with_retries(lambda: _subscribe_impl())
            except asyncio.CancelledError:
                break
            except Exception:
                log.exception('EventDispatcher.subscribe(): unexpected-error')


class EventProducer(aobject):
    redis_producer: aioredis.Redis
    producer_lock: asyncio.Lock

    _connector: RedisConnectorFunc
    _log_events: bool

    def __init__(self, connector: RedisConnectorFunc, log_events: bool = False) -> None:
        self._connector = connector
        self._log_events = log_events

    async def __ainit__(self) -> None:
        self.redis_producer = await self._connector()
        self.producer_lock = asyncio.Lock()

    async def close(self) -> None:
        self.redis_producer.close()
        await self.redis_producer.wait_closed()

    async def produce_event(
        self,
        event: AbstractEvent,
        *,
        source: str = 'manager',
    ) -> None:
        raw_msg = msgpack.packb({
            'name': event.name,
            'source': source,
            'args': event.serialize(),
        })
        async with self.producer_lock:
            def _pipe_builder():
                pipe = self.redis_producer.pipeline()
                pipe.rpush('events.prodcons', raw_msg)
                pipe.publish('events.pubsub', raw_msg)
                return pipe
            await redis.execute_with_retries(_pipe_builder)
