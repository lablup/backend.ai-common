'''
A base class for event streaming channel.
The underlying broker is RabbitMQ.

It is encouraged to subclass and add your own notification methods.
'''

import asyncio
import enum
import json
import os
import secrets
from typing import Optional, Any
import warnings

import aioamqp
# from aioamqp.envelope import Envelope
# from aioamqp.properties import Properties

from .argparse import HostPortPair


class ExchangeTypes(enum.Enum):
    DIRECT = 'direct'
    FANOUT = 'fanout'
    TOPIC = 'topic'
    HEADERS = 'headers'


class AMQPBroker:

    '''
    AMQP API wrapper based on aioamqp.
    '''

    exchange_name: str = ''  # uses the default direct exchange if empty
    exchange_type: ExchangeTypes = ExchangeTypes.DIRECT

    def __init__(self, broker_addr, loop=None):
        self.transport = None
        self.protocol = None
        self.channel = None
        self.loop = loop if loop else asyncio.get_event_loop()
        assert isinstance(broker_addr, HostPortPair)
        self.broker_addr = broker_addr

    async def init(self, *, user=None, passwd=None, vhost=None):
        # Create a channel.
        self.transport, self.protocol = await aioamqp.connect(
            *self.broker_addr.as_sockaddr(),
            login=user, password=passwd, virtualhost=vhost)
        self.channel = await self.protocol.channel()

        # Declare the exchange and queue.
        if self.exchange_name == '':
            assert self.exchange_type == ExchangeTypes.DIRECT
        if self.exchange_type == ExchangeTypes.FANOUT and hasattr(self, 'topic'):
            warnings.warn('fanout exchanges will ignore routing keys', RuntimeWarning)
        await self.channel.exchange_declare(self.exchange_name, self.exchange_type.value)

    async def close(self):
        await self.channel.close()
        await self.protocol.close()
        self.transport.close()


class Publisher(AMQPBroker):

    def __init__(self, *args, encoder=json.dumps, **kwargs):
        super().__init__(*args, **kwargs)
        self.encoder = encoder

    async def publish(self, msg: Any,
                      routing_key: str=''):
        await self.channel.publish(
            self.encoder(msg),
            exchange_name=self.exchange_name,
            routing_key=routing_key
        )


class Subscriber(AMQPBroker):

    queue_name: str = ''  # random-generated if empty
    prefetch_count: int = 0  # per-consumer prefetch limit

    def __init__(self, *args, topic: Optional[str]=None, decoder=json.loads, **kwargs):
        super().__init__(*args, **kwargs)
        self.topic = topic
        self.decoder = decoder
        hash = secrets.token_hex(6)
        pid = os.getpid()
        self._consumer_tag = f'ctag:{self.queue_name}@{pid}.{hash}'
        self._subscribers = []
        self._subscribing = False

    @property
    def consumer_tag(self):
        return self._consumer_tag

    async def init(self, *args, **kwargs):
        await super().init(*args, **kwargs)
        await self.channel.basic_qos(
            prefetch_count=self.prefetch_count,
            connection_global=False)
        result = await self.channel.queue_declare(self.queue_name)
        if self.queue_name is None:
            self.queue_name = result['queue']
        await self.channel.queue_bind(self.queue_name, self.exchange_name, self.topic)

    async def close(self):
        await self.channel.basic_cancel(self._consumer_tag, no_wait=True)
        await super().close()

    async def _cb_wrap(self, channel, body, envelope, props):
        body = self.decoder(body)
        tasks = []
        for cb in self._subscribers:
            tasks.append(self.loop.create_task(cb(body, envelope, props)))
        await asyncio.gather(*tasks)

    async def subscribe(self, cb):
        self._subscribers.append(cb)
        if not self._subscribing:
            self._subscribing = True
            opts = {
                'queue_name': self.queue_name,
                'no_ack': True,
                'no_local': True,
                'consumer_tag': self._consumer_tag,
            }
            await self.channel.basic_consume(self._cb_wrap, **opts)
