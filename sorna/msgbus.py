'''
A base class for event streaming channel.
The underlying broker is RabbitMQ.

It is encouraged to subclass and add your own notification methods.
'''

import asyncio

import aioamqp

from .argparse import HostPortPair


class MsgChannel:

    name = 'default'

    def __init__(self, broker_addr):
        self.transport = None
        self.protocol = None
        assert isinstance(broker_addr, HostPortPair)
        self.addr = broker_addr

    async def init(self, login, password, vhost):
        self.transport, self.protocol = await aioamqp.connect(
            *self.addr.as_sockaddr(),
            login=login, password=password, virtualhost=vhost)
        self.channel = await self.protocol.channel()
        await self.channel.queue_declare(queue_name=self.name)

    async def close(self):
        await self.protocol.close()
        self.transport.close()

    async def publish(self, msg):
        pass

    async def bind(self):
        pass
