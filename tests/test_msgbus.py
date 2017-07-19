import asyncio
import os

import pytest

from sorna.common.argparse import host_port_pair
from sorna.common.msgbus import Publisher, Subscriber, ExchangeTypes


@pytest.fixture
def mq_args():
    mq_addr = host_port_pair(os.environ.get('SORNA_MQ_ADDR', 'localhost:5682'))
    mq_user = os.environ.get('SORNA_MQ_LOGIN', 'sorna')
    mq_pass = os.environ.get('SORNA_MQ_PASS', 'develove')
    mq_vhost = os.environ.get('SORNA_NAMESPACE', 'local')
    return mq_addr, mq_user, mq_pass, mq_vhost


class BasicPublisher(Publisher):
    exchange_name = 'testing'
    exchange_type = ExchangeTypes.DIRECT


class BasicSubscriber(Subscriber):
    exchange_name = 'testing'
    exchange_type = ExchangeTypes.DIRECT
    queue_name = 'dummy'


@pytest.fixture
def publisher(event_loop, mq_args):
    mq_addr, mq_user, mq_pass, mq_vhost = mq_args
    pub = BasicPublisher(mq_addr)

    async def init():
        await pub.init(user=mq_user, passwd=mq_pass, vhost=mq_vhost)

    async def shutdown():
        await pub.close()

    event_loop.run_until_complete(init())
    try:
        yield pub
    finally:
        event_loop.run_until_complete(shutdown())


@pytest.fixture
def subscriber(event_loop, mq_args):
    mq_addr, mq_user, mq_pass, mq_vhost = mq_args
    sub = BasicSubscriber(mq_addr, topic='dummy')

    async def init():
        await sub.init(user=mq_user, passwd=mq_pass, vhost=mq_vhost)

    async def shutdown():
        await sub.close()

    event_loop.run_until_complete(init())
    try:
        yield sub
    finally:
        event_loop.run_until_complete(shutdown())


@pytest.mark.asyncio
async def test_pub_sub(publisher, subscriber):
    pub = publisher
    sub = subscriber
    recv = []

    assert sub.consumer_tag != ''

    async def cb(body, envelope, props):
        await asyncio.sleep(0.05)
        recv.append(body)

    # subscriber can attach multiple callbacks.
    await sub.subscribe(cb)

    assert sub._subscribing

    await sub.subscribe(cb)
    await pub.publish({'a': 42}, routing_key='dummy')

    await asyncio.sleep(0.2)

    assert len(recv) == 2
    assert recv[0]['a'] == 42
    assert recv[1]['a'] == 42


@pytest.mark.asyncio
async def test_pub_sub_cb_error(publisher, subscriber):
    pub = publisher
    sub = subscriber
    recv = []
    idx = [1, 2, 3, 4]

    assert sub.consumer_tag != ''

    async def cb(body, envelope, props):
        my_idx = idx.pop(0)
        await asyncio.sleep(0.05)
        if my_idx == 2:
            raise ZeroDivisionError
        recv.append(body)

    await sub.subscribe(cb)
    await sub.subscribe(cb)
    await sub.subscribe(cb)
    await sub.subscribe(cb)
    await pub.publish({'a': 42}, routing_key='dummy')

    await asyncio.sleep(0.2)

    # only successful callbacks records their results.
    assert len(recv) == 3
    assert recv[0]['a'] == 42
    assert recv[1]['a'] == 42
    assert recv[2]['a'] == 42
