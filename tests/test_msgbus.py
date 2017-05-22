import asyncio
import os

import pytest

from sorna.argparse import host_port_pair
from sorna.msgbus import MsgChannel


@pytest.fixture
async def channel():
    addr = os.environ.get('SORNA_MQ_ADDR', 'localhost:5682')
    c = MsgChannel(host_port_pair(addr))
    await c.init(
        os.environ.get('SORNA_MQ_LOGIN', 'sorna'),
        os.environ.get('SORNA_MQ_PASS', 'develove'),
        os.environ.get('SORNA_NAMESPACE', 'local'))
    return c


@pytest.mark.asyncio
async def test_channel(channel):
    c = await channel
    await c.close()
