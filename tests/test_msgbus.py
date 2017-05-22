import asyncio

import pytest

from sorna.argparse import host_port_pair
from sorna.msgbus import MsgChannel


@pytest.mark.asyncio
async def test_channel():
    c = MsgChannel(host_port_pair('localhost:5682'))
    await c.init('sorna', 'develove', 'local')
    await c.close()
