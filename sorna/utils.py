#! /usr/bin/env python3
import asyncio
import aiohttp

async def curl(url, default_value=None, loop=None):
    try:
        with aiohttp.Timeout(0.2):
            resp = await aiohttp.get(url, loop=loop)
            if resp.status == 200:
                body = await resp.text()
                return body.strip()
            else:
                return default_value
    except (asyncio.TimeoutError, aiohttp.errors.ClientOSError):
        return default_value

async def get_instance_id(loop=None):
    return (await curl('http://169.254.169.254/latest/meta-data/instance-id', 'i-00000000', loop=loop))

async def get_instance_ip(loop=None):
    return (await curl('http://169.254.169.254/latest/meta-data/local-ipv4', '127.0.0.1', loop=loop))

async def get_instance_type(loop=None):
    return (await curl('http://169.254.169.254/latest/meta-data/instance-type', 'unknown', loop=loop))


class AsyncBarrier:
    '''
    This class provides a simplified asyncio-version of threading.Barrier class.
    '''

    num_parties = 1
    loop = None
    cond = None

    def __init__(self, num_parties, loop=None):
        self.num_parties = num_parties
        self.count = 0
        self.loop = loop if loop else asyncio.get_event_loop()
        self.cond = asyncio.Condition(loop=self.loop)

    async def wait(self):
        async with self.cond:
            self.count += 1
            if self.count == self.num_parties:
                self.cond.notify_all()
            else:
                while self.count < self.num_parties:
                    await self.cond.wait()

    def reset(self):
        self.count = 0
        # FIXME: if there are waiting coroutines, let them
        #        raise BrokenBarrierError like threading.Barrier

