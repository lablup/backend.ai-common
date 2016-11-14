#! /usr/bin/env python3
import asyncio
import base64
from collections import OrderedDict
import socket
import uuid

import aiohttp


def odict(*args):
    '''
    A short-hand for the constructor of OrderedDict.
    :code:`odict(('a':1), ('b':2))` is equivalent to
    :code:`OrderedDict([('a':1), ('b':2)])`.
    '''
    return OrderedDict(args)

def generate_uuid():
    u = uuid.uuid4()
    # Strip the last two padding characters because u always has fixed length.
    return base64.urlsafe_b64encode(u.bytes)[:-2].decode('ascii')

def nmget(o, key_path, def_val=None, path_delimiter='.'):
    '''
    A short-hand for retrieving a value from nested mappings
    ("nested-mapping-get").  At each level it checks if the given "path"
    component in the given key exists and return the default value whenever
    fails.

    Example:
    >>> o = {'a':{'b':1}}
    >>> nmget(o, 'a', 0)
    {'b': 1}
    >>> nmget(o, 'a.b', 0)
    1
    >>> nmget(o, 'a/b', 0, '/')
    1
    >>> nmget(o, 'a.c', 0)
    0
    '''
    pieces = key_path.split(path_delimiter)
    while pieces:
        p = pieces.pop(0)
        if o is None or p not in o:
            return def_val
        o = o[p]
    return o


async def curl(url, default_value=None, loop=None, timeout=0.2):
    try:
        with aiohttp.ClientSession(loop=loop) as sess:
            async with sess.get(url, timeout=timeout) as resp:
                if resp.status == 200:
                    body = await resp.text()
                    return body.strip()
                else:
                    return default_value
    except (asyncio.TimeoutError, aiohttp.errors.ClientOSError):
        return default_value

async def get_instance_id(loop=None):
    return (await curl('http://169.254.169.254/latest/meta-data/instance-id', 'i-{}'.format(socket.gethostname()), loop=loop))

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

