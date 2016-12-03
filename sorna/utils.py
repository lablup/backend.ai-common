#! /usr/bin/env python3
import asyncio
import base64
from collections import OrderedDict
from itertools import chain
import numbers
import socket
import uuid

import aiohttp
from async_timeout import timeout as _timeout


def odict(*args):
    '''
    A short-hand for the constructor of OrderedDict.
    :code:`odict(('a':1), ('b':2))` is equivalent to
    :code:`OrderedDict([('a':1), ('b':2)])`.
    '''
    return OrderedDict(args)


def dict2kvlist(o):
    '''
    Serializes a dict-like object into a generator of the flatten list of
    repeating key-value pairs.  It is useful when using HMSET method in Redis.

    Example:
    >>> list(dict2kvlist({'a': 1, 'b': 2}))
    ['a', 1, 'b', 2]
    '''
    return chain.from_iterable((k, v) for k, v in o.items())


def generate_uuid():
    u = uuid.uuid4()
    # Strip the last two padding characters because u always has fixed length.
    return base64.urlsafe_b64encode(u.bytes)[:-2].decode('ascii')


def nmget(o, key_path, def_val=None, path_delimiter='.', null_as_default=True):
    '''
    A short-hand for retrieving a value from nested mappings
    ("nested-mapping-get").  At each level it checks if the given "path"
    component in the given key exists and return the default value whenever
    fails.

    Example:
    >>> o = {'a':{'b':1}, 'x': None}
    >>> nmget(o, 'a', 0)
    {'b': 1}
    >>> nmget(o, 'a.b', 0)
    1
    >>> nmget(o, 'a/b', 0, '/')
    1
    >>> nmget(o, 'a.c', 0)
    0
    >>> nmget(o, 'x', 0)
    0
    >>> nmget(o, 'x', 0, null_as_default=False)
    None
    '''
    pieces = key_path.split(path_delimiter)
    while pieces:
        p = pieces.pop(0)
        if o is None or p not in o:
            return def_val
        o = o[p]
    if o is None and null_as_default:
        return def_val
    return o


def readable_size_to_bytes(expr):
    if isinstance(expr, numbers.Real):
        return int(expr)
    elif isinstance(expr, str):
        try:
            v = float(expr)
            return int(v)
        except ValueError:
            suffix = expr[-1]
            suffix_map = {
                't': 1024 * 1024 * 1024 * 1024,
                'T': 1024 * 1024 * 1024 * 1024,
                'g': 1024 * 1024 * 1024,
                'G': 1024 * 1024 * 1024,
                'm': 1024 * 1024,
                'M': 1024 * 1024,
                'k': 1024,
                'K': 1024,
            }
            return int(float(expr[:-1]) * suffix_map[suffix])
    else:
        raise ValueError('unconvertible type')


async def curl(url, default_value=None, loop=None, timeout=0.2):
    try:
        async with aiohttp.ClientSession(loop=loop) as sess:
            with _timeout(timeout):
                async with sess.get(url) as resp:
                    assert resp.status == 200
                    body = await resp.text()
                    return body.strip()
    except (asyncio.TimeoutError, aiohttp.errors.ClientOSError, AssertionError) as e:
        if callable(default_value):
            return default_value()
        return default_value


async def get_instance_id(loop=None):
    return (await curl('http://169.254.169.254/latest/meta-data/instance-id',
                       lambda: 'i-{}'.format(socket.gethostname()), loop=loop))


async def get_instance_ip(loop=None):
    return (await curl('http://169.254.169.254/latest/meta-data/local-ipv4',
                       '127.0.0.1', loop=loop))


async def get_instance_type(loop=None):
    return (await curl('http://169.254.169.254/latest/meta-data/instance-type',
                       'unknown', loop=loop))


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
