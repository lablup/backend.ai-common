#! /usr/bin/env python3
import asyncio
import aiohttp

@asyncio.coroutine
def curl(url, default_value=None, loop=None):
    try:
        resp = yield from asyncio.wait_for(aiohttp.get(url, loop=loop),
                                           timeout=0.2, loop=loop)
        if resp.status == 200:
            body = yield from resp.text()
            return body.strip()
        else:
            return default_value
    except (asyncio.TimeoutError, aiohttp.errors.ClientOSError):
        return default_value

@asyncio.coroutine
def get_instance_id(loop=None):
    return (yield from curl('http://169.254.169.254/latest/meta-data/instance-id', 'i-00000000', loop=loop))

@asyncio.coroutine
def get_instance_ip(loop=None):
    return (yield from curl('http://169.254.169.254/latest/meta-data/local-ipv4', '127.0.0.1', loop=loop))

@asyncio.coroutine
def get_instance_type(loop=None):
    return (yield from curl('http://169.254.169.254/latest/meta-data/instance-type', 'unknown', loop=loop))
