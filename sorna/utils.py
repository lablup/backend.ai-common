#! /usr/bin/env python3
import asyncio
import aiohttp

async def curl(url, default_value=None, loop=None):
    try:
        resp = await asyncio.wait_for(aiohttp.get(url, loop=loop),
                                      timeout=0.2, loop=loop)
        async with resp:
            if resp.status == 200:
                body = await resp.text()
                return body.strip()
            else:
                return default_value
    except (asyncio.TimeoutError, aiohttp.errors.ClientOSError):
        return default_value

async def get_instance_id(loop=None):
    return await curl('http://169.254.169.254/latest/meta-data/instance-id', 'i-00000000', loop=loop)

async def get_instance_ip(loop=None):
    return await curl('http://169.254.169.254/latest/meta-data/local-ipv4', '127.0.0.1', loop=loop)

async def get_instance_type(loop=None):
    return await curl('http://169.254.169.254/latest/meta-data/instance-type', 'unknown', loop=loop)
