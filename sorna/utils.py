#! /usr/bin/env python3
import asyncio
import aiohttp

async def curl(url, default_value=None):
    try:
        resp = await asyncio.wait_for(aiohttp.get(url), 0.2)
    except (asyncio.TimeoutError, aiohttp.errors.ClientOSError):
        return default_value
    if resp.status == 200:
        body = await resp.text()
        return body.strip()
    else:
        return default_value

async def get_instance_id():
    return await curl('http://instance-data/latest/meta-data/instance-id', 'i-00000000')

async def get_instance_ip():
    return await curl('http://instance-data/latest/meta-data/local-ipv4', '127.0.0.1')

async def get_instance_type():
    return await curl('http://instance-data/latest/meta-data/instance-type', 'unknown')
