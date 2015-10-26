#! /usr/bin/env python3
import asyncio
import aiohttp

async def get_instance_ip():
    try:
        url = 'http://instance-data/latest/meta-data/local-ipv4'
        resp = await asyncio.wait_for(aiohttp.get(url), 0.2)
    except (asyncio.TimeoutError, aiohttp.errors.ClientOSError):
        return '127.0.0.1'
    if resp.status == 200:
        body = await resp.text()
        return body.strip()
    else:
        return '127.0.0.1'

