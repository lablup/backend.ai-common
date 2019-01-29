import ipaddress
import json
import logging
import re
from typing import Any, Mapping, Sequence, Tuple, Union

import aiohttp
import yarl

from .logging import BraceStyleAdapter
from .etcd import (
    AsyncEtcd,
    quote as etcd_quote,
    unquote as etcd_unquote,
)
from .exception import UnknownImageRegistry

__all__ = (
    'default_registry',
    'default_repository',
    'login',
    'get_known_registries',
    'is_known_registry',
    'get_registry_info',
)

log = BraceStyleAdapter(logging.Logger('ai.backend.common.docker'))

default_registry = 'index.docker.io'
default_repository = 'lablup'


async def login(
        sess: aiohttp.ClientSession,
        registry_url: yarl.URL,
        credentials: dict,
        scope: str) -> dict:
    '''
    Authorize to the docker registry using the given credentials and token scope, and returns a set
    of required aiohttp.ClientSession.request() keyword arguments for further API requests.

    Some registry servers only rely on HTTP Basic Authentication without token-based access controls
    (usually via nginx proxy). We do support them also. :)
    '''
    if credentials.get('username') and credentials.get('password'):
        basic_auth = aiohttp.BasicAuth(
            credentials['username'], credentials['password'],
        )
    else:
        basic_auth = None
    realm = registry_url / 'token'  # fallback
    service = 'registry'            # fallback
    async with sess.get(registry_url / 'v2/', auth=basic_auth) as resp:
        ping_status = resp.status
        www_auth_header = resp.headers.get('WWW-Authenticate')
        if www_auth_header:
            match = re.search(r'realm="([^"]+)"', www_auth_header)
            if match:
                realm = match.group(1)
            match = re.search(r'service="([^"]+)"', www_auth_header)
            if match:
                service = match.group(1)
    if ping_status == 200:
        log.debug('docker-registry: {0} -> basic-auth', registry_url)
        return {'auth': basic_auth, 'headers': {}}
    elif ping_status == 404:
        raise RuntimeError(f'Unsupported docker registry: {registry_url}! '
                           '(API v2 not implemented)')
    elif ping_status == 401:
        params = {
            'scope': scope,
            'offline_token': 'true',
            'client_id': 'docker',
            'service': service,
        }
        async with sess.get(realm, params=params, auth=basic_auth) as resp:
            log.debug('docker-registry: {0} -> {1}', registry_url, realm)
            if resp.status == 200:
                data = json.loads(await resp.read())
                token = data.get('token', None)
                return {'auth': None, 'headers': {
                    'Authorization': f'Bearer {token}'
                }}
    raise RuntimeError('authentication for docker registry '
                       'f{registry_url} failed')


async def get_known_registries(etcd: AsyncEtcd) -> Mapping[str, yarl.URL]:
    pairs = await etcd.get_prefix('config/docker/registry/')
    results = {}
    for key, value in pairs:
        parts = key.split('/')
        if len(parts) == 4:
            name = etcd_unquote(parts[-1])
            results[name] = value
    return results


def is_known_registry(val: str,
                      known_registries: Union[Mapping[str, Any], Sequence[str]]):
    if val == default_registry:
        return True
    if known_registries and val in known_registries:
        return True
    try:
        url = yarl.URL('//' + val)
        if url.host and ipaddress.ip_address(url.host):
            return True
    except ValueError:
        pass
    return False


async def get_registry_info(etcd: AsyncEtcd, name: str) -> Tuple[yarl.URL, dict]:
    reg_path = f'config/docker/registry/{etcd_quote(name)}'
    item = await etcd.get_prefix_dict(reg_path)
    if not item:
        raise UnknownImageRegistry(name)
    registry_addr = item['']
    creds = {}
    username = item.get('username')
    if username is not None:
        creds['username'] = username
    password = item.get('password')
    if password is not None:
        creds['password'] = password
    return yarl.URL(registry_addr), creds
