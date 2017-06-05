import secrets
import socket
import random

import pytest
from aioresponses import aioresponses

import sorna.common.identity
from sorna.common.testutils import mock_awaitable


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_id(mocker, provider):
    sorna.common.identity.current_provider = provider
    sorna.common.identity._defined = False
    sorna.common.identity._define_functions()

    with aioresponses() as m:
        random_id = secrets.token_hex(16)
        if provider == 'amazon':
            m.get('http://169.254.169.254/latest/meta-data/instance-id',
                  body=random_id)
            ret = await sorna.common.identity.get_instance_id()
            assert ret == random_id
        elif provider == 'azure':
            m.get('http://169.254.169.254/metadata/instance',
                payload={
                    'compute': {
                        'vmId': random_id,
                    }
                })
            ret = await sorna.common.identity.get_instance_id()
            assert ret == random_id
        elif provider == 'google':
            m.get('http://metadata.google.internal/computeMetadata/v1/instance/id',
                  body=random_id)
            ret = await sorna.common.identity.get_instance_id()
            assert ret == random_id
        elif provider == 'unknown':
            ret = await sorna.common.identity.get_instance_id()
            assert ret == f'i-{socket.gethostname()}'


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_id_failures(mocker, provider):
    sorna.common.identity.current_provider = provider
    sorna.common.identity._defined = False
    sorna.common.identity._define_functions()

    with aioresponses() as m:
        # If we don't set any mocked responses, aioresponses will raise ClientConnectionError.
        random_id = secrets.token_hex(16)
        ret = await sorna.common.identity.get_instance_id()
        assert ret == f'i-{socket.gethostname()}'


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_ip(mocker, provider):
    sorna.common.identity.current_provider = provider
    sorna.common.identity._defined = False
    sorna.common.identity._define_functions()

    with aioresponses() as m:
        random_ip = '.'.join(str(random.randint(0, 255)) for _ in range(4))
        if provider == 'amazon':
            m.get('http://169.254.169.254/latest/meta-data/local-ipv4',
                  body=random_ip)
            ret = await sorna.common.identity.get_instance_ip()
            assert ret == random_ip
        elif provider == 'azure':
            m.get('http://169.254.169.254/metadata/instance',
                payload={
                    'network': {
                        'interface': [
                            {
                                'ipv4': {
                                    'ipaddress': [
                                        {'ipaddress': random_ip},
                                    ],
                                },
                            },
                        ],
                    }
                })
            ret = await sorna.common.identity.get_instance_ip()
            assert ret == random_ip
        elif provider == 'google':
            m.get('http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip',
                  body=random_ip)
            ret = await sorna.common.identity.get_instance_ip()
            assert ret == random_ip
        elif provider == 'unknown':
            ret = await sorna.common.identity.get_instance_ip()
            assert ret == '127.0.0.1'


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_type(mocker, provider):
    sorna.common.identity.current_provider = provider
    sorna.common.identity._defined = False
    sorna.common.identity._define_functions()

    with aioresponses() as m:
        random_type = secrets.token_hex(16)
        if provider == 'amazon':
            m.get('http://169.254.169.254/latest/meta-data/instance-type',
                  body=random_type)
            ret = await sorna.common.identity.get_instance_type()
            assert ret == random_type
        elif provider == 'azure':
            m.get('http://169.254.169.254/metadata/instance',
                payload={
                    'compute': {
                        'vmSize': random_type,
                    }
                })
            ret = await sorna.common.identity.get_instance_type()
            assert ret == random_type
        elif provider == 'google':
            m.get('http://metadata.google.internal/computeMetadata/v1/instance/machine-type',
                  body=random_type)
            ret = await sorna.common.identity.get_instance_type()
            assert ret == random_type
        elif provider == 'unknown':
            ret = await sorna.common.identity.get_instance_type()
            assert ret == 'unknown'
