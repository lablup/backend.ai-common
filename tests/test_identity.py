import secrets
import socket
import random

import pytest
from aioresponses import aioresponses

import ai.backend.common.identity


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_id(mocker, provider):
    ai.backend.common.identity.current_provider = provider
    ai.backend.common.identity._defined = False
    ai.backend.common.identity._define_functions()

    with aioresponses() as m:
        random_id = secrets.token_hex(16)
        if provider == 'amazon':
            m.get('http://169.254.169.254/latest/meta-data/instance-id',
                  body=random_id)
            ret = await ai.backend.common.identity.get_instance_id()
            assert ret == random_id
        elif provider == 'azure':
            m.get(
                'http://169.254.169.254/metadata/instance',
                payload={
                    'compute': {
                        'vmId': random_id,
                    }
                })
            ret = await ai.backend.common.identity.get_instance_id()
            assert ret == random_id
        elif provider == 'google':
            m.get('http://metadata.google.internal/computeMetadata/v1/instance/id',
                  body=random_id)
            ret = await ai.backend.common.identity.get_instance_id()
            assert ret == random_id
        elif provider == 'unknown':
            ret = await ai.backend.common.identity.get_instance_id()
            assert ret == f'i-{socket.gethostname()}'


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_id_failures(mocker, provider):
    ai.backend.common.identity.current_provider = provider
    ai.backend.common.identity._defined = False
    ai.backend.common.identity._define_functions()

    with aioresponses():
        # If we don't set any mocked responses, aioresponses will raise ClientConnectionError.
        ret = await ai.backend.common.identity.get_instance_id()
        assert ret == f'i-{socket.gethostname()}'


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_ip(mocker, provider):
    ai.backend.common.identity.current_provider = provider
    ai.backend.common.identity._defined = False
    ai.backend.common.identity._define_functions()

    with aioresponses() as m:
        random_ip = '.'.join(str(random.randint(0, 255)) for _ in range(4))
        if provider == 'amazon':
            m.get('http://169.254.169.254/latest/meta-data/local-ipv4',
                  body=random_ip)
            ret = await ai.backend.common.identity.get_instance_ip()
            assert ret == random_ip
        elif provider == 'azure':
            m.get(
                'http://169.254.169.254/metadata/instance',
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
                    },
                })
            ret = await ai.backend.common.identity.get_instance_ip()
            assert ret == random_ip
        elif provider == 'google':
            m.get('http://metadata.google.internal/computeMetadata/v1/instance/network-interfaces/0/ip',
                  body=random_ip)
            ret = await ai.backend.common.identity.get_instance_ip()
            assert ret == random_ip
        elif provider == 'unknown':
            ret = await ai.backend.common.identity.get_instance_ip()
            assert ret == '127.0.0.1'


@pytest.mark.asyncio
@pytest.mark.parametrize('provider', ['amazon', 'google', 'azure', 'unknown'])
async def test_get_instance_type(mocker, provider):
    ai.backend.common.identity.current_provider = provider
    ai.backend.common.identity._defined = False
    ai.backend.common.identity._define_functions()

    with aioresponses() as m:
        random_type = secrets.token_hex(16)
        if provider == 'amazon':
            m.get('http://169.254.169.254/latest/meta-data/instance-type',
                  body=random_type)
            ret = await ai.backend.common.identity.get_instance_type()
            assert ret == random_type
        elif provider == 'azure':
            m.get(
                'http://169.254.169.254/metadata/instance',
                payload={
                    'compute': {
                        'vmSize': random_type,
                    }
                })
            ret = await ai.backend.common.identity.get_instance_type()
            assert ret == random_type
        elif provider == 'google':
            m.get('http://metadata.google.internal/computeMetadata/v1/instance/machine-type',
                  body=random_type)
            ret = await ai.backend.common.identity.get_instance_type()
            assert ret == random_type
        elif provider == 'unknown':
            ret = await ai.backend.common.identity.get_instance_type()
            assert ret == 'unknown'
