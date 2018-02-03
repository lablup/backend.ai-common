import secrets
import socket
import random
from unittest.mock import patch, MagicMock

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
                'http://169.254.169.254/metadata/instance?version=2017-03-01',
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
            with patch('socket.gethostname', return_value='myname') as mocked_host:
                ret = await ai.backend.common.identity.get_instance_id()
                assert ret == f'i-myname'


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
                'http://169.254.169.254/metadata/instance?version=2017-03-01',
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
            mocked_path = MagicMock()
            mocked_path.read_text.return_value = '\n'.join([
                '127.0.0.1\tlocalhost',
                '10.1.2.3\tmyname',  # my name!
                '',
                '# The following lines are desirable for IPv6 capable hosts',
                '::1\t  localhost ip6-localhost ip6-loopback',
                'fe00::0 ip6-localnet',
                'ff00::0 ip6-mcastprefix',
                'ff02::1 ip6-allnodes',
                'ff02::2 ip6-allrouters',
            ])
            with patch('ai.backend.common.identity.Path', return_value=mocked_path), \
                 patch('socket.gethostname', return_value='myname'):
                ret = await ai.backend.common.identity.get_instance_ip()
                assert ret == '10.1.2.3'
            mocked_path = MagicMock()
            mocked_path.read_text.return_value = '\n'.join([
                '127.0.0.1\tlocalhost',
                '# 10.1.2.3\tmyname',  # commented
                '10.1.2.3\tnotmyname',  # not my name...
                '',
                '# The following lines are desirable for IPv6 capable hosts',
                '::1\t  localhost ip6-localhost ip6-loopback',
                'fe00::0 ip6-localnet',
                'ff00::0 ip6-mcastprefix',
                'ff02::1 ip6-allnodes',
                'ff02::2 ip6-allrouters',
            ])
            with patch('ai.backend.common.identity.Path', return_value=mocked_path), \
                 patch('socket.gethostname', return_value='myname'):
                ret = await ai.backend.common.identity.get_instance_ip()
                assert ret == '127.0.0.1'
            mocked_path = MagicMock()
            mocked_path.side_effect = FileNotFoundError('no such file')
            with patch('ai.backend.common.identity.Path', return_value=mocked_path), \
                 patch('socket.gethostname', return_value='myname'):
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
                'http://169.254.169.254/metadata/instance?version=2017-03-01',
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
