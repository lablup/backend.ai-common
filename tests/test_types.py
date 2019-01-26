import collections
import typing
from ai.backend.common.exception import AliasResolutionFailed
from ai.backend.common.docker import default_registry, default_repository
from ai.backend.common.types import BinarySize, ImageRef, PlatformTagSet

import pytest


def test_binary_size():
    assert 1 == BinarySize.from_str('1 byte')
    with pytest.raises(ValueError):
        BinarySize.from_str('1.1')
    assert 11021204 == BinarySize.from_str('11_021_204')
    assert 12345 == BinarySize.from_str('12345 bytes')
    assert 12345 == BinarySize.from_str('12345 B')
    assert 12345 == BinarySize.from_str('12_345 bytes')
    assert 99 == BinarySize.from_str('99 bytes')
    assert 1024 == BinarySize.from_str('1 KiB')
    assert 2048 == BinarySize.from_str('2 KiBytes')
    assert 127303 == BinarySize.from_str('124.32 KiB')
    assert str(BinarySize(1)) == '1 byte'
    assert str(BinarySize(2)) == '2 bytes'
    assert str(BinarySize(1024)) == '1 KiB'
    assert str(BinarySize(2048)) == '2 KiB'
    assert str(BinarySize(105935)) == '103.45 KiB'
    assert str(BinarySize(127303)) == '124.32 KiB'
    assert str(BinarySize(1048576)) == '1 MiB'

    # short-hand formats
    assert 2 ** 30 == BinarySize.from_str('1g')
    assert 1048576 == BinarySize.from_str('1m')
    assert 524288 == BinarySize.from_str('0.5m')
    assert 524288 == BinarySize.from_str('512k')
    assert '{:g}'.format(BinarySize(930)) == '930'
    assert '{:g}'.format(BinarySize(1024)) == '1k'
    assert '{:g}'.format(BinarySize(524288)) == '512k'
    assert '{:g}'.format(BinarySize(1048576)) == '1m'
    assert '{:g}'.format(BinarySize(2 ** 30)) == '1g'


def test_image_ref_typing():
    ref = ImageRef('c')
    assert isinstance(ref, collections.abc.Hashable)


def test_image_ref_parsing():
    ref = ImageRef('c')
    assert ref.name == f'{default_repository}/c'
    assert ref.tag == 'latest'
    assert ref.registry == default_registry
    assert ref.tag_set == ('latest', set())

    ref = ImageRef('c:gcc6.3-alpine3.8')
    assert ref.name == f'{default_repository}/c'
    assert ref.tag == 'gcc6.3-alpine3.8'
    assert ref.registry == default_registry
    assert ref.tag_set == ('gcc6.3', {'alpine'})

    ref = ImageRef('python:3.6-ubuntu')
    assert ref.name == f'{default_repository}/python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == default_registry
    assert ref.tag_set == ('3.6', {'ubuntu'})

    ref = ImageRef('kernel-python:3.6-ubuntu')
    assert ref.name == f'{default_repository}/kernel-python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == default_registry
    assert ref.tag_set == ('3.6', {'ubuntu'})

    ref = ImageRef('lablup/python-tensorflow:1.10-py36-ubuntu')
    assert ref.name == 'lablup/python-tensorflow'
    assert ref.tag == '1.10-py36-ubuntu'
    assert ref.registry == default_registry
    assert ref.tag_set == ('1.10', {'ubuntu', 'py'})

    ref = ImageRef('lablup/kernel-python:3.6-ubuntu')
    assert ref.name == 'lablup/kernel-python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == default_registry
    assert ref.tag_set == ('3.6', {'ubuntu'})

    # To parse registry URLs correctly, we first need to give
    # the valid registry URLs!
    ref = ImageRef('myregistry.org/lua', [])
    assert ref.name == f'myregistry.org/lua'
    assert ref.tag == 'latest'
    assert ref.registry == default_registry
    assert ref.tag_set == ('latest', set())

    ref = ImageRef('myregistry.org/lua', ['myregistry.org'])
    assert ref.name == f'lablup/lua'
    assert ref.tag == 'latest'
    assert ref.registry == 'myregistry.org'
    assert ref.tag_set == ('latest', set())

    ref = ImageRef('myregistry.org/lua:5.3-alpine', ['myregistry.org'])
    assert ref.name == f'lablup/lua'
    assert ref.tag == '5.3-alpine'
    assert ref.registry == 'myregistry.org'
    assert ref.tag_set == ('5.3', {'alpine'})

    # Non-standard port number should be a part of the known registry value.
    ref = ImageRef('myregistry.org:999/mybase/python:3.6-cuda9-ubuntu',
                   ['myregistry.org:999'])
    assert ref.name == 'mybase/python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == 'myregistry.org:999'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('myregistry.org/mybase/moon/python:3.6-cuda9-ubuntu',
                   ['myregistry.org'])
    assert ref.name == 'mybase/moon/python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == 'myregistry.org'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    # IP addresses are treated as valid registry URLs.
    ref = ImageRef('127.0.0.1:5000/python:3.6-cuda9-ubuntu')
    assert ref.name == f'{default_repository}/python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == '127.0.0.1:5000'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    # IPv6 addresses must be bracketted.
    ref = ImageRef('::1/python:3.6-cuda9-ubuntu')
    assert ref.name == f'::1/python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == default_registry
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('[::1]/python:3.6-cuda9-ubuntu')
    assert ref.name == f'{default_repository}/python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == '[::1]'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('[::1]:5000/python:3.6-cuda9-ubuntu')
    assert ref.name == f'{default_repository}/python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == '[::1]:5000'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('[212c:9cb9:eada:e57b:84c9:6a9:fbec:bdd2]:1024/python')
    assert ref.name == f'{default_repository}/python'
    assert ref.tag == 'latest'
    assert ref.registry == '[212c:9cb9:eada:e57b:84c9:6a9:fbec:bdd2]:1024'
    assert ref.tag_set == ('latest', set())

    with pytest.raises(ValueError):
        ref = ImageRef('a:!')

    with pytest.raises(ValueError):
        ref = ImageRef('127.0.0.1:5000/a:-x-')

    with pytest.raises(ValueError):
        ref = ImageRef('http://127.0.0.1:5000/xyz')

    with pytest.raises(ValueError):
        ref = ImageRef('//127.0.0.1:5000/xyz')


def test_image_ref_formats():
    ref = ImageRef('python:3.6-cuda9-ubuntu', [])
    assert ref.canonical == 'index.docker.io/lablup/python:3.6-cuda9-ubuntu'
    assert ref.short == 'lablup/python:3.6-cuda9-ubuntu'
    assert str(ref) == ref.canonical
    assert repr(ref) == f'<ImageRef: "{ref.canonical}">'

    ref = ImageRef('myregistry.org/user/python:3.6-cuda9-ubuntu', ['myregistry.org'])
    assert ref.canonical == 'myregistry.org/user/python:3.6-cuda9-ubuntu'
    assert ref.short == 'user/python:3.6-cuda9-ubuntu'
    assert str(ref) == ref.canonical
    assert repr(ref) == f'<ImageRef: "{ref.canonical}">'


@pytest.mark.asyncio
async def test_image_ref_resolve(etcd):
    await etcd.put('config/docker/registry/myregistry.org', 'https://myregistry.org')
    await etcd.put('images/index.docker.io/lablup%2Fpython/3.6', 'abcd')
    await etcd.put('images/index.docker.io/lablup%2Fpython/3.5', 'abef')
    await etcd.put('images/myregistry.org/python/3.6-ubuntu', 'eeab')
    await etcd.put('images/_aliases/python',        'python:latest')
    await etcd.put('images/_aliases/python:latest', 'lablup/python:3.6')
    await etcd.put('images/_aliases/mypython',      'myregistry.org/python:3.6')
    await etcd.put('images/_aliases/infinite-loop', 'infinite-loop')

    # single-shot resolution
    ref = await ImageRef.resolve_alias('python:latest', etcd)
    assert ref.registry == 'index.docker.io'
    assert ref.name == 'lablup/python'
    assert ref.tag == '3.6'

    # aliasing may be nested.
    ref = await ImageRef.resolve_alias('python', etcd)
    assert ref.registry == 'index.docker.io'
    assert ref.name == 'lablup/python'
    assert ref.tag == '3.6'

    # without alias, it falls back to the normal parsing.
    ref = await ImageRef.resolve_alias('lablup/python', etcd)
    assert ref.registry == 'index.docker.io'
    assert ref.name == 'lablup/python'
    assert ref.tag == 'latest'

    # self-alias results in an infinite loop.
    # the maximum depth of nesting is 8.
    with pytest.raises(AliasResolutionFailed):
        await ImageRef.resolve_alias('infinite-loop', etcd)

    # resolution with custom registry
    ref = await ImageRef.resolve_alias('mypython', etcd)
    assert ref.registry == 'myregistry.org'
    assert ref.name == 'lablup/python'
    assert ref.tag == '3.6'


def test_platform_tag_set_typing():
    tags = PlatformTagSet(['py36', 'cuda9'])
    assert isinstance(tags, collections.abc.Mapping)
    assert isinstance(tags, typing.Mapping)
    assert not isinstance(tags, collections.abc.MutableMapping)
    assert not isinstance(tags, typing.MutableMapping)


def test_platform_tag_set():
    tags = PlatformTagSet(['py36', 'cuda9', 'ubuntu16.04', 'mkl2018.3'])
    assert 'py' in tags
    assert 'cuda' in tags
    assert 'ubuntu' in tags
    assert 'mkl' in tags
    assert tags['py'] == '36'
    assert tags['cuda'] == '9'
    assert tags['ubuntu'] == '16.04'
    assert tags['mkl'] == '2018.3'

    with pytest.raises(ValueError):
        tags = PlatformTagSet(['cuda9', 'cuda8'])

    tags = PlatformTagSet(['myplatform9b1', 'other'])
    assert 'myplatform' in tags
    assert tags['myplatform'] == '9b1'
    assert 'other' in tags
    assert tags['other'] == ''

    with pytest.raises(ValueError):
        tags = PlatformTagSet(['1234'])
