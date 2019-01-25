import collections
import typing
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
    assert ref.resolve_required()
    assert ref.name == 'c'
    assert ref.tag == 'latest'
    assert ref.registry == ''

    ref = ImageRef('python:3.6-ubuntu')
    assert ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == ''
    assert ref.tag_set == ('3.6', {'ubuntu'})

    ref = ImageRef('kernel-python:3.6-ubuntu')
    assert ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == ''
    assert ref.tag_set == ('3.6', {'ubuntu'})

    ref = ImageRef('lablup/python-tensorflow:1.10-py36-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python-tensorflow'
    assert ref.tag == '1.10-py36-ubuntu'
    assert ref.registry == 'lablup'
    assert ref.tag_set == ('1.10', {'ubuntu', 'py'})

    ref = ImageRef('lablup/kernel-python:3.6-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == 'lablup'
    assert ref.tag_set == ('3.6', {'ubuntu'})

    ref = ImageRef('myregistry.org/lua')
    assert ref.resolve_required()
    assert ref.name == 'lua'
    assert ref.tag == 'latest'
    assert ref.registry == 'myregistry.org'

    ref = ImageRef('myregistry.org/lua:latest')
    assert ref.resolve_required()
    assert ref.name == 'lua'
    assert ref.tag == 'latest'
    assert ref.registry == 'myregistry.org'
    assert ref.tag_set == ('latest', set())

    ref = ImageRef('myregistry.org/python:3.6-cuda9-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == 'myregistry.org'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('myregistry.org:999/some/path/python:3.6-cuda9-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == 'myregistry.org:999/some/path'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('myregistry.org/kernel-python:3.6-cuda9-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == 'myregistry.org'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('127.0.0.1:5000/kernel-python:3.6-cuda9-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == '127.0.0.1:5000'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('[::1]:5000/kernel-python:3.6-cuda9-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == '[::1]:5000'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('[212c:9cb9:eada:e57b:84c9:6a9:fbec:bdd2]:1024/python')
    assert ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == 'latest'
    assert ref.registry == '[212c:9cb9:eada:e57b:84c9:6a9:fbec:bdd2]:1024'

    with pytest.raises(ValueError):
        ref = ImageRef(';')

    with pytest.raises(ValueError):
        ref = ImageRef('a-')

    with pytest.raises(ValueError):
        ref = ImageRef('-a')

    with pytest.raises(ValueError):
        ref = ImageRef('a:b;')


def test_image_ref_formats():
    ref = ImageRef('myregistry.org/kernel-python:3.6-cuda9-ubuntu')
    assert ref.canonical == 'myregistry.org/kernel-python:3.6-cuda9-ubuntu'
    assert ref.short == 'python:3.6-cuda9-ubuntu'
    assert str(ref) == ref.canonical
    assert repr(ref) == f'<ImageRef: "{ref.canonical}">'

    ref = ImageRef('python')
    assert ref.canonical == '(unknown)/kernel-python:latest'
    assert ref.short == 'python:latest'
    assert str(ref) == ref.canonical
    assert repr(ref) == f'<ImageRef: "{ref.canonical}">'


def test_image_ref_validation():
    assert not ImageRef.is_kernel('x'), \
           'Docker images must have "kernel-" prefixes to become a Backend.AI kernel image.'
    assert not ImageRef.is_kernel('lablup/x'), \
           'Docker images must have "kernel-" prefixes to become a Backend.AI kernel image.'
    assert not ImageRef.is_kernel('kernel-x'), \
           'In Backend.AI, the "latest" tag is only for metadata aliases, not for actual Docker image tags.'
    assert not ImageRef.is_kernel('kernel-x:latest'), \
           'In Backend.AI, the "latest" tag is only for metadata aliases, not for actual Docker image tags.'
    assert ImageRef.is_kernel('kernel-x:5.0-ubuntu')
    assert not ImageRef.is_kernel('lablup/kernel-x'), \
           'In Backend.AI, the "latest" tag is only for metadata aliases, not for actual Docker image tags.'
    assert ImageRef.is_kernel('lablup/kernel-x:5.0-ubuntu')
    assert ImageRef.is_kernel('myregistry.org/kernel-x:5.0-ubuntu')
    assert not ImageRef.is_kernel(';')


@pytest.mark.asyncio
async def test_image_ref_resolve_empty(etcd):
    ref = ImageRef('python')
    assert ref.resolve_required()
    with pytest.raises(RuntimeError):
        # With an empty etcd configs, it should raise an error.
        await ref.resolve(etcd)


@pytest.mark.asyncio
async def test_image_ref_resolve_default_registry(etcd):
    await etcd.put('nodes/docker_registry', 'lablup')
    await etcd.put('images/python/tags/3.6-ubuntu', 'abcd')
    await etcd.put('images/python/tags/3.5-ubuntu', 'abef')
    await etcd.put('images/python/tags/latest', ':3.6-ubuntu')
    await etcd.put('images/_aliases/python', 'python:latest')

    ref = ImageRef('python:latest')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'lablup'
    assert ref.tag == '3.6-ubuntu'

    ref = ImageRef('python')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'lablup'
    assert ref.tag == '3.6-ubuntu'

    ref = ImageRef('lablup/python')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'lablup'
    assert ref.tag == '3.6-ubuntu'

    ref = ImageRef('myregistry.org/python')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'myregistry.org'
    assert ref.tag == '3.6-ubuntu'


@pytest.mark.asyncio
async def test_image_ref_resolve_custom_registry(etcd):
    await etcd.put('nodes/docker_registry', 'myregistry.org')
    await etcd.put('images/python/tags/3.7-ubuntu', 'abcd')
    await etcd.put('images/python/tags/3.6-ubuntu', 'abef')
    await etcd.put('images/python/tags/latest', ':3.7-ubuntu')
    await etcd.put('images/python/tags/stable', ':3.6-ubuntu')
    await etcd.put('images/_aliases/python', 'python:latest')

    ref = ImageRef('python:latest')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'myregistry.org'
    assert ref.tag == '3.7-ubuntu'

    ref = ImageRef('python:stable')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'myregistry.org'
    assert ref.tag == '3.6-ubuntu'

    ref = ImageRef('python')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'myregistry.org'
    assert ref.tag == '3.7-ubuntu'

    ref = ImageRef('lablup/python')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'lablup'
    assert ref.tag == '3.7-ubuntu'

    ref = ImageRef('myregistry.org/python')
    assert ref.resolve_required()
    await ref.resolve(etcd)
    assert ref.registry == 'myregistry.org'
    assert ref.tag == '3.7-ubuntu'


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
