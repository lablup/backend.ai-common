import collections
import functools
import itertools
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


def test_platform_tag_set_abbreviations():
    pass


def test_image_ref_generate_aliases():
    ref = ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04')
    aliases = ref.generate_aliases()
    possible_names = ['python-tensorflow', 'tensorflow']
    possible_platform_tags = [
        ['1.5'],
        ['', 'py', 'py3', 'py36'],
        ['', 'ubuntu', 'ubuntu16', 'ubuntu16.04'],
    ]
    # combinations of abbreviated/omitted platforms tags
    for name, ptags in itertools.product(
            possible_names,
            itertools.product(*possible_platform_tags)):
        assert f"{name}:{'-'.join(t for t in ptags if t)}" in aliases


def test_image_ref_generate_aliases_with_accelerator():
    ref = ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04-cuda10.0')
    aliases = ref.generate_aliases()
    possible_names = ['python-tensorflow', 'tensorflow']
    possible_platform_tags = [
        ['1.5'],
        ['', 'py', 'py3', 'py36'],
        ['', 'ubuntu', 'ubuntu16', 'ubuntu16.04'],
        ['cuda', 'cuda10', 'cuda10.0'],  # cannot be empty!
    ]
    # combinations of abbreviated/omitted platforms tags
    for name, ptags in itertools.product(
            possible_names,
            itertools.product(*possible_platform_tags)):
        assert f"{name}:{'-'.join(t for t in ptags if t)}" in aliases


def test_image_ref_generate_aliases_of_names():
    # an alias may include only last framework name in the name.
    ref = ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04-cuda10.0')
    aliases = ref.generate_aliases()
    assert 'python-tensorflow' in aliases
    assert 'tensorflow' in aliases
    assert 'python' not in aliases


def test_image_ref_generate_aliases_disallowed():
    # an alias must include the main platform version tag
    ref = ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04-cuda10.0')
    aliases = ref.generate_aliases()
    # always the main version must be included!
    assert 'python-tensorflow:py3' not in aliases
    assert 'python-tensorflow:py36' not in aliases
    assert 'python-tensorflow:ubuntu' not in aliases
    assert 'python-tensorflow:ubuntu16.04' not in aliases
    assert 'python-tensorflow:cuda' not in aliases
    assert 'python-tensorflow:cuda10.0' not in aliases


def test_image_ref_ordering():
    # ordering is defined as the tuple-ordering of platform tags.
    # (tag components that come first have higher priority when comparing.)
    r1 = ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04-cuda10.0')
    r2 = ImageRef('lablup/python-tensorflow:1.7-py36-ubuntu16.04-cuda10.0')
    r3 = ImageRef('lablup/python-tensorflow:1.7-py37-ubuntu18.04-cuda9.0')
    assert r1 < r2
    assert r1 < r3
    assert r2 < r3

    # only the image-refs with same names can be compared.
    rx = ImageRef('lablup/python:3.6-ubuntu')
    with pytest.raises(ValueError):
        rx < r1
    with pytest.raises(ValueError):
        r1 < rx


def test_image_ref_merge_aliases():
    # After merging, aliases that indicates two or more references should
    # indicate most recent versions.
    refs = [
        ImageRef('lablup/python:3.7-ubuntu18.04'),                           # 0
        ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04-cuda10.0'),  # 1
        ImageRef('lablup/python-tensorflow:1.7-py36-ubuntu16.04-cuda10.0'),  # 2
        ImageRef('lablup/python-tensorflow:1.7-py37-ubuntu16.04-cuda9.0'),   # 3
        ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04'),           # 4
        ImageRef('lablup/python-tensorflow:1.7-py36-ubuntu16.04'),           # 5
        ImageRef('lablup/python-tensorflow:1.7-py37-ubuntu16.04'),           # 6
    ]
    aliases = [ref.generate_aliases() for ref in refs]
    aliases = functools.reduce(ImageRef.merge_aliases, aliases)
    assert aliases['python-tensorflow'] is refs[6]
    assert aliases['python-tensorflow:1.5'] is refs[4]
    assert aliases['python-tensorflow:1.7'] is refs[6]
    assert aliases['python-tensorflow:1.7-py36'] is refs[5]
    assert aliases['python-tensorflow:1.5'] is refs[4]
    assert aliases['python-tensorflow:1.5-cuda'] is refs[1]
    assert aliases['python-tensorflow:1.7-cuda10'] is refs[2]
    assert aliases['python-tensorflow:1.7-cuda9'] is refs[3]
    assert aliases['python'] is refs[0]
