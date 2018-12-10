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


def test_image_ref_parsing():
    ref = ImageRef('c')
    assert ref.resolve_required()
    assert ref.name == 'c'
    assert ref.tag is None
    assert ref.registry == 'lablup'

    ref = ImageRef('python:3.6-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == 'lablup'
    assert ref.tag_set == ('3.6', {'ubuntu'})

    ref = ImageRef('kernel-python:3.6-ubuntu')
    assert not ref.resolve_required()
    assert ref.name == 'python'
    assert ref.tag == '3.6-ubuntu'
    assert ref.registry == 'lablup'
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
    assert ref.tag is None
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

    with pytest.raises(ValueError):
        ref = ImageRef(';')

    with pytest.raises(ValueError):
        ref = ImageRef('a-')

    with pytest.raises(ValueError):
        ref = ImageRef('-a')

    with pytest.raises(ValueError):
        ref = ImageRef('a:b;')

    with pytest.raises(ValueError):
        ref = ImageRef('!/a:b')


def test_image_ref_formats():
    ref = ImageRef('myregistry.org/kernel-python:3.6-cuda9-ubuntu')
    assert ref.canonical == 'myregistry.org/kernel-python:3.6-cuda9-ubuntu'
    assert ref.short == 'python:3.6-cuda9-ubuntu'
    assert str(ref) == ref.canonical
    assert repr(ref) == f'<ImageRef: "{ref.canonical}">'


def test_image_ref_validation():
    assert not ImageRef.is_kernel('x'), \
           'Docker images must have "kernel-" prefixes to become a Backend.AI kernel image.'
    assert not ImageRef.is_kernel('lablup/x'), \
           'Docker images must have "kernel-" prefixes to become a Backend.AI kernel image.'
    assert ImageRef.is_kernel('kernel-x')
    assert not ImageRef.is_kernel('kernel-x:latest'), \
           'In Backend.AI, the "latest" tag is only for metadata aliases, not for actual Docker image tags.'
    assert ImageRef.is_kernel('kernel-x:5.0-ubuntu')
    assert ImageRef.is_kernel('lablup/kernel-x')
    assert ImageRef.is_kernel('lablup/kernel-x:5.0-ubuntu')
    assert ImageRef.is_kernel('myregistry.org/kernel-x:5.0-ubuntu')
    assert not ImageRef.is_kernel(';')


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
