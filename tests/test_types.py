import collections
from decimal import Decimal
import functools
import itertools
import typing
from ai.backend.common.exception import AliasResolutionFailed
from ai.backend.common.docker import default_registry, default_repository
from ai.backend.common.types import (
    BinarySize, ImageRef, PlatformTagSet, ResourceSlot,
    DefaultForUnspecified,
)

import pytest


def test_binary_size():
    assert 1 == BinarySize.from_str('1 byte')
    assert 19291991 == BinarySize.from_str(19291991)
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
    assert '{: }'.format(BinarySize(930)) == '930'
    assert '{:k}'.format(BinarySize(1024)) == '1k'
    assert '{:k}'.format(BinarySize(524288)) == '512k'
    assert '{:k}'.format(BinarySize(1048576)) == '1024k'
    assert '{:m}'.format(BinarySize(524288)) == '0.5m'
    assert '{:m}'.format(BinarySize(1048576)) == '1m'
    assert '{:g}'.format(BinarySize(2 ** 30)) == '1g'
    with pytest.raises(ValueError):
        '{:x}'.format(BinarySize(1))
    with pytest.raises(ValueError):
        '{:qqqq}'.format(BinarySize(1))
    with pytest.raises(ValueError):
        '{:}'.format(BinarySize(1))
    assert '{:s}'.format(BinarySize(930)) == '930'
    assert '{:s}'.format(BinarySize(1024)) == '1k'
    assert '{:s}'.format(BinarySize(524288)) == '512k'
    assert '{:s}'.format(BinarySize(1048576)) == '1m'
    assert '{:s}'.format(BinarySize(2 ** 30)) == '1g'


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
    assert ref.name == f'lua'
    assert ref.tag == 'latest'
    assert ref.registry == 'myregistry.org'
    assert ref.tag_set == ('latest', set())

    ref = ImageRef('myregistry.org/lua:5.3-alpine', ['myregistry.org'])
    assert ref.name == f'lua'
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
    assert ref.name == f'python'
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
    assert ref.name == f'python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == '[::1]'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('[::1]:5000/python:3.6-cuda9-ubuntu')
    assert ref.name == f'python'
    assert ref.tag == '3.6-cuda9-ubuntu'
    assert ref.registry == '[::1]:5000'
    assert ref.tag_set == ('3.6', {'ubuntu', 'cuda'})

    ref = ImageRef('[212c:9cb9:eada:e57b:84c9:6a9:fbec:bdd2]:1024/python')
    assert ref.name == f'python'
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
    assert ref.name == 'python'
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

    # test case added for explicit behavior documentation
    # ImageRef(...:ubuntu16.04) > ImageRef(...:ubuntu) == False
    # ImageRef(...:ubuntu16.04) > ImageRef(...:ubuntu) == False
    # by keeping naming convetion, no need to handle these cases
    r4 = ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu16.04-cuda9.0')
    r5 = ImageRef('lablup/python-tensorflow:1.5-py36-ubuntu-cuda9.0')
    assert not r4 > r5
    assert not r5 > r4


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


def test_resource_slot_serialization():
    st = {'a': 'count', 'b': 'bytes'}
    r1 = ResourceSlot.from_user_input({'a': '1', 'b': '2g'}, st)
    r2 = ResourceSlot.from_user_input({'a': '2', 'b': '1g'}, st)
    r3 = ResourceSlot.from_user_input({'a': '1'}, st)
    with pytest.raises(ValueError):
        ResourceSlot.from_user_input({'x': '1'}, st)

    assert r1['a'] == Decimal(1)
    assert r2['a'] == Decimal(2)
    assert r3['a'] == Decimal(1)
    assert r1['b'] == Decimal(2 * (2**30))
    assert r2['b'] == Decimal(1 * (2**30))
    assert r3['b'] == Decimal(0)

    x = r2 - r3
    assert x['a'] == Decimal(1)
    assert x['b'] == Decimal(1 * (2**30))

    # Conversely, to_json() stringifies the decimal values as-is,
    # while to_humanized() takes the explicit slot type information
    # to generate human-readable strings.

    assert r1.to_json() == {'a': '1', 'b': '2147483648'}
    assert r2.to_json() == {'a': '2', 'b': '1073741824'}
    assert r3.to_json() == {'a': '1', 'b': '0'}
    assert r1.to_humanized(st) == {'a': '1', 'b': '2g'}
    assert r2.to_humanized(st) == {'a': '2', 'b': '1g'}
    assert r3.to_humanized(st) == {'a': '1', 'b': '0'}
    assert r1 == ResourceSlot.from_json({'a': '1', 'b': '2147483648'})
    assert r2 == ResourceSlot.from_json({'a': '2', 'b': '1073741824'})
    assert r3 == ResourceSlot.from_json({'a': '1', 'b': '0'})

    # The result for "unspecified" fields may be different
    # depending on the policy options.

    r1 = ResourceSlot.from_policy({
        'total_resource_slots': {'a': '10'},
        'default_for_unspecified': DefaultForUnspecified.UNLIMITED,
    }, st)
    assert r1['a'] == Decimal(10)
    assert r1['b'] == Decimal('Infinity')
    r2 = ResourceSlot.from_policy({
        'total_resource_slots': {'a': '10'},
        'default_for_unspecified': DefaultForUnspecified.LIMITED,
    }, st)
    assert r2['a'] == Decimal(10)
    assert r2['b'] == Decimal(0)


def test_resource_slot_serialization_prevent_scientific_notation():
    r1 = ResourceSlot({'a': '2E+1', 'b': '200'})
    assert r1.to_json()['a'] == '20'
    assert r1.to_json()['b'] == '200'


def test_resource_slot_serialization_filter_null():
    r1 = ResourceSlot({'a': '1', 'x': None})
    assert r1.to_json()['a'] == '1'
    assert 'x' not in r1.to_json()


def test_resource_slot_serialization_typeless():
    r1 = ResourceSlot.from_user_input({'a': '1', 'cuda.mem': '2g'}, None)
    assert r1['a'] == Decimal(1)
    assert r1['cuda.mem'] == Decimal(2 * (2**30))

    r1 = ResourceSlot.from_user_input({'a': 'inf', 'cuda.mem': 'inf'}, None)
    assert r1['a'].is_infinite()
    assert r1['cuda.mem'].is_infinite()

    with pytest.raises(ValueError):
        r1 = ResourceSlot.from_user_input({'a': '1', 'cuda.smp': '2g'}, None)

    r1 = ResourceSlot.from_user_input({'a': 'inf', 'cuda.smp': 'inf'}, None)
    assert r1['a'].is_infinite()
    assert r1['cuda.smp'].is_infinite()


def test_resource_slot_comparison():
    r1 = ResourceSlot.from_json({'a': '3', 'b': '200'})
    r2 = ResourceSlot.from_json({'a': '4', 'b': '100'})
    r3 = ResourceSlot.from_json({'a': '2'})
    r4 = ResourceSlot.from_json({'a': '1'})
    r5 = ResourceSlot.from_json({'b': '100', 'a': '4'})

    assert r1 != r2
    assert r1 != r3
    assert r2 != r3
    assert r3 != r4
    assert r2 == r5

    assert not r2 < r1
    assert not r2 <= r1
    assert r4 < r1
    assert r4 <= r1
    assert r3 < r1
    assert r3 <= r1
    with pytest.raises(ValueError):
        r3 > r1
    with pytest.raises(ValueError):
        r3 >= r1

    assert not r2 > r1
    assert not r2 >= r1
    assert r1 > r3
    assert r1 >= r3
    assert r1 > r4
    assert r1 >= r4
    with pytest.raises(ValueError):
        r1 < r3
    with pytest.raises(ValueError):
        r1 <= r3

    r1 = ResourceSlot.from_json({'a': '3', 'b': '200'})
    r3 = ResourceSlot.from_json({'a': '3'})
    assert r3.eq_contained(r1)
    with pytest.raises(ValueError):
        r3.eq_contains(r1)
    with pytest.raises(ValueError):
        r1.eq_contained(r3)
    assert r1.eq_contains(r3)
