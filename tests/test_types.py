from decimal import Decimal
from ai.backend.common.types import (
    BinarySize, ResourceSlot,
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


def test_resource_slot_serialization():

    # from_user_input() and from_policy() takes the explicit slot type information to
    # convert human-readable values to raw decimal values,
    # while from_json() treats those values as stringified decimal expressions "as-is".

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
