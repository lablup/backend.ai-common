from sorna.common import msgpack


def test_msgpack_with_unicode():
    # msgpack-python module requires special treatment
    # to distinguish unicode strings and binary data
    # correctly, and sorna.common.msgpack wraps it for that.

    data = [b'\xff', '한글', 12345, 12.5]
    packed = msgpack.packb(data)
    unpacked = msgpack.unpackb(packed)

    # We also use tuples when unpacking for performance.
    assert unpacked == tuple(data)
