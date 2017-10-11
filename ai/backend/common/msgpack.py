'''
Wrapper of msgpack-python with good defaults.
'''

import msgpack as _msgpack


def packb(data, **kwargs):
    return _msgpack.packb(data, use_bin_type=True, **kwargs)


def unpackb(packed, **kwargs):
    return _msgpack.unpackb(packed, encoding='utf8', use_list=False, **kwargs)
