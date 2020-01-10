'''
Wrapper of msgpack-python with good defaults.
'''

from typing import Any

import msgpack as _msgpack


def packb(data: Any, **kwargs) -> bytes:
    return _msgpack.packb(data, use_bin_type=True, **kwargs)


def unpackb(packed: bytes, **kwargs) -> Any:
    return _msgpack.unpackb(packed, raw=False, use_list=False, **kwargs)
