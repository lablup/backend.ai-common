from __future__ import annotations

from typing import (
    Sequence,
    Tuple,
)
import attr


@attr.define
class RedisClusterInfo:
    worker_addrs: Sequence[Tuple[str, int]]
    worker_containers: Sequence[str]
    sentinel_addrs: Sequence[Tuple[str, int]]
    sentinel_containers: Sequence[str]
