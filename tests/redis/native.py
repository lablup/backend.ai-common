from __future__ import annotations

import asyncio
import contextlib
from pathlib import Path
import signal
import textwrap
from typing import (
    AsyncIterator,
    List,
    Tuple,
)

from .types import (
    AbstractRedisSentinelCluster,
    AbstractRedisNode,
    RedisClusterInfo,
)


class NativeRedisNode(AbstractRedisNode):

    proc: asyncio.subprocess.Process | None

    def __init__(self, node_type: str, port: int, start_args: List[str | Path]) -> None:
        self.node_type = node_type
        self.port = port
        self.start_args = start_args
        self.proc = None

    @property
    def addr(self) -> Tuple[str, int]:
        return ('127.0.0.1', self.port)

    def __str__(self) -> str:
        if self.proc is None:
            return "NativeRedisNode(not-running)"
        return f"NativeRedisNode(pid:{self.proc.pid})"

    async def pause(self) -> None:
        assert self.proc is not None
        self.proc.send_signal(signal.SIGSTOP)
        await asyncio.sleep(0)

    async def unpause(self) -> None:
        assert self.proc is not None
        self.proc.send_signal(signal.SIGCONT)
        await asyncio.sleep(0)

    async def stop(self, force_kill: bool = False) -> None:
        assert self.proc is not None
        try:
            if force_kill:
                self.proc.kill()
            else:
                self.proc.terminate()
            exit_code = await self.proc.wait()
            print(f"Redis {self.node_type} (pid:{self.proc.pid}) has terminated with exit code {exit_code}.")
        except ProcessLookupError:
            print(f"Redis {self.node_type} (pid:{self.proc.pid}) already terminated")
        finally:
            self.proc = None

    async def start(self) -> None:
        assert self.proc is None
        self.proc = await asyncio.create_subprocess_exec(
            *self.start_args,
            stdout=asyncio.subprocess.DEVNULL,
            stderr=asyncio.subprocess.DEVNULL,
            start_new_session=True,  # prevent signal propagation
        )
        print(f"Redis {self.node_type} (pid:{self.proc.pid}, port:{self.port}) started.")


class NativeRedisSentinelCluster(AbstractRedisSentinelCluster):

    @contextlib.asynccontextmanager
    async def make_cluster(self) -> AsyncIterator[RedisClusterInfo]:
        nodes = []
        sentinels = []
        sentinel_config = textwrap.dedent(f"""
        sentinel resolve-hostnames yes
        sentinel monitor {self.service_name} 127.0.0.1 16379 2
        sentinel auth-pass {self.service_name} {self.password}
        sentinel down-after-milliseconds {self.service_name} 1000
        sentinel failover-timeout {self.service_name} 5000
        sentinel parallel-syncs {self.service_name} 2
        protected-mode no
        """).lstrip()
        for node_port in [16379, 16380, 16381]:
            rdb_path = Path(f"node.{node_port}.rdb")
            try:
                rdb_path.unlink()
            except FileNotFoundError:
                pass
            node = NativeRedisNode(
                "node",
                node_port,
                [
                    "redis-server",
                    "--port", str(node_port),
                    "--requirepass", self.password,
                    "--masterauth", self.password,
                    "--cluster-announce-ip", "127.0.0.1",
                    "--min-slaves-to-write", "1",
                    "--min-slaves-max-lag", "10",
                    "--dbfilename", rdb_path,
                ],
            )
            await node.start()
            nodes.append(node)
        for sentinel_port in [26379, 26380, 26381]:
            # Redis sentinels store their states in the config files (not rdb!),
            # so the files should be separate to each sentinel instance.
            sentinel_conf_path = Path(f"sentinel.{sentinel_port}.conf")
            sentinel_conf_path.write_text(sentinel_config)
            sentinel = NativeRedisNode(
                "sentinel",
                sentinel_port,
                [
                    "redis-server",
                    sentinel_conf_path,
                    "--port", str(sentinel_port),
                    "--sentinel",
                ],
            )
            await sentinel.start()
            sentinels.append(sentinel)
        try:
            yield RedisClusterInfo(
                node_addrs=[
                    ('127.0.0.1', 16379),
                    ('127.0.0.1', 16380),
                    ('127.0.0.1', 16381),
                ],
                nodes=nodes,
                sentinel_addrs=[
                    ('127.0.0.1', 26379),
                    ('127.0.0.1', 26380),
                    ('127.0.0.1', 26381),
                ],
                sentinels=sentinels,
            )
        except asyncio.CancelledError:
            raise
        finally:
            await asyncio.gather(*[sentinel.stop() for sentinel in sentinels])
            await asyncio.gather(*[node.stop() for node in nodes])


async def main():
    loop = asyncio.get_running_loop()

    async def redis_task():
        native_cluster = NativeRedisSentinelCluster("develove", "testing")
        async with native_cluster.make_cluster():
            while True:
                await asyncio.sleep(10)

    t = asyncio.create_task(redis_task())
    loop.add_signal_handler(signal.SIGINT, t.cancel)
    loop.add_signal_handler(signal.SIGTERM, t.cancel)
    try:
        await t
    except asyncio.CancelledError:
        pass


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        print("Terminated.")
