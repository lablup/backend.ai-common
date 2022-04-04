import fcntl
import logging
from concurrent.futures import Executor
from io import IOBase
from pathlib import Path
from typing import Optional

from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
    wait_random,
)

from etcetra.client import EtcdConnectionManager, EtcdCommunicator

from ai.backend.common.etcd import AsyncEtcd

from .distributed import AbstractDistributedLock
from .logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__name__))


class FileLock(AbstractDistributedLock):

    default_timeout: float = 3  # not allow infinite timeout for safety

    _fp: IOBase | None
    _locked: bool = False

    def __init__(
        self,
        path: Path,
        *,
        timeout: Optional[float] = None,
        executor: Optional[Executor] = None,
        debug: bool = False,
    ) -> None:
        self._fp = None
        self._path = path
        self._timeout = timeout if timeout is not None else self.default_timeout
        self._executor = executor
        self._debug = debug

    @property
    def locked(self) -> bool:
        return self._locked

    def __del__(self) -> None:
        if self._fp is not None:
            self.release()
            log.debug("file lock implicitly released: {}", self._path)

    async def acquire(self) -> None:
        assert self._fp is None
        assert not self._locked
        self._path.touch(exist_ok=True)
        self._fp = open(self._path, "rb")
        try:
            async for attempt in AsyncRetrying(
                wait=wait_exponential(multiplier=0.02, min=0.02, max=1.0) + wait_random(0, 0.05),
                stop=stop_after_delay(self._timeout),
                retry=retry_if_exception_type(BlockingIOError),
            ):
                with attempt:
                    fcntl.flock(self._fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._locked = True
                    if self._debug:
                        log.debug("file lock acquired: {}", self._path)
        except RetryError:
            raise TimeoutError(f"failed to lock file: {self._path}")

    def release(self) -> None:
        assert self._fp is not None
        if self._locked:
            fcntl.flock(self._fp, fcntl.LOCK_UN)
            self._locked = False
            if self._debug:
                log.debug("file lock explicitly released: {}", self._path)
        self._fp.close()
        self._fp = None

    async def __aenter__(self) -> None:
        await self.acquire()

    async def __aexit__(self, *exc_info) -> bool | None:
        self.release()
        return None


class EtcdLock(AbstractDistributedLock):

    _con_mgr: EtcdConnectionManager
    _debug: bool
    default_timeout: float = 3  # not allow infinite timeout for safety

    def __init__(
        self,
        lock_name: str,
        etcd: AsyncEtcd,
        *,
        timeout: Optional[float] = None,
        debug: bool = False,
    ) -> None:
        _timeout = timeout if timeout is not None else self.default_timeout
        self._con_mgr = etcd.etcd.with_lock(lock_name, timeout=_timeout)
        self._debug = debug

    async def __aenter__(self) -> EtcdCommunicator:
        communicator = await self._con_mgr.__aenter__()
        if self._debug:
            log.debug('etcd lock acquired: {}', self._con_mgr._lock_key)
        return communicator

    async def __aexit__(self, *exc_info) -> Optional[bool]:
        await self._con_mgr.__aexit__(*exc_info)
        if self._debug:
            log.debug('etcd lock released: {}', self._con_mgr._lock_key)
        return None
