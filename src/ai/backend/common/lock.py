import fcntl
import logging
from concurrent.futures import Executor
from io import FileIO
from pathlib import Path
from typing import Any, Optional

from tenacity import (
    AsyncRetrying,
    RetryError,
    retry_if_exception_type,
    stop_after_delay,
    wait_exponential,
)

from .distributed import AbstractDistributedLock
from .logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__name__))


class FileLock(AbstractDistributedLock):

    default_timeout: float = 3  # not allow infinite timeout for safety

    _fp: FileIO | None
    _locked: bool = False

    def __init__(
        self,
        path: Path,
        *,
        mode: str = "rb",
        timeout: Optional[float] = None,
        executor: Optional[Executor] = None,
        debug: bool = False,
    ) -> None:
        self._fp = None
        self._path = path
        self._mode = mode
        self._timeout = timeout if timeout is not None else self.default_timeout
        self._executor = executor
        self._debug = debug

    @property
    def locked(self) -> bool:
        return self._locked

    def __del__(self) -> None:
        if self._fp is not None:
            self._fp.close()
            self._fp = None
            if self._debug:
                log.debug("file lock implicitly released: {}", self._path)

    async def __aenter__(self) -> None:
        assert self._fp is None
        assert not self._locked
        self._path.touch(exist_ok=True)
        self._fp = open(self._path, self._mode)
        try:
            async for attempt in AsyncRetrying(
                wait=wait_exponential(multiplier=0.02, min=0.02, max=1.0),
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

    async def __aexit__(self, *exc_info) -> bool | None:
        if self._locked:
            fcntl.flock(self._fp, fcntl.LOCK_UN)
            self._locked = False
            if self._debug:
                log.debug("file lock explicitly released: {}", self._path)
        self._fp.close()
        self._fp = None
        return None
