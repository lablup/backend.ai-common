import asyncio
import fcntl
import logging
import time
from concurrent.futures import Executor
from pathlib import Path
from typing import Any, Optional

from .distributed import AbstractDistributedLock
from .logging import BraceStyleAdapter

log = BraceStyleAdapter(logging.getLogger(__name__))


class FileLock(AbstractDistributedLock):

    default_timeout: float = 3  # not allow infinite timeout for safety

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
        self._path = path
        self._mode = mode
        self._timeout = timeout if timeout is not None else self.default_timeout
        self._executor = executor
        self._debug = debug

    @property
    def locked(self) -> bool:
        return self._locked

    async def __aenter__(self) -> Any:

        def _lock():
            start_time = time.perf_counter()
            self._path.touch(exist_ok=True)
            self._fp = open(self._path, self._mode)
            while True:
                try:
                    fcntl.flock(self._fp, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    self._locked = True
                    if self._debug:
                        log.debug("file lock acquired: {}", self._path)
                    return self._fp
                except BlockingIOError:
                    # Failed to get file lock. Waiting until timeout ...
                    if (
                        self._timeout > 0 and
                        time.perf_counter() - start_time > self._timeout
                    ):
                        raise TimeoutError(f"failed to lock file: {self._path}")
                time.sleep(0.1)

        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self._executor, _lock)

    async def __aexit__(self, *exc_info) -> bool | None:
        if self._locked:
            fcntl.flock(self._fp, fcntl.LOCK_UN)
            self._locked = False
            if self._debug:
                log.debug("file lock released: {}", self._path)
        self._fp.close()
        self.f_fp = None
        return None
