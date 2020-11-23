import asyncio
from functools import partial
import re
import time
from typing import Callable

import psutil


current_loop: Callable[[], asyncio.AbstractEventLoop]
if hasattr(asyncio, 'get_running_loop'):
    current_loop = asyncio.get_running_loop  # type: ignore
else:
    current_loop = asyncio.get_event_loop    # type: ignore


class FstabEntry:
    """
    Entry class represents a non-comment line on the `fstab` file.
    """
    def __init__(self, device, mountpoint, fstype, options, d=0, p=0):
        self.device = device
        self.mountpoint = mountpoint
        self.fstype = fstype
        if not options:
            options = 'defaults'
        self.options = options
        self.d = d
        self.p = p

    def __eq__(self, o):
        return str(self) == str(o)

    def __str__(self):
        return "{} {} {} {} {} {}".format(self.device,
                                          self.mountpoint,
                                          self.fstype,
                                          self.options,
                                          self.d,
                                          self.p)


class Fstab:
    """
    Reader/writer for fstab file.
    Takes aiofile pointer for async I/O. It should be writable if add/remove
    operations are needed.

    NOTE: This class references Jorge Niedbalski R.'s gist snippet.
          We have been converted it to be compatible with Python 3
          and to support async I/O.
          (https://gist.github.com/niedbalski/507e974ed2d54a87ad37)
    """
    def __init__(self, fp):
        self._fp = fp

    def _hydrate_entry(self, line):
        return FstabEntry(*[x for x in line.strip('\n').split(' ') if x not in ('', None)])

    async def get_entries(self):
        await self._fp.seek(0)
        for line in await self._fp.readlines():
            try:
                line = line.strip()
                if not line.startswith("#"):
                    yield self._hydrate_entry(line)
            except TypeError:
                pass

    async def get_entry_by_attr(self, attr, value):
        async for entry in self.get_entries():
            e_attr = getattr(entry, attr)
            if e_attr == value:
                return entry
        return None

    async def add_entry(self, entry):
        if await self.get_entry_by_attr('device', entry.device):
            return False
        await self._fp.write(str(entry) + '\n')
        await self._fp.truncate()
        return entry

    async def add(self, device, mountpoint, fstype, options=None, d=0, p=0):
        return await self.add_entry(FstabEntry(device, mountpoint, fstype, options, d, p))

    async def remove_entry(self, entry):
        await self._fp.seek(0)
        lines = await self._fp.readlines()
        found = False
        for index, line in enumerate(lines):
            try:
                if not line.strip().startswith("#"):
                    if self._hydrate_entry(line) == entry:
                        found = True
                        break
            except TypeError:
                pass
        if not found:
            return False
        lines.remove(line)
        await self._fp.seek(0)
        await self._fp.write(''.join(lines))
        await self._fp.truncate()
        return True

    async def remove_by_mountpoint(self, mountpoint):
        entry = await self.get_entry_by_attr('mountpoint', mountpoint)
        if entry:
            return await self.remove_entry(entry)
        return False


async def host_health_check() -> dict:
    loop = current_loop()

    def _disk_usage(dir) -> float:
        return psutil.disk_usage(dir).percent

    # Check host disk status (where common package exists)
    disk_pct: float = await loop.run_in_executor(None, partial(_disk_usage, __file__))
    disk_status: str = 'ok'
    disk_message: str = ''
    if disk_pct > 90:
        disk_status = 'error'
        disk_message = 'host disk is almost full'
    elif disk_pct > 70:
        disk_status = 'warning'
        disk_message = 'host disk space not much left'

    # Check disk where docker root directory located
    if disk_status == 'ok':
        proc = await asyncio.create_subprocess_exec(
            *['docker', 'info'],
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        raw_out, raw_err = await proc.communicate()
        out = raw_out.decode('utf-8')
        err = raw_err.decode('utf-8')
        if err and 'command not found' in err:
            # docker not installed
            pass
        elif err:
            disk_status = 'error'
            disk_message = 'check docker daemon status'
        elif 'Docker Root Dir' in out:
            m = re.search('Docker Root Dir: (.*)', out)
            if m and m.group(1):
                docker_root = m.group(1)
                try:
                    docker_disk_pct: float = await loop.run_in_executor(
                        None, partial(_disk_usage, docker_root)
                    )
                    if docker_disk_pct > 90:
                        disk_status = 'error'
                        disk_message = 'docker disk is almost full'
                    elif docker_disk_pct > 70:
                        disk_status = 'warning'
                        disk_message = 'docker disk space not much left'
                except FileNotFoundError:
                    disk_status = 'error'
                    disk_message = 'docker root directory not found'

    return {
        'uptime': time.time() - psutil.boot_time(),
        'disk': {
            'status': disk_status,
            'message': disk_message,
        },
    }
