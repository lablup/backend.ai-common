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
