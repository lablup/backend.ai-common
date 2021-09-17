import asyncio
from typing import (
    Sequence,
)


async def simple_run_cmd(cmdargs: Sequence[str], **kwargs) -> asyncio.subprocess.Process:
    p = await asyncio.create_subprocess_exec(*cmdargs, **kwargs)
    await p.wait()
    return p
