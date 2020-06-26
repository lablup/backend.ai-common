import asyncio
import base64
from collections import OrderedDict
from contextlib import closing
from datetime import timedelta
import enum
import inspect
from itertools import chain
import numbers
from pathlib import Path
import random
import re
import sys
import socket
from typing import (
    Any, Union, Type,
    Iterator, Callable, Awaitable,
    Tuple,
    cast,
)
import uuid

import aiohttp
from async_timeout import timeout as _timeout
import janus

from .types import BinarySize, Sentinel


def env_info():
    """
    Returns a string that contains the Python version and runtime path.
    """
    v = sys.version_info
    pyver = f'Python {v.major}.{v.minor}.{v.micro}'
    if v.releaselevel == 'alpha':
        pyver += 'a'
    if v.releaselevel == 'beta':
        pyver += 'b'
    if v.releaselevel == 'candidate':
        pyver += 'rc'
    if v.releaselevel != 'final':
        pyver += str(v.serial)
    return f'{pyver} (env: {sys.prefix})'


def odict(*args):
    """
    A short-hand for the constructor of OrderedDict.
    :code:`odict(('a',1), ('b',2))` is equivalent to
    :code:`OrderedDict([('a',1), ('b',2)])`.
    """
    return OrderedDict(args)


def dict2kvlist(o):
    """
    Serializes a dict-like object into a generator of the flatten list of
    repeating key-value pairs.  It is useful when using HMSET method in Redis.

    Example:
    >>> list(dict2kvlist({'a': 1, 'b': 2}))
    ['a', 1, 'b', 2]
    """
    return chain.from_iterable((k, v) for k, v in o.items())


def generate_uuid() -> str:
    u = uuid.uuid4()
    # Strip the last two padding characters because u always has fixed length.
    return base64.urlsafe_b64encode(u.bytes)[:-2].decode('ascii')


def get_random_seq(length: float, num_points: int, min_distance: float) -> Iterator[float]:
    """
    Generate a random sequence of numbers within the range [0, length]
    with the given number of points and the minimum distance between the points.

    Note that X ( = the minimum distance d x the number of points N) must be equivalent to or smaller than
    the length L + d to guarantee the the minimum distance between the points.
    If X == L + d, the points are always equally spaced with d.

    :return: An iterator over the generated sequence
    """
    assert num_points * min_distance <= length + min_distance, \
           'There are too many points or it has a too large distance which cannot be fit into the given length.'
    extra = length - (num_points - 1) * min_distance
    ro = [random.uniform(0, 1) for _ in range(num_points + 1)]
    sum_ro = sum(ro)
    rn = [extra * r / sum_ro for r in ro[0:num_points]]
    spacing = [min_distance + rn[i] for i in range(num_points)]
    cumulative_sum = 0.0
    for s in spacing:
        cumulative_sum += s
        yield cumulative_sum - min_distance


def find_free_port(bind_addr: str = '127.0.0.1') -> int:
    with closing(socket.socket(socket.AF_INET, socket.SOCK_STREAM)) as s:
        s.bind((bind_addr, 0))
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        return s.getsockname()[1]


def nmget(o, key_path, def_val=None, path_delimiter='.', null_as_default=True):
    """
    A short-hand for retrieving a value from nested mappings
    ("nested-mapping-get"). At each level it checks if the given "path"
    component in the given key exists and return the default value whenever
    fails.

    Example:
    >>> o = {'a':{'b':1}, 'x': None}
    >>> nmget(o, 'a', 0)
    {'b': 1}
    >>> nmget(o, 'a.b', 0)
    1
    >>> nmget(o, 'a/b', 0, '/')
    1
    >>> nmget(o, 'a.c', 0)
    0
    >>> nmget(o, 'x', 0)
    0
    >>> nmget(o, 'x', 0, null_as_default=False)
    None
    """
    pieces = key_path.split(path_delimiter)
    while pieces:
        p = pieces.pop(0)
        if o is None or p not in o:
            return def_val
        o = o[p]
    if o is None and null_as_default:
        return def_val
    return o


def readable_size_to_bytes(expr):
    if isinstance(expr, numbers.Real):
        return BinarySize(expr)
    return BinarySize.from_str(expr)


def str_to_timedelta(tstr):
    """
    Convert humanized timedelta string into a Python timedelta object.

    Example:
    >>> str_to_timedelta('30min')
    datetime.timedelta(seconds=1800)
    >>> str_to_timedelta('1d1hr')
    datetime.timedelta(days=1, seconds=3600)
    >>> str_to_timedelta('2hours 15min')
    datetime.timedelta(seconds=8100)
    >>> str_to_timedelta('20sec')
    datetime.timedelta(seconds=20)
    >>> str_to_timedelta('300')
    datetime.timedelta(seconds=300)
    >>> str_to_timedelta('-1day')
    datetime.timedelta(days=-1)
    """
    _rx = re.compile(r'(?P<sign>[+|-])?\s*'
                     r'((?P<days>\d+(\.\d+)?)(d|day|days))?\s*'
                     r'((?P<hours>\d+(\.\d+)?)(h|hr|hrs|hour|hours))?\s*'
                     r'((?P<minutes>\d+(\.\d+)?)(m|min|mins|minute|minutes))?\s*'
                     r'((?P<seconds>\d+(\.\d+)?)(s|sec|secs|second|seconds))?$')
    ts = _rx.match(tstr)
    if not ts:
        try:
            return timedelta(seconds=float(tstr))  # consider bare number string as seconds
        except TypeError:
            pass
        raise ValueError('Invalid time expression')
    ts = ts.groupdict()
    sign = ts.pop('sign', None)
    if set(ts.values()) == {None}:
        raise ValueError('Invalid time expression')
    params = {n: -float(t) if sign == '-' else float(t) for n, t in ts.items() if t}
    return timedelta(**params)


async def curl(url, default_value=None, params=None, headers=None, timeout=0.2):
    try:
        async with aiohttp.ClientSession() as sess:
            with _timeout(timeout):
                async with sess.get(url, params=params, headers=headers) as resp:
                    assert resp.status == 200
                    body = await resp.text()
                    return body.strip()
    except (asyncio.TimeoutError, aiohttp.ClientError, AssertionError):
        if callable(default_value):
            return default_value()
        return default_value


class StringSetFlag(enum.Flag):

    def __eq__(self, other):
        return self.value == other

    def __hash__(self):
        return hash(self.value)

    def __or__(self, other):
        if isinstance(other, type(self)):
            other = other.value
        if not isinstance(other, (set, frozenset)):
            other = set((other,))
        return set((self.value,)) | other

    __ror__ = __or__

    def __and__(self, other):
        if isinstance(other, (set, frozenset)):
            return self.value in other
        if isinstance(other, str):
            return self.value == other
        raise TypeError

    def __rand__(self, other):
        if isinstance(other, (set, frozenset)):
            return self.value in other
        if isinstance(other, str):
            return self.value == other
        raise TypeError

    def __xor__(self, other):
        if isinstance(other, (set, frozenset)):
            return set((self.value,)) ^ other
        if isinstance(other, str):
            if other == self.value:
                return set()
            else:
                return other
        raise TypeError

    def __rxor__(self, other):
        if isinstance(other, (set, frozenset)):
            return other ^ set((self.value,))
        if isinstance(other, str):
            if other == self.value:
                return set()
            else:
                return other
        raise TypeError

    def __str__(self):
        return self.value


class AsyncBarrier:
    """
    This class provides a simplified asyncio-version of threading.Barrier class.
    """

    num_parties: int = 1
    cond: asyncio.Condition

    def __init__(self, num_parties: int):
        self.num_parties = num_parties
        self.count = 0
        self.cond = asyncio.Condition()

    async def wait(self):
        async with self.cond:
            self.count += 1
            if self.count == self.num_parties:
                self.cond.notify_all()
            else:
                while self.count < self.num_parties:
                    await self.cond.wait()

    def reset(self):
        self.count = 0
        # FIXME: if there are waiting coroutines, let them
        #        raise BrokenBarrierError like threading.Barrier


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


current_loop: Callable[[], asyncio.AbstractEventLoop]
if hasattr(asyncio, 'get_running_loop'):
    current_loop = asyncio.get_running_loop  # type: ignore
else:
    current_loop = asyncio.get_event_loop    # type: ignore


async def run_through(
    *awaitable_or_callables: Union[Callable[[], None], Awaitable[None]],
    ignored_exceptions: Tuple[Type[Exception], ...],
) -> None:
    """
    A syntactic sugar to simplify the code patterns like:

    .. code-block:: python3

       try:
           await do1()
       except MyError:
           pass
       try:
           await do2()
       except MyError:
           pass
       try:
           await do3()
       except MyError:
           pass

    Using ``run_through()``, it becomes:

    .. code-block:: python3

       await run_through(
           do1(),
           do2(),
           do3(),
           ignored_exceptions=(MyError,),
       )
    """
    for f in awaitable_or_callables:
        try:
            if inspect.iscoroutinefunction(f):
                await f()  # type: ignore
            elif inspect.isawaitable(f):
                await f  # type: ignore
            else:
                f()  # type: ignore
        except Exception as e:
            if isinstance(e, cast(Tuple[Any, ...], ignored_exceptions)):
                continue
            raise


class AsyncFileWriter:
    """
    This class provides a context manager for making sequential async
    writes using janus queue.
    """
    def __init__(
            self,
            loop: asyncio.AbstractEventLoop,
            target_filename: Union[str, Path],
            access_mode: str,
            decode: Callable[[str], bytes] = None,
            max_chunks: int = None) -> None:
        if max_chunks is None:
            max_chunks = 0
        self._q: janus.Queue[Union[bytes, str, Sentinel]] = janus.Queue(maxsize=max_chunks)
        self._loop = loop
        self._target_filename = target_filename
        self._access_mode = access_mode
        self._decode = decode

    async def __aenter__(self):
        self._fut = self._loop.run_in_executor(None, self._write)
        return self

    def _write(self):
        with open(self._target_filename, self._access_mode) as f:
            while True:
                item = self._q.sync_q.get()
                if item is Sentinel.TOKEN:
                    break
                if self._decode is not None:
                    item = self._decode(item)
                f.write(item)
                self._q.sync_q.task_done()

    async def __aexit__(self, exc_type, exc, tb):
        await self._q.async_q.put(Sentinel.TOKEN)
        try:
            await self._fut
        finally:
            self._q.close()
            await self._q.wait_closed()

    async def write(self, item):
        await self._q.async_q.put(item)
