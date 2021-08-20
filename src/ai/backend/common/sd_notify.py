"""
Simple systemd's daemon status notification UDP client implemented in Python 3.
The methods will silently becomes no-op if NOTIFY_SOCKET environment variable is not set.

Usage:

.. code-block::

    import asyncio
    import sd_notify

    notify = sd_notify.Notifier()

    # Report a status message
    await notify.update_status("Initialising my service...")
    await asyncio.sleep(3)

    # Report that the program init is complete
    await notify.ready()
    await notify.update_status("Waiting for web requests...")
    await asyncio.sleep(3)

    # Report an error to the service manager
    await notify.notify_error("An irrecoverable error occured!")
    # The service manager will probably kill the program here
"""

from __future__ import annotations

import asyncio
import os
import socket

import asyncudp


class SystemDNotifier():

    socket: asyncudp.Socket | None
    address: str | None

    def __init__(self) -> None:
        self.socket = None
        self.address = os.getenv("NOTIFY_SOCKET", None)

    @property
    def enabled(self) -> bool:
        return (self.address is not None)

    async def _send(self, msg: str) -> None:
        """Send string `msg` as bytes on the notification socket"""
        if self.address is None:
            return
        loop = asyncio.get_running_loop()
        if self.socket is None:
            self.socket = asyncudp.Socket(
                *(await loop.create_datagram_endpoint(
                    asyncudp._SocketProtocol,
                    family=socket.AF_UNIX,
                    remote_addr=self.address,  # type: ignore
                ))
            )
        self.socket.sendto(msg.encode())

    async def ready(self) -> None:
        """Report ready service state, i.e. completed initialisation"""
        await self._send("READY=1\n")

    async def update_status(self, msg: str) -> None:
        """Set a service status message"""
        await self._send("STATUS=%s\n" % (msg,))

    async def notify(self) -> None:
        """Report a healthy service state"""
        await self._send("WATCHDOG=1\n")

    async def notify_error(self, msg: str = None) -> None:
        """
        Report a watchdog error. This program will likely be killed by the
        service manager.
        If `msg` is not None, it will be reported as an error message to the
        service manager.
        """
        if msg:
            await self.update_status(msg)
        await self._send("WATCHDOG=trigger\n")
