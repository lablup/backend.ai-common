"""
Simple systemd's daemon status notification UDP client implemented in Python 3.
The methods will silently becomes no-op if NOTIFY_SOCKET environment variable is not set.

Usage:

.. code-block::

    import sd_notify
    notify = sd_notify.Notifier()

    # Report a status message
    notify.status("Initialising my service...")
    time.sleep(3)ß
    # Report that the program init is complete
    notify.ready()
    notify.statßus("Waiting for web requests...")
    time.sleep(3)
    # Report an error to the service manager
    notify.notify_error("An irrecoverable error occured!")
    # The service manager will probably kill the program here
    time.sleep(3)
"""

from __future__ import annotations

import asyncio
import os
import socket

import asyncudp


class SystemDNotifier():

    socket: asyncudp.Socket | None
    address: str | None

    def __init__(self):
        self.socket = None
        self.address = os.getenv("NOTIFY_SOCKET", None)

    async def _send(self, msg):
        """Send string `msg` as bytes on the notification socket"""
        if self.address is None:
            return
        loop = asyncio.get_running_loop()
        if self.socket is None:
            self.socket = asyncudp.Socket(
                *(await loop.create_datagram_endpoint(
                    asyncudp._SocketProtocol,
                    family=socket.AF_UNIX,
                    remote_addr=self.address,
                ))
            )
        self.socket.sendto(msg.encode())

    async def ready(self):
        """Report ready service state, i.e. completed initialisation"""
        await self._send("READY=1\n")

    async def update_status(self, msg):
        """Set a service status message"""
        await self._send("STATUS=%s\n" % (msg,))

    async def notify(self):
        """Report a healthy service state"""
        await self._send("WATCHDOG=1\n")

    async def notify_error(self, msg=None):
        """
        Report a watchdog error. This program will likely be killed by the
        service manager.
        If `msg` is not None, it will be reported as an error message to the
        service manager.
        """
        if msg:
            await self.update_status(msg)
        await self._send("WATCHDOG=trigger\n")
