#! /usr/bin/env python3

import enum

class ManagerRequestTypes(enum.IntEnum):
    PING    = 0
    CREATE  = 1
    DESTROY = 2
    EXECUTE = 3

class ManagerResponseTypes(enum.IntEnum):
    PONG          = 0
    SUCCESS       = 200
    INVALID_INPUT = 400
    FAILURE       = 500

class AgentRequestTypes(enum.IntEnum):
    HEARTBEAT   = 0
    EXECUTE     = 1
    SOCKET_INFO = 2

