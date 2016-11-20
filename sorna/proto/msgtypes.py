#! /usr/bin/env python3

import enum


class SornaResponseTypes(enum.IntEnum):
    PONG          = 0
    SUCCESS       = 200
    INVALID_INPUT = 400
    FAILURE       = 500


class ManagerRequestTypes(enum.IntEnum):
    PING          = 0  # used for service status monitoring
    HEARTBEAT     = 1  # (unused)
    GET_OR_CREATE = 2
    DESTROY       = 3


class AgentRequestTypes(enum.IntEnum):
    PING           = 0  # used for service status monitoring
    EXECUTE        = 1
    CREATE_KERNEL  = 2
    DESTROY_KERNEL = 3
    RESTART_KERNEL = 4
