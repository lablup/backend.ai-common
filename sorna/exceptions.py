#! /usr/bin/env python3


class InstanceNotAvailableError(RuntimeError):
    pass

class InstanceNotFoundError(RuntimeError):
    pass

class KernelNotFoundError(RuntimeError):
    pass

class AgentError(RuntimeError):
    pass

class ManagerError(RuntimeError):
    pass

class QuotaExceededError(AgentError):
    pass

class KernelCreationFailedError(AgentError):
    pass

class KernelDestructionFailedError(AgentError):
    pass

