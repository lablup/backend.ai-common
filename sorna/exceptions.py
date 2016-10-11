from .utils import odict


class SornaError(RuntimeError):
    http_status = 500
    error_id    = 'https://api.sorna.io/probs/general-error'
    error_title = 'General Sorna API Error.'

    def __init__(self, *args, **kwargs):
        super().__init__(self, *args, **kwargs)
        self.args = (self.error_id, self.error_title, *self.args)

    def serialize(self):
        return odict(('type', self.error_id),
                     ('title', self.error_title))


class InstanceNotAvailableError(SornaError):
    http_status = 503
    error_id    = 'https://api.sorna.io/probs/instance-not-available'
    error_title = 'There is no available instance.'


class InstanceNotFoundError(SornaError):
    http_status = 404
    error_id    = 'https://api.sorna.io/probs/instance-not-found'
    error_title = 'No such instance.'


class KernelNotFoundError(SornaError):
    http_status = 404
    error_id    = 'https://api.sorna.io/probs/kernel-not-found'
    error_title = 'No such kernel.'


class QuotaExceededError(SornaError):
    http_status = 412
    error_id    = 'https://api.sorna.io/probs/quota-exceeded'
    error_title = 'You have reached your resource limit.'


class KernelCreationFailedError(SornaError):
    http_status = 500
    error_id    = 'https://api.sorna.io/probs/kernel-ceration-failed'
    error_title = 'Kernel creation has failed.'


class KernelDestructionFailedError(SornaError):
    http_status = 500
    error_id    = 'https://api.sorna.io/probs/kernel-destruction-failed'
    error_title = 'Kernel destruction has failed.'


