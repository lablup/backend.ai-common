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


class InstanceNotAvailable(SornaError):
    http_status = 503
    error_id    = 'https://api.sorna.io/probs/instance-not-available'
    error_title = 'There is no available instance.'


class InstanceNotFound(SornaError):
    http_status = 404
    error_id    = 'https://api.sorna.io/probs/instance-not-found'
    error_title = 'No such instance.'


class KernelNotFound(SornaError):
    http_status = 404
    error_id    = 'https://api.sorna.io/probs/kernel-not-found'
    error_title = 'No such kernel.'


class QuotaExceeded(SornaError):
    http_status = 412
    error_id    = 'https://api.sorna.io/probs/quota-exceeded'
    error_title = 'You have reached your resource limit.'


class RateLimitExceeded(SornaError):
    http_status = 429
    error_id    = 'https://api.sorna.io/probs/rate-limit-exceeded'
    error_title = 'You have reached your API query rate limit.'


class KernelCreationFailed(SornaError):
    http_status = 500
    error_id    = 'https://api.sorna.io/probs/kernel-ceration-failed'
    error_title = 'Kernel creation has failed.'


class KernelDestructionFailed(SornaError):
    http_status = 500
    error_id    = 'https://api.sorna.io/probs/kernel-destruction-failed'
    error_title = 'Kernel destruction has failed.'


class KernelExecutionFailed(SornaError):
    http_status = 500
    error_id    = 'https://api.sorna.io/probs/kernel-execution-failed'
    error_title = 'Executing user code in the kernel has failed.'
