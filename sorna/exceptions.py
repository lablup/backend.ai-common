'''
This module defines a series of Sorna-specific errors
based on HTTP Error classes from aiohttp.
Raising a SornaError automatically is automatically
mapped to a corresponding HTTP error with RFC7807-style
JSON-encoded description in its response body.
'''

from aiohttp import web
import simplejson as json

from .utils import odict


class SornaError(web.HTTPError):
    status_code = 500
    error_type  = 'https://api.sorna.io/probs/general-error'
    error_title = 'General Sorna API Error.'

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.args = (self.status_code, self.reason, self.error_type)
        self.empty_body = False
        self.body = json.dumps({
            'type': self.error_type,
            'title': self.error_title,
        }).encode()

    def serialize(self):
        return odict(('type', self.error_type),
                     ('title', self.error_title))


class InvalidAuthParameters(web.HTTPBadRequest, SornaError):
    error_type  = 'https://api.sorna.io/probs/invalid-auth-params'
    error_title = 'Missing or invalid authorization parameters.'


class AuthorizationFailed(web.HTTPUnauthorized, SornaError):
    error_type  = 'https://api.sorna.io/probs/auth-failed'
    error_title = 'Credential/signature mismatch.'


class InstanceNotAvailable(web.HTTPServiceUnavailable, SornaError):
    error_type  = 'https://api.sorna.io/probs/instance-not-available'
    error_title = 'There is no available instance.'


class InstanceNotFound(web.HTTPNotFound, SornaError):
    error_type  = 'https://api.sorna.io/probs/instance-not-found'
    error_title = 'No such instance.'


class KernelNotFound(web.HTTPNotFound, SornaError):
    error_type  = 'https://api.sorna.io/probs/kernel-not-found'
    error_title = 'No such kernel.'


class QuotaExceeded(web.HTTPPreconditionFailed, SornaError):
    error_type  = 'https://api.sorna.io/probs/quota-exceeded'
    error_title = 'You have reached your resource limit.'


class RateLimitExceeded(web.HTTPTooManyRequests, SornaError):
    error_type  = 'https://api.sorna.io/probs/rate-limit-exceeded'
    error_title = 'You have reached your API query rate limit.'


class KernelCreationFailed(web.HTTPInternalServerError, SornaError):
    error_type  = 'https://api.sorna.io/probs/kernel-ceration-failed'
    error_title = 'Kernel creation has failed.'


class KernelDestructionFailed(web.HTTPInternalServerError, SornaError):
    error_type  = 'https://api.sorna.io/probs/kernel-destruction-failed'
    error_title = 'Kernel destruction has failed.'


class KernelExecutionFailed(web.HTTPInternalServerError, SornaError):
    error_type  = 'https://api.sorna.io/probs/kernel-execution-failed'
    error_title = 'Executing user code in the kernel has failed.'
