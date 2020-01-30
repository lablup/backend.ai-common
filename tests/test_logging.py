import logging
import os
from pathlib import Path

from ai.backend.common.logging import Logger, BraceStyleAdapter


test_log_config = {
    'level': 'DEBUG',
    'drivers': ['console'],
    'pkg-ns': {'': 'DEBUG'},
    'console': {
        'colored': True,
    }
}

test_log_path = Path(f'/tmp/backend.ai/ipc/manager-logger-{os.getpid()}.sock')

log = BraceStyleAdapter(logging.getLogger('ai.backend.common.testing'))


def test_logger():
    test_log_path.parent.mkdir(parents=True, exist_ok=True)
    log_endpoint = f'ipc://{test_log_path}'
    logger = Logger(test_log_config, is_master=True, log_endpoint=log_endpoint)
    with logger:
        log.warning('blizard warning {}', 123)


class NotPicklableClass:
    """A class that cannot be pickled."""

    def __reduce__(self):
        raise TypeError('this is not picklable')


class NotUnpicklableClass:
    """A class that is pickled successfully but cannot be unpickled."""

    def __init__(self, x):
        if x == 1:
            raise TypeError('this is not unpicklable')

    def __reduce__(self):
        return type(self), (1, )


def test_logger_not_picklable():
    test_log_path.parent.mkdir(parents=True, exist_ok=True)
    log_endpoint = f'ipc://{test_log_path}'
    logger = Logger(test_log_config, is_master=True, log_endpoint=log_endpoint)
    with logger:
        log.warning('blizard warning {}', NotPicklableClass())


def test_logger_not_unpicklable():
    test_log_path.parent.mkdir(parents=True, exist_ok=True)
    log_endpoint = f'ipc://{test_log_path}'
    logger = Logger(test_log_config, is_master=True, log_endpoint=log_endpoint)
    with logger:
        log.warning('blizard warning {}', NotUnpicklableClass(0))
