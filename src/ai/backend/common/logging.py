from collections import OrderedDict
from datetime import datetime
import itertools
import logging, logging.config, logging.handlers
import multiprocessing as mp
import os
from pathlib import Path
import signal
import urllib.request, urllib.error

from setproctitle import setproctitle
from pythonjsonlogger.jsonlogger import JsonFormatter
import zmq

__all__ = ('Logger', 'BraceStyleAdapter')

_logging_ctx = zmq.Context()


class LogstashHandler(logging.Handler):

    def __init__(self, endpoint):
        super().__init__()
        self.endpoint = endpoint
        self._cached_priv_ip = None
        self._cached_inst_id = None
        self._ctx = _logging_ctx
        self._sock = self._ctx.socket(zmq.PUB)
        self._sock.setsockopt(zmq.LINGER, 50)
        self._sock.setsockopt(zmq.SNDHWM, 20)
        self._sock.connect(self.endpoint)

    def emit(self, record):
        now = datetime.now().isoformat()
        tags = set()
        extra_data = dict()

        if record.exc_info:
            tags.add('has_exception')
            if self.formatter:
                extra_data['exception'] = self.formatter.formatException(record.exc_info)
            else:
                extra_data['exception'] = logging._defaultFormatter.formatException(record.exc_info)

        # This log format follows logstash's event format.
        log = OrderedDict([
            ('@timestamp', now),
            ('@version', 1),
            ('host', self._get_my_private_ip()),
            ('instance', self._get_my_instance_id()),
            ('logger', record.name),
            ('loc', '{0}:{1}:{2}'.format(record.pathname, record.funcName, record.lineno)),
            ('message', record.getMessage()),
            ('level', record.levelname),
            ('tags', list(tags)),
        ])
        log.update(extra_data)
        self._sock.send_json(log)

    def _get_my_private_ip(self):
        # See the full list of EC2 instance metadata at
        # http://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ec2-instance-metadata.html
        if self._cached_priv_ip is None:
            try:
                resp = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/local-ipv4', timeout=0.2)
                self._cached_priv_ip = resp.read().decode('ascii')
            except urllib.error.URLError:
                self._cached_priv_ip = 'unavailable'
        return self._cached_priv_ip

    def _get_my_instance_id(self):
        if self._cached_inst_id is None:
            try:
                resp = urllib.request.urlopen('http://169.254.169.254/latest/meta-data/instance-id', timeout=0.2)
                self._cached_inst_id = resp.read().decode('ascii')
            except urllib.error.URLError:
                self._cached_inst_id = 'i-00000000'
        return self._cached_inst_id


class CustomJsonFormatter(JsonFormatter):

    def add_fields(self, log_record, record, message_dict):
        super().add_fields(log_record, record, message_dict)
        if not log_record.get('timestamp'):
            # this doesn't use record.created, so it is slightly off
            now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%S.%fZ')
            log_record['timestamp'] = now
        if log_record.get('level', record.levelname):
            log_record['level'] = log_record['level'].upper()
        else:
            log_record['level'] = record.levelname


def log_worker(config, parent_pid, log_queue):
    setproctitle(f'backend.ai: logger pid({parent_pid})')
    if config.log_file is not None:
        fmt = '(timestamp) (level) (name) (processName) (message)'
        file_handler = logging.handlers.RotatingFileHandler(
            filename=config.log_file,
            backupCount=config.log_file_count,
            maxBytes=1048576 * config.log_file_size,
            encoding='utf-8',
        )
        file_handler.setLevel(logging.DEBUG)
        file_handler.setFormatter(CustomJsonFormatter(fmt))
    while True:
        rec = log_queue.get()
        if rec is None:
            break
        file_handler.emit(rec)


class BraceMessage:

    __slots__ = ('fmt', 'args')

    def __init__(self, fmt, args):
        self.fmt = fmt
        self.args = args

    def __str__(self):
        return self.fmt.format(*self.args)


class BraceStyleAdapter(logging.LoggerAdapter):

    def __init__(self, logger, extra=None):
        super().__init__(logger, extra)

    def log(self, level, msg, *args, **kwargs):
        if self.isEnabledFor(level):
            msg, kwargs = self.process(msg, kwargs)
            self.logger._log(level, BraceMessage(msg, args), (), **kwargs)


class Logger():

    def __init__(self, config):
        self.log_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
                'colored': {
                    '()': 'coloredlogs.ColoredFormatter',
                    'format': '%(asctime)s %(levelname)s %(name)s '
                              '[%(process)d] %(message)s',
                    'field_styles': {'levelname': {'color': 248, 'bold': True},
                                     'name': {'color': 246, 'bold': False},
                                     'process': {'color': 'cyan'},
                                     'asctime': {'color': 240}},
                    'level_styles': {'debug': {'color': 'green'},
                                     'verbose': {'color': 'green', 'bright': True},
                                     'info': {'color': 'cyan', 'bright': True},
                                     'notice': {'color': 'cyan', 'bold': True},
                                     'warning': {'color': 'yellow'},
                                     'error': {'color': 'red', 'bright': True},
                                     'success': {'color': 77},
                                     'critical': {'background': 'red', 'color': 255, 'bold': True}},
                },
            },
            'handlers': {
                'console': {
                    'class': 'logging.StreamHandler',
                    'level': 'DEBUG',
                    'formatter': 'colored',
                    'stream': 'ext://sys.stderr',
                },
                'null': {
                    'class': 'logging.NullHandler',
                },
            },
            'loggers': {
                '': {
                    'handlers': ['console'],
                    'level': 'INFO',
                },
            },
        }
        self.cli_config = config

    @staticmethod
    def update_log_args(parser):
        parser.add('--debug', env_var='BACKEND_DEBUG',
                   action='store_true', default=False,
                   help='Set the debug mode and verbose logging. (default: false)')
        parser.add('-v', '--verbose', env_var='BACKEND_VERBOSE',
                   action='store_true', default=False,
                   help='Set even more verbose logging which includes all SQL '
                        'statements issued. (default: false)')
        parser.add('--log-file', env_var='BACKEND_LOG_FILE',
                   type=Path, default=None,
                   help='If set to a file path, line-by-line JSON logs will be '
                        'recorded there.  It also automatically rotates the logs '
                        'using dotted number suffixes. (default: None)')
        parser.add('--log-file-count', env_var='BACKEND_LOG_FILE_COUNT',
                   type=int, default=10,
                   help='The maximum number of rotated log files (default: 10)')
        parser.add('--log-file-size', env_var='BACKEND_LOG_FILE_SIZE',
                   type=float, default=10.0,
                   help='The maximum size of each log file in MiB '
                        '(default: 10 MiB)')

    def add_pkg(self, pkgpath):
        self.log_config['loggers'][pkgpath] = {
            'handlers': ['console'],
            'propagate': False,
            'level': 'DEBUG' if self.cli_config.debug else 'INFO',
        }

    def __enter__(self):
        self.log_queue = mp.Queue()
        if self.cli_config.log_file is not None:
            self.log_config['handlers']['fileq'] = {
                'class': 'logging.handlers.QueueHandler',
                'level': 'DEBUG',
                'queue': self.log_queue,
            }
            for l in self.log_config['loggers'].values():
                l['handlers'].append('fileq')
        logging.config.dictConfig(self.log_config)
        # block signals that may interrupt/corrupt logging
        stop_signals = {signal.SIGINT, signal.SIGTERM}
        signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
        proc = mp.Process(target=log_worker, name='Logger',
                          args=(self.cli_config, os.getpid(), self.log_queue))
        proc.start()
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
        # reset process counter
        mp.process._process_counter = itertools.count(0)

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.log_queue.put(None)
        self.log_queue.close()
        self.log_queue.join_thread()
