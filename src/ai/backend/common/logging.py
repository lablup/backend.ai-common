from collections import OrderedDict
from datetime import datetime
import itertools
import json
import logging, logging.config, logging.handlers
import multiprocessing as mp
import os
from pathlib import Path
import signal
import socket
import ssl

from setproctitle import setproctitle
from pythonjsonlogger.jsonlogger import JsonFormatter
import trafaret as t
import zmq

from . import config
from . import validators as tx
from .logging_utils import BraceStyleAdapter
from .exception import ConfigurationError

__all__ = ('Logger', 'BraceStyleAdapter')


loglevel_iv = t.Enum('DEBUG', 'INFO', 'WARNING', 'ERROR', 'CRITICAL', 'NOTSET')
logformat_iv = t.Enum('simple', 'verbose')

logging_config_iv = t.Dict({
    t.Key('level', default='INFO'): loglevel_iv,
    t.Key('pkg-ns'): t.Mapping(t.String(allow_blank=True), loglevel_iv),
    t.Key('drivers', default=['console']): t.List(t.Enum('console', 'logstash', 'file')),
    t.Key('console', default=None): t.Null | t.Dict({
        t.Key('colored', default=True): t.Bool,
        t.Key('format', default='verbose'): logformat_iv,
    }).allow_extra('*'),
    t.Key('file', default=None): t.Null | t.Dict({
        t.Key('path'): tx.Path(type='dir', auto_create=True),
        t.Key('filename'): t.String,
        t.Key('backup-count', default='5'): t.Int[1:100],
        t.Key('rotation-size', default='10M'): tx.BinarySize,
        t.Key('format', default='verbose'): logformat_iv,
    }).allow_extra('*'),
    t.Key('logstash', default=None): t.Null | t.Dict({
        t.Key('endpoint'): tx.HostPortPair,
        t.Key('protocol', default='tcp'): t.Enum('zmq.push', 'zmq.pub', 'tcp', 'udp'),
        t.Key('ssl-enabled', default=True): t.Bool,
        t.Key('ssl-verify', default=True): t.Bool,
        # NOTE: logstash does not have format optoin.
    }).allow_extra('*'),
}).allow_extra('*')


class LogstashHandler(logging.Handler):

    def __init__(self, endpoint, protocol: str, *,
                 ssl_enabled: bool = True,
                 ssl_verify: bool = True,
                 myhost: str = None):
        super().__init__()
        self._endpoint = endpoint
        self._protocol = protocol
        self._ssl_enabled = ssl_enabled
        self._ssl_verify = ssl_verify
        self._myhost = myhost
        self._sock = None
        self._sslctx = None
        self._zmqctx = None

    def _setup_transport(self):
        if self._sock is not None:
            return
        if self._protocol == 'zmq.push':
            self._zmqctx = zmq.Context()
            sock = self._zmqctx.socket(zmq.PUSH)
            sock.setsockopt(zmq.LINGER, 50)
            sock.setsockopt(zmq.SNDHWM, 20)
            sock.connect(f'tcp://{self._endpoint[0]}:{self._endpoint[1]}')
            self._sock = sock
        elif self._protocol == 'zmq.pub':
            self._zmqctx = zmq.Context()
            sock = self._zmqctx.socket(zmq.PUB)
            sock.setsockopt(zmq.LINGER, 50)
            sock.setsockopt(zmq.SNDHWM, 20)
            sock.connect(f'tcp://{self._endpoint[0]}:{self._endpoint[1]}')
            self._sock = sock
        elif self._protocol == 'tcp':
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            if self._ssl_enabled:
                self._sslctx = ssl.create_default_context()
                if not self._ssl_verify:
                    self._sslctx.check_hostname = False
                    self._sslctx.verify_mode = ssl.CERT_NONE
                sock = self._sslctx.wrap_socket(sock, server_hostname=self._endpoint[0])
            sock.connect((str(self._endpoint.host), self._endpoint.port,))
            self._sock = sock
        elif self._protocol == 'udp':
            sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            sock.connect((str(self._endpoint.host), self._endpoint.port,))
            self._sock = sock
        else:
            raise ConfigurationError({'logging.LogstashHandler': f'unsupported protocol: {self._protocol}'})

    def cleanup(self):
        if self._sock:
            self._sock.close()
        self._sslctx = None
        if self._zmqctx:
            self._zmqctx.term()

    def emit(self, record):
        self._setup_transport()
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
            ('@timestamp', datetime.now().isoformat()),
            ('@version', 1),
            ('host', self._myhost),
            ('logger', record.name),
            ('path', record.pathname),
            ('func', record.funcName),
            ('lineno', record.lineno),
            ('message', record.getMessage()),
            ('level', record.levelname),
            ('tags', list(tags)),
        ])
        log.update(extra_data)
        if self._protocol.startswith('zmq'):
            self._sock.send_json(log)
        else:
            # TODO: reconnect if disconnected
            self._sock.sendall(json.dumps(log).encode('utf-8'))


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


def log_worker(daemon_config, parent_pid, log_queue):
    setproctitle(f'backend.ai: logger pid({parent_pid})')
    file_handler = None
    logstash_handler = None

    if 'file' in daemon_config['drivers']:
        drv_config = daemon_config['file']
        fmt = '(timestamp) (level) (name) (processName) (message)'
        file_handler = logging.handlers.RotatingFileHandler(
            filename=drv_config['path'] / drv_config['filename'],
            backupCount=drv_config['backup-count'],
            maxBytes=drv_config['rotation-size'],
            encoding='utf-8',
        )
        file_handler.setLevel(daemon_config['level'])
        file_handler.setFormatter(CustomJsonFormatter(fmt))

    if 'logstash' in daemon_config['drivers']:
        drv_config = daemon_config['logstash']
        logstash_handler = LogstashHandler(
            endpoint=drv_config['endpoint'],
            protocol=drv_config['protocol'],
            ssl_enabled=drv_config['ssl-enabled'],
            ssl_verify=drv_config['ssl-verify'],
            myhost='hostname',  # TODO: implement
        )
        logstash_handler.setLevel(daemon_config['level'])

    try:
        while True:
            rec = log_queue.get()
            if rec is None:
                break
            if file_handler:
                file_handler.emit(rec)
            if logstash_handler:
                logstash_handler.emit(rec)
    finally:
        if logstash_handler:
            logstash_handler.cleanup()


class Logger():

    def __init__(self, daemon_config):
        legacy_logfile_path = os.environ.get('BACKEND_LOG_FILE')
        if legacy_logfile_path:
            p = Path(legacy_logfile_path)
            config.override_key(daemon_config, ('file', 'path'), p.parent)
            config.override_key(daemon_config, ('file', 'filename'), p.name)
        config.override_with_env(daemon_config, ('file', 'backup-count'), 'BACKEND_LOG_FILE_COUNT')
        legacy_logfile_size = os.environ.get('BACKEND_LOG_FILE_SIZE')
        if legacy_logfile_size:
            legacy_logfile_size = f'{legacy_logfile_size}M'
            config.override_with_env(daemon_config, ('file', 'rotation-size'), legacy_logfile_size)

        cfg = logging_config_iv.check(daemon_config)

        def _check_driver_config_exists_if_activated(cfg, driver):
            if driver in cfg['drivers'] and cfg[driver] is None:
                raise ConfigurationError({'logging': f'{driver} driver is activated but no config given.'})

        _check_driver_config_exists_if_activated(cfg, 'console')
        _check_driver_config_exists_if_activated(cfg, 'file')
        _check_driver_config_exists_if_activated(cfg, 'logstash')

        self.daemon_config = cfg
        self.log_formats = {
            'simple': '%(levelname)s %(message)s',
            'verbose': '%(asctime)s %(levelname)s %(name)s [%(process)d] %(message)s',
        }
        self.log_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'formatters': {
            },
            'handlers': {
                'null': {'class': 'logging.NullHandler'},
            },
            'loggers': {
                '': {'handlers': [], 'level': cfg['level']},
                **{k: {'handlers': [], 'level': v, 'propagate': False} for k, v in cfg['pkg-ns'].items()},
            },
        }

    def __enter__(self):
        self.log_queue = mp.Queue()

        if 'console' in self.daemon_config['drivers']:
            drv_config = self.daemon_config['console']
            if drv_config['colored']:
                self.log_config['formatters']['console'] = {
                    '()': 'coloredlogs.ColoredFormatter',
                    'format': self.log_formats[drv_config['format']],
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
                }
            else:
                self.log_config['formatters']['console'] = {
                    'class': 'logging.Formatter',
                    'format': self.log_formats[drv_config['format']],
                }
            self.log_config['handlers']['console'] = {
                'class': 'logging.StreamHandler',
                'level': self.daemon_config['level'],
                'formatter': 'console',
                'stream': 'ext://sys.stderr',
            }
            for _logger in self.log_config['loggers'].values():
                _logger['handlers'].append('console')

        def _activate_aggregator():
            if 'aggregator' not in self.log_config['handlers']:
                self.log_config['handlers']['aggregator'] = {
                    'class': 'logging.handlers.QueueHandler',
                    'level': self.daemon_config['level'],
                    'queue': self.log_queue,
                }
                for _logger in self.log_config['loggers'].values():
                    _logger['handlers'].append('aggregator')

        if 'file' in self.daemon_config['drivers']:
            _activate_aggregator()

        if 'logstash' in self.daemon_config['drivers']:
            _activate_aggregator()

        logging.config.dictConfig(self.log_config)
        # block signals that may interrupt/corrupt logging
        stop_signals = {signal.SIGINT, signal.SIGTERM}
        signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
        self.proc = mp.Process(
            target=log_worker, name='Logger',
            args=(self.daemon_config, os.getpid(), self.log_queue))
        self.proc.start()
        signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
        # reset process counter
        mp.process._process_counter = itertools.count(0)

    def __exit__(self, *exc_info_args):
        self.log_queue.put(None)
        self.proc.join()
