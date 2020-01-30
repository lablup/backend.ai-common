from collections import OrderedDict
from contextvars import ContextVar
from datetime import datetime
import itertools
import json
import logging, logging.config, logging.handlers
import multiprocessing as mp
import os
from pathlib import Path
import pickle
from typing import (
    Any,
    Mapping, MutableMapping,
)
import signal
import socket
import ssl
import sys

from setproctitle import setproctitle
from pythonjsonlogger.jsonlogger import JsonFormatter
import trafaret as t
from tblib import pickling_support
import yarl
import zmq

from . import config
from . import validators as tx
from .logging_utils import BraceStyleAdapter
from .exception import ConfigurationError

# public APIs of this module
__all__ = (
    'Logger',
    'BraceStyleAdapter',
    'LogstashHandler',
    'is_active',
)

is_active: ContextVar[bool] = ContextVar('is_active', default=False)

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


def log_worker(daemon_config: Mapping[str, Any], parent_pid: int, log_endpoint: str) -> None:
    setproctitle(f'backend.ai: logger pid({parent_pid})')
    console_handler = None
    file_handler = None
    logstash_handler = None

    log_formats = {
        'simple': '%(levelname)s %(message)s',
        'verbose': '%(asctime)s %(levelname)s %(name)s [%(process)d] %(message)s',
    }

    if 'console' in daemon_config['drivers']:
        drv_config = daemon_config['console']
        console_formatter: logging.Formatter
        if drv_config['colored']:
            import coloredlogs
            console_formatter = coloredlogs.ColoredFormatter(
                log_formats[drv_config['format']],
                field_styles={'levelname': {'color': 248, 'bold': True},
                              'name': {'color': 246, 'bold': False},
                              'process': {'color': 'cyan'},
                              'asctime': {'color': 240}},
                level_styles={'debug': {'color': 'green'},
                              'verbose': {'color': 'green', 'bright': True},
                              'info': {'color': 'cyan', 'bright': True},
                              'notice': {'color': 'cyan', 'bold': True},
                              'warning': {'color': 'yellow'},
                              'error': {'color': 'red', 'bright': True},
                              'success': {'color': 77},
                              'critical': {'background': 'red', 'color': 255, 'bold': True}},
            )
        else:
            console_formatter = logging.Formatter(
                log_formats[drv_config['format']],
            )
        console_handler = logging.StreamHandler(
            stream=sys.stderr,
        )
        console_handler.setLevel(daemon_config['level'])
        console_handler.setFormatter(console_formatter)

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

    zctx = zmq.Context()
    agg_sock = zctx.socket(zmq.PULL)
    agg_sock.setsockopt(zmq.LINGER, 100)
    agg_sock.bind(log_endpoint)
    ep_url = yarl.URL(log_endpoint)
    if ep_url.scheme.lower() == 'ipc':
        os.chmod(ep_url.path, 0o777)
    # The log worker must be terminated via explicit sentinel.
    stop_signals = {signal.SIGINT, signal.SIGTERM}
    signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
    try:
        while True:
            data = agg_sock.recv()
            try:
                rec = pickle.loads(data)
            except TypeError:
                # We have an unpickling error.
                # Change into a self-created log record with exception info.
                rec = logging.makeLogRecord({
                    'name': __name__,
                    'msg': 'Cannot unpickle the log record (raw data: %r)',
                    'levelno': logging.ERROR,
                    'levelname': 'error',
                    'args': (data,),  # attach the original data for inspection
                    'exc_info': sys.exc_info(),
                })
            if rec is None:
                break
            if console_handler:
                console_handler.emit(rec)
            try:
                if file_handler:
                    file_handler.emit(rec)
                if logstash_handler:
                    logstash_handler.emit(rec)
            except OSError:
                # don't terminate the log worker.
                continue
    finally:
        if logstash_handler:
            logstash_handler.cleanup()
        agg_sock.close()
        zctx.term()


class RelayHandler(logging.Handler):

    def __init__(self, *, endpoint: str) -> None:
        super().__init__()
        self.endpoint = endpoint
        self._zctx = zmq.Context()
        if endpoint:
            self._sock = self._zctx.socket(zmq.PUSH)
            self._sock.setsockopt(zmq.LINGER, 100)
            self._sock.connect(self.endpoint)
        else:
            self._sock = None

    def close(self) -> None:
        if self._sock is not None:
            self._sock.close()
        self._zctx.term()

    def _fallback(self, record: logging.LogRecord) -> None:
        if record is None:
            return
        formatter = logging._defaultFormatter  # type: ignore  # noqa
        print(formatter.format(record), file=sys.stderr)

    def emit(self, record: logging.LogRecord) -> None:
        if self._sock is None:
            self._fallback(record)
            return
        try:
            pickled_rec = pickle.dumps(record)
        except TypeError:
            # We have a pickling error.
            # Change it into a self-created picklable log record with exception info.
            record = logging.makeLogRecord({
                'name': __name__,
                'msg': 'Cannot pickle the log record',
                'levelno': logging.ERROR,
                'levelname': 'error',
                'exc_info': sys.exc_info(),
            })
            pickled_rec = pickle.dumps(record)
        try:
            self._sock.send(pickled_rec)
        except zmq.ZMQError:
            self._fallback(record)


class Logger():

    is_master: bool
    log_endpoint: str
    daemon_config: Mapping[str, Any]
    log_config: MutableMapping[str, Any]
    proc: mp.Process

    def __init__(self, daemon_config: MutableMapping[str, Any], *,
                 is_master: bool, log_endpoint: str) -> None:
        legacy_logfile_path = os.environ.get('BACKEND_LOG_FILE')
        pickling_support.install()  # enable pickling of tracebacks
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

        self.is_master = is_master
        self.log_endpoint = log_endpoint
        self.daemon_config = cfg
        self.log_config = {
            'version': 1,
            'disable_existing_loggers': False,
            'handlers': {
                'null': {'class': 'logging.NullHandler'},
            },
            'loggers': {
                '': {'handlers': [], 'level': cfg['level']},
                **{k: {'handlers': [], 'level': v, 'propagate': False} for k, v in cfg['pkg-ns'].items()},
            },
        }

    def __enter__(self):
        if is_active.get():
            raise RuntimeError('You cannot activate two or more loggers at the same time.')
        self.log_config['handlers']['relay'] = {
            'class': 'ai.backend.common.logging.RelayHandler',
            'level': self.daemon_config['level'],
            'endpoint': self.log_endpoint,
        }
        for _logger in self.log_config['loggers'].values():
            _logger['handlers'].append('relay')
        logging.config.dictConfig(self.log_config)
        self._is_active_token = is_active.set(True)
        # block signals that may interrupt/corrupt logging
        if self.is_master and self.log_endpoint:
            self.relay_handler = logging.getLogger('').handlers[0]
            assert isinstance(self.relay_handler, RelayHandler)
            stop_signals = {signal.SIGINT, signal.SIGTERM}
            signal.pthread_sigmask(signal.SIG_BLOCK, stop_signals)
            self.proc = mp.Process(
                target=log_worker, name='Logger',
                args=(self.daemon_config, os.getpid(), self.log_endpoint))
            self.proc.start()
            signal.pthread_sigmask(signal.SIG_UNBLOCK, stop_signals)
            # reset process counter
            mp.process._process_counter = itertools.count(0)

    def __exit__(self, *exc_info_args):
        # Resetting generates "different context" errors.
        # Since practically we only need to check activeness in alembic scripts
        # and it should be active until the program terminates,
        # just leave it as-is.
        is_active.reset(self._is_active_token)
        if self.is_master and self.log_endpoint:
            self.relay_handler.emit(None)
            self.proc.join()
            ep_url = yarl.URL(self.log_endpoint)
            if ep_url.scheme.lower() == 'ipc':
                os.unlink(ep_url.path)
