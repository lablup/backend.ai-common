import os
from pathlib import Path
import sys
from typing import Any, Mapping, Optional, Tuple

import toml
import trafaret as t

from . import validators as tx
from .etcd import AsyncEtcd, ConfigScopes
from .exception import ConfigurationError

__all__ = (
    'ConfigurationError',
    'etcd_config_iv',
    'read_from_file',
    'read_from_etcd',
    'override_key',
    'override_with_env',
    'check',
    'merge',
)


etcd_config_iv = t.Dict({
    t.Key('etcd'): t.Dict({
        t.Key('namespace'): t.String,
        t.Key('addr', ('127.0.0.1', 2379)): tx.HostPortPair,
        t.Key('user', default=''): t.Null | t.String(allow_blank=True),
        t.Key('password', default=''): t.Null | t.String(allow_blank=True),
    }).allow_extra('*'),
}).allow_extra('*')


def read_from_file(toml_path: Optional[str], daemon_name: str):
    if toml_path is None:
        toml_path_from_env = os.environ.get('BACKEND_CONFIG_FILE', None)
        if not toml_path_from_env:
            toml_paths = [
                Path.cwd() / f'{daemon_name}.toml',
            ]
            if sys.platform.startswith('linux') or sys.platform.startswith('darwin'):
                toml_paths += [
                    Path.home() / '.config' / 'backend.ai' / f'{daemon_name}.toml',
                    Path(f'/etc/backend.ai/{daemon_name}.toml'),
                ]
            else:
                raise ConfigurationError({
                    'read_from_file()': f"Unsupported platform for config path auto-discovery: {sys.platform}",
                })
        else:
            toml_paths = [toml_path_from_env]
        for toml_path in toml_paths:
            if toml_path.is_file():
                return toml.loads(toml_path.read_text()), toml_path
        else:
            searched_paths = ','.join(map(str, toml_paths))
            raise ConfigurationError({
                'read_from_file()': f"Could not read config from: {searched_paths}",
            })
    else:
        toml_path = Path(toml_path)
        try:
            config = toml.loads(toml_path.read_text())
        except IOError:
            raise ConfigurationError({
                'read_from_file()': f"Could not read config from: {toml_path}",
            })
        else:
            return config, toml_path


async def read_from_etcd(etcd_config: Mapping[str, Any], scope_prefix_map: Mapping[ConfigScopes, str]) \
                        -> Mapping[str, Any]:
    etcd = AsyncEtcd(etcd_config['addr'], etcd_config['namespace'], scope_prefix_map)
    raw_value = await etcd.get('daemon/config')
    if raw_value is None:
        return None
    return toml.loads(raw_value)


def override_key(table: Mapping[str, Any], key_path: Tuple[str, ...], value: str):
    for k in key_path[:-1]:
        if k not in table:
            table[k] = {}
        table = table[k]
    table[key_path[-1]] = value


def override_with_env(table: Mapping[str, Any], key_path: Tuple[str, ...], env_key: str):
    val = os.environ.get(env_key, None)
    if val is None:
        return
    override_key(table, key_path, val)


def check(table: Any, iv: t.Trafaret):
    try:
        config = iv.check(table)
    except t.DataError as e:
        raise ConfigurationError(e.as_dict())
    else:
        return config


def merge(table: Mapping[str, Any], updates: Mapping[str, Any]) -> Mapping[str, Any]:
    result = {**table}
    for k, v in updates.items():
        if isinstance(v, Mapping):
            orig = result.get(k, {})
            assert isinstance(orig, Mapping)
            result[k] = merge(orig, v)
        else:
            result[k] = v
    return result
