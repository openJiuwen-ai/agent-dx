#!/usr/bin/env python3
"""Use the existing CI registry secrets without writing credentials into artifacts."""

import argparse
import base64
import json
import os
import signal
import subprocess
import sys
import tempfile
from pathlib import Path

DEFAULT_REPOSITORY = 'swr.cn-southwest-2.myhuaweicloud.com/yuanrong-dev/adx-e2e'


def registry_config(env):
    raw = env.get('SWR_DOCKER_CONFIG_JSON')
    if raw:
        try:
            config = json.loads(raw)
            if not isinstance(config.get('auths'), dict):
                raise ValueError()
        except (ValueError, AttributeError):
            raise ValueError('invalid CI registry configuration') from None
        return config
    username, password = env.get('SWR_USERNAME'), env.get('SWR_PASSWORD')
    if username and password:
        registry = env.get('ADX_E2E_IMAGE_REPOSITORY', DEFAULT_REPOSITORY).split('/')[0]
        auth = base64.b64encode((username + ':' + password).encode()).decode()
        return {'auths': {registry: {'auth': auth}}}
    return None


def docker_config_dir(env):
    configured = env.get('DOCKER_CONFIG')
    if configured:
        return Path(configured)
    if 'HOME' in env:
        home = env.get('HOME')
        return Path(home) / '.docker' if home else None
    try:
        return Path.home() / '.docker'
    except RuntimeError:
        return None


def docker_cli_plugin_dirs(env):
    dirs = []
    config_dir = docker_config_dir(env)
    if config_dir:
        config_path = config_dir / 'config.json'
        if config_path.is_file():
            try:
                config = json.loads(config_path.read_text())
            except (OSError, ValueError):
                config = {}
            extra_dirs = config.get('cliPluginsExtraDirs', []) if isinstance(config, dict) else []
            if isinstance(extra_dirs, list):
                dirs.extend(value for value in extra_dirs if isinstance(value, str) and value)
        local_plugins = config_dir / 'cli-plugins'
        if local_plugins.is_dir():
            dirs.append(str(local_plugins))
    desktop_plugins = Path('/Applications/Docker.app/Contents/Resources/cli-plugins')
    if sys.platform == 'darwin' and desktop_plugins.is_dir():
        dirs.append(str(desktop_plugins))
    result = []
    seen = set()
    for value in dirs:
        if value not in seen:
            result.append(value)
            seen.add(value)
    return result


def docker_config(env):
    config = registry_config(env)
    if config is None:
        return None
    plugin_dirs = docker_cli_plugin_dirs(env)
    if plugin_dirs:
        config = dict(config)
        config['cliPluginsExtraDirs'] = plugin_dirs
    return config


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--docker', action='store_true')
    p.add_argument('command', nargs=argparse.REMAINDER)
    a = p.parse_args()
    command = a.command[1:] if a.command[:1] == ['--'] else a.command
    if not command:
        raise ValueError('child command required')
    env = dict(os.environ)
    env.setdefault('ADX_E2E_IMAGE_REPOSITORY', DEFAULT_REPOSITORY)
    config = docker_config(env) if a.docker else registry_config(env)
    with tempfile.TemporaryDirectory(prefix='adx-ci-registry-') as temp:
        if config:
            path = Path(temp) / 'config.json'
            path.write_text(json.dumps(config))
            path.chmod(0o600)
            env['ADX_E2E_REGISTRY_AUTH_FILE'] = str(path)
            if a.docker:
                env['DOCKER_CONFIG'] = temp
        child = subprocess.Popen(command, env=env)

        def forward(signum, frame):
            if child.poll() is None:
                child.send_signal(signum)

        for sig in (signal.SIGTERM, signal.SIGINT):
            signal.signal(sig, forward)
        return child.wait()


if __name__ == '__main__':
    raise SystemExit(main())
