#!/usr/bin/env python3
"""Use the existing CI registry secrets without writing credentials into artifacts."""
import argparse
import base64
import json
import os
from pathlib import Path
import subprocess
import signal
import tempfile

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
    config = registry_config(env)
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
