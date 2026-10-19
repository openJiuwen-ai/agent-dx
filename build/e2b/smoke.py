#!/usr/bin/env python3
"""Exercise a real ADX E2B deployment with the pinned Python SDK.

Requires E2B 2.52.0 and an envd-only template (e.g. the runc CCE profile).
Credentials are read from files and never included in the JSON evidence.
This does not substitute for Firecracker checkpoint/recovery acceptance.
"""
import argparse
import json
import logging
import os
import posixpath
import ssl
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path


def require(condition, message):
    """Raise even when Python assertions are disabled with -O."""
    if not condition:
        raise AssertionError(message)


def main():
    output_logger = logging.Logger(__name__, level=logging.INFO)
    output_logger.addHandler(logging.StreamHandler(sys.stdout))
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--api-url', required=True)
    parser.add_argument('--sandbox-url')
    parser.add_argument('--key-file', type=Path, required=True)
    parser.add_argument('--other-key-file', type=Path, required=True)
    parser.add_argument('--ca', type=Path, required=True)
    parser.add_argument('--template', default='base')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if not args.api_url.startswith('https://'):
        parser.error('an HTTPS public Ingress is required')
    os.environ['SSL_CERT_FILE'] = str(args.ca.resolve())
    os.environ.pop('E2B_DEBUG', None)
    import httpx
    from importlib.metadata import version
    sdk_version = version("e2b")
    if sdk_version != "2.52.0":
        raise ValueError("this acceptance script requires E2B 2.52.0")
    from e2b import PtySize, Sandbox, SandboxQuery, SandboxState

    key = args.key_file.read_text().strip()
    other_key = args.other_key_file.read_text().strip()
    options = dict(api_key=key, api_url=args.api_url,
                   sandbox_url=args.sandbox_url or args.api_url,
                   debug=False, retries=0, request_timeout=120)
    checks = []
    sandboxes = []
    evidence = {'sdk_version': sdk_version, 'template': args.template,
                'checks': checks, 'checkpoint_validation': 'not performed'}
    with httpx.Client(base_url=args.api_url, verify=ssl.create_default_context(cafile=str(args.ca)),
                      http2=True, timeout=120) as client:
        def api(method, path, *, credential=key, **kwargs):
            return client.request(method, path, headers={'X-API-Key': credential}, **kwargs)

        try:
            metadata = {'run': str(uuid.uuid4()), 'encoding': '中文 + & ='}
            sandbox = Sandbox.create(args.template, timeout=60, metadata=metadata,
                                     envs={'SMOKE_VALUE': 'environment-ok'}, **options)
            sandboxes.append(sandbox)
            evidence['sandbox_id'] = sandbox.sandbox_id
            info = sandbox.get_info()
            require(info.metadata == metadata, "sandbox metadata differs from create request")
            checks.append('SDK create and detail after joint readiness')
            require(
                sandbox.commands.run('printf "$SMOKE_VALUE"').stdout == 'environment-ok',
                "sandbox environment value is unavailable",
            )
            sandbox.files.write('/tmp/e2b-smoke.txt', 'file-ok')
            require(sandbox.files.read('/tmp/e2b-smoke.txt') == 'file-ok', "sandbox file read failed")
            sandbox.commands.run('printf private > /home/user/e2b-private; chmod 600 /home/user/e2b-private; '
                                 'mkdir /home/user/e2b-private-dir; chmod 700 /home/user/e2b-private-dir')
            require(sandbox.files.read('/home/user/e2b-private') == 'private', "private file read failed")
            sandbox.files.write('/home/user/e2b-private', 'overwrite-ok')
            require(
                sandbox.files.read('/home/user/e2b-private') == 'overwrite-ok',
                "private file overwrite failed",
            )
            sandbox.files.write('/home/user/e2b-private-dir/child', 'directory-ok')
            require(
                [item.name for item in sandbox.files.list('/home/user/e2b-private-dir')] == ['child'],
                "private directory write failed",
            )
            sandbox.files.remove('/home/user/e2b-private-dir')
            require(
                sandbox.commands.run('test ! -e /home/user/e2b-private-dir').exit_code == 0,
                "private directory removal failed",
            )
            checks.append('SDK private file overwrite and private directory operations')
            terminal = sandbox.pty.create(PtySize(cols=80, rows=24), timeout=5)
            sandbox.pty.send_stdin(terminal.pid, b'printf pty-ok\nexit\n')
            output = []
            result = terminal.wait(on_pty=lambda data: output.append(data.decode()))
            require(
                result.exit_code == 0 and 'pty-ok' in ''.join(output),
                "PTY command did not complete successfully",
            )
            checks.append('SDK commands, environment, files and PTY')
            server = sandbox.commands.run('/bin/busybox httpd -f -p 8080 -h /tmp',
                                          background=True)
            try:
                routing = {'E2b-Sandbox-Id': sandbox.sandbox_id,
                           'E2b-Sandbox-Port': '8080'}
                for _ in range(20):
                    response = client.get('/e2b-smoke.txt', headers=routing)
                    if response.status_code == 200:
                        break
                    time.sleep(0.1)
                require(
                    response.status_code == 200 and response.text == 'file-ok',
                    f'application forwarding returned {response.status_code}: {response.text[:256]}',
                )
            finally:
                server.kill()
            checks.append('public application port forwards to the guest')
            page = Sandbox.list(query=SandboxQuery(metadata=metadata, state=[SandboxState.RUNNING]),
                                limit=1, **options).next_items()
            require(
                [item.sandbox_id for item in page] == [sandbox.sandbox_id],
                "metadata query did not return the created sandbox",
            )
            checks.append('SDK metadata filter and pagination response')
            detail_path = posixpath.join('/sandboxes', sandbox.sandbox_id)
            response = api('GET', detail_path)
            require(
                response.status_code == 200 and response.http_version == 'HTTP/2',
                "management forwarding did not use expected HTTP versions",
            )
            checks.append('HTTP/2 management forwarding to HTTP/1 backend')
            require(
                api('GET', detail_path, credential=other_key).status_code in (403, 404),
                "other tenant can read sandbox detail",
            )
            require(
                api('GET', detail_path, credential='invalid-api-key').status_code == 401,
                "invalid API key was accepted",
            )
            checks.append('tenant isolation and API authentication')
            routing = {'E2b-Sandbox-Id': sandbox.sandbox_id, 'E2b-Sandbox-Port': '49983',
                       'X-Access-Token': 'invalid-envd-token'}
            require(
                client.post('/process.Process/List', headers=routing, json={}).status_code == 401,
                "invalid envd token was accepted",
            )
            require(
                client.post('/init', headers=routing, json={}).status_code == 403,
                "envd initialization endpoint was not blocked",
            )
            routing['E2b-Sandbox-Port'] = '50090'
            require(
                client.get('/health', headers=routing).status_code == 403,
                "control port was not blocked",
            )
            checks.append('envd authentication and blocked initialization/control ports')
            for path, payload in [
                (posixpath.join(detail_path, 'pause'), {'memory': True}),
                (posixpath.join(detail_path, 'snapshots'), {}),
            ]:
                response = api('POST', path, json=payload)
                require(response.status_code == 400, "unsupported capability was accepted")
                require(
                    response.json().get('error_code') == 'capability_not_supported',
                    "unsupported capability returned the wrong error",
                )
            response = api('POST', '/v2/sandboxes', json={'templateID': args.template,
                           'envVars': {'ADX_ENVD_PATH': '/untrusted'}})
            require(response.status_code == 400, "reserved environment variable was accepted")
            checks.append('unsupported capabilities and reserved environment rejected')
            sandbox.set_timeout(5)
            with ThreadPoolExecutor(max_workers=1) as pool:
                command = pool.submit(sandbox.commands.run,
                                      'sleep 8; printf stream-ok', timeout=12)
                time.sleep(2)
                sandbox.set_timeout(15)
                require(
                    command.result(timeout=12).stdout == 'stream-ok',
                    "renewed sandbox command stream did not complete",
                )
            checks.append('renewal preserves a command stream past its original expiry')
            sandbox.set_timeout(15)
            before = sandbox.get_info().end_at
            connected = Sandbox.connect(sandbox.sandbox_id, timeout=1, **options)
            require(connected.get_info().end_at >= before, "connect shortened the sandbox lease")
            require(
                connected.commands.run('printf connect-ok').stdout == 'connect-ok',
                "connected sandbox command failed",
            )
            checks.append('running Connect does not shorten lease')
            sandbox.kill()
            require(api('GET', detail_path).status_code == 404, "deleted sandbox remains accessible")
            sandboxes.remove(sandbox)
            checks.append('explicit deletion and removal of stored control')

            expiring = Sandbox.create(args.template, timeout=60, **options)
            sandboxes.append(expiring)
            expiring.set_timeout(8)
            expires = expiring.get_info().end_at
            while time.time() < expires.timestamp() - 1:
                require(
                    expiring.commands.run('printf active').stdout == 'active',
                    "sandbox expired before its absolute timeout",
                )
                time.sleep(0.4)
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                response = api('GET', f'/sandboxes/{expiring.sandbox_id}')
                if response.status_code == 404:
                    break
                time.sleep(0.5)
            else:
                raise AssertionError('absolute expiry did not delete an active sandbox')
            sandboxes.remove(expiring)
            checks.append('absolute timeout expires despite command activity')
            failing = Sandbox.create(args.template, timeout=60, **options)
            sandboxes.append(failing)
            native_path = '/api/instances'
            native_headers = {'Authorization': 'Bearer ' + key}
            native_params = {'instance_id': failing.sandbox_id}
            before = client.get(native_path, headers=native_headers, params=native_params)
            require(
                before.status_code == 200 and before.json()[0]['status'] == 'running',
                "native instance did not report the sandbox as running",
            )
            credentials = api('GET', f'/sandboxes/{failing.sandbox_id}').json()
            failing_headers = {'E2b-Sandbox-Id': failing.sandbox_id,
                               'E2b-Sandbox-Port': '49983',
                               'X-Access-Token': credentials['envdAccessToken']}
            try:
                failing.commands.run('kill -9 $(pidof envd)', user='root')
            except Exception:
                # Killing the owner of this RPC also closes its response stream.
                pass
            deadline = time.monotonic() + 15
            while time.monotonic() < deadline:
                # A refused envd connection alone does not prove that the
                # platform withdrew a stale Ready record.
                native = client.get(native_path, headers=native_headers, params=native_params)
                platform_failed = native.status_code == 404 or (
                    native.status_code == 200 and native.json()[0]['status'] == 'failed')
                response = client.get('/health', headers=failing_headers)
                if platform_failed and response.status_code in (404, 503):
                    break
                time.sleep(0.5)
            else:
                raise AssertionError('envd failure did not withdraw the data-plane route')
            failing.kill()
            sandboxes.remove(failing)
            checks.append('envd exit withdraws route and sandbox can be deleted')
            evidence['result'] = 'passed'
        except BaseException:
            evidence['result'] = 'failed'
            raise
        finally:
            for sandbox in sandboxes:
                sandbox.kill()
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(json.dumps(evidence, ensure_ascii=False, indent=2) + '\n')
    output_logger.info(json.dumps(evidence, ensure_ascii=False))


if __name__ == '__main__':
    main()
