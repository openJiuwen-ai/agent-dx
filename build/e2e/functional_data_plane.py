#!/usr/bin/env python3
"""Installed SDK functional acceptance for the complete data-plane surface."""

import asyncio
import http.server
import json
import logging
import ssl
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

PORT = 18081
TLS_PORT = 18082
EXPECTED_BODY = "ADX-FORWARDED-PORT-OK"
HOST_EXPECTED_BODY = "ADX-HOST-PATH-OK"
TUNNEL_BODY = "ADX-REVERSE-TUNNEL-OK"
SERVER_COMMAND = (
    rf'''perl -MSocket -e '$|=1; '''
    rf'''socket(S,PF_INET,SOCK_STREAM,getprotobyname("tcp")); '''
    rf'''setsockopt(S,SOL_SOCKET,SO_REUSEADDR,1); bind(S,sockaddr_in('''
    rf'''{PORT}'''
    rf''',INADDR_ANY)) or die $!; listen(S,10); while(accept(C,S)){{ '''
    rf'''$request=<C>; '''
    rf'''$body=index($request,"/functional/host?x=1")>=0?"'''
    rf'''{HOST_EXPECTED_BODY}'''
    rf'''":"'''
    rf'''{EXPECTED_BODY}'''
    rf'''"; print C "HTTP/1.1 200 OK\r\nContent-Length: '''
    rf'''".length($body)."\r\nConnection: close\r\n\r\n".$body; '''
    rf'''close C; }}' '''
)


class _TunnelUpstream(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        body = f'{TUNNEL_BODY}:{self.path}'.encode()
        self.send_response(200)
        self.send_header('Content-Length', str(len(body)))
        self.send_header('Content-Type', 'text/plain')
        self.end_headers()
        self.wfile.write(body)

    def log_message(self, _format, *_args):
        pass


def _tunnel_fetch_command(url):
    target = urllib.parse.urlsplit(url.rstrip('/') + '/functional')
    if target.scheme != 'http' or target.hostname != '127.0.0.1' or not target.port:
        raise ValueError(f'invalid sandbox-side tunnel URL: {url}')
    path = target.path or '/'
    return (
        rf'''perl -MSocket -e '$|=1; '''
        rf'''socket(S,PF_INET,SOCK_STREAM,getprotobyname("tcp")); '''
        rf'''connect(S,sockaddr_in('''
        rf'''{target.port}'''
        rf''',inet_aton("127.0.0.1"))) or die $!; $r="GET '''
        rf'''{path}'''
        rf''' HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"; '''
        rf'''syswrite(S,$r)==length($r) or die $!; '''
        rf'''while(($n=sysread(S,$b,8192))>0){{print $b;}} defined($n) '''
        rf'''or die $!;' '''
    )


def _fetch_forwarded(sandbox, ca_path, port=PORT, authenticated=True):
    request = urllib.request.Request(
        sandbox.get_port_url(port),
        headers=sandbox.get_port_auth_headers() if authenticated else {},
    )
    return _fetch_request(request, ca_path)


def _fetch_host_forwarded(sandbox, ca_path, port=PORT, authenticated=True):
    gateway = urllib.parse.urlsplit(sandbox.get_port_url(port))
    request = urllib.request.Request(
        f'{gateway.scheme}://{gateway.netloc}/functional/host?x=1',
        headers={
            'Host': f'{sandbox.id}-{port}.example.test',
            **(sandbox.get_port_auth_headers() if authenticated else {}),
        },
    )
    return _fetch_request(request, ca_path)


def _fetch_request(request, ca_path):
    context = ssl.create_default_context(cafile=str(ca_path))
    deadline = time.monotonic() + 45
    last_error = None
    while time.monotonic() < deadline:
        try:
            with urllib.request.urlopen(request, context=context, timeout=5) as response:
                return response.read().decode()
        except urllib.error.HTTPError as error:
            # Authentication failures are a terminal response from a ready Ingress.
            # Retrying them hides the policy result and turns the negative check
            # into a misleading readiness timeout.
            if error.code in (401, 403):
                raise
            last_error = error
            time.sleep(1)
        except (urllib.error.URLError, TimeoutError) as error:
            last_error = error
            time.sleep(1)
    raise TimeoutError(f"forwarded port was not ready: {last_error}")


def run(connection, image, output, ca_path):
    # Keep module-level protocol helpers dependency-free so the CI driver
    # contract suite does not depend on an installed public SDK. The real
    # functional case runs inside the packaged client environment and imports
    # the SDK at execution time.
    from adx_sandbox import (
        CommandConflict,
        CommandNotFound,
        CommandStatus,
        DataPlaneSecurityPolicy,
        Sandbox,
        resources,
    )

    checks = {}
    cases = []
    sandbox = None
    server = None
    security_sandbox = None
    security_server = None
    tunnel_sandbox = None
    tunnel_upstream = None
    tunnel_thread = None

    def passed(case_id, started):
        seconds = round(time.monotonic() - started, 3)
        cases.append({'id': case_id, 'status': 'passed', 'seconds': seconds})
        print(f'[SDK CASE PASS] {case_id} ({seconds:.3f}s)', flush=True)

    def begin(case_id):
        print(f'[SDK CASE RUN] {case_id}', flush=True)
        return time.monotonic()

    try:
        started = begin('resources.current-capacity')
        discovered = resources(connection=connection)
        by_id = {node.id: node for node in discovered}
        if not ({'node1', 'node2'}.issubset(by_id)):
            raise AssertionError(by_id)
        for node_id in ('node1', 'node2'):
            node = by_id[node_id]
            if not (node.status == 0):
                raise AssertionError(node)  # Public resources contract: 0 is accepting allocations.
            if not (all(node.capacity.get(name, 0) > 0 for name in ('CPU', 'Memory', 'Disk'))):
                raise AssertionError()
            if not (
                all(0 <= node.allocatable.get(name, -1) <= node.capacity[name] for name in ('CPU', 'Memory', 'Disk'))
            ):
                raise AssertionError(node)
            if not (isinstance(node.labels, dict)):
                raise AssertionError(node)
        checks['resource_discovery'] = sorted(by_id)
        passed('resources.current-capacity', started)

        started = begin('sandbox.create-query-reattach')
        sandbox = Sandbox(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            env={'E2E_FUNCTIONAL_ENV': 'functional'},
            cwd='/tmp',
            labels={'adx.e2e': 'data-plane'},
            node_id='node1',
            port_forwardings=[PORT],
            connection=connection,
            create_timeout=150,
        )
        if not (sandbox.sandbox_id == sandbox.id):
            raise AssertionError()
        info = sandbox.get_info()
        if not (info.id == sandbox.id and info.state == 'running'):
            raise AssertionError()
        attached = Sandbox.from_id(sandbox.id, connection=connection)
        try:
            result = attached.commands.run(
                "printf '%s:%s' \"$PWD\" \"$E2E_FUNCTIONAL_ENV\"", cwd='/tmp', envs={'E2E_FUNCTIONAL_ENV': 'functional'}
            )
            if not (result.exit_code == 0 and result.stdout == '/tmp:functional'):
                raise AssertionError(result)
        finally:
            attached.close()
        checks['query_and_reattach'] = True
        passed('sandbox.create-query-reattach', started)

        started = begin('command.stdin-handle-recovery')
        stdin_handle = sandbox.commands.run('cat', background=True, stdin=True, command_id='functional-stdin')
        stdin_handle.send_stdin('line-one\nline-two\n')
        stdin_handle.close_stdin()
        stdin_result = stdin_handle.wait(timeout=20)
        if not (stdin_result.exit_code == 0 and stdin_result.stdout == 'line-one\nline-two\n'):
            raise AssertionError()
        recovered = sandbox.commands.get('functional-stdin').wait(timeout=2)
        if not (recovered.stdout == stdin_result.stdout):
            raise AssertionError()
        if not (any(command.id == 'functional-stdin' for command in sandbox.commands.list())):
            raise AssertionError()
        if not (stdin_handle.id == 'functional-stdin'):
            raise AssertionError()
        if not (stdin_handle.sandbox_id == sandbox.id):
            raise AssertionError()
        passed('command.stdin-handle-recovery', started)

        started = begin('command.collection-stdin-async-wait')
        collection_stdin = sandbox.commands.run(
            'cat', background=True, stdin=True, command_id='functional-collection-stdin'
        )
        if collection_stdin.poll() not in (CommandStatus.PENDING, CommandStatus.RUNNING):
            raise AssertionError()
        sandbox.commands.send_stdin(collection_stdin.id, 'collection-api\n')
        sandbox.commands.close_stdin(collection_stdin.id)
        collection_result = asyncio.run(collection_stdin.wait_async(timeout=20))
        if not (collection_result.exit_code == 0):
            raise AssertionError()
        if not (collection_result.stdout == 'collection-api\n'):
            raise AssertionError()
        passed('command.collection-stdin-async-wait', started)

        started = begin('command.idempotent-replay-and-conflict')
        stable = sandbox.commands.run(
            'sleep 0.2; printf stable-command',
            background=True,
            command_id='functional-stable-command',
        )
        replay = sandbox.commands.run(
            'sleep 0.2; printf stable-command',
            background=True,
            command_id='functional-stable-command',
        )
        if not (replay.id == stable.id):
            raise AssertionError()
        try:
            sandbox.commands.run(
                'printf changed-command',
                background=True,
                command_id='functional-stable-command',
            )
        except CommandConflict:
            logging.getLogger(__name__).debug("Best-effort operation failed", exc_info=True)
        else:
            raise AssertionError('changed command reused an existing command ID')
        stable_result = stable.wait(timeout=20)
        if not (stable_result.exit_code == 0 and stable_result.stdout == 'stable-command'):
            raise AssertionError()
        passed('command.idempotent-replay-and-conflict', started)

        started = begin('command.not-found-and-wait-timeout')
        try:
            sandbox.commands.get('functional-command-does-not-exist')
        except CommandNotFound:
            logging.getLogger(__name__).debug("Best-effort operation failed", exc_info=True)
        else:
            raise AssertionError('missing command did not raise CommandNotFound')
        waiting = sandbox.commands.run('sleep 120', background=True, command_id='functional-wait-timeout')
        timeout_result = waiting.wait(timeout=0.05)
        if not (timeout_result.status == CommandStatus.RUNNING):
            raise AssertionError()
        if not (timeout_result.error_code == 'WAIT_TIMEOUT'):
            raise AssertionError()
        if not (waiting.kill()):
            raise AssertionError()
        if not (waiting.wait(timeout=20).status == CommandStatus.KILLED):
            raise AssertionError()
        passed('command.not-found-and-wait-timeout', started)

        started = begin('command.collection-kill')
        killed = sandbox.commands.run('sleep 120', background=True, command_id='functional-kill')
        if not (sandbox.commands.kill(killed.id)):
            raise AssertionError()
        killed_result = killed.wait(timeout=20)
        if not (killed_result.status.value == 'KILLED'):
            raise AssertionError(killed_result)
        checks['recoverable_commands'] = True
        passed('command.collection-kill', started)

        async def shell_roundtrip():
            shell = await sandbox.shells.create(cwd='/tmp', envs={'ADX_SHELL_VALUE': 'seeded'})
            try:
                if not (shell.session_id):
                    raise AssertionError()
                initial = await shell.run('printf "%s:%s" "$PWD" "$ADX_SHELL_VALUE"')
                first = await shell.run('export ADX_SHELL_VALUE=persisted')
                second = await shell.run('printf "%s:%s" "$PWD" "$ADX_SHELL_VALUE"')
                failed = await shell.run("sh -c 'printf shell-failed; exit 7'")
                if not (initial.exit_code == 0 and initial.stdout == '/tmp:seeded'):
                    raise AssertionError(initial)
                if not (first.exit_code == 0):
                    raise AssertionError()
                if not (second.exit_code == 0 and second.stdout == '/tmp:persisted'):
                    raise AssertionError(second)
                if not (failed.exit_code == 7 and failed.stdout == 'shell-failed'):
                    raise AssertionError(failed)
            finally:
                shell.close()
            disposable = await sandbox.shells.create()
            await disposable.kill()

        started = begin('shell.state-env-cwd-and-exit')
        asyncio.run(shell_roundtrip())
        passed('shell.state-env-cwd-and-exit', started)

        async def terminal_shell_exit():
            shell = await sandbox.shells.create()
            try:
                result = await shell.run('printf shell-ended; exit 9', timeout=5)
                if not (result.exit_code == 9):
                    raise AssertionError(result)
                if 'shell-ended' not in result.stdout:
                    raise AssertionError(result)
            finally:
                shell.close()

        started = begin('shell.terminal-exit-does-not-hang')
        asyncio.run(terminal_shell_exit())
        sandbox.shells.close()
        checks['stateful_shell'] = True
        passed('shell.terminal-exit-does-not-hang', started)

        started = begin('filesystem.text-crud')
        sandbox.files.make_dir('/tmp/adx-functional')
        sandbox.files.write('/tmp/adx-functional/source.txt', 'file-api')
        info = sandbox.files.get_info('/tmp/adx-functional/source.txt')
        if not (info.type == 'file' and info.size == len('file-api')):
            raise AssertionError()
        sandbox.files.rename('/tmp/adx-functional/source.txt', '/tmp/adx-functional/renamed.txt')
        if not (sandbox.files.read('/tmp/adx-functional/renamed.txt') == 'file-api'):
            raise AssertionError()
        if not (any(entry.name == 'renamed.txt' for entry in sandbox.files.list('/tmp/adx-functional'))):
            raise AssertionError()
        sandbox.files.remove('/tmp/adx-functional/renamed.txt')
        if not (not sandbox.files.exists('/tmp/adx-functional/renamed.txt')):
            raise AssertionError()
        passed('filesystem.text-crud', started)

        started = begin('filesystem.binary-read-write')
        binary = b'functional-binary\x00\xff\n' * 8192
        written = sandbox.files.write('/tmp/adx-functional/binary.bin', binary)
        if not (written.size == len(binary)):
            raise AssertionError(written)
        if not (sandbox.files.read('/tmp/adx-functional/binary.bin', format='bytes') == binary):
            raise AssertionError()
        passed('filesystem.binary-read-write', started)

        started = begin('filesystem.directory-copy-and-depth')
        with tempfile.TemporaryDirectory() as temporary:
            source = Path(temporary) / 'source'
            target = Path(temporary) / 'target'
            source.mkdir()
            (source / 'nested').mkdir()
            payload = b'copy-roundtrip\x00\xff' * 131072
            (source / 'nested' / 'payload.bin').write_bytes(payload)
            sandbox.files.copy_from_local(str(source), '/tmp/adx-functional/copied')
            entries = sandbox.files.list('/tmp/adx-functional/copied', depth=3)
            if not (any(entry.path.endswith('/nested/payload.bin') for entry in entries)):
                raise AssertionError(entries)
            sandbox.files.copy_to_local('/tmp/adx-functional/copied', str(target))
            if not ((target / 'nested' / 'payload.bin').read_bytes() == payload):
                raise AssertionError()
        checks['filesystem_crud_and_copy'] = True
        passed('filesystem.directory-copy-and-depth', started)

        started = begin('pty.output-resize')
        pty_output = bytearray()
        session = sandbox.pty.create(
            ['/bin/sh', '-lc', 'sleep 0.2; printf pty-functional'],
            rows=24,
            cols=80,
            on_data=pty_output.extend,
            timeout=20,
        )
        try:
            session.resize(rows=40, cols=100)
            if not (session.wait(20) == 0):
                raise AssertionError()
        finally:
            session.close()
        if b'pty-functional' not in pty_output:
            raise AssertionError(bytes(pty_output))
        if not (session.done and session.exit_code == 0):
            raise AssertionError()
        checks['pty'] = True
        passed('pty.output-resize', started)

        started = begin('pty.stdin-eof-and-session-state')
        interactive_output = bytearray()
        interactive = sandbox.pty.create(
            ['/bin/sh', '-lc', 'value=$(cat); printf "pty-input:%s" "$value"'],
            on_data=interactive_output.extend,
            timeout=20,
        )
        try:
            if not (interactive.session_id and not interactive.done):
                raise AssertionError()
            interactive.send_stdin(b'from-sdk')
            interactive.close_stdin()
            if not (interactive.wait(20) == 0):
                raise AssertionError()
            if not (interactive.done and interactive.exit_code == 0):
                raise AssertionError()
        finally:
            interactive.close()
        if b'pty-input:from-sdk' not in interactive_output:
            raise AssertionError(bytes(interactive_output))
        passed('pty.stdin-eof-and-session-state', started)

        started = begin('port-forward.tls-token-route')
        server = sandbox.commands.run(SERVER_COMMAND, background=True, command_id='functional-http')
        try:
            _fetch_forwarded(sandbox, ca_path, authenticated=False)
        except urllib.error.HTTPError as error:
            if error.code not in (401, 403):
                raise AssertionError(error)
        else:
            raise AssertionError('tls-token forwarded port accepted a request without a token')
        if not (_fetch_forwarded(sandbox, ca_path) == EXPECTED_BODY):
            raise AssertionError()
        checks['forwarded_port'] = True
        passed('port-forward.tls-token-route', started)

        started = begin('port-forward.host-subdomain-route')
        try:
            _fetch_host_forwarded(sandbox, ca_path, authenticated=False)
        except urllib.error.HTTPError as error:
            if error.code not in (401, 403):
                raise AssertionError(error)
        else:
            raise AssertionError('Host forwarded port accepted a request without a token')
        if not (_fetch_host_forwarded(sandbox, ca_path) == HOST_EXPECTED_BODY):
            raise AssertionError()
        checks['host_subdomain_forwarding'] = True
        passed('port-forward.host-subdomain-route', started)

        started = begin('port-forward.per-instance-tls-route')
        security_sandbox = Sandbox(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            node_id='node1',
            port_forwardings=[TLS_PORT],
            data_plane_security=DataPlaneSecurityPolicy(port_forward_mode='tls'),
            connection=connection,
            create_timeout=150,
        )
        tls_command = SERVER_COMMAND.replace(str(PORT), str(TLS_PORT))
        security_server = security_sandbox.commands.run(tls_command, background=True, command_id='functional-http-tls')
        if not (_fetch_forwarded(security_sandbox, ca_path, port=TLS_PORT, authenticated=False) == EXPECTED_BODY):
            raise AssertionError()
        checks['per_instance_data_plane_security'] = True
        passed('port-forward.per-instance-tls-route', started)

        started = begin('reverse-tunnel.sdk-upstream-roundtrip')
        tunnel_upstream = http.server.ThreadingHTTPServer(('127.0.0.1', 0), _TunnelUpstream)
        tunnel_thread = threading.Thread(
            target=tunnel_upstream.serve_forever,
            name='adx-e2e-reverse-tunnel-upstream',
            daemon=True,
        )
        tunnel_thread.start()
        upstream_port = tunnel_upstream.server_address[1]
        tunnel_sandbox = Sandbox(
            image=image,
            runtime='runc',
            cpu=500,
            memory=512,
            idle_timeout=0,
            node_id='node1',
            upstream=f'http://127.0.0.1:{upstream_port}',
            connection=connection,
            create_timeout=150,
            tunnel_connect_timeout=30,
        )
        tunnel_url = tunnel_sandbox.get_tunnel_url()
        command = _tunnel_fetch_command(tunnel_url)
        tunnel_result = None
        for attempt in range(3):
            tunnel_result = tunnel_sandbox.commands.run(command)
            if tunnel_result.exit_code == 0 and f'{TUNNEL_BODY}:/functional' in tunnel_result.stdout:
                break
            if attempt < 2:
                time.sleep(1)
        if not (tunnel_result is not None):
            raise AssertionError()
        if not (tunnel_result.exit_code == 0):
            raise AssertionError(tunnel_result)
        if f'{TUNNEL_BODY}:/functional' not in tunnel_result.stdout:
            raise AssertionError(tunnel_result)
        checks['reverse_tunnel'] = True
        passed('reverse-tunnel.sdk-upstream-roundtrip', started)

        result = {
            'status': 'passed',
            'instance_id': sandbox.id,
            'checks': checks,
            'cases': cases,
        }
        Path(output).write_text(json.dumps(result, indent=2) + '\n')
        print(json.dumps(result), flush=True)
    finally:
        if server is not None:
            try:
                server.kill()
            except Exception:
                logging.getLogger(__name__).debug("Best-effort operation failed", exc_info=True)
        if security_server is not None:
            try:
                security_server.kill()
            except Exception:
                logging.getLogger(__name__).debug("Best-effort operation failed", exc_info=True)
        if security_sandbox is not None:
            try:
                security_sandbox.kill()
            finally:
                security_sandbox.close()
        if tunnel_sandbox is not None:
            try:
                tunnel_sandbox.kill()
            finally:
                tunnel_sandbox.close()
        if tunnel_upstream is not None:
            tunnel_upstream.shutdown()
            tunnel_upstream.server_close()
        if tunnel_thread is not None:
            tunnel_thread.join(timeout=5)
        if sandbox is not None:
            try:
                sandbox.kill()
            finally:
                sandbox.close()
