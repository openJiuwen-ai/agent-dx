#!/usr/bin/env python3
"""Installed SDK functional acceptance for the complete data-plane surface."""
import asyncio
import http.server
import json
from pathlib import Path
import ssl
import tempfile
import threading
import time
import urllib.error
import urllib.parse
import urllib.request

PORT = 18081
TLS_PORT = 18082
EXPECTED_BODY = "ADX-FORWARDED-PORT-OK"
TUNNEL_BODY = "ADX-REVERSE-TUNNEL-OK"
SERVER_COMMAND = rf'''perl -MSocket -e '$|=1; socket(S,PF_INET,SOCK_STREAM,getprotobyname("tcp")); setsockopt(S,SOL_SOCKET,SO_REUSEADDR,1); bind(S,sockaddr_in({PORT},INADDR_ANY)) or die $!; listen(S,10); while(accept(C,S)){{ print C "HTTP/1.1 200 OK\r\nContent-Length: {len(EXPECTED_BODY)}\r\nConnection: close\r\n\r\n{EXPECTED_BODY}"; close C; }}' '''


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
    return rf'''perl -MSocket -e '$|=1; socket(S,PF_INET,SOCK_STREAM,getprotobyname("tcp")); connect(S,sockaddr_in({target.port},inet_aton("127.0.0.1"))) or die $!; $r="GET {path} HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\n\r\n"; syswrite(S,$r)==length($r) or die $!; while(($n=sysread(S,$b,8192))>0){{print $b;}} defined($n) or die $!;' '''


def _fetch_forwarded(sandbox, ca_path, port=PORT, authenticated=True):
    request = urllib.request.Request(
        sandbox.get_port_url(port),
        headers=sandbox.get_port_auth_headers() if authenticated else {},
    )
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
        assert {'node1', 'node2'}.issubset(by_id), by_id
        for node_id in ('node1', 'node2'):
            node = by_id[node_id]
            assert node.status == 0, node  # Public resources contract: 0 is accepting allocations.
            assert all(node.capacity.get(name, 0) > 0 for name in ('CPU', 'Memory', 'Disk'))
            assert all(
                0 <= node.allocatable.get(name, -1) <= node.capacity[name]
                for name in ('CPU', 'Memory', 'Disk')
            ), node
            assert isinstance(node.labels, dict), node
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
        assert sandbox.sandbox_id == sandbox.id
        info = sandbox.get_info()
        assert info.id == sandbox.id and info.state == 'running'
        attached = Sandbox.from_id(sandbox.id, connection=connection)
        try:
            result = attached.commands.run("printf '%s:%s' \"$PWD\" \"$E2E_FUNCTIONAL_ENV\"", cwd='/tmp', envs={'E2E_FUNCTIONAL_ENV': 'functional'})
            assert result.exit_code == 0 and result.stdout == '/tmp:functional', result
        finally:
            attached.close()
        checks['query_and_reattach'] = True
        passed('sandbox.create-query-reattach', started)

        started = begin('command.stdin-handle-recovery')
        stdin_handle = sandbox.commands.run(
            'cat', background=True, stdin=True, command_id='functional-stdin'
        )
        stdin_handle.send_stdin('line-one\nline-two\n')
        stdin_handle.close_stdin()
        stdin_result = stdin_handle.wait(timeout=20)
        assert stdin_result.exit_code == 0 and stdin_result.stdout == 'line-one\nline-two\n'
        recovered = sandbox.commands.get('functional-stdin').wait(timeout=2)
        assert recovered.stdout == stdin_result.stdout
        assert any(command.id == 'functional-stdin' for command in sandbox.commands.list())
        assert stdin_handle.id == 'functional-stdin'
        assert stdin_handle.sandbox_id == sandbox.id
        passed('command.stdin-handle-recovery', started)

        started = begin('command.collection-stdin-async-wait')
        collection_stdin = sandbox.commands.run(
            'cat', background=True, stdin=True, command_id='functional-collection-stdin'
        )
        assert collection_stdin.poll() in (CommandStatus.PENDING, CommandStatus.RUNNING)
        sandbox.commands.send_stdin(collection_stdin.id, 'collection-api\n')
        sandbox.commands.close_stdin(collection_stdin.id)
        collection_result = asyncio.run(collection_stdin.wait_async(timeout=20))
        assert collection_result.exit_code == 0
        assert collection_result.stdout == 'collection-api\n'
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
        assert replay.id == stable.id
        try:
            sandbox.commands.run(
                'printf changed-command',
                background=True,
                command_id='functional-stable-command',
            )
        except CommandConflict:
            pass
        else:
            raise AssertionError('changed command reused an existing command ID')
        stable_result = stable.wait(timeout=20)
        assert stable_result.exit_code == 0 and stable_result.stdout == 'stable-command'
        passed('command.idempotent-replay-and-conflict', started)

        started = begin('command.not-found-and-wait-timeout')
        try:
            sandbox.commands.get('functional-command-does-not-exist')
        except CommandNotFound:
            pass
        else:
            raise AssertionError('missing command did not raise CommandNotFound')
        waiting = sandbox.commands.run(
            'sleep 120', background=True, command_id='functional-wait-timeout'
        )
        timeout_result = waiting.wait(timeout=0.05)
        assert timeout_result.status == CommandStatus.RUNNING
        assert timeout_result.error_code == 'WAIT_TIMEOUT'
        assert waiting.kill()
        assert waiting.wait(timeout=20).status == CommandStatus.KILLED
        passed('command.not-found-and-wait-timeout', started)

        started = begin('command.collection-kill')
        killed = sandbox.commands.run(
            'sleep 120', background=True, command_id='functional-kill'
        )
        assert sandbox.commands.kill(killed.id)
        killed_result = killed.wait(timeout=20)
        assert killed_result.status.value == 'KILLED', killed_result
        checks['recoverable_commands'] = True
        passed('command.collection-kill', started)

        async def shell_roundtrip():
            shell = await sandbox.shells.create(
                cwd='/tmp', envs={'ADX_SHELL_VALUE': 'seeded'}
            )
            try:
                assert shell.session_id
                initial = await shell.run('printf "%s:%s" "$PWD" "$ADX_SHELL_VALUE"')
                first = await shell.run('export ADX_SHELL_VALUE=persisted')
                second = await shell.run('printf "%s:%s" "$PWD" "$ADX_SHELL_VALUE"')
                failed = await shell.run("sh -c 'printf shell-failed; exit 7'")
                assert initial.exit_code == 0 and initial.stdout == '/tmp:seeded', initial
                assert first.exit_code == 0
                assert second.exit_code == 0 and second.stdout == '/tmp:persisted', second
                assert failed.exit_code == 7 and failed.stdout == 'shell-failed', failed
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
                assert result.exit_code == 9, result
                assert 'shell-ended' in result.stdout, result
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
        assert info.type == 'file' and info.size == len('file-api')
        sandbox.files.rename(
            '/tmp/adx-functional/source.txt', '/tmp/adx-functional/renamed.txt'
        )
        assert sandbox.files.read('/tmp/adx-functional/renamed.txt') == 'file-api'
        assert any(entry.name == 'renamed.txt' for entry in sandbox.files.list('/tmp/adx-functional'))
        sandbox.files.remove('/tmp/adx-functional/renamed.txt')
        assert not sandbox.files.exists('/tmp/adx-functional/renamed.txt')
        passed('filesystem.text-crud', started)

        started = begin('filesystem.binary-read-write')
        binary = b'functional-binary\x00\xff\n' * 8192
        written = sandbox.files.write('/tmp/adx-functional/binary.bin', binary)
        assert written.size == len(binary), written
        assert sandbox.files.read('/tmp/adx-functional/binary.bin', format='bytes') == binary
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
            assert any(entry.path.endswith('/nested/payload.bin') for entry in entries), entries
            sandbox.files.copy_to_local('/tmp/adx-functional/copied', str(target))
            assert (target / 'nested' / 'payload.bin').read_bytes() == payload
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
            assert session.wait(20) == 0
        finally:
            session.close()
        assert b'pty-functional' in pty_output, bytes(pty_output)
        assert session.done and session.exit_code == 0
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
            assert interactive.session_id and not interactive.done
            interactive.send_stdin(b'from-sdk')
            interactive.close_stdin()
            assert interactive.wait(20) == 0
            assert interactive.done and interactive.exit_code == 0
        finally:
            interactive.close()
        assert b'pty-input:from-sdk' in interactive_output, bytes(interactive_output)
        passed('pty.stdin-eof-and-session-state', started)

        started = begin('port-forward.tls-token-route')
        server = sandbox.commands.run(
            SERVER_COMMAND, background=True, command_id='functional-http'
        )
        try:
            _fetch_forwarded(sandbox, ca_path, authenticated=False)
        except urllib.error.HTTPError as error:
            assert error.code in (401, 403), error
        else:
            raise AssertionError('tls-token forwarded port accepted a request without a token')
        assert _fetch_forwarded(sandbox, ca_path) == EXPECTED_BODY
        checks['forwarded_port'] = True
        passed('port-forward.tls-token-route', started)

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
        security_server = security_sandbox.commands.run(
            tls_command, background=True, command_id='functional-http-tls'
        )
        assert _fetch_forwarded(
            security_sandbox, ca_path, port=TLS_PORT, authenticated=False
        ) == EXPECTED_BODY
        checks['per_instance_data_plane_security'] = True
        passed('port-forward.per-instance-tls-route', started)

        started = begin('reverse-tunnel.sdk-upstream-roundtrip')
        tunnel_upstream = http.server.ThreadingHTTPServer(
            ('127.0.0.1', 0), _TunnelUpstream
        )
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
            if (
                tunnel_result.exit_code == 0
                and f'{TUNNEL_BODY}:/functional' in tunnel_result.stdout
            ):
                break
            if attempt < 2:
                time.sleep(1)
        assert tunnel_result is not None
        assert tunnel_result.exit_code == 0, tunnel_result
        assert f'{TUNNEL_BODY}:/functional' in tunnel_result.stdout, tunnel_result
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
                pass
        if security_server is not None:
            try:
                security_server.kill()
            except Exception:
                pass
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
