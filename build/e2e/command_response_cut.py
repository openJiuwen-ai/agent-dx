"""Lose real Execd command-start replies and recover by stable command ID."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import ssl
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener


class CommandResponseCutProxy:
    """Forward commands to the real Ingress, then cut successful start replies."""

    def __init__(self, upstream, *, certificate=None, private_key=None, ca=None,
                 cut_start=True, reject_watch=False):
        self.upstream = upstream.rstrip('/')
        self.certificate = certificate
        self.private_key = private_key
        self.ca = ca
        self.cut_start = cut_start
        self.reject_watch = reject_watch
        self.cut_attempts = []
        self.start_attempts = []
        self.watch_attempts = 0
        self._lock = threading.Lock()
        self._server = None
        self._thread = None

    @property
    def port(self):
        return self._server.server_port

    def __enter__(self):
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                if (proxy.reject_watch
                        and self.path.startswith('/api/sandbox/v1/commands/watch')):
                    with proxy._lock:
                        proxy.watch_attempts += 1
                    self.send_error(503, 'command watch unavailable')
                    return
                self._forward()

            def do_POST(self):
                self._forward()

            def _forward(self):
                body = (self.rfile.read(int(self.headers['Content-Length']))
                        if self.command == 'POST' else None)
                headers = {
                    key: value for key, value in self.headers.items()
                    if key.lower() not in ('host', 'connection', 'content-length',
                                           'accept-encoding', 'transfer-encoding')
                }
                request = Request(proxy.upstream + self.path, data=body,
                                  headers=headers, method=self.command)
                handlers = [ProxyHandler({})]
                if proxy.upstream.startswith('https:'):
                    handlers.append(HTTPSHandler(
                        context=ssl.create_default_context(cafile=str(proxy.ca))))
                opener = build_opener(*handlers)
                try:
                    response = opener.open(request, timeout=60)
                except HTTPError as error:
                    response = error
                with response:
                    payload = response.read()
                    status = response.status
                    content_type = response.headers.get(
                        'Content-Type', 'application/octet-stream')

                start = None
                if self.command == 'POST' and self.path.startswith('/direct/'):
                    submitted = json.loads(body)
                    if submitted.get('action') == 'process.start':
                        start = submitted
                        with proxy._lock:
                            proxy.start_attempts.append({
                                'request_id': self.headers.get('X-ADX-Request-ID'),
                                'command_id': start['args']['command_id'],
                            })
                if proxy.cut_start and start is not None and status == 200:
                    result = json.loads(payload)
                    if not result.get('error'):
                        attempt = {
                            'request_id': self.headers.get('X-ADX-Request-ID'),
                            'command_id': start['args']['command_id'],
                            'result': result,
                        }
                        with proxy._lock:
                            proxy.cut_attempts.append(attempt)
                        # The upstream accepted the command; lose only the
                        # response bytes on this SDK-to-Ingress connection.
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return

                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        self._server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        if self.certificate is not None:
            context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            context.load_cert_chain(str(self.certificate), str(self.private_key))
            self._server.socket = context.wrap_socket(self._server.socket, server_side=True)
        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        return self

    def __exit__(self, *_args):
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)


def run(connection, image, output, secrets):
    """Recover a real command whose every successful start response was lost."""
    from adx_sandbox import (
        CommandSubmissionError, ConnectionConfig, Sandbox,
    )
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'command-cut-' + uuid.uuid4().hex[:12]
    command_id = 'command-cut-' + uuid.uuid4().hex[:12]
    marker = '/tmp/' + command_id + '.marker'
    sandbox = None
    attached = None
    recovered_handle = None
    deleted = False
    proxy = CommandResponseCutProxy(
        'https://127.0.0.1:8443', certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key', ca=secrets / 'tls/ca.pem',
    )
    try:
        sandbox = Sandbox(
            name=name, image=image, runtime='runc', node_id='node1',
            cpu=500, memory=512, idle_timeout=0, detached=True,
            connection=connection, create_timeout=150,
        )
        initial = json.loads(catalog()['environment:' + sandbox.id])
        initial_backend = labeled_backend(sandbox.id)
        assert len(initial_backend) == 1, initial_backend

        with proxy:
            proxied = ConnectionConfig(
                server_address=f'127.0.0.1:{proxy.port}',
                token=connection.token, use_tls=True, verify_tls=True,
            )
            attached = Sandbox.from_id(sandbox.id, connection=proxied)
            try:
                attached.commands.run(
                    f"printf x >> {marker}; printf command-cut-complete",
                    background=True, command_id=command_id,
                )
            except CommandSubmissionError as error:
                if (error.command_id != command_id or not error.may_have_started
                        or error.sandbox_id != sandbox.id or not error.request_id):
                    raise AssertionError(f'command unknown-outcome identity lost: {error}') from error
                report['request_id'] = error.request_id
            else:
                raise AssertionError('SDK did not surface the lost command result')

            attempts = proxy.cut_attempts
            assert len(attempts) == 3, attempts
            assert {item['request_id'] for item in attempts} == {report['request_id']}
            assert {item['command_id'] for item in attempts} == {command_id}
            assert all(item['result'].get('pid', 0) > 0 for item in attempts), attempts
            attached.close()
            attached = None

        # A fresh client on the healthy public entry recovers the committed
        # command without replaying process.start or changing its command ID.
        recovered_handle = Sandbox.from_id(sandbox.id, connection=connection)
        recovered = recovered_handle.commands.get(command_id).wait(timeout=30)
        assert recovered.exit_code == 0 and recovered.stdout == 'command-cut-complete', recovered
        marker_result = recovered_handle.commands.run(f'cat {marker}')
        assert marker_result.exit_code == 0 and marker_result.stdout == 'x', marker_result
        recovered_handle.commands.run(f'rm {marker}')
        recovered_handle.close()
        recovered_handle = None

        final = json.loads(catalog()['environment:' + sandbox.id])
        assert final['assignment']['generation'] == initial['assignment']['generation']
        assert labeled_backend(sandbox.id) == initial_backend
        Sandbox.delete(sandbox.id, connection=connection)
        deleted = True
        _wait_deleted(sandbox.id, connection, timeout=60)
        result = json.loads(catalog()['environment:' + sandbox.id])['result']
        assert result['state'] == 'Deleted' and not result['resources_held'], result
        assert not labeled_backend(sandbox.id)

        report['status'] = 'passed'
        report['cases'].append({
            'id': 'reliability.command-start-response-cut',
            'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': sandbox.id,
            'command_id': command_id,
            'request_id': report['request_id'],
            'start_attempts': len(attempts),
            'backend': initial_backend[0],
        })
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if attached is not None:
            attached.close()
        if recovered_handle is not None:
            recovered_handle.close()
        if sandbox is not None:
            if not deleted:
                try:
                    Sandbox.delete(sandbox.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
            sandbox.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        report['cut_attempts'] = proxy.cut_attempts
        output.write_text(json.dumps(report, indent=2) + '\n')
