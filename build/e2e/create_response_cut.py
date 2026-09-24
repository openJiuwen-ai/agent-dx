"""Cut one real HTTP create response after its final event, then pass retries through."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import ssl
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener


def final_event(payload):
    """Extract the authoritative final event from an SSE response."""
    for block in payload.replace(b'\r\n', b'\n').split(b'\n\n'):
        lines = block.decode('utf-8').splitlines()
        if not any(line.strip() == 'event: final' for line in lines):
            continue
        data = '\n'.join(line[5:].lstrip() for line in lines if line.startswith('data:'))
        if data:
            result = json.loads(data)
            if isinstance(result, dict):
                return result
    raise ValueError('upstream create response has no final event')


class ResponseCutProxy:
    """A one-shot TLS-capable response fault outside the SDK and API Server."""

    def __init__(self, upstream, *, certificate=None, private_key=None, ca=None,
                 on_first_final=None):
        self.upstream = upstream.rstrip('/')
        self.ca = ca
        self.on_first_final = on_first_final
        self.attempts = []
        self.first_final = None
        self.first_evidence = None
        self.first_error = None
        self._lock = threading.Lock()
        self._server = None
        self._thread = None
        self.certificate = certificate
        self.private_key = private_key

    @property
    def port(self):
        return self._server.server_port

    def __enter__(self):
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                request = json.loads(body)
                request_id = self.headers['X-Request-Id']
                with proxy._lock:
                    proxy.attempts.append((request_id, request['name']))
                    first = len(proxy.attempts) == 1

                headers = {
                    key: value for key, value in self.headers.items()
                    if key.lower() not in ('host', 'connection', 'content-length',
                                           'accept-encoding', 'transfer-encoding')
                }
                upstream_request = Request(proxy.upstream + self.path, data=body,
                                           headers=headers, method='POST')
                handlers = [ProxyHandler({})]
                if proxy.upstream.startswith('https:'):
                    handlers.append(HTTPSHandler(
                        context=ssl.create_default_context(cafile=str(proxy.ca))))
                opener = build_opener(*handlers)
                try:
                    response = opener.open(upstream_request, timeout=180)
                except HTTPError as error:
                    response = error
                with response:
                    payload = response.read()
                    status = response.status
                    content_type = response.headers.get('Content-Type', 'application/octet-stream')

                if first and status == 200:
                    try:
                        proxy.first_final = final_event(payload)
                        if proxy.first_final.get('status') != 'running':
                            raise ValueError('first create did not commit a running instance')
                        if proxy.on_first_final is not None:
                            proxy.first_evidence = proxy.on_first_final(proxy.first_final)
                    except Exception as error:
                        proxy.first_error = error
                    # The upstream completed. Lose only its response to the SDK.
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
    """Prove a lost committed response converges through the installed public SDK."""
    from adx_sandbox import ConnectionConfig, Sandbox
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    name = 'response-cut-' + uuid.uuid4().hex
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    sandbox = None
    instance_id = None
    deleted = False

    def committed(final):
        sid = final.get('sandboxId') or final.get('instanceId')
        if not sid:
            raise AssertionError('first committed final omitted its instance ID')
        record = json.loads(catalog()['environment:' + sid])
        result = record['result']
        backends = labeled_backend(sid)
        assert result['state'] == 'Running' and len(backends) == 1, (result, backends)
        return {
            'instance_id': sid,
            'generation': record['assignment']['generation'],
            'runtime_id': result['runtime_id'],
            'backend': backends[0],
        }

    proxy = ResponseCutProxy(
        'https://127.0.0.1:8443', certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key', ca=secrets / 'tls/ca.pem',
        on_first_final=committed,
    )
    try:
        with proxy:
            proxy_connection = ConnectionConfig(
                server_address=f'127.0.0.1:{proxy.port}', token=connection.token,
                use_tls=True, verify_tls=True,
            )
            sandbox = Sandbox(
                image=image, runtime='runc', cpu=500, memory=512,
                idle_timeout=0, detached=True, node_id='node1', name=name,
                connection=proxy_connection, create_timeout=150,
            )
            instance_id = sandbox.id
            sandbox.close()
            sandbox = None

        if proxy.first_error is not None:
            raise AssertionError(f'first committed response observation failed: {proxy.first_error}')
        first = proxy.first_evidence
        assert first is not None and first['instance_id'] == instance_id, first
        assert 2 <= len(proxy.attempts) <= 3, proxy.attempts
        request_ids = {request_id for request_id, _ in proxy.attempts}
        names = {request_name for _, request_name in proxy.attempts}
        assert len(request_ids) == 1 and names == {name}, proxy.attempts

        record = json.loads(catalog()['environment:' + instance_id])
        assert record['assignment']['generation'] == first['generation'], record['assignment']
        assert record['result']['runtime_id'] == first['runtime_id'], record['result']
        assert labeled_backend(instance_id) == [first['backend']]
        attached = Sandbox.from_id(instance_id, connection=connection)
        try:
            command = attached.commands.run('printf response-cut-recovered')
            assert command.exit_code == 0 and command.stdout == 'response-cut-recovered'
        finally:
            attached.close()
        Sandbox.delete(instance_id, connection=connection)
        deleted = True
        _wait_deleted(instance_id, connection, timeout=60)
        final = json.loads(catalog()['environment:' + instance_id])['result']
        assert final['state'] == 'Deleted' and not final['resources_held'], final
        assert not labeled_backend(instance_id)
        report['status'] = 'passed'
        report['cases'].append({
            'id': 'reliability.create-final-response-cut', 'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': instance_id, 'request_id': next(iter(request_ids)),
            'attempts': len(proxy.attempts), 'generation': first['generation'],
            'backend': first['backend'],
        })
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if sandbox is not None:
            sandbox.close()
        if instance_id is None and proxy.first_final is not None:
            instance_id = proxy.first_final.get('sandboxId') or proxy.first_final.get('instanceId')
        if instance_id and not deleted:
            try:
                Sandbox.delete(instance_id, connection=connection)
            except Exception as error:
                report['cleanup_errors'].append(str(error))
        if report['cleanup_errors']:
            report['status'] = 'failed'
        report['attempts'] = proxy.attempts
        report['first_committed'] = proxy.first_evidence
        output.write_text(json.dumps(report, indent=2) + '\n')
