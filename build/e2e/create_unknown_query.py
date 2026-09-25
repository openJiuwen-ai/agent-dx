"""Exercise a 404 read after create transport loss, before its write completes."""

from concurrent.futures import ThreadPoolExecutor
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
import ssl
import threading
import time
import uuid
from urllib.error import HTTPError
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener

if __package__:
    from .create_response_cut import final_event
else:
    from create_response_cut import final_event


class UnknownQueryProxy:
    """Lose the first create response; hold its upstream write until a real 404."""

    def __init__(self, upstream, *, certificate=None, private_key=None, ca=None,
                 on_first_final=None):
        self.upstream = upstream.rstrip('/')
        self.certificate = certificate
        self.private_key = private_key
        self.ca = ca
        self.on_first_final = on_first_final
        self.attempts = []
        self.first_cut = threading.Event()
        self.release_first = threading.Event()
        self.first_finished = threading.Event()
        self.first_final = None
        self.first_evidence = None
        self.first_error = None
        self._lock = threading.Lock()
        self._server = None
        self._thread = None
        self._first_worker = None

    @property
    def port(self):
        return self._server.server_port

    def _forward(self, path, body, headers):
        upstream_request = Request(self.upstream + path, data=body,
                                   headers=headers, method='POST')
        handlers = [ProxyHandler({})]
        if self.upstream.startswith('https:'):
            handlers.append(HTTPSHandler(
                context=ssl.create_default_context(cafile=str(self.ca))))
        opener = build_opener(*handlers)
        try:
            response = opener.open(upstream_request, timeout=180)
        except HTTPError as error:
            response = error
        with response:
            return (response.status,
                    response.headers.get('Content-Type', 'application/octet-stream'),
                    response.read())

    def _finish_first(self, path, body, headers):
        try:
            if not self.release_first.wait(30):
                raise TimeoutError('first create was not released after the 404 probe')
            status, _content_type, payload = self._forward(path, body, headers)
            if status != 200:
                raise AssertionError(f'first create returned HTTP {status}: {payload[:512]}')
            self.first_final = final_event(payload)
            if self.first_final.get('status') != 'running':
                raise AssertionError(f'first create did not reach Running: {self.first_final}')
            if self.on_first_final is not None:
                self.first_evidence = self.on_first_final(self.first_final)
        except Exception as error:
            self.first_error = error
        finally:
            self.first_finished.set()

    def __enter__(self):
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers['Content-Length']))
                request = json.loads(body)
                request_id = self.headers['X-Request-Id']
                headers = {
                    key: value for key, value in self.headers.items()
                    if key.lower() not in ('host', 'connection', 'content-length',
                                           'accept-encoding', 'transfer-encoding')
                }
                with proxy._lock:
                    proxy.attempts.append((request_id, request['name']))
                    first = len(proxy.attempts) == 1
                if first:
                    proxy._first_worker = threading.Thread(
                        target=proxy._finish_first,
                        args=(self.path, body, headers), daemon=True,
                    )
                    proxy._first_worker.start()
                    self.close_connection = True
                    self.connection.shutdown(socket.SHUT_RDWR)
                    self.connection.close()
                    proxy.first_cut.set()
                    return

                if not proxy.first_finished.wait(180):
                    self.send_error(504, 'first create is still in flight')
                    return
                if proxy.first_error is not None:
                    self.send_error(502, f'first create failed: {proxy.first_error}')
                    return
                status, content_type, payload = proxy._forward(self.path, body, headers)
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
        self.release_first.set()
        self._server.shutdown()
        self._server.server_close()
        self._thread.join(timeout=2)
        if self._first_worker is not None:
            self._first_worker.join(timeout=2)


def run(connection, image, output, secrets):
    """Check the installed SDK, real API lookup, Redis owner and physical backend."""
    from adx_sandbox import ConnectionConfig, Sandbox, SandboxNotFound
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend, persisted_runtime_id

    started = time.monotonic()
    name = 'unknown-query-' + uuid.uuid4().hex
    instance_id = 'default-' + name
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    deleted = False
    sandbox = None

    def committed(final):
        sid = final.get('sandboxId') or final.get('instanceId')
        if sid != instance_id:
            raise AssertionError(f'first create changed instance identity: {sid}')
        record = json.loads(catalog()['environment:' + sid])
        result = record['result']
        backends = labeled_backend(sid)
        assert result['state'] == 'Running' and len(backends) == 1, (result, backends)
        return {
            'instance_id': sid,
            'generation': record['assignment']['generation'],
            'runtime_id': persisted_runtime_id(result),
            'backend': backends[0],
        }

    proxy = UnknownQueryProxy(
        'https://127.0.0.1:8443', certificate=secrets / 'tls/ingress.pem',
        private_key=secrets / 'tls/ingress.key', ca=secrets / 'tls/ca.pem',
        on_first_final=committed,
    )
    try:
        with proxy, ThreadPoolExecutor(max_workers=1) as executor:
            proxy_connection = ConnectionConfig(
                server_address=f'127.0.0.1:{proxy.port}', token=connection.token,
                use_tls=True, verify_tls=True,
            )
            future = executor.submit(
                Sandbox, image=image, runtime='runc', cpu=500, memory=512,
                idle_timeout=0, detached=True, node_id='node1', name=name,
                connection=proxy_connection, create_timeout=150,
            )
            try:
                if not proxy.first_cut.wait(10):
                    raise AssertionError('first create was not observed by the fault proxy')
                try:
                    missing = Sandbox.from_id(instance_id, connection=connection)
                except SandboxNotFound:
                    report['query_404_after_unknown'] = True
                else:
                    missing.close()
                    raise AssertionError('in-flight create unexpectedly appeared before forwarding')
                if 'environment:' + instance_id in catalog():
                    raise AssertionError('first create reached Redis before the 404 probe')
            finally:
                proxy.release_first.set()
            sandbox = future.result(timeout=180)
            if not proxy.first_finished.wait(5) or proxy.first_error is not None:
                raise AssertionError(f'original create did not commit: {proxy.first_error}')
            if sandbox.id != instance_id:
                raise AssertionError(f'SDK changed instance ID: {sandbox.id}')
            sandbox.close()
            sandbox = None

        first = proxy.first_evidence
        if first is None:
            raise AssertionError('missing first create physical evidence')
        if not 2 <= len(proxy.attempts) <= 3:
            raise AssertionError(f'unexpected retry count: {proxy.attempts}')
        request_ids = {request_id for request_id, _ in proxy.attempts}
        names = {request_name for _, request_name in proxy.attempts}
        if len(request_ids) != 1 or names != {name}:
            raise AssertionError(f'create retried with a different identity: {proxy.attempts}')
        record = json.loads(catalog()['environment:' + instance_id])
        if (record['assignment']['generation'] != first['generation']
                or persisted_runtime_id(record['result']) != first['runtime_id']
                or labeled_backend(instance_id) != [first['backend']]):
            raise AssertionError('retry created a second assignment or backend')
        attached = Sandbox.from_id(instance_id, connection=connection)
        try:
            command = attached.commands.run('printf unknown-query-recovered')
            assert command.exit_code == 0 and command.stdout == 'unknown-query-recovered'
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
            'id': 'reliability.unknown-query-404-same-create', 'status': 'passed',
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
        proxy.release_first.set()
        if proxy.first_cut.is_set() and not proxy.first_finished.wait(30):
            report['cleanup_errors'].append('original create did not settle before cleanup')
        if sandbox is not None:
            sandbox.close()
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
