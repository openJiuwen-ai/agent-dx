"""Lose a committed upload chunk reply and verify offset-based recovery."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import hashlib
import json
import os
from pathlib import Path
import socket
import ssl
import tempfile
import threading
import time
from urllib.error import HTTPError
from urllib.parse import parse_qs, urlsplit
from urllib.request import HTTPSHandler, ProxyHandler, Request, build_opener
import uuid


CHUNK_SIZE = 64 * 1024


class UploadResponseCutProxy:
    """Forward binary chunks, cutting only the first successful chunk reply."""

    def __init__(self, upstream, *, certificate=None, private_key=None, ca=None,
                 cut_upload=True, download_cut_bytes=None):
        self.upstream = upstream.rstrip('/')
        self.certificate = certificate
        self.private_key = private_key
        self.ca = ca
        self.cut_upload = cut_upload
        self.download_cut_bytes = download_cut_bytes
        self.cut_chunk = None
        self.cut_download = None
        self.chunks = []
        self.status_offsets = []
        self.download_attempts = []
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
                    content_range = response.headers.get('Content-Range')

                parsed_url = urlsplit(self.path)
                params = parse_qs(parsed_url.query)
                if (self.command == 'GET'
                        and parsed_url.path.endswith('/upload/status')
                        and status == 200):
                    with proxy._lock:
                        proxy.status_offsets.append(int(json.loads(payload)['offset']))
                if (self.command == 'POST' and parsed_url.path.endswith('/upload')
                        and params.get('uploadId')):
                    chunk = {
                        'upload_id': params['uploadId'][0],
                        'offset': int(params['offset'][0]),
                        'size': len(body),
                        'status': status,
                    }
                    with proxy._lock:
                        proxy.chunks.append(chunk)
                        if proxy.cut_upload and proxy.cut_chunk is None and status == 200:
                            result = json.loads(payload)
                            if not result.get('error'):
                                proxy.cut_chunk = {
                                    **chunk, 'committed_offset': int(result['offset']),
                                }
                                cut = True
                            else:
                                cut = False
                        else:
                            cut = False
                    if cut:
                        # Execd has flushed this chunk and returned its new
                        # offset; the SDK never receives that acknowledgment.
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return

                if (self.command == 'GET'
                        and parsed_url.path.endswith('/download')
                        and params.get('type') == ['file']):
                    range_header = self.headers.get('Range')
                    attempt = {
                        'range': range_header, 'status': status,
                        'size': len(payload),
                    }
                    with proxy._lock:
                        proxy.download_attempts.append(attempt)
                        cut_download = (proxy.download_cut_bytes is not None
                                        and proxy.cut_download is None
                                        and range_header is None
                                        and status == 200
                                        and len(payload) > proxy.download_cut_bytes)
                        if cut_download:
                            proxy.cut_download = {
                                **attempt, 'bytes_sent': proxy.download_cut_bytes,
                            }
                    if cut_download:
                        self.send_response(status)
                        self.send_header('Content-Type', content_type)
                        self.send_header('Content-Length', str(len(payload)))
                        self.end_headers()
                        self.wfile.write(payload[:proxy.download_cut_bytes])
                        self.wfile.flush()
                        self.close_connection = True
                        self.connection.shutdown(socket.SHUT_RDWR)
                        self.connection.close()
                        return

                self.send_response(status)
                self.send_header('Content-Type', content_type)
                self.send_header('Content-Length', str(len(payload)))
                if content_range is not None:
                    self.send_header('Content-Range', content_range)
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
    """Use the installed SDK with real Execd and verify the final file digest."""
    from route_ready import wait_for_route
    from adx_sandbox import ConnectionConfig, Sandbox
    from functional_lifecycle import _wait_deleted
    from node import catalog, labeled_backend

    started = time.monotonic()
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': []}
    name = 'upload-cut-' + uuid.uuid4().hex[:12]
    remote_path = '/tmp/' + name + '.bin'
    sandbox = None
    attached = None
    recovered = None
    deleted = False
    proxy = UploadResponseCutProxy(
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
        backend = labeled_backend(sandbox.id)
        assert len(backend) == 1, backend
        wait_for_route(sandbox)

        with tempfile.TemporaryDirectory(prefix='adx-upload-cut-') as directory:
            source = Path(directory) / 'source.bin'
            target = Path(directory) / 'target.bin'
            payload = bytes(range(256)) * (CHUNK_SIZE * 3 // 256) + b'final-chunk'
            source.write_bytes(payload)
            expected_digest = hashlib.sha256(payload).hexdigest()
            with proxy:
                prior = {
                    key: os.environ.get(key)
                    for key in ('ADX_RESUME_MIN_SIZE', 'ADX_RESUME_CHUNK_SIZE')
                }
                try:
                    os.environ['ADX_RESUME_MIN_SIZE'] = '1'
                    os.environ['ADX_RESUME_CHUNK_SIZE'] = str(CHUNK_SIZE)
                    proxied = ConnectionConfig(
                        server_address=f'127.0.0.1:{proxy.port}',
                        token=connection.token, use_tls=True, verify_tls=True,
                    )
                    attached = Sandbox.from_id(sandbox.id, connection=proxied)
                    attached.files.copy_from_local(str(source), remote_path)
                    attached.close()
                    attached = None
                finally:
                    for key, value in prior.items():
                        if value is None:
                            os.environ.pop(key, None)
                        else:
                            os.environ[key] = value

            cut = proxy.cut_chunk
            assert cut is not None and cut['offset'] == 0, cut
            assert cut['committed_offset'] == CHUNK_SIZE, cut
            assert proxy.status_offsets == [0, CHUNK_SIZE], proxy.status_offsets
            assert {item['upload_id'] for item in proxy.chunks} == {cut['upload_id']}
            assert [item['offset'] for item in proxy.chunks] == [
                0, CHUNK_SIZE, 2 * CHUNK_SIZE, 3 * CHUNK_SIZE,
            ], proxy.chunks
            assert all(item['status'] == 200 for item in proxy.chunks), proxy.chunks

            recovered = Sandbox.from_id(sandbox.id, connection=connection)
            recovered.files.copy_to_local(remote_path, str(target))
            actual_digest = hashlib.sha256(target.read_bytes()).hexdigest()
            assert actual_digest == expected_digest, (actual_digest, expected_digest)
            recovered.close()
            recovered = None

        final = json.loads(catalog()['environment:' + sandbox.id])
        assert final['assignment']['generation'] == initial['assignment']['generation']
        assert labeled_backend(sandbox.id) == backend
        Sandbox.delete(sandbox.id, connection=connection)
        deleted = True
        _wait_deleted(sandbox.id, connection, timeout=60)
        result = json.loads(catalog()['environment:' + sandbox.id])['result']
        assert result['state'] == 'Deleted' and not result['resources_held'], result
        assert not labeled_backend(sandbox.id)

        report['status'] = 'passed'
        report['cases'].append({
            'id': 'file.resumable-upload-response-cut', 'status': 'passed',
            'seconds': round(time.monotonic() - started, 3),
            'instance_id': sandbox.id,
            'upload_id': cut['upload_id'],
            'cut_offset': cut['committed_offset'],
            'chunks': len(proxy.chunks),
            'sha256': expected_digest,
            'backend': backend[0],
        })
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if attached is not None:
            attached.close()
        if recovered is not None:
            recovered.close()
        if sandbox is not None:
            if not deleted:
                try:
                    Sandbox.delete(sandbox.id, connection=connection)
                except Exception as error:
                    report['cleanup_errors'].append(str(error))
            sandbox.close()
        if report['cleanup_errors']:
            report['status'] = 'failed'
        report['cut_chunk'] = proxy.cut_chunk
        report['status_offsets'] = proxy.status_offsets
        report['chunks'] = proxy.chunks
        output.write_text(json.dumps(report, indent=2) + '\n')
