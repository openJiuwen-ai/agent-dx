"""A transient 404 after a lost create response must not change create identity."""

import importlib.util
import json
from http.client import RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import unittest
from urllib.error import HTTPError
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


class CreateUnknownQueryTests(unittest.TestCase):
    def test_query_404_does_not_cancel_original_inflight_create(self):
        received = []

        class Upstream(BaseHTTPRequestHandler):
            def do_GET(self):
                self.send_response(404 if not received else 200)
                self.end_headers()

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                received.append((self.headers['X-Request-Id'], body['name']))
                final = json.dumps({
                    'status': 'running', 'sandboxId': body['name'],
                    'requestId': self.headers['X-Request-Id'],
                }).encode()
                payload = b'event: final\ndata: ' + final + b'\n\n'
                self.send_response(200)
                self.send_header('Content-Type', 'text/event-stream')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        server = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        spec = importlib.util.spec_from_file_location(
            'build.e2e.create_unknown_query', ROOT / 'create_unknown_query.py')
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            upstream = f'http://127.0.0.1:{server.server_port}'
            with module.UnknownQueryProxy(upstream) as proxy:
                url = f'http://127.0.0.1:{proxy.port}/api/sandbox/v1/sandboxes'
                request = Request(url, data=json.dumps({'name': 'same-capsule'}).encode(),
                                  headers={'X-Request-Id': 'same-request',
                                           'Content-Type': 'application/json'})
                with self.assertRaises(RemoteDisconnected):
                    urlopen(request, timeout=5)
                self.assertTrue(proxy.first_cut.wait(5))
                with self.assertRaises(HTTPError) as missing:
                    urlopen(upstream + '/api/instances?instance_id=same-capsule', timeout=5)
                self.assertEqual(missing.exception.code, 404)
                self.assertEqual(received, [])

                result = []

                def retry():
                    with urlopen(request, timeout=10) as response:
                        result.append(response.read())

                retry_thread = threading.Thread(target=retry)
                retry_thread.start()
                self.assertFalse(proxy.first_finished.is_set())
                proxy.release_first.set()
                retry_thread.join(timeout=10)
                self.assertFalse(retry_thread.is_alive())
                self.assertIsNone(proxy.first_error)
                self.assertEqual(len(result), 1)
                self.assertIn(b'event: final', result[0])
                self.assertEqual(proxy.attempts,
                                 [('same-request', 'same-capsule')] * 2)
                self.assertEqual(received, proxy.attempts)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
