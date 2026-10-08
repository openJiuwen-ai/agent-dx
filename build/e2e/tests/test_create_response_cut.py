"""A real HTTP proxy must cut one committed create response, then replay it."""

import importlib.util
import json
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from http.client import RemoteDisconnected
from pathlib import Path
import threading
import unittest
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


class CreateResponseCutTests(unittest.TestCase):
    def test_first_final_is_lost_and_retry_keeps_identity(self):
        received = []

        class Upstream(BaseHTTPRequestHandler):
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
        spec = importlib.util.spec_from_file_location('response_cut', ROOT / 'create_response_cut.py')
        proxy_module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(proxy_module)
            upstream = f'http://127.0.0.1:{server.server_port}'
            with proxy_module.ResponseCutProxy(upstream) as proxy:
                url = f'http://127.0.0.1:{proxy.port}/api/sandbox/v1/sandboxes'
                request = Request(url, data=json.dumps({'name': 'same-capsule'}).encode(),
                                  headers={'X-Request-Id': 'same-request',
                                           'Content-Type': 'application/json'})
                with self.assertRaises(RemoteDisconnected):
                    urlopen(request, timeout=5)
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(response.status, 200)
                    self.assertIn('event: final', response.read().decode())
                self.assertEqual(proxy.first_final['sandboxId'], 'same-capsule')
                self.assertEqual(proxy.attempts, [('same-request', 'same-capsule')] * 2)
                self.assertEqual(received, proxy.attempts)
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
