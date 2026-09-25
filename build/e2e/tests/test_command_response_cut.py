"""The fault proxy must cut only committed command-start responses."""

import importlib.util
import json
from http.client import RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import unittest
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


class CommandResponseCutTests(unittest.TestCase):
    def test_capability_filter_preserves_other_real_advertised_features(self):
        class Upstream(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                payload = json.dumps({
                    'protocol_version': 1,
                    'capabilities': [
                        'stable-command-id', 'recoverable-command-result',
                        'multiplexed-command-watch',
                    ],
                }).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        spec = importlib.util.spec_from_file_location(
            'command_response_cut', ROOT / 'command_response_cut.py')
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            with module.CommandResponseCutProxy(
                f'http://127.0.0.1:{upstream.server_port}',
                cut_start=False, strip_watch_capability=True,
            ) as proxy:
                request = Request(
                    f'http://127.0.0.1:{proxy.port}/direct/example/invoke',
                    data=json.dumps({
                        'action': 'process.capabilities', 'args': {},
                    }).encode(),
                )
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(json.load(response)['capabilities'], [
                        'stable-command-id', 'recoverable-command-result',
                    ])
                self.assertEqual(len(proxy.capability_attempts), 1)
                self.assertEqual(proxy.start_attempts, [])
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=2)

    def test_watch_can_fail_while_normal_http_query_stays_available(self):
        class Upstream(BaseHTTPRequestHandler):
            def do_GET(self):
                payload = b'{"status":"running"}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def do_POST(self):
                self.rfile.read(int(self.headers['Content-Length']))
                payload = b'{"pid":42}'
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        spec = importlib.util.spec_from_file_location(
            'command_response_cut', ROOT / 'command_response_cut.py')
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            with module.CommandResponseCutProxy(
                f'http://127.0.0.1:{upstream.server_port}',
                cut_start=False, reject_watch=True,
            ) as proxy:
                base = f'http://127.0.0.1:{proxy.port}'
                with self.assertRaisesRegex(Exception, '503'):
                    urlopen(base + '/api/sandbox/v1/commands/watch', timeout=5)
                with urlopen(base + '/api/instances', timeout=5) as response:
                    self.assertEqual(json.load(response), {'status': 'running'})
                request = Request(
                    base + '/direct/example/invoke',
                    data=json.dumps({
                        'action': 'process.start',
                        'args': {'command_id': 'stable-command'},
                    }).encode(),
                    headers={'X-ADX-Request-ID': 'stable-request'},
                )
                with urlopen(request, timeout=5) as response:
                    self.assertEqual(json.load(response), {'pid': 42})
                self.assertEqual(proxy.watch_attempts, 1)
                self.assertEqual(proxy.start_attempts, [{
                    'request_id': 'stable-request',
                    'command_id': 'stable-command',
                }])
                self.assertEqual(proxy.cut_attempts, [])
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=2)

    def test_query_can_fail_after_command_start_while_watch_is_unavailable(self):
        class Upstream(BaseHTTPRequestHandler):
            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                payload = json.dumps({'status': 'RUNNING'} if body['action'] == 'process.get'
                                     else {'pid': 42}).encode()
                self.send_response(200)
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        spec = importlib.util.spec_from_file_location(
            'command_response_cut', ROOT / 'command_response_cut.py')
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            with module.CommandResponseCutProxy(
                f'http://127.0.0.1:{upstream.server_port}',
                cut_start=False, reject_watch=True, reject_get_on_watch=True,
            ) as proxy:
                url = f'http://127.0.0.1:{proxy.port}/direct/example/invoke'
                def invoke(action):
                    return Request(url, data=json.dumps({
                        'action': action, 'args': {'command_id': 'stable-command'},
                    }).encode())

                with urlopen(invoke('process.start'), timeout=5) as response:
                    self.assertEqual(json.load(response)['pid'], 42)
                with urlopen(invoke('process.get'), timeout=5) as response:
                    self.assertEqual(json.load(response)['status'], 'RUNNING')
                with self.assertRaisesRegex(Exception, '503'):
                    urlopen(f'http://127.0.0.1:{proxy.port}/api/sandbox/v1/commands/watch',
                            timeout=5)
                with self.assertRaisesRegex(Exception, '503'):
                    urlopen(invoke('process.get'), timeout=5)
                self.assertEqual(proxy.rejected_get_attempts, 1)
                self.assertEqual(proxy.watch_attempts, 1)
                self.assertEqual(len(proxy.start_attempts), 1)
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=2)

    def test_start_is_forwarded_with_same_identity_but_each_response_is_lost(self):
        received = []

        class Upstream(BaseHTTPRequestHandler):
            def do_GET(self):
                self._reply({'instance': 'ready'})

            def do_POST(self):
                body = json.loads(self.rfile.read(int(self.headers['Content-Length'])))
                received.append((self.headers['X-ADX-Request-ID'], body))
                if body['action'] == 'process.start':
                    self._reply({'pid': 42, 'command_id': body['args']['command_id']})
                else:
                    self._reply({'protocol_version': 1, 'capabilities': ['stable-command-id']})

            def _reply(self, body):
                payload = json.dumps(body).encode()
                self.send_response(200)
                self.send_header('Content-Type', 'application/json')
                self.send_header('Content-Length', str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, *_args):
                pass

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        spec = importlib.util.spec_from_file_location(
            'command_response_cut', ROOT / 'command_response_cut.py')
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            with module.CommandResponseCutProxy(
                f'http://127.0.0.1:{upstream.server_port}') as proxy:
                url = f'http://127.0.0.1:{proxy.port}/direct/example/invoke'
                capability = {
                    'action': 'process.capabilities', 'args': {},
                    'requestId': 'capability-request',
                }
                with urlopen(Request(
                    url, data=json.dumps(capability).encode(),
                    headers={'X-ADX-Request-ID': 'capability-request',
                             'Content-Type': 'application/json'},
                ), timeout=5) as response:
                    self.assertEqual(json.load(response)['protocol_version'], 1)
                self.assertEqual(proxy.cut_attempts, [])
                request = Request(
                    url,
                    data=json.dumps({
                        'action': 'process.start',
                        'args': {'command_id': 'stable-command'},
                        'requestId': 'same-request',
                    }).encode(),
                    headers={'X-ADX-Request-ID': 'same-request',
                             'Content-Type': 'application/json'},
                )
                for _ in range(3):
                    with self.assertRaises(RemoteDisconnected):
                        urlopen(request, timeout=5)
                with urlopen(f'http://127.0.0.1:{proxy.port}/api/instances',
                             timeout=5) as response:
                    self.assertEqual(json.load(response), {'instance': 'ready'})
                self.assertEqual(len(proxy.cut_attempts), 3)
                self.assertEqual(
                    [attempt['request_id'] for attempt in proxy.cut_attempts],
                    ['same-request'] * 3,
                )
                self.assertEqual(
                    [attempt['result']['pid'] for attempt in proxy.cut_attempts],
                    [42] * 3,
                )
                self.assertEqual(received,
                                 [('capability-request', capability)] +
                                 [('same-request', {
                                     'action': 'process.start',
                                     'args': {'command_id': 'stable-command'},
                                     'requestId': 'same-request',
                                 })] * 3)
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
