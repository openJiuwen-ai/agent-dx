"""A partial download must resume from the bytes already persisted locally."""

import importlib.util
from http.client import IncompleteRead
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import unittest
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


class DownloadResponseCutTests(unittest.TestCase):
    def test_partial_body_is_followed_by_range_request(self):
        payload = b'first-second'

        class Upstream(BaseHTTPRequestHandler):
            def do_GET(self):
                range_header = self.headers.get('Range')
                start = int(range_header.removeprefix('bytes=').removesuffix('-')) \
                    if range_header else 0
                result = payload[start:]
                self.send_response(206 if range_header else 200)
                self.send_header('Content-Length', str(len(result)))
                self.send_header('Content-Type', 'application/octet-stream')
                if range_header:
                    self.send_header('Content-Range',
                                     f'bytes {start}-{len(payload)-1}/{len(payload)}')
                self.end_headers()
                self.wfile.write(result)

            def log_message(self, *_args):
                pass

        upstream = ThreadingHTTPServer(('127.0.0.1', 0), Upstream)
        thread = threading.Thread(target=upstream.serve_forever, daemon=True)
        thread.start()
        spec = importlib.util.spec_from_file_location(
            'upload_response_cut', ROOT / 'upload_response_cut.py')
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            with module.UploadResponseCutProxy(
                f'http://127.0.0.1:{upstream.server_port}',
                cut_upload=False, download_cut_bytes=5,
            ) as proxy:
                url = f'http://127.0.0.1:{proxy.port}/direct/example/download?path=/tmp/file&type=file'
                with self.assertRaises(IncompleteRead) as interrupted:
                    with urlopen(url, timeout=5) as response:
                        response.read()
                self.assertEqual(interrupted.exception.partial, payload[:5])
                with urlopen(Request(url, headers={'Range': 'bytes=5-'}),
                             timeout=5) as response:
                    remainder = response.read()
                    self.assertEqual(response.status, 206)
                    self.assertEqual(response.headers['Content-Range'],
                                     'bytes 5-11/12')
                self.assertEqual(interrupted.exception.partial + remainder, payload)
                self.assertEqual(proxy.cut_download['bytes_sent'], 5)
                self.assertEqual(proxy.download_attempts, [
                    {'range': None, 'status': 200, 'size': len(payload)},
                    {'range': 'bytes=5-', 'status': 206, 'size': len(payload) - 5},
                ])
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
