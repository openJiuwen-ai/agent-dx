"""A lost chunk acknowledgment must remain observable through upload status."""

import importlib.util
import json
from http.client import RemoteDisconnected
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
import threading
import unittest
from urllib.parse import parse_qs, urlsplit
from urllib.request import Request, urlopen


ROOT = Path(__file__).resolve().parents[1]


class UploadResponseCutTests(unittest.TestCase):
    def test_committed_first_chunk_loses_ack_but_status_exposes_offset(self):
        stored = bytearray()

        class Upstream(BaseHTTPRequestHandler):
            def do_GET(self):
                self._reply({'offset': len(stored)})

            def do_POST(self):
                path = urlsplit(self.path)
                query = parse_qs(path.query)
                if path.path.endswith('/upload/commit'):
                    self._reply({'committed': True, 'size': len(stored)})
                    return
                expected = int(query['offset'][0])
                assert expected == len(stored), (expected, len(stored))
                stored.extend(self.rfile.read(int(self.headers['Content-Length'])))
                self._reply({'offset': len(stored), 'bytes_written': len(stored) - expected})

            def _reply(self, value):
                payload = json.dumps(value).encode()
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
            'upload_response_cut', ROOT / 'upload_response_cut.py')
        module = importlib.util.module_from_spec(spec)
        try:
            spec.loader.exec_module(module)
            with module.UploadResponseCutProxy(
                f'http://127.0.0.1:{upstream.server_port}') as proxy:
                base = f'http://127.0.0.1:{proxy.port}/direct/example'
                query = '?path=/tmp/upload.bin&uploadId=stable-upload&totalSize=10'
                with urlopen(base + '/upload/status' + query, timeout=5) as response:
                    self.assertEqual(json.load(response)['offset'], 0)
                first = Request(base + '/upload' + query + '&offset=0', data=b'first',
                                method='POST')
                with self.assertRaises(RemoteDisconnected):
                    urlopen(first, timeout=5)
                with urlopen(base + '/upload/status' + query, timeout=5) as response:
                    self.assertEqual(json.load(response)['offset'], 5)
                second = Request(base + '/upload' + query + '&offset=5', data=b'after',
                                 method='POST')
                with urlopen(second, timeout=5) as response:
                    self.assertEqual(json.load(response)['offset'], 10)
                with urlopen(Request(base + '/upload/commit' + query, data=b'',
                                     method='POST'), timeout=5) as response:
                    self.assertTrue(json.load(response)['committed'])
                self.assertEqual(stored, b'firstafter')
                self.assertEqual(proxy.cut_chunk['upload_id'], 'stable-upload')
                self.assertEqual(proxy.cut_chunk['offset'], 0)
                self.assertEqual(proxy.cut_chunk['committed_offset'], 5)
                self.assertEqual([event['offset'] for event in proxy.chunks], [0, 5])
                self.assertEqual(proxy.status_offsets, [0, 5])
        finally:
            upstream.shutdown()
            upstream.server_close()
            thread.join(timeout=2)


if __name__ == '__main__':
    unittest.main()
