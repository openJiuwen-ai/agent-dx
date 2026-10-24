"""Exercise the lifecycle acceptance probe against separately served metric tiers."""

import ast
import threading
import unittest
import urllib.request
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch
from wsgiref.simple_server import WSGIRequestHandler, make_server


class LifecycleMetricsTests(unittest.TestCase):
    def test_probe_reads_both_tiers_and_requires_the_requested_instance(self):
        source = Path(__file__).resolve().parents[1] / 'firecracker/sdk_node_lifecycle.py'
        # Load only the probe, without deploying a fixture or executing SDK faults.
        probe = next(n for n in ast.walk(ast.parse(source.read_text()))
                     if isinstance(n, ast.FunctionDef) and n.name == 'sampled_stats')
        namespace = {'urllib': urllib, 's': SimpleNamespace(id='env-1')}
        exec(compile(ast.Module(body=[probe], type_ignores=[]), str(source), 'exec'), namespace)
        summary = 'adx_node_reserved_cpu_millis 1000\n'
        detail = 'adx_environment_memory_usage_bytes{environment_id="env-1",runtime_id="env-1-1"} 1024\n'
        bodies = {'/metrics': summary, '/metrics/environments': detail}
        requests = []

        def serve_metrics(environ, start_response):
            request_path = environ.get('PATH_INFO', '')
            requests.append(request_path)
            status = '200 OK' if request_path in bodies else '404 Not Found'
            start_response(status, [('Content-Type', 'text/plain')])
            return [bodies.get(request_path, '').encode()]

        class Handler(WSGIRequestHandler):
            def log_message(self, *_args):
                pass

        server = make_server('127.0.0.1', 0, serve_metrics, handler_class=Handler)
        worker = threading.Thread(target=server.serve_forever, daemon=True)
        worker.start()
        urlopen = urllib.request.urlopen

        def local_open(url, **kwargs):
            self.assertTrue(url.startswith('http://127.0.0.1:17003/'))
            return urlopen(url.replace(':17003/', f':{server.server_port}/'), **kwargs)

        try:
            with patch('urllib.request.urlopen', side_effect=local_open):
                self.assertEqual(namespace['sampled_stats'](), (summary, detail))
                self.assertEqual(set(requests), {'/metrics', '/metrics/environments'})
                bodies['/metrics/environments'] = detail.replace('env-1', 'env-10')
                self.assertIsNone(namespace['sampled_stats']())
                bodies['/metrics/environments'] = ''
                self.assertIsNone(namespace['sampled_stats']())
                bodies['/metrics/environments'] = detail
                bodies['/metrics'] = ''
                self.assertIsNone(namespace['sampled_stats']())
        finally:
            server.shutdown()
            worker.join()
            server.server_close()


if __name__ == '__main__':
    unittest.main()
