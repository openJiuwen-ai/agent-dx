import contextlib
import http.client
import importlib.util
import io
import json
import os
from pathlib import Path
import shlex
import socket
import subprocess
import sys
import tempfile
import time
import types
import unittest
from unittest import mock
import urllib.error
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('live_driver', ROOT / 'run.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)
functional_spec = importlib.util.spec_from_file_location(
    'functional_data_plane', ROOT / 'functional_data_plane.py'
)
functional_data_plane = importlib.util.module_from_spec(functional_spec)
sdk_stub = types.ModuleType('adx_sandbox')
for exported in (
    'CommandConflict', 'CommandNotFound', 'CommandStatus', 'CommandWaitTimeout',
    'DataPlaneSecurityPolicy', 'Sandbox', 'resources',
):
    setattr(sdk_stub, exported, object)
with mock.patch.dict(sys.modules, {'adx_sandbox': sdk_stub}):
    functional_spec.loader.exec_module(functional_data_plane)


class LiveOutputTests(unittest.TestCase):
    def test_forwarded_server_distinguishes_host_path_from_default_path(self):
        with socket.socket() as listener:
            listener.bind(('127.0.0.1', 0))
            port = listener.getsockname()[1]
        command = functional_data_plane.SERVER_COMMAND.replace(
            str(functional_data_plane.PORT), str(port))
        server = subprocess.Popen(shlex.split(command), stdout=subprocess.DEVNULL,
                                  stderr=subprocess.PIPE)
        try:
            deadline = time.monotonic() + 3
            while True:
                try:
                    client = http.client.HTTPConnection('127.0.0.1', port, timeout=1)
                    client.request('GET', '/')
                    default = client.getresponse().read().decode()
                    client.close()
                    break
                except OSError:
                    if time.monotonic() >= deadline:
                        raise
                    time.sleep(.05)
            client = http.client.HTTPConnection('127.0.0.1', port, timeout=1)
            client.request('GET', '/functional/host?x=1')
            host = client.getresponse().read().decode()
            client.close()
            self.assertEqual(default, functional_data_plane.EXPECTED_BODY)
            self.assertEqual(host, functional_data_plane.HOST_EXPECTED_BODY)
        finally:
            server.terminate()
            server.communicate(timeout=2)

    def test_host_forward_uses_instance_subdomain_and_preserves_guest_path(self):
        class Sandbox:
            id = 'default-sandbox-123'

            @staticmethod
            def get_port_url(port):
                return f'https://127.0.0.1:8443/default-sandbox-123/{port}'

            @staticmethod
            def get_port_auth_headers():
                return {'Authorization': 'Bearer test-token'}

        response = mock.MagicMock()
        response.__enter__.return_value.read.return_value = b'forwarded'
        with mock.patch.object(
            functional_data_plane.ssl, 'create_default_context', return_value=object()
        ), mock.patch.object(
            functional_data_plane.urllib.request, 'urlopen', return_value=response
        ) as opener:
            self.assertEqual(functional_data_plane._fetch_host_forwarded(
                Sandbox(), Path('/unused'), port=18081
            ), 'forwarded')
        request = opener.call_args.args[0]
        self.assertEqual(request.full_url, 'https://127.0.0.1:8443/functional/host?x=1')
        self.assertEqual(request.get_header('Host'),
                         'default-sandbox-123-18081.example.test')
        self.assertEqual(request.get_header('Authorization'), 'Bearer test-token')

    def test_forwarded_port_auth_denial_is_not_retried_as_readiness(self):
        class Sandbox:
            @staticmethod
            def get_port_url(port):
                return f'https://localhost:{port}'

            @staticmethod
            def get_port_auth_headers():
                return {}

        denied = urllib.error.HTTPError(
            'https://localhost:18081', 401, 'Unauthorized', {}, None
        )
        with mock.patch.object(
            functional_data_plane.ssl, 'create_default_context', return_value=object()
        ), mock.patch.object(
            functional_data_plane.urllib.request, 'urlopen', side_effect=denied
        ) as request, self.assertRaises(urllib.error.HTTPError) as raised:
            functional_data_plane._fetch_forwarded(Sandbox(), Path('/unused'))
        self.assertEqual(raised.exception.code, 401)
        request.assert_called_once()

    def test_child_output_is_visible_before_child_exits(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            seen = root / 'seen'
            class Sink(io.StringIO):
                def write(self, text):
                    if text == 'FIRST\n':
                        seen.touch()
                    return super().write(text)
            sink = Sink()
            program = ('import pathlib,time; print("FIRST",flush=True); '
                       f'p=pathlib.Path({str(seen)!r}); end=time.monotonic()+2\n'
                       'while not p.exists() and time.monotonic()<end: time.sleep(.01)\n'
                       'assert p.exists(), "output was buffered"\nprint("DONE",flush=True)')
            with contextlib.redirect_stdout(sink):
                result = driver.Run(root).command([sys.executable, '-u', '-c', program], timeout=4, label='live output test')
            self.assertEqual(result, 'FIRST\nDONE\n')
            self.assertIn('[EXIT 001.log] code=0', sink.getvalue())

    def test_failure_streams_stderr_and_redacts_secrets_in_both_outputs(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); run = driver.Run(root)
            run.redactions.add('sensitive-fixture-value')
            console = io.StringIO()
            with contextlib.redirect_stdout(console), self.assertRaisesRegex(RuntimeError, '37'):
                run.command([sys.executable, '-u', '-c',
                             'import sys; print("sensitive-fixture-value",file=sys.stderr); sys.exit(37)'])
            self.assertNotIn('sensitive-fixture-value', console.getvalue())
            self.assertEqual((root / '001.log').read_text(), '[REDACTED]\n')
            self.assertIn('[REDACTED]', console.getvalue())

    def test_timeout_terminates_process_and_keeps_partial_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp); start = time.monotonic()
            with contextlib.redirect_stdout(io.StringIO()), self.assertRaises(subprocess.TimeoutExpired):
                driver.Run(root).command([sys.executable, '-u', '-c',
                                         'import os,time; print(os.getpid(),flush=True); time.sleep(30)'], timeout=.4)
            self.assertLess(time.monotonic() - start, 3)
            pid = int((root / '001.log').read_text().strip())
            with self.assertRaises(ProcessLookupError): os.kill(pid, 0)

    def test_case_failure_never_reports_pass_and_retains_earlier_success(self):
        with tempfile.TemporaryDirectory() as temp:
            run = driver.Run(Path(temp)); checks = []
            def execute(*args, **kwargs):
                if 'sdk' in args:
                    result = run.output / 'sdk/sdk-result.json'
                    result.parent.mkdir(parents=True, exist_ok=True)
                    result.write_text(json.dumps({'instances': []}))
                if 'capacity' in args: raise RuntimeError('capacity assertion failed')
            run.execute = execute
            console = io.StringIO()
            with contextlib.redirect_stdout(console), self.assertRaisesRegex(RuntimeError, 'capacity assertion'):
                run.scenarios(checks)
            self.assertEqual(checks, ['sdk', 'data-plane', 'lifecycle', 'auth'])
            self.assertEqual([r['status'] for r in run.case_results],
                             ['passed', 'passed', 'passed', 'passed', 'failed'])
            self.assertIn('[RUN] capacity', console.getvalue())
            self.assertIn('[FAIL] capacity', console.getvalue())
            self.assertNotIn('[PASS] capacity', console.getvalue())
            self.assertEqual(json.loads((Path(temp) / 'case-results.json').read_text()), run.case_results)

    def test_junit_has_per_case_results_skips_and_cleanup_failure(self):
        spec = importlib.util.spec_from_file_location('live_kube_driver', ROOT / 'kubernetes/run.py')
        module = importlib.util.module_from_spec(spec); spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as temp:
            path = Path(temp) / 'junit.xml'
            report = {'cases': [{'name': 'sdk', 'status': 'passed', 'seconds': 1},
                                {'name': 'auth', 'status': 'failed', 'seconds': 2, 'error': 'denied'}],
                      'error': 'auth failed', 'cleanup_errors': ['namespace remains']}
            module.write_junit(path, report)
            suite = ET.parse(path).getroot()
            self.assertEqual(suite.attrib, {'name': 'platform-kubernetes-e2e', 'tests': '12', 'failures': '2', 'skipped': '9'})
            self.assertIsNotNone(suite.find("testcase[@name='auth']/failure"))
            self.assertIsNotNone(suite.find("testcase[@name='cleanup']/failure"))
