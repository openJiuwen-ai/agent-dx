import importlib.util
import unittest
import tempfile
import json
import subprocess
import sys
import socket
import ssl
import threading
from wsgiref.simple_server import make_server
import urllib.request
from pathlib import Path

spec = importlib.util.spec_from_file_location(
    'activator_acceptance', Path(__file__).resolve().parents[1] / 'activator.py'
)
acceptance = importlib.util.module_from_spec(spec)


class ActivatorAcceptanceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec.loader.exec_module(acceptance)

    def test_publish_uses_assigned_version_and_does_not_replay_writes(self):
        class PublicationRecorded(Exception):
            pass

        with tempfile.TemporaryDirectory() as directory:
            private = Path(directory)
            (private / 'activator-image').write_text('image@sha256:synthetic')
            case = acceptance.Acceptance.__new__(acceptance.Acceptance)
            case.private, case.endpoints, case.result = private, ('one', 'two'), {}
            calls = []

            def success(endpoint, path, method='GET', payload=None):
                calls.append((endpoint, path, method, payload))
                if method == 'POST':
                    self.assertNotIn('version', payload)
                    return {}, {'template': {**payload, 'version': '7'}}
                self.assertEqual(path, '/api/agent/v2/templates/activator-e2e/versions/7')
                return {}, {'template': {**calls[0][3], 'version': '7'}}

            case.success = success

            def passed(*args, **kwargs):
                raise PublicationRecorded()

            case.passed = passed

            def unexpected_retry(*args):
                self.fail('publication must not retry an ambiguous write')

            case.wait = unexpected_retry
            with self.assertRaises(PublicationRecorded):
                case.create()
            self.assertEqual(sum(call[2] == 'POST' for call in calls), 1)
            self.assertEqual(case.result['template_version'], '7')
            self.assertIn('/versions/7/bindings', case.base)

    def test_missing_physical_cleanup_cannot_pass(self):
        result = {
            'mode': 'embedded',
            'status': 'passed',
            'cases': [{'id': case, 'status': 'passed'} for case in acceptance.CASES],
            'cleanup_errors': [],
            'environment_ids': ['adx-first', 'adx-second'],
            'backend_counts': [0, 1],
            'remaining_bindings': 0,
            'remaining_environments': 0,
        }
        with self.assertRaisesRegex(ValueError, 'incomplete'):
            acceptance.verify(result)
        result['backend_counts'] = [0, 0]
        acceptance.verify(result)
        result['environment_ids'] = ['adx-first', 'adx-first']
        with self.assertRaises(ValueError):
            acceptance.verify(result)

    def test_both_modes_have_targeted_deployment_and_driver(self):
        driver_spec = importlib.util.spec_from_file_location(
            'deployment_driver', Path(__file__).resolve().parents[1] / 'run.py'
        )
        driver = importlib.util.module_from_spec(driver_spec)
        driver_spec.loader.exec_module(driver)
        for mode in ('standalone', 'embedded'):
            case = 'activator-' + mode
            self.assertEqual(driver.selected_checks('standalone', case), (case,))
            self.assertIn('ADX_E2E_ACTIVATOR_MODE=' + mode, driver.setup_environment(case))

    def test_configured_modes_keep_credentials_private_and_share_redis(self):
        fixture_spec = importlib.util.spec_from_file_location(
            'activator_fixture', Path(__file__).resolve().parents[1] / 'activator_fixture.py'
        )
        fixture = importlib.util.module_from_spec(fixture_spec)
        fixture_spec.loader.exec_module(fixture)
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            private = root / 'private'
            private.mkdir()
            for mode in ('standalone', 'embedded'):
                with self.subTest(mode=mode):
                    state = root / mode
                    state.mkdir()
                    services = [{'role': 'apiserver', 'env': {}}, {'role': 'ingress', 'env': {}}]
                    fixture.configure(mode, 'node2', services, 'redis://:secret@coordinator:6379/', private, state)
                    agent = json.loads((state / 'agent.json').read_text())
                    self.assertEqual(agent['timeout_seconds'], 120)
                    self.assertNotIn(
                        'secret',
                        (state / 'agent.json').read_text()
                        if mode == 'standalone'
                        else json.dumps(agent.get('activator')),
                    )
                    self.assertEqual((state / 'agent.json').stat().st_mode & 0o777, 0o600)
                    self.assertEqual(services[0]['env']['ADX_AGENT_CONFIG'], services[1]['env']['ADX_AGENT_CONFIG'])
                    if mode == 'standalone':
                        activator = json.loads((state / 'activator.json').read_text())
                        self.assertEqual(activator['redis_url'], 'redis://:secret@coordinator:6379/')
                        self.assertEqual(activator['environment_url'], 'http://127.0.0.1:8888')
                        self.assertEqual(len(agent['activator']['urls']), 2)
                    else:
                        self.assertEqual(agent['embedded']['namespace'], 'acceptance-agent')
                        self.assertEqual(agent['embedded']['redis_url'], 'redis://:secret@coordinator:6379/')

    def test_fixture_certificate_verifies_second_public_entrypoint(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory) / 'tls'
            generator = Path(__file__).resolve().parents[2] / 'ci/rpc_certificates.py'
            subprocess.run([sys.executable, str(generator), str(root)], check=True, capture_output=True)
            listener = socket.socket()
            listener.bind(('127.0.0.1', 0))
            listener.listen(1)
            address = listener.getsockname()
            server_context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            server_context.load_cert_chain(root / 'ingress.pem', root / 'ingress.key')

            def serve():
                try:
                    connection, _ = listener.accept()
                    with connection:
                        with server_context.wrap_socket(connection, server_side=True) as stream:
                            stream.sendall(b'tls-ok')
                except ssl.SSLError:
                    pass
                finally:
                    listener.close()

            thread = threading.Thread(target=serve, daemon=True)
            thread.start()
            try:
                context = ssl.create_default_context(cafile=str(root / 'ca.pem'))
                with socket.create_connection(address, timeout=3) as connection:
                    with context.wrap_socket(connection, server_hostname='node2') as stream:
                        self.assertEqual(stream.recv(6), b'tls-ok')
            finally:
                thread.join(timeout=3)

    def test_non_json_auth_rejection_keeps_http_status(self):
        def reject(_environ, start_response):
            start_response('401 Unauthorized', [('Content-Type', 'text/plain'), ('Content-Length', '12')])
            return [b'Unauthorized']

        server = make_server('127.0.0.1', 0, reject)
        thread = threading.Thread(target=server.handle_request, daemon=True)
        thread.start()
        client = acceptance.Acceptance.__new__(acceptance.Acceptance)
        client.token = 'invalid-test-key'
        client.opener = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        try:
            status, _, body = client.request(f'http://127.0.0.1:{server.server_port}', '/agent/http/echo')
            self.assertEqual(status, 401)
            self.assertEqual(body, 'Unauthorized')
        finally:
            thread.join(timeout=3)
            server.server_close()
