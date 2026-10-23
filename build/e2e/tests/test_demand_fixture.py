import copy
import http.client
import ipaddress
import json
import os
import re
from pathlib import Path
import shutil
import subprocess
import ssl
import sys
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'firecracker'))
from demand_contract import CASES, verify
from demand_faults import ResumeResponseCut
from s3_client import Client
from storage_fixture import external


class DemandFixtureTests(unittest.TestCase):
    def test_resume_reply_is_cut_only_after_upstream_commit_and_retry_is_forwarded(self):
        with tempfile.TemporaryDirectory() as directory:
            secrets = Path(directory)
            subprocess.run([sys.executable, str(ROOT.parent / 'ci/rpc_certificates.py'), str(secrets / 'tls')], check=True, capture_output=True)
            committed = threading.Event()
            calls = []
            class Handler(BaseHTTPRequestHandler):
                def do_POST(self):
                    self.rfile.read(int(self.headers.get('Content-Length', 0)))
                    calls.append(self.headers.get('X-ADX-Request-ID'))
                    committed.set()
                    self.send_response(200)
                    self.send_header('Content-Length', '2')
                    self.end_headers()
                    self.wfile.write(b'{}')
                def log_message(self, *_args): pass
            server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
            tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
            tls.load_cert_chain(str(secrets / 'tls/ingress.pem'), str(secrets / 'tls/ingress.key'))
            server.socket = tls.wrap_socket(server.socket, server_side=True)
            thread = threading.Thread(target=server.serve_forever, daemon=True)
            thread.start()
            def observed():
                self.assertTrue(committed.is_set())
                self.assertEqual(len(calls), 1)
                return {'backend': 'one', 'committed_at': 1}
            try:
                with ResumeResponseCut(f'https://127.0.0.1:{server.server_port}', secrets, observed) as proxy:
                    context = ssl.create_default_context(cafile=str(secrets / 'tls/ca.pem'))
                    first = http.client.HTTPSConnection('127.0.0.1', proxy.port, context=context, timeout=5)
                    first.request('POST', '/resume', b'{}', {'X-ADX-Request-ID': 'same-operation'})
                    with self.assertRaises(http.client.RemoteDisconnected): first.getresponse()
                    first.close()
                    retry = http.client.HTTPSConnection('127.0.0.1', proxy.port, context=context, timeout=5)
                    retry.request('POST', '/resume', b'{}', {'X-ADX-Request-ID': 'same-operation'})
                    self.assertEqual(retry.getresponse().read(), b'{}')
                    retry.close()
                    self.assertIsNone(proxy.error)
                    self.assertEqual(proxy.commit['backend'], 'one')
                    self.assertEqual(proxy.attempts, ['same-operation'] * 2)
                    self.assertEqual(calls, ['same-operation'] * 2)
            finally:
                server.shutdown()
                server.server_close()
                thread.join(2)

    def config(self, root):
        path = root / 'obs.json'
        path.write_text(json.dumps({'bucket': 'test-bucket', 'region': 'cn-north-4', 'endpoint': 'https://obs.cn-north-4.myhuaweicloud.com', 'prefix': 'adx/e2e/dedicated', 'access_key': 'fixture-access', 'secret_key': 'fixture-secret'}))
        path.chmod(0o600)
        return path

    def test_obs_credentials_private_public_evidence_scoped(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run = root / 'run-unique'
            (run / 'secrets').mkdir(parents=True)
            (run / 'evidence').mkdir()
            config = self.config(root)
            public = external(run, config)
            self.assertEqual(public['prefix'], 'adx/e2e/dedicated/run-unique')
            self.assertEqual(public['endpoint'], 'https://test-bucket.obs.cn-north-4.myhuaweicloud.com')
            self.assertNotIn('fixture-secret', (run / 'evidence/storage.json').read_text())
            self.assertEqual((run / 'secrets/s3-key').stat().st_mode & 0o777, 0o600)
            config.chmod(0o644)
            with self.assertRaises(ValueError):
                external(run, config)

    def test_obs_sigv4_uses_virtual_host_and_bucketless_signed_path(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / 'secrets').mkdir()
            (root / 'evidence').mkdir()
            external(root, self.config(root))
            client = Client(root)
            class Response:
                headers = {'x-obs-request-id': 'observed-request'}
                def __enter__(self): return self
                def __exit__(self, *_args): pass
                def read(self): return b'probe'
            with patch('s3_client.urllib.request.urlopen', return_value=Response()) as opening:
                self.assertEqual(client.request('GET', '/test-bucket/path/object'), b'probe')
            request = opening.call_args.args[0]
            self.assertEqual(request.full_url, 'https://test-bucket.obs.cn-north-4.myhuaweicloud.com/path/object')
            self.assertIn('/cn-north-4/s3/aws4_request', request.headers['Authorization'])
            self.assertEqual(client.last_request_id, 'observed-request')
            with self.assertRaises(ValueError):
                client.request('DELETE', '/different-bucket/object')

    def complete(self):
        cases = [{'name': name, 'passed': True} for name in CASES]
        cases[1].update(same_backend=True, checkpoint_created=False)
        cases[2].update(captured_before_request=True, request_waited=True)
        cases[6].update(old_backend_removed=True, state_at_crash='Paused')
        cases[8].update(admitted_at=10, expires_at=12, completed_at=13)
        cases[9].update(same_execution=True, attempts=2, commit_before_cut=True)
        cases[10].update(automatic_expiry=None, manual_expiry=None, dormant_after_coordinator_restart=True,
                         traffic_after_manual_pause=True, continuity=True, target='node2',
                         epoch_before=1, epoch_after=2, session_before='before', session_after='after')
        if len(cases) > 11:
            cases[11].update(source_healthy=True, local_rejected=True,
                             local_error='insufficient resources or node admission closed',
                             source_cpu_capacity=4000, source_cpu_reserved=4000,
                             local_attempt_at=10, local_rejected_at=11, peer_release_at=12, transferred_at=13,
                             source='node1', target='node2', generation_before=1, generation_after=2,
                             wake_operation_preserved=True, request_count=12, unique_nonces=12,
                             source_restore_count=0, peer_restore_count=1)
        return {'status': 'passed', 'cases': cases, 'storage': {'kind': 'real_obs'}, 'obs_empty': True, 'cleanup_errors': [], 'inventories': {'node1': 0, 'node2': 0}}

    def test_capacity_transfer_requires_owner_rejection_before_transfer(self):
        self.assertEqual(len(CASES), 12)
        complete = self.complete()
        verify(complete)
        for key, value in [('source_healthy', False), ('local_rejected', False),
                           ('local_error', 'timeout'), ('source_cpu_reserved', 3000),
                           ('transferred_at', 10), ('wake_operation_preserved', False),
                           ('request_count', 1), ('unique_nonces', 11),
                           ('source_restore_count', 1), ('peer_restore_count', 2)]:
            with self.subTest(key=key):
                bad = copy.deepcopy(complete)
                bad['cases'][11][key] = value
                with self.assertRaises(ValueError): verify(bad)

    def test_wake_identity_is_preserved_across_distinct_node_execution_id(self):
        from demand_contract import same_wake
        rejected = {'resume': {'operation_id': 'wake-one'}}
        transferred = {'resume': {'operation_id': 'wake-one'}}
        completed = {'resume': None, 'result': {'state': 'Running',
                     'last_operation': {'kind': 'Resume', 'id': 'recover-2'}}}
        self.assertTrue(same_wake(rejected, transferred, completed))
        transferred['resume']['operation_id'] = 'wake-two'
        self.assertFalse(same_wake(rejected, transferred, completed))
        transferred['resume']['operation_id'] = 'wake-one'
        completed['resume'] = {'operation_id': 'wake-one'}
        self.assertFalse(same_wake(rejected, transferred, completed))

    def test_physical_audit_rejects_extra_capacity_restore(self):
        from demand_physical import audit
        result = self.complete()
        self.assertEqual(len(result['cases']), 12)
        expected = [0, 0, 1, 1, 1, 1, 1, 0, 1, 1, 2, 1]
        result['cases'][5]['old_backend_removed'] = False
        result['cases'][6]['old_backend_removed'] = False
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            logs = {'node1': [], 'node2': []}
            for index, count in enumerate(expected):
                case = result['cases'][index]
                case['instance_id'] = f'env-{index}'
                node = 'node2' if index == 11 else 'node1'
                for attempt in range(count):
                    backend = f'sbox-{index}-{attempt}'
                    logs[node].extend([f'firecracker: restored environment {backend} pid=123',
                                       f'Type:create EnvironmentID:{backend} adx.environment_id {case["instance_id"]}'])
            for node, lines in logs.items(): (root/f'{node}-sandboxd-service.log').write_text('\n'.join(lines)+'\n')
            self.assertEqual(len(audit(root, result)), 12)
            with (root/'node2-sandboxd-service.log').open('a') as log:
                log.write('firecracker: restored environment sbox-extra pid=124\nType:create EnvironmentID:sbox-extra adx.environment_id env-11\n')
            with self.assertRaises(ValueError): audit(root, result)

    def test_missing_real_fault_boundary_cannot_pass(self):
        complete = self.complete()
        verify(complete)
        mutations = [(1, 'same_backend', False), (1, 'checkpoint_created', True), (2, 'captured_before_request', False), (6, 'old_backend_removed', False), (8, 'completed_at', 11), (9, 'attempts', 1), (9, 'commit_before_cut', False), (10, 'automatic_expiry', 10), (10, 'traffic_after_manual_pause', False), (10, 'session_after', 'before')]
        for index, key, value in mutations:
            with self.subTest(key=key):
                bad = copy.deepcopy(complete)
                bad['cases'][index][key] = value
                with self.assertRaises(ValueError): verify(bad)
        bad = copy.deepcopy(complete)
        bad['cases'].pop()
        with self.assertRaises(ValueError): verify(bad)
        bad = copy.deepcopy(complete)
        bad['storage']['kind'] = 'minio'
        with self.assertRaises(ValueError): verify(bad)

    def test_real_obs_configuration_disables_gc_and_keeps_virtual_host_env(self):
        from transfer_config import configure
        with tempfile.TemporaryDirectory() as directory:
            base = Path(directory)
            artifact = base / 'package/runtime/adx-runtime-rootfs.img'
            artifact.parent.mkdir(parents=True)
            artifact.write_bytes(b'fixture')
            (base / 'e2e').mkdir()
            shutil.copy2(ROOT.parent / 'ci/rpc_certificates.py', base / 'e2e/rpc_certificates.py')
            run = base / 'run'
            env = {**os.environ, 'ADX_FC_BASE': str(base), 'ADX_FC_RUN_ROOT': str(run), 'ADX_FC_OBS_CONFIG': str(self.config(base)), 'ADX_FC_SUITE': 'demand'}
            subprocess.run([sys.executable, str(ROOT / 'firecracker/configure.py'), 'node1'], env=env, check=True, capture_output=True)
            configure(run)
            guest_networks = []
            for node in ('node1', 'node2'):
                service = json.loads((run / node / 'deployment.yaml').read_text())['services'][0]
                self.assertNotIn('checkpoint_gc', service['config'])
                self.assertGreater(service['config']['rpc_timeout_seconds'], 50,
                                   'readiness budget must cover the 45-second TTL boundary hold')
                self.assertEqual(service['env']['AWS_VIRTUAL_HOSTED_STYLE_REQUEST'], 'true')
                self.assertEqual(service['env']['AWS_TIMEOUT'], '240s')
                self.assertEqual(service['config']['checkpoint_storage']['alias'], 'obs')
                self.assertTrue(service['config']['checkpoint_storage']['endpoint'].startswith('https://test-bucket.'))
                self.assertIn('default_overlay_size_bytes = 33554432', (run / node / 'sandboxd/config.toml').read_text())
                text = (run / node / 'sandboxd/config.toml').read_text()
                guest_network = ipaddress.ip_interface(re.search(r'ip_range = "([^"]+)"', text)[1]).network
                allowed = [ipaddress.ip_network(cidr) for cidr in service['env']['ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS'].split(',')]
                self.assertIn(guest_network, allowed, 'the actual guest subnet must match Relay admission')
                guest_networks.append(guest_network)
            self.assertEqual(len(set(guest_networks)), 2, 'execution nodes require distinct guest pools')
