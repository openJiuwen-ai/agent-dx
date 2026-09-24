import json
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

from e2e.multivm.auth_accept import run_acceptance
from e2e.tests.test_multivm_local_first import inventory


class AuthAcceptanceTests(unittest.TestCase):
    def exercise(self, denied_read=403):
        actions = []
        self.actions = actions

        class Sandbox:
            id = 'sandbox-owned'

            def __init__(self, **options):
                self.commands = SimpleNamespace(run=lambda _command: SimpleNamespace(
                    exit_code=0, stdout='still-owned'))
                actions.append(('create', options['node_id']))

            def is_running(self):
                return True

            def kill(self):
                actions.append(('kill', self.id))

            def close(self):
                actions.append(('close', self.id))

        revoked = False

        def request(method, path, token, body=None, params=None):
            nonlocal revoked
            actions.append((method, path, token))
            if method == 'POST' and path == '/api/admin/v1/keys':
                self.assertEqual(token, 'admin')
                self.assertEqual(body['tenantId'], 'adx-3vm-auth')
                return 201, {'key': {'id': 'key-1'}, 'apiKey': 'other'}
            if path == '/api/admin/v1/keys':
                if token == 'owner':
                    return 403, {}
                self.assertEqual(params, {'tenantId': 'adx-3vm-auth'})
                return 200, {'items': [{'id': 'key-1'}]}
            if path == '/api/admin/v1/keys/key-1':
                self.assertEqual(token, 'admin')
                revoked = True
                return 204, {}
            if path == '/api/sandbox/v1/snapshots':
                return (401 if revoked else 200), {}
            if path.startswith('/api/instances'):
                return (401 if token == 'invalid-acceptance-key' else denied_read), {}
            if path == '/api/sandbox/v1/sandboxes/sandbox-owned':
                return (401 if token == 'invalid-acceptance-key' else 403), {}
            raise AssertionError(f'unexpected request: {method} {path}')

        with tempfile.TemporaryDirectory() as directory:
            result = run_acceptance(
                inventory(), object(), 'image', Path(directory),
                owner_token='owner', admin_token='admin', sandbox_factory=Sandbox,
                request=request, verify=lambda _inventory: ['verified'])
            report = json.loads((Path(directory) / 'auth-result.json').read_text())
        return result, report, actions

    def test_invalid_and_other_tenant_cannot_read_or_delete(self):
        result, report, actions = self.exercise()
        self.assertEqual(result['status'], 'passed')
        self.assertEqual(report['status'], 'passed')
        self.assertIn('temporary-key-revoked', result['checks'])
        self.assertIn(('kill', 'sandbox-owned'), actions)
        self.assertIn(('close', 'sandbox-owned'), actions)
        self.assertNotIn('other', json.dumps(report))

    def test_cross_tenant_read_acceptance_fails_and_cleans(self):
        with self.assertRaisesRegex(AssertionError, 'tenant read'):
            self.exercise(denied_read=200)
        self.assertIn(('DELETE', '/api/admin/v1/keys/key-1', 'admin'), self.actions)
        self.assertIn(('kill', 'sandbox-owned'), self.actions)
        self.assertIn(('close', 'sandbox-owned'), self.actions)
