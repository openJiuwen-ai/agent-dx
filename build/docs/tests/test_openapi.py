import copy
import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('openapi_check', Path(__file__).resolve().parents[1] / 'openapi_check.py')
openapi_check = importlib.util.module_from_spec(spec)
spec.loader.exec_module(openapi_check)


class OpenApiContractTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.control = openapi_check.load('environment.yaml')
        cls.data = openapi_check.load('data-plane.yaml')

    def test_current_contracts_cover_implemented_routes_and_actions(self):
        self.assertEqual(openapi_check.audit(self.control, self.data), [])

    def test_missing_admin_route_is_detected(self):
        doc = copy.deepcopy(self.control)
        del doc['paths']['/api/admin/v1/keys']['post']
        self.assertIn('POST /api/admin/v1/keys', '\n'.join(openapi_check.audit(doc, self.data)))

    def test_missing_create_field_is_detected(self):
        doc = copy.deepcopy(self.control)
        del doc['components']['schemas']['CreateEnvironmentRequest']['properties']['cwd']
        self.assertIn('missing create field: cwd', '\n'.join(openapi_check.audit(doc, self.data)))

    def test_missing_runtime_action_and_missing_typed_args_are_detected(self):
        doc = copy.deepcopy(self.data)
        schemas = doc['components']['schemas']
        schemas['ProcessStartRequest']['properties']['action']['enum'].remove('process.start')
        self.assertIn('process.start', '\n'.join(openapi_check.audit(self.control, doc)))
        doc = copy.deepcopy(self.data)
        doc['components']['schemas']['ProcessStartRequest']['properties']['args'] = {'type': 'object'}
        self.assertIn('typed args', '\n'.join(openapi_check.audit(self.control, doc)))

    def test_new_source_route_and_action_require_contract_updates(self):
        routes = openapi_check.control_routes() | {('GET', '/api/environment/v1/new-feature')}
        actions = openapi_check.runtime_actions() | {'process.new-feature'}
        errors = openapi_check.audit(self.control, self.data, routes=routes, actions=actions)
        self.assertIn('GET /api/environment/v1/new-feature', '\n'.join(errors))
        self.assertIn('process.new-feature', '\n'.join(errors))

    def test_dangling_reference_is_detected(self):
        doc = copy.deepcopy(self.data)
        doc['components']['schemas']['ProcessStartRequest']['properties']['args'] = {'$ref': '#/components/schemas/Absent'}
        self.assertIn('Absent', '\n'.join(openapi_check.audit(self.control, doc)))

    def test_request_schema_rejects_wrong_action_args(self):
        good = {'action': 'process.start', 'args': {'command_id': 'cmd-1', 'command': 'pwd', 'cwd': '/tmp', 'want_stdin': True}}
        self.assertEqual(openapi_check.payload_errors(self.data, 'InvokeRequest', good), [])
        for bad in (
            {'action': 'process.start', 'args': {'command': 'pwd', 'want_stdin': 'yes'}},
            {'action': 'file.rename', 'args': {'path': '/tmp/a'}},
            {'action': 'file.write_chunk', 'args': {'path': '/tmp/a', 'data': 'xyz'}},
            {'action': 'shell.run', 'args': {'command': 'pwd'}},
            {'action': 'process.wait', 'args': {'timeout': 1}},
        ):
            with self.subTest(bad=bad):
                self.assertTrue(openapi_check.payload_errors(self.data, 'InvokeRequest', bad))

    def test_response_schema_covers_running_spawn_failure_and_wait_timeout(self):
        for response in (
            {'command_id': 'cmd-1', 'pid': 42, 'status': 'RUNNING', 'error': None, 'error_code': None},
            {'command_id': 'cmd-1', 'pid': -1, 'status': 'FAILED', 'running': False, 'cmd': 'pwd', 'state_version': 2, 'stdout': '', 'stderr': '', 'exit_code': None, 'exit_signal': None, 'error_code': 'SPAWN_FAILED', 'error_message': 'No such file or directory'},
            {'status': 'running', 'error_code': 'WAIT_TIMEOUT', 'error_message': 'command wait timed out', 'error': None},
        ):
            with self.subTest(response=response):
                self.assertEqual(openapi_check.payload_errors(self.data, 'InvokeResult', response), [])
        self.assertTrue(openapi_check.payload_errors(self.data, 'CommandRecord', {'command_id': 'cmd', 'pid': 'wrong'}))

    def test_file_search_request_covers_defaults_and_runtime_limits(self):
        for args in (
            {'path': '/workspace'},
            {'path': '/workspace', 'pattern': '*.py', 'exclude_patterns': ['*/.git/*'],
             'depth': 64, 'max_results': 10000, 'max_entries': 1000000, 'timeout_ms': 30000},
        ):
            with self.subTest(args=args):
                self.assertEqual(openapi_check.payload_errors(
                    self.data, 'InvokeRequest', {'action': 'file.search', 'args': args}), [])
        for change in (
            {'path': ''}, {'pattern': 1}, {'exclude_patterns': ['*'] * 65},
            {'exclude_patterns': [None]}, {'depth': 0}, {'depth': 65},
            {'max_results': 10001}, {'max_entries': 1000001},
            {'timeout_ms': 0}, {'timeout_ms': 30001},
        ):
            with self.subTest(change=change):
                self.assertTrue(openapi_check.payload_errors(
                    self.data, 'InvokeRequest',
                    {'action': 'file.search', 'args': {'path': '/workspace', **change}}))

    def test_file_search_response_preserves_scanned_count_and_atomic_errors(self):
        for result in (
            {'entries': [], 'scanned_entries': 0, 'error': None},
            {'entries': [], 'error_code': 'RESOURCE_EXHAUSTED',
             'error': 'file search max_results exceeded; narrow the search'},
        ):
            with self.subTest(result=result):
                self.assertEqual(openapi_check.payload_errors(self.data, 'FileSearchResult', result), [])
                self.assertEqual(openapi_check.payload_errors(self.data, 'InvokeResult', result), [])
        self.assertTrue(openapi_check.payload_errors(
            self.data, 'FileSearchResult', {'entries': [], 'scanned_entries': -1, 'error': None}))
        self.assertTrue(openapi_check.payload_errors(
            self.data, 'FileSearchResult', {'entries': [], 'error': None}))

    def test_sdk_nullable_overrides_and_shell_timeout_are_supported(self):
        for request in (
            {'action': 'process.exec', 'args': {'cmd': 'pwd', 'cwd': None, 'envs': None, 'timeout': 5}},
            {'action': 'process.start', 'args': {'command': 'pwd', 'cwd': None, 'envs': None, 'want_stdin': False}},
            {'action': 'shell.run', 'args': {'session_id': 'sh-1', 'command': 'pwd', 'timeout': 60}},
        ):
            with self.subTest(request=request):
                self.assertEqual(openapi_check.payload_errors(self.data, 'InvokeRequest', request), [])

    def test_key_request_and_list_use_real_wire_fields(self):
        self.assertEqual(openapi_check.payload_errors(self.control, 'CreateTenantKeyRequest', {'tenantId': 'team-a', 'expiresAtUnixSeconds': 0}), [])
        self.assertTrue(openapi_check.payload_errors(self.control, 'CreateTenantKeyRequest', {'tenant': 'team-a'}))
        self.assertTrue(openapi_check.payload_errors(self.control, 'TenantKeyList', {'items': [], 'apiKey': 'secret'}))

    def test_affinity_enums_and_label_requirements_match_the_contract(self):
        good = {'kind': 0, 'affinity': 2, 'weight': 1, 'labelOps': [{'type': 0, 'labelKey': 'NODE_ID', 'labelValues': ['node-1']}]}
        self.assertEqual(openapi_check.payload_errors(self.control, 'ScheduleAffinity', good), [])
        for change in ({'kind': 2}, {'weight': 1001}, {'preferredAntiOtherLabels': True}, {'labelOps': [{'type': 0, 'labelKey': 'NODE_ID'}]}):
            with self.subTest(change=change):
                self.assertTrue(openapi_check.payload_errors(self.control, 'ScheduleAffinity', {**good, **change}))

    def test_all_declared_examples_validate(self):
        self.assertEqual(openapi_check.example_errors(self.control), [])
        self.assertEqual(openapi_check.example_errors(self.data), [])


if __name__ == '__main__':
    unittest.main()
