import importlib.util
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location('telemetry_trace', Path(__file__).parents[1] / 'telemetry.py')
telemetry = importlib.util.module_from_spec(spec)
spec.loader.exec_module(telemetry)


class TraceCollectionTests(unittest.TestCase):
    def test_embedded_ingress_uses_api_process_log_service(self):
        self.assertEqual(
            telemetry.required_log_services('node1'),
            {'node1', 'coordinator', 'api', 'redis'},
        )
        self.assertNotIn('ingress', telemetry.required_log_services('node1'))

    def test_ids_without_parent_relationship_do_not_prove_queue_propagation(self):
        rows = [
            {
                'traceId': 'a',
                'spanId': '1',
                'parentSpanId': '0',
                'name': 'node.create_environment',
                'service': 'adxlet',
            },
            {'traceId': 'a', 'spanId': '2', 'parentSpanId': '1', 'name': 'environment.queue', 'service': 'adxlet'},
            {
                'traceId': 'a',
                'spanId': '3',
                'parentSpanId': 'missing',
                'name': 'environment.execute',
                'service': 'adxlet',
            },
        ]
        with self.assertRaises(AssertionError):
            telemetry.check_trace_links(rows)

    def test_real_parent_chain_passes(self):
        rows = [
            {
                'traceId': 'a',
                'spanId': '1',
                'parentSpanId': '0',
                'name': 'node.create_environment',
                'service': 'adxlet',
            },
            {'traceId': 'a', 'spanId': '2', 'parentSpanId': '1', 'name': 'environment.queue', 'service': 'adxlet'},
            {'traceId': 'a', 'spanId': '3', 'parentSpanId': '2', 'name': 'environment.execute', 'service': 'adxlet'},
        ]
        self.assertEqual(telemetry.check_trace_links(rows), 1)

    def test_matching_parent_id_from_another_trace_is_rejected(self):
        rows = [
            {'traceId': 'b', 'spanId': '2', 'parentSpanId': '0', 'name': 'environment.queue'},
            {'traceId': 'a', 'spanId': '3', 'parentSpanId': '2', 'name': 'environment.execute'},
        ]
        with self.assertRaises(AssertionError):
            telemetry.check_trace_links(rows)


class HttpTraceContractTests(unittest.TestCase):

    def test_execd_named_http_span_with_parent_proves_context(self):
        rows = [{'service': 'adx-execd', 'name': 'POST /invoke', 'parentSpanId': 'a', 'attributes': [{'key': 'http.route', 'value': {'stringValue': '/invoke'}}]}]
        self.assertTrue(telemetry.has_execd_http_context(rows))

    def test_other_service_or_root_http_span_does_not_prove_execd_context(self):
        for (service, parent) in [('adx-apiserver', 'a'), ('adx-execd', '0000000000000000')]:
            rows = [{'service': service, 'name': 'POST /invoke', 'parentSpanId': parent, 'attributes': [{'key': 'http.route', 'value': {'stringValue': '/invoke'}}]}]
            self.assertFalse(telemetry.has_execd_http_context(rows))

    def test_create_trace_requires_two_linked_http_spans_and_all_stages(self):
        rows = [{'traceId': 't', 'spanId': '1', 'name': 'POST /api/sandbox/v1/sandboxes', 'service': 'adx-apiserver'}, {'traceId': 't', 'spanId': '2', 'parentSpanId': '1', 'name': 'POST /api/sandbox/v1/sandboxes', 'service': 'adx-apiserver'}]
        rows += [{'traceId': 't', 'spanId': str(i + 3), 'name': name} for (i, name) in enumerate(['coordinator.create_environment', 'node.create_environment', 'environment.queue', 'environment.execute', 'coordinator.commit_environment'])]
        self.assertEqual(telemetry.complete_create_trace_ids(rows), ['t'])
        rows[1]['parentSpanId'] = 'missing'
        self.assertEqual(telemetry.complete_create_trace_ids(rows), [])
        rows[1]['parentSpanId'] = '1'
        rows.pop()
        self.assertEqual(telemetry.complete_create_trace_ids(rows), [])
