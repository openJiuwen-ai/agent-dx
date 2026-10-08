import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('telemetry_trace',Path(__file__).parents[1]/'telemetry.py')
telemetry=importlib.util.module_from_spec(spec);spec.loader.exec_module(telemetry)
class TraceCollectionTests(unittest.TestCase):
    def test_embedded_ingress_uses_api_process_log_service(self):
        self.assertEqual(
            telemetry.required_log_services('node1'),
            {'node1','coordinator','api','redis'},
        )
        self.assertNotIn('ingress',telemetry.required_log_services('node1'))

    def test_ids_without_parent_relationship_do_not_prove_queue_propagation(self):
        rows=[{'traceId':'a','spanId':'1','parentSpanId':'0','name':'node.create_environment','service':'adxlet'},
              {'traceId':'a','spanId':'2','parentSpanId':'1','name':'environment.queue','service':'adxlet'},
              {'traceId':'a','spanId':'3','parentSpanId':'missing','name':'environment.execute','service':'adxlet'}]
        with self.assertRaises(AssertionError): telemetry.check_trace_links(rows)
    def test_real_parent_chain_passes(self):
        rows=[{'traceId':'a','spanId':'1','parentSpanId':'0','name':'node.create_environment','service':'adxlet'},
              {'traceId':'a','spanId':'2','parentSpanId':'1','name':'environment.queue','service':'adxlet'},
              {'traceId':'a','spanId':'3','parentSpanId':'2','name':'environment.execute','service':'adxlet'}]
        self.assertEqual(telemetry.check_trace_links(rows),1)
    def test_matching_parent_id_from_another_trace_is_rejected(self):
        rows=[{'traceId':'b','spanId':'2','parentSpanId':'0','name':'environment.queue'},
              {'traceId':'a','spanId':'3','parentSpanId':'2','name':'environment.execute'}]
        with self.assertRaises(AssertionError): telemetry.check_trace_links(rows)
