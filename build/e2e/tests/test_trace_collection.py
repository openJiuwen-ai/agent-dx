import importlib.util
from pathlib import Path
import unittest
spec=importlib.util.spec_from_file_location('telemetry_trace',Path(__file__).parents[1]/'telemetry.py')
telemetry=importlib.util.module_from_spec(spec);spec.loader.exec_module(telemetry)
class TraceCollectionTests(unittest.TestCase):
    def test_ids_without_parent_relationship_do_not_prove_queue_propagation(self):
        rows=[{'traceId':'a','spanId':'1','parentSpanId':'0','name':'node.create_instance','service':'adx-node-manager'},
              {'traceId':'a','spanId':'2','parentSpanId':'1','name':'instance.queue','service':'adx-node-manager'},
              {'traceId':'a','spanId':'3','parentSpanId':'missing','name':'instance.execute','service':'adx-node-manager'}]
        with self.assertRaises(AssertionError): telemetry.check_trace_links(rows)
    def test_real_parent_chain_passes(self):
        rows=[{'traceId':'a','spanId':'1','parentSpanId':'0','name':'node.create_instance','service':'adx-node-manager'},
              {'traceId':'a','spanId':'2','parentSpanId':'1','name':'instance.queue','service':'adx-node-manager'},
              {'traceId':'a','spanId':'3','parentSpanId':'2','name':'instance.execute','service':'adx-node-manager'}]
        self.assertEqual(telemetry.check_trace_links(rows),1)
    def test_matching_parent_id_from_another_trace_is_rejected(self):
        rows=[{'traceId':'b','spanId':'2','parentSpanId':'0','name':'instance.queue'},
              {'traceId':'a','spanId':'3','parentSpanId':'2','name':'instance.execute'}]
        with self.assertRaises(AssertionError): telemetry.check_trace_links(rows)
