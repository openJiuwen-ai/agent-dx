import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import time
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('live_driver', ROOT / 'run.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)


class LiveOutputTests(unittest.TestCase):
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
                if 'capacity' in args: raise RuntimeError('capacity assertion failed')
            run.execute = execute
            console = io.StringIO()
            with contextlib.redirect_stdout(console), self.assertRaisesRegex(RuntimeError, 'capacity assertion'):
                run.scenarios(checks)
            self.assertEqual(checks, ['sdk', 'auth'])
            self.assertEqual([r['status'] for r in run.case_results], ['passed', 'passed', 'failed'])
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
            self.assertEqual(suite.attrib, {'name': 'platform-kubernetes-e2e', 'tests': '6', 'failures': '2', 'skipped': '3'})
            self.assertIsNotNone(suite.find("testcase[@name='auth']/failure"))
            self.assertIsNotNone(suite.find("testcase[@name='cleanup']/failure"))
