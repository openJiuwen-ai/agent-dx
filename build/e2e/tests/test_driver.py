import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
import xml.etree.ElementTree as ET

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('e2e_driver', ROOT / 'run.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)

class AcceptanceGateTests(unittest.TestCase):
    def test_profiles_separate_l0_from_standalone_and_full(self):
        self.assertEqual(driver.required_for_profile('l0'), ('l0', 'auth'))
        self.assertEqual(
            set(driver.required_for_profile('standalone')),
            {'sdk', 'auth', 'capacity', 'placement', 'local-first', 'node-failure', 'restart', 'stop'},
        )
        self.assertEqual(driver.required_for_profile('full'), driver.required_for_profile('standalone'))
        with self.assertRaisesRegex(ValueError, 'unknown E2E profile'):
            driver.required_for_profile('unknown')

    def test_l0_report_only_requires_l0_cases(self):
        report = driver.finish_report(None, [], ['l0', 'auth'], driver.required_for_profile('l0'))
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['required_checks'], ['l0', 'auth'])

    def test_junit_reports_each_required_case_and_cleanup(self):
        report = driver.finish_report(None, [], ['l0'], driver.required_for_profile('l0'))
        report['cases'] = [{'name': 'l0', 'status': 'passed', 'seconds': 1.25}]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'junit.xml'
            driver.write_junit(path, report)
            suite = ET.parse(path).getroot()
        self.assertEqual([case.attrib['name'] for case in suite.findall('testcase')],
                         ['l0', 'auth', 'cleanup'])
        self.assertEqual(suite.attrib['tests'], '3')
        self.assertEqual(suite.attrib['skipped'], '1')

    def test_cleanup_failure_cannot_pass(self):
        report = driver.finish_report(None, ['container remains'], ['sdk', 'auth', 'capacity', 'restart', 'stop'])
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['cleanup_errors'], ['container remains'])

    def test_first_failure_survives_cleanup_failure(self):
        report = driver.finish_report('SDK failed', ['cleanup failed'], [])
        self.assertEqual(report['error'], 'SDK failed')
        self.assertEqual(report['status'], 'failed')

    def test_missing_required_scenario_cannot_pass(self):
        self.assertEqual(driver.finish_report(None, [], ['sdk'])['status'], 'failed')

    def test_complete_clean_run_passes(self):
        self.assertEqual(driver.finish_report(None, [], ['sdk', 'auth', 'capacity', 'placement', 'local-first', 'node-failure', 'restart', 'stop'])['status'], 'passed')

    def test_missing_node_failure_scenario_cannot_pass(self):
        report = driver.finish_report(None, [], ['sdk', 'auth', 'capacity', 'placement', 'local-first', 'restart', 'stop'])
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['missing_checks'], ['node-failure'])

    def test_old_seven_scenarios_without_local_first_cannot_pass(self):
        report = driver.finish_report(None, [], ['sdk', 'auth', 'capacity', 'placement', 'node-failure', 'restart', 'stop'])
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['missing_checks'], ['local-first'])

    def test_old_five_scenarios_without_placement_cannot_pass(self):
        report = driver.finish_report(None, [], ['sdk', 'auth', 'capacity', 'restart', 'stop'])
        self.assertEqual(report['status'], 'failed')
        self.assertIn('placement', report['missing_checks'])

    def test_runtime_architecture_must_match_artifact(self):
        with self.assertRaisesRegex(ValueError, 'architecture'):
            driver.validate_identity({'target':'aarch64-unknown-linux-gnu','commit':'a'*40,'dirty':False}, 'amd64', 'a'*40, True)

    def test_ci_rejects_dirty_or_wrong_commit(self):
        for commit, dirty in [('b'*40,False), ('a'*40,True)]:
            with self.assertRaises(ValueError):
                driver.validate_identity({'target':'x86_64-unknown-linux-gnu','commit':commit,'dirty':dirty}, 'amd64', 'a'*40, True)

    def test_modified_bundle_is_rejected_before_deployment(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);(p/'images.tar').write_bytes(b'changed')
            (p/'bundle.json').write_text(json.dumps({'schema_version':1,'archive_sha256':'0'*64}))
            with self.assertRaises(ValueError):driver.verify_bundle(p)

    def test_makefile_has_separate_local_and_kubernetes_profile_defaults(self):
        makefile = (ROOT.parents[1] / 'Makefile').read_text()
        self.assertIn('E2E_PROFILE ?= standalone', makefile)
        self.assertIn('K8S_E2E_PROFILE ?= k8s-basic', makefile)
        k8s_target = makefile.split('platform-k8s-e2e:', 1)[1]
        self.assertIn('$(K8S_E2E_PROFILE)', k8s_target)
        self.assertNotIn('--profile "$(E2E_PROFILE)"', k8s_target)

class RegistryImportTests(unittest.TestCase):
    def test_archive_import_checks_manifest_and_compresses_layers(self):
        import gzip, hashlib, http.server, io, tarfile, threading
        spec=importlib.util.spec_from_file_location('registry_publish',ROOT/'publish.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        blobs={};published=[]
        class Handler(http.server.BaseHTTPRequestHandler):
            def log_message(self,*args):pass
            def do_GET(self):self.send_response(200);self.end_headers()
            def do_POST(self):
                self.send_response(202);self.send_header('Location','/upload?id=fixture');self.end_headers()
            def do_PUT(self):
                data=self.rfile.read(int(self.headers['Content-Length']))
                digest='sha256:'+hashlib.sha256(data).hexdigest()
                if self.path.startswith('/upload'):
                    self.assertion=digest in self.path;blobs[digest]=data
                else:published.append(json.loads(data))
                self.send_response(201);self.send_header('Docker-Content-Digest',digest);self.end_headers()
        server=http.server.ThreadingHTTPServer(('127.0.0.1',0),Handler)
        thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
        self.addCleanup(server.server_close);self.addCleanup(server.shutdown)
        module.BASE='http://127.0.0.1:'+str(server.server_port)
        with tempfile.TemporaryDirectory() as d:
            archive=Path(d)/'image.tar'
            with tarfile.open(archive,'w') as tar:
                for name,data in [('manifest.json',json.dumps([{'Config':'config.json','Layers':['layer.tar']}]).encode()),('config.json',b'{}'),('layer.tar',b'fixture layer')]:
                    info=tarfile.TarInfo(name);info.size=len(data);tar.addfile(info,io.BytesIO(data))
            digest=module.publish(archive)
        self.assertTrue(digest.startswith('sha256:'))
        self.assertEqual(len(published),1)
        layer=published[0]['layers'][0]
        self.assertEqual(gzip.decompress(blobs[layer['digest']]),b'fixture layer')
        self.assertEqual(layer['size'],len(blobs[layer['digest']]))
