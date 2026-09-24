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
    def test_entrypoint_fixture_outlives_instance_startup(self):
        source=(ROOT/'prepare.py').read_text()
        self.assertIn('sleep 30; echo adx-entrypoint-stderr',source)
        self.assertNotIn('sleep 5; echo adx-entrypoint-stderr',source)

    def test_profiles_separate_l0_from_standalone_and_full(self):
        self.assertEqual(driver.required_for_profile('l0'), ('l0', 'auth'))
        self.assertEqual(
            set(driver.required_for_profile('standalone')),
            {'sdk', 'data-plane', 'lifecycle', 'auth', 'capacity', 'placement',
             'local-first', 'node-failure', 'sandboxd-restart', 'restart', 'stop'},
        )
        self.assertEqual(
            driver.required_for_profile('k8s-basic'),
            ('sdk', 'auth', 'capacity', 'placement', 'local-first'),
        )
        self.assertEqual(driver.required_for_profile('full'), driver.required_for_profile('standalone'))
        with self.assertRaisesRegex(ValueError, 'unknown E2E profile'):
            driver.required_for_profile('unknown')

    def test_l0_report_only_requires_l0_cases(self):
        report = driver.finish_report(None, [], ['l0', 'auth'], driver.required_for_profile('l0'))
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['required_checks'], ['l0', 'auth'])

    def test_k8s_basic_excludes_extended_and_fault_scenarios(self):
        required = driver.required_for_profile('k8s-basic')
        report = driver.finish_report(None, [], list(required), required)
        self.assertEqual(report['status'], 'passed')
        self.assertEqual(report['missing_checks'], [])
        self.assertTrue(
            {'data-plane', 'lifecycle', 'node-failure', 'sandboxd-restart', 'restart', 'stop'}.isdisjoint(required)
        )

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

    def test_sdk_subcases_are_extracted_and_reported_individually(self):
        output = '\n'.join([
            'ordinary log output',
            json.dumps({
                'status': 'passed',
                'cases': [
                    {'id': 'command.foreground', 'status': 'passed', 'seconds': 0.2},
                    {'id': 'filesystem.copy', 'status': 'passed', 'seconds': 0.4},
                ],
            }),
            '[SCENARIO COMPLETE] data-plane',
        ])
        subcases = driver.sdk_subcases_from_output(output)
        self.assertEqual(
            [case['id'] for case in subcases],
            ['command.foreground', 'filesystem.copy'],
        )
        report = driver.finish_report(None, [], ['data-plane'], ('data-plane',))
        report['cases'] = [{
            'name': 'data-plane',
            'status': 'passed',
            'seconds': 1.0,
            'subcases': subcases,
        }]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / 'junit.xml'
            driver.write_junit(path, report)
            suite = ET.parse(path).getroot()
        self.assertEqual(
            [case.attrib['name'] for case in suite.findall('testcase')],
            ['data-plane/command.foreground', 'data-plane/filesystem.copy', 'cleanup'],
        )
        self.assertEqual(suite.attrib['tests'], '3')

    def test_placement_boolean_cases_are_extracted(self):
        output = json.dumps({
            'status': 'passed',
            'cases': [
                {'name': 'sandboxd runtime inventory', 'passed': True},
                {'name': 'environment affinity OR', 'passed': True},
                {'name': 'reverse instance anti-affinity', 'passed': True},
                {'name': 'unavailable runtime stays unassigned', 'passed': True},
            ],
        })
        self.assertEqual(
            driver.sdk_subcases_from_output(output),
            [
                {'id': 'sandboxd runtime inventory', 'status': 'passed', 'seconds': 0.0},
                {'id': 'environment affinity OR', 'status': 'passed', 'seconds': 0.0},
                {'id': 'reverse instance anti-affinity', 'status': 'passed', 'seconds': 0.0},
                {'id': 'unavailable runtime stays unassigned', 'status': 'passed', 'seconds': 0.0},
            ],
        )

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
        self.assertEqual(driver.finish_report(None, [], [
            'sdk', 'data-plane', 'lifecycle', 'auth', 'capacity', 'placement',
            'local-first', 'node-failure', 'sandboxd-restart', 'restart', 'stop'])['status'], 'passed')

    def test_sandboxd_restart_checks_existing_instances_before_cleanup(self):
        with tempfile.TemporaryDirectory() as directory:
            run = driver.Run(Path(directory))
            run.nodes = ['node1', 'node2']
            calls = []
            run.execute = lambda node, *args, **kwargs: calls.append(('execute', node, args[-1])) or ''
            run.helper = lambda node, *args, **kwargs: calls.append(('helper', node, args[0])) or ''
            checks = []
            run.scenarios(checks, ('sandboxd-restart',))
            self.assertEqual(checks, ['sandboxd-restart'])
            self.assertEqual(calls, [
                ('execute', 'node1', 'create-marker'),
                ('helper', 'node1', 'restart-sandboxd'),
                ('helper', 'node2', 'restart-sandboxd'),
                ('execute', 'node1', 'recovered-marker'),
                ('execute', 'node1', 'cleanup-live'),
                ('helper', 'node1', 'empty'),
                ('helper', 'node2', 'empty'),
            ])

    def test_old_eight_scenarios_without_functional_data_plane_cannot_pass(self):
        report = driver.finish_report(None, [], [
            'sdk', 'auth', 'capacity', 'placement', 'local-first',
            'node-failure', 'sandboxd-restart', 'restart', 'stop'])
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['missing_checks'], ['data-plane', 'lifecycle'])

    def test_missing_node_failure_scenario_cannot_pass(self):
        report = driver.finish_report(None, [], ['sdk', 'data-plane', 'lifecycle',
                                                'auth', 'capacity', 'placement',
                                                'local-first', 'sandboxd-restart', 'restart', 'stop'])
        self.assertEqual(report['status'], 'failed')
        self.assertEqual(report['missing_checks'], ['node-failure'])

    def test_old_seven_scenarios_without_local_first_cannot_pass(self):
        report = driver.finish_report(None, [], ['sdk', 'data-plane', 'lifecycle',
                                                'auth', 'capacity', 'placement',
                                                'node-failure', 'sandboxd-restart', 'restart', 'stop'])
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

    def test_complete_bundle_requires_node_execd_and_entrypoint_archives(self):
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)
            for name,payload in (
                ('images.tar',b'node'),('execd.tar',b'execd'),
                ('entrypoint.tar',b'entrypoint'),
            ):
                (p/name).write_bytes(payload)
            manifest={
                'schema_version':1,
                'archive_sha256':driver.sha(p/'images.tar'),
                'execd_archive_sha256':driver.sha(p/'execd.tar'),
                'entrypoint_archive_sha256':driver.sha(p/'entrypoint.tar'),
            }
            (p/'bundle.json').write_text(json.dumps(manifest))
            self.assertEqual(driver.verify_bundle(p),manifest)
            (p/'entrypoint.tar').write_bytes(b'replaced')
            with self.assertRaisesRegex(ValueError,'entrypoint'):
                driver.verify_bundle(p)

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
