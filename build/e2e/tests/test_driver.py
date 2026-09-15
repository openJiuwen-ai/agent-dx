import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('e2e_driver', ROOT / 'run.py')
driver = importlib.util.module_from_spec(spec)
spec.loader.exec_module(driver)

class AcceptanceGateTests(unittest.TestCase):
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
        self.assertEqual(driver.finish_report(None, [], ['sdk', 'auth', 'capacity', 'restart', 'stop'])['status'], 'passed')

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
