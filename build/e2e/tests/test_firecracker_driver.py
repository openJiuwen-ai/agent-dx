import importlib.util
from pathlib import Path
import tempfile
import unittest
import json
import sys

ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/'firecracker'))

class FirecrackerEvidenceTests(unittest.TestCase):
    def test_fault_oracles_read_the_nested_runtime_record(self):
        from runtime_record import runtime_id, runtime_ip
        record = {'state': 'Running', 'runtime': {'id': 'backend-1', 'ip': '10.0.0.8'}}
        self.assertEqual(runtime_id(record), 'backend-1')
        self.assertEqual(runtime_ip(record), '10.0.0.8')
        with self.assertRaises(KeyError):
            runtime_id({'state': 'Running', 'runtime_id': 'legacy-backend'})

    def test_entrypoint_case_uses_the_execd_terminal_status_contract(self):
        source=(ROOT/'firecracker/sdk_checkpoint.py').read_text()
        self.assertIn("entrypoint_info['status_kind'] == 'exited'",source)
        self.assertIn("entrypoint_info['exit_code'] == 7",source)
        self.assertNotIn("entrypoint_info['status_kind'] == 'exit_code'",source)

    def test_paused_restart_targets_the_selected_instance(self):
        checkpoint=(ROOT/'firecracker/sdk_checkpoint.py').read_text()
        restart=(ROOT/'firecracker/restart_paused.py').read_text()
        self.assertIn('[*a.restart_command, sandbox.id]',checkpoint)
        self.assertIn("key='environment:'+INSTANCE_ID",restart)
        self.assertNotIn('len(records)==1',restart)

    def test_runtime_network_policy_uses_a_fresh_execution(self):
        source=(ROOT/'firecracker/sdk_checkpoint.py').read_text()
        network_case=source.index("networked = Sandbox(")
        checkpoint_case=source.index("sandbox = Sandbox(labels={'app':'checkpoint-source'}")
        self.assertLess(network_case,checkpoint_case)
        self.assertIn('networked.update_network_policy(NetworkPolicy.block())',source)
        self.assertIn('networked.update_network_policy(None)',source)
        self.assertIn("os.environ['ADX_FC_EGRESS_PROBE_HOST']",source)
        self.assertIn('/bin/bash', source)
        self.assertIn('/dev/tcp/', source)
        self.assertIn('[[ "$line" == HTTP/* ]]', source)
        self.assertNotIn('/bin/busybox', source)
        self.assertNotIn('nc -z', source)
        fixture=(ROOT/'firecracker/node.py').read_text()
        self.assertIn("probe_namespace='adx-probe-'+suffix",fixture)
        self.assertIn("'198.18.0.2/30'",fixture)
        self.assertIn("'ip','netns','delete',probe_namespace",fixture)

    def test_runtime_profile_uses_local_erofs_for_vm_and_pinned_oci_for_kubernetes(self):
        import fc_runtime_profile
        with tempfile.TemporaryDirectory() as d:
            root=Path(d); package=root/'package'; artifact=package/'runtime/adx-runtime-rootfs.img'
            artifact.parent.mkdir(parents=True);artifact.write_bytes(b'erofs')
            custom='/etc/adx/custom-image-process.json'
            local=fc_runtime_profile.resolve(package,False,root/'missing',custom)
            self.assertEqual(local['rootfs']['runtime_class'],'firecracker')
            self.assertEqual(local['rootfs']['path'],str(artifact.resolve()))
            self.assertEqual(local['bootstrap']['root'],str(artifact.resolve()))
            self.assertEqual(local['bootstrap']['image_process_config'],custom)
            image=root/'runtime-image';image.write_text('registry.example/adx-runtime@sha256:'+'a'*64)
            remote=fc_runtime_profile.resolve(package,True,image,custom)
            self.assertEqual(remote['rootfs']['image'],image.read_text())
            self.assertEqual(remote['bootstrap']['image'],image.read_text())
            self.assertEqual(remote['bootstrap']['image_process_config'],custom)
            with self.assertRaisesRegex(RuntimeError,'must be absolute'):
                fc_runtime_profile.resolve(package,False,root/'missing','etc/relative.json')
            image.write_text('registry.example/adx-runtime:latest')
            with self.assertRaisesRegex(RuntimeError,'digest pinned'):
                fc_runtime_profile.resolve(package,True,image)

    def test_missing_or_duplicate_cases_cannot_pass(self):
        import acceptance
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            (root/'sdk').mkdir(); (root/'lifecycle').mkdir()
            (root/'result.json').write_text(json.dumps({'status':'passed'}))
            for group in ('sdk','lifecycle'):
                (root/group/'result.json').write_text(json.dumps({'status':'passed','cases':[{'name':'duplicate','passed':True}]*5}))
            with self.assertRaises(ValueError): acceptance.verify(root)
    def test_manifest_requires_kvm_and_pinned_images_and_selected_node(self):
        import acceptance
        image='registry.example/node@sha256:'+'a'*64
        pod=acceptance.pod('adx-e2e-test',image,'amd64','worker-a',True)
        spec=pod['spec'];self.assertFalse(spec['automountServiceAccountToken']);self.assertNotIn('hostPID',spec);self.assertNotIn('hostNetwork',spec)
        self.assertEqual(spec['nodeSelector']['kubernetes.io/hostname'],'worker-a')
        self.assertIn({'name':'kvm','hostPath':{'path':'/dev/kvm','type':'CharDevice'}},spec['volumes'])
        with self.assertRaises(ValueError): acceptance.pod('adx-e2e-test','node:latest','amd64','worker-a',True)

    def test_kit_rejects_altered_files_and_backend_identity(self):
        import kit,hashlib
        with tempfile.TemporaryDirectory() as d:
            root=Path(d);files={}
            for name in kit.REQUIRED:
                p=root/name;p.parent.mkdir(parents=True,exist_ok=True);p.write_bytes(name.encode());files[name]=hashlib.sha256(p.read_bytes()).hexdigest()
            backend={'target':'aarch64-unknown-linux-gnu','sandboxd_revision':'pinned','files':{'sandboxd':files['bin/sandboxd'],'sbox':files['bin/sbox'],'redis-cli':files['tools/redis-cli']}}
            (root/'manifest.json').write_text(json.dumps({'schema_version':1,'target':backend['target'],'sandboxd_revision':'pinned','files':files}))
            kit.verify(root,backend)
            (root/'artifacts/Image').write_bytes(b'altered')
            with self.assertRaises(ValueError):kit.verify(root,backend)

    def test_complete_evidence_passes_but_cleanup_error_fails(self):
        import acceptance
        with tempfile.TemporaryDirectory() as d:
            root=Path(d)
            for group,names in acceptance.CASES.items():
                (root/group).mkdir();(root/group/'result.json').write_text(json.dumps({'status':'passed','cases':[{'name':name,'passed':True} for name in names]}))
            (root/'result.json').write_text(json.dumps({'status':'passed'}))
            (root/'sdk/snapshot-collected-before-clone-resume.json').write_text(json.dumps({'snapshot_id':'saved','state':'Deleted','references':[]}))
            (root/'orphan-gc.json').write_text(json.dumps({**{k:True for k in ('passed','current_session_preserved','retired_session_removed','foreign_preserved','unmarked_preserved')},'registered_checkpoint_preserved':'saved'}))
            (root/'snapshots-final.json').write_text(json.dumps({'saved':{'state':'Deleted','references':[]}}))
            (root/'catalog-final.json').write_text(json.dumps({'environment:a':{'result':{'state':'Deleted','resources_held':False}}}))
            (root/'s3-final.xml').write_text('<ListBucketResult />');(root/'inventory-final.txt').write_text('ID STATUS\n')
            self.assertEqual(len(acceptance.verify(root)),sum(map(len,acceptance.CASES.values())))
            (root/'result.json').write_text(json.dumps({'status':'passed','stop_error':'failed'}))
            with self.assertRaises(ValueError):acceptance.verify(root)

    def test_fc_cleanup_refuses_replaced_namespace(self):
        spec=importlib.util.spec_from_file_location('fc_kube',ROOT/'firecracker/kubernetes.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        with tempfile.TemporaryDirectory() as d:
            run=module.FirecrackerRun(Path(d),Path('/test/kubeconfig'));run.namespace_attempted=True;run.namespace_uid='original';calls=[]
            def command(*args,**kwargs):
                calls.append(args);return json.dumps({'metadata':{'uid':'replacement','labels':{'adx.e2e.run':run.id}}})
            run.kube=command
            self.assertTrue(run.cleanup());self.assertFalse(any('delete' in c for c in calls))

    def test_ci_summary_does_not_pass_missing_or_partial_result(self):
        import subprocess
        script=ROOT.parents[1]/'.buildkite/fc-summary.py'
        with tempfile.TemporaryDirectory() as d:
            directory=Path(d)/'out/buildkite/firecracker'
            result=subprocess.run([sys.executable,str(script),'--exit-code','0'],cwd=d,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
            self.assertIn('FAIL',(directory/'summary.md').read_text())
            (directory/'result.json').write_text(json.dumps({'status':'passed','cases':[{'name':str(n)} for n in range(9)]}))
            result=subprocess.run([sys.executable,str(script),'--exit-code','0'],cwd=d,capture_output=True,text=True)
            self.assertNotEqual(result.returncode,0)
