import importlib.util
from pathlib import Path
import sys
import unittest
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT))
spec=importlib.util.spec_from_file_location('k8s_manifest',ROOT/'kubernetes/manifest.py')
k8s=importlib.util.module_from_spec(spec);spec.loader.exec_module(k8s)

class KubernetesDeploymentTests(unittest.TestCase):
    def resources(self):return k8s.resources('adx-e2e-test','registry.example/node@sha256:'+'a'*64,'amd64',True)
    def test_two_pods_are_scoped_and_process_deployed(self):
        pods=[x for x in self.resources() if x['kind']=='Pod']
        self.assertEqual([p['metadata']['name'] for p in pods],['node1','node2'])
        for pod in pods:
            self.assertEqual(pod['metadata']['namespace'],'adx-e2e-test')
            spec=pod['spec'];self.assertFalse(spec['automountServiceAccountToken'])
            self.assertNotIn('hostNetwork',spec);self.assertNotIn('hostPID',spec)
            self.assertEqual(spec['nodeSelector']['kubernetes.io/arch'],'amd64')
            self.assertTrue(spec['containers'][0]['securityContext']['privileged'])
            self.assertFalse(any('hostPath' in v for v in spec['volumes']))
    def test_collector_is_independent_and_has_shared_log_storage(self):
        for pod in (p for p in self.resources() if p['kind']=='Pod'):
            containers={c['name']:c for c in pod['spec']['containers']}
            self.assertEqual(set(containers),{'platform','collector'})
            collector=containers['collector']
            self.assertNotIn('privileged',collector['securityContext'])
            self.assertFalse(collector['securityContext']['allowPrivilegeEscalation'])
            mounts={m['name'] for m in collector['volumeMounts']}
            self.assertIn('state',mounts);self.assertIn('evidence',mounts)
            self.assertNotIn('images',mounts)
            self.assertNotIn('shareProcessNamespace',pod['spec'])

    def test_eligible_nodes_preserve_architecture_and_pod_isolation(self):
        objects=k8s.resources('adx-e2e-test','registry.example/node@sha256:'+'a'*64,'amd64',False,['worker-a','worker-b'])
        for pod in (o for o in objects if o['kind']=='Pod'):
            spec=pod['spec']
            terms=spec['affinity']['nodeAffinity']['requiredDuringSchedulingIgnoredDuringExecution']['nodeSelectorTerms']
            self.assertEqual(terms[0]['matchFields'][0],{'key':'metadata.name','operator':'In','values':['worker-a','worker-b']})
            self.assertEqual(spec['nodeSelector']['kubernetes.io/arch'],'amd64')
            self.assertNotIn('nodeName',spec)

    def test_shared_keys_are_read_only_and_evidence_is_separate(self):
        pod=next(r for r in self.resources() if r['kind']=='Pod')
        mounts={m['mountPath']:m for m in pod['spec']['containers'][0]['volumeMounts']}
        self.assertTrue(mounts['/secrets']['readOnly'])
        self.assertNotEqual(mounts['/evidence']['name'],mounts['/secrets']['name'])
        volume=next(v for v in pod['spec']['volumes'] if v['name']=='images')
        self.assertEqual(volume['emptyDir']['medium'],'Memory')
    def test_service_dns_matches_platform_configuration(self):
        services={r['metadata']['name']:r for r in self.resources() if r['kind']=='Service'}
        self.assertEqual(set(services),{'master','node2'})
        ports={p['port'] for p in services['master']['spec']['ports']}
        self.assertTrue({6379,17000,17001,18443,8443}.issubset(ports))
    def test_namespace_and_image_must_be_explicit(self):
        for namespace,image in [('default','registry.example/node@sha256:'+'a'*64),('adx-e2e-test','node:latest')]:
            with self.assertRaises(ValueError):k8s.resources(namespace,image,'amd64',False)

class KubernetesLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('k8s_run',ROOT/'kubernetes/run.py')
        cls.module=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.module)

    def test_cleanup_never_deletes_replaced_namespace(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'))
            run.namespace_attempted=True;run.namespace_uid='original'
            calls=[]
            def kube(*args,**kwargs):
                calls.append(args)
                return json.dumps({'metadata':{'uid':'replacement','labels':{'adx.e2e.run':run.id}}})
            run.kube=kube
            errors=run.cleanup()
            self.assertTrue(errors)
            self.assertFalse(any('delete' in call for call in calls))

    def test_failed_stop_still_deletes_owned_namespace_and_fails_cleanup(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'))
            run.namespace_attempted=True;run.namespace_uid='owned';run.nodes=['node1','node2']
            calls=[];deleted=False
            def kube(*args,**kwargs):
                nonlocal deleted
                calls.append(args)
                if args[:2]==('get','namespace'):
                    return '' if deleted else json.dumps({'metadata':{'uid':'owned','labels':{'adx.e2e.run':run.id}}})
                if args[:2]==('delete','namespace'):deleted=True
                return ''
            run.kube=kube
            run.helper=lambda *a,**k:None
            def execute(*args,**kwargs):raise RuntimeError('stop failed')
            run.execute=execute
            errors=run.cleanup()
            self.assertTrue(deleted)
            self.assertEqual(len(errors),2)
            self.assertEqual([a[3].split(':')[0] for a in calls if len(a)>3 and a[2]=='cp'],['node2','node1'])

    def test_cleanup_stops_services_before_copying_final_evidence(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'))
            run.namespace_attempted=True;run.namespace_uid='owned';run.nodes=['node1']
            events=[];deleted=False
            def kube(*args,**kwargs):
                nonlocal deleted
                if args[:2]==('get','namespace'):
                    return '' if deleted else json.dumps({'metadata':{'uid':'owned','labels':{'adx.e2e.run':run.id}}})
                if len(args)>2 and args[2]=='cp':events.append('copy')
                if args[:2]==('delete','namespace'):deleted=True
                return ''
            run.kube=kube
            run.execute=lambda *a,**k:events.append('stop')
            run.helper=lambda *a,**k:events.append('collect')
            self.assertEqual(run.cleanup(),[])
            self.assertEqual(events,['stop','collect','copy'])

    def test_registry_manifest_cannot_point_to_another_bundle(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            p=Path(d);(p/'bundle.json').write_text(json.dumps({'schema_version':1,'image_ids':{}}))
            (p/'registry.json').write_text(json.dumps({'schema_version':1,'bundle_sha256':'wrong','image_ids':{}}))
            with self.assertRaisesRegex(ValueError,'do not match'):
                self.module.identity(p/'bundle.json',p/'registry.json')

    def test_registry_manifest_requires_all_three_immutable_images(self):
        import hashlib,json,tempfile
        with tempfile.TemporaryDirectory() as d:
            p=Path(d)
            image_ids={'node':'node-id','rrt':'rrt-id','entrypoint':'entrypoint-id'}
            bundle={
                'schema_version':1,'image_ids':image_ids,'architecture':'amd64',
                'package':{'target':'x86_64-unknown-linux-gnu','commit':'a'*40,'dirty':False},
            }
            bundle_path=p/'bundle.json'
            bundle_path.write_text(json.dumps(bundle))
            digest=lambda value:'registry.example/'+value+'@sha256:'+hashlib.sha256(value.encode()).hexdigest()
            registry={
                'schema_version':1,'bundle_sha256':self.module.common.sha(bundle_path),
                'image_ids':image_ids,
                'references':{name:digest(name) for name in image_ids},
            }
            registry_path=p/'registry.json';registry_path.write_text(json.dumps(registry))
            loaded,published=self.module.identity(bundle_path,registry_path)
            self.assertEqual(loaded,bundle)
            self.assertEqual(set(published['references']),set(image_ids))
            del registry['references']['entrypoint']
            registry_path.write_text(json.dumps(registry))
            with self.assertRaisesRegex(ValueError,'entrypoint'):
                self.module.identity(bundle_path,registry_path)

    def test_pipeline_deploys_through_kubeconfig_without_docker(self):
        script=(ROOT.parents[1]/'.buildkite/run-e2e.sh').read_text()
        self.assertIn('build/e2e/kubernetes/run.py',script)
        self.assertIn('--kubeconfig',script)
        self.assertIn('ADX_E2E_PROFILE:-k8s-basic',script)
        self.assertNotIn('docker ',script)

    def test_pipeline_can_reuse_an_exact_prior_image_build(self):
        repository=ROOT.parents[1]
        script=(repository/'.buildkite/run-e2e.sh').read_text()
        pipeline=(repository/'.buildkite/pipeline.yml').read_text()
        summary=(repository/'.buildkite/summary.py').read_text()
        self.assertIn('ADX_E2E_ARTIFACT_BUILD',script)
        self.assertIn('ADX_E2E_ARTIFACT_COMMIT',script)
        self.assertIn('--build "$artifact_build"',script)
        self.assertIn('build.env("ADX_E2E_ARTIFACT_BUILD") == null',pipeline)
        self.assertIn("os.environ.get('ADX_E2E_ARTIFACT_COMMIT'",summary)

    def test_reused_product_image_receives_current_test_harness(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            output=Path(d)/'output';output.mkdir()
            run=self.module.KubernetesRun(output,Path('/fixture/kubeconfig'))
            run.nodes=['node1','node2']
            calls=[]
            run.kube=lambda *args,**kwargs:calls.append(args) or ''
            run.execute=lambda *args,**kwargs:''
            run.sync_harness('f'*40)
            copies=[call for call in calls if 'cp' in call]
            self.assertEqual([call[4].split(':')[0] for call in copies],['node1','node2'])
            manifest=json.loads((output/'harness.json').read_text())
            self.assertEqual(manifest['commit'],'f'*40)
            self.assertIn('functional_lifecycle.py',manifest['files'])
            self.assertNotIn('tests/test_kubernetes.py',manifest['files'])

    def test_harness_sync_precedes_runtime_preflight_and_setup(self):
        source=(ROOT/'kubernetes/run.py').read_text()
        sync=source.index('self.sync_harness(')
        preflight=source.index("'/opt/adx/e2e/preflight.py'")
        setup=source.index("'/opt/adx/e2e/node.py', 'setup'")
        self.assertLess(sync,preflight)
        self.assertLess(sync,setup)

    def test_full_profile_requires_two_distinct_physical_workers(self):
        same = [
            {'pod': 'node1', 'host': 'worker-a', 'ip': '10.0.0.1'},
            {'pod': 'node2', 'host': 'worker-a', 'ip': '10.0.0.2'},
        ]
        with self.assertRaisesRegex(RuntimeError, 'distinct Kubernetes workers'):
            self.module.validate_physical_placement(same, True)
        self.module.validate_physical_placement(same, False)
        split = [dict(same[0]), {**same[1], 'host': 'worker-b'}]
        self.module.validate_physical_placement(split, True)

    def test_generated_credentials_cover_every_projected_secret_file(self):
        import base64,tempfile
        with tempfile.TemporaryDirectory() as d:
            data=self.module.credentials(Path(d),'registry.example/user@sha256:'+'b'*64,
                                         'registry.example/rrt@sha256:'+'c'*64)
            objects=k8s.resources('adx-e2e-test','registry.example/node@sha256:'+'a'*64,'amd64')
            pod=next(o for o in objects if o['kind']=='Pod')
            secret=next(v['secret'] for v in pod['spec']['volumes'] if v['name']=='credentials')
            self.assertTrue(all(item['key'] in data for item in secret['items']))
            self.assertIn({'key':'admin-key','path':'admin-key'}, secret['items'])
            self.assertNotIn('ca.key',data)
            self.assertNotEqual(data['admin-key'],data['api-key'])
            self.assertNotIn('ca.key',data)
            self.assertEqual(base64.b64decode(data['image']).decode(),'registry.example/user@sha256:'+'b'*64)
            self.assertEqual(base64.b64decode(data['runtime-image']).decode(),'registry.example/rrt@sha256:'+'c'*64)

class HostPrerequisiteTests(unittest.TestCase):
    def test_missing_kernel_capabilities_fail_before_services_start(self):
        import tempfile
        from preflight import check
        def mount_probe(_image):
            return None
        with tempfile.TemporaryDirectory() as d:
            proc=Path(d)
            (proc/'filesystems').write_text('nodev\ttmpfs\n')
            with self.assertRaisesRegex(RuntimeError,'erofs, bridge_netfilter'):
                check(proc,mount_probe=mount_probe)
            (proc/'filesystems').write_text('\terofs\n')
            bridge=proc/'sys/net/bridge/bridge-nf-call-iptables'
            bridge.parent.mkdir(parents=True)
            bridge.write_text('0\n')
            with self.assertRaisesRegex(RuntimeError,'bridge_netfilter'):
                check(proc,mount_probe=mount_probe)
            bridge.write_text('1\n')
            self.assertEqual(check(proc,mount_probe=mount_probe),
                             {'erofs':True,'erofs_mount':True,'bridge_netfilter':True})

    def test_erofs_name_without_a_working_mount_fails_preflight(self):
        import tempfile
        from preflight import check
        with tempfile.TemporaryDirectory() as d:
            proc=Path(d)
            (proc/'filesystems').write_text('\terofs\n')
            bridge=proc/'sys/net/bridge/bridge-nf-call-iptables'
            bridge.parent.mkdir(parents=True)
            bridge.write_text('1\n')
            def unsupported(_image):
                raise OSError('mount returned operation not supported')
            with self.assertRaisesRegex(RuntimeError,'erofs_mount.*operation not supported'):
                check(proc,mount_probe=unsupported)

    def test_oci_runtime_does_not_require_erofs(self):
        import tempfile
        from preflight import check
        with tempfile.TemporaryDirectory() as d:
            proc=Path(d)
            (proc/'filesystems').write_text('nodev\ttmpfs\n')
            bridge=proc/'sys/net/bridge/bridge-nf-call-iptables'
            bridge.parent.mkdir(parents=True)
            bridge.write_text('1\n')
            self.assertEqual(check(proc,runtime_source='image'),
                             {'bridge_netfilter':True})
