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
            self.assertEqual(spec['terminationGracePeriodSeconds'],15)
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
            self.assertEqual(terms,[
                {'matchFields':[{'key':'metadata.name','operator':'In','values':[name]}]}
                for name in ('worker-a','worker-b')])
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
        self.assertEqual(set(services),{'coordinator','node2'})
        ports={p['port'] for p in services['coordinator']['spec']['ports']}
        self.assertTrue({6379,17000,17001,18443,8443}.issubset(ports))
    def test_namespace_and_image_must_be_explicit(self):
        for namespace,image in [('default','registry.example/node@sha256:'+'a'*64),('adx-e2e-test','node:latest')]:
            with self.assertRaises(ValueError):k8s.resources(namespace,image,'amd64',False)

    def test_persistent_redis_uses_dedicated_pod_and_pvc(self):
        objects=k8s.resources('adx-e2e-test','registry.example/node@sha256:'+'a'*64,
                              'amd64',redis_storage_class='fast-rwo')
        by_kind_name={(o['kind'],o['metadata']['name']):o for o in objects}
        claim=by_kind_name['PersistentVolumeClaim','redis-data']
        self.assertEqual(claim['spec']['storageClassName'],'fast-rwo')
        self.assertEqual(claim['spec']['accessModes'],['ReadWriteOnce'])
        redis=by_kind_name['Pod','redis']
        self.assertEqual(redis['spec']['volumes'][0]['persistentVolumeClaim']['claimName'],'redis-data')
        self.assertEqual(redis['spec']['containers'][0]['command'],['/opt/adx/package/bin/redis-server'])
        self.assertIn('/secrets/redis.acl',redis['spec']['containers'][0]['args'])
        self.assertEqual(by_kind_name['Service','redis']['spec']['selector'],redis['metadata']['labels'])
        nodes=[by_kind_name['Pod',name] for name in ('node1','node2')]
        for pod in nodes:
            env={entry['name']:entry['value'] for entry in pod['spec']['containers'][0]['env']
                 if 'value' in entry}
            self.assertEqual(env['ADX_E2E_REDIS_HOST'],'redis')
            self.assertNotIn('redis-data',{v['name'] for v in pod['spec']['volumes']})

    def test_persistent_redis_requires_valid_storage_class(self):
        with self.assertRaisesRegex(ValueError,'storage class'):
            k8s.resources('adx-e2e-test','registry.example/node@sha256:'+'a'*64,
                          'amd64',redis_storage_class='not/valid')

    def test_default_storage_class_requires_one_dynamic_default(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('k8s_run',ROOT/'kubernetes/run.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        dynamic={'metadata':{'name':'csi-default','annotations':{
            'storageclass.kubernetes.io/is-default-class':'true'}},
            'provisioner':'csi.example.test'}
        self.assertEqual(module.default_redis_storage_class({'items':[dynamic]}),
                         'csi-default')
        with self.assertRaisesRegex(ValueError,'default dynamic StorageClass'):
            module.default_redis_storage_class({'items':[]})
        with self.assertRaisesRegex(ValueError,'default dynamic StorageClass'):
            module.default_redis_storage_class({'items':[dynamic,dynamic]})
        static={**dynamic,'provisioner':'kubernetes.io/no-provisioner'}
        with self.assertRaisesRegex(ValueError,'default dynamic StorageClass'):
            module.default_redis_storage_class({'items':[static]})

    def test_storage_class_inventory_records_only_selection_fields(self):
        import importlib.util
        spec=importlib.util.spec_from_file_location('k8s_run',ROOT/'kubernetes/run.py')
        module=importlib.util.module_from_spec(spec);spec.loader.exec_module(module)
        inventory=module.storage_class_inventory({'items':[
            {'metadata':{'name':'disk-b','annotations':{'secret.example/token':'private'}},
             'provisioner':'csi.example/disk','volumeBindingMode':'WaitForFirstConsumer',
             'reclaimPolicy':'Delete'},
            {'metadata':{'name':'local-a'},'provisioner':'kubernetes.io/no-provisioner'}]})
        self.assertEqual(inventory,[
            {'name':'disk-b','provisioner':'csi.example/disk',
             'volume_binding_mode':'WaitForFirstConsumer','reclaim_policy':'Delete',
             'default':False,'dynamic':True},
            {'name':'local-a','provisioner':'kubernetes.io/no-provisioner',
             'volume_binding_mode':None,'reclaim_policy':None,
             'default':False,'dynamic':False}])
        self.assertNotIn('private',str(inventory))

class KubernetesLifecycleTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec=importlib.util.spec_from_file_location('k8s_run',ROOT/'kubernetes/run.py')
        cls.module=importlib.util.module_from_spec(spec);spec.loader.exec_module(cls.module)

    def test_ingress_restart_selects_independent_process_at_setup(self):
        runner=(ROOT/'kubernetes/run.py').read_text()
        self.assertIn("*common.setup_environment(self.selected_case)",runner)
        self.assertIn("self.selected_case = selected_case",runner)

    def test_redis_pod_restart_recreates_only_redis_and_retains_claim(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'),
                                          profile='full',selected_case='redis-pod-restart')
            run.redis_pod_manifest={'kind':'Pod','metadata':{'name':'redis','namespace':run.id}}
            events=[]
            def kube(*args,**kwargs):
                events.append(args)
                if args[-4:]==('get','pod','redis','-o'):
                    raise AssertionError('unexpected query shape')
                if args[2:5]==('get','pod','redis'):
                    count=sum(call[2:5]==('get','pod','redis') for call in events)
                    return json.dumps({'metadata':{'uid':'old' if count==1 else 'new'}})
                if args[2:5]==('get','pvc','redis-data'):
                    return json.dumps({'metadata':{'uid':'same-pvc'}})
                return ''
            run.kube=kube
            run.apply=lambda obj:events.append(('apply',obj['kind'],obj['metadata']['name']))
            result=run.restart_redis_pod()
            self.assertEqual(result,{'pod_before':'old','pod_after':'new','pvc_uid':'same-pvc'})
            self.assertIn(('apply','Pod','redis'),events)
            self.assertTrue(any(call[2:5]==('delete','pod','redis') for call in events
                                if len(call)>=5 and call[0]!='apply'))
            self.assertFalse(any(call[2:5]==('delete','pod','node1') for call in events
                                 if len(call)>=5 and call[0]!='apply'))

    def test_redis_pod_case_checks_live_backend_and_public_sdk_after_replacement(self):
        import tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'),
                                          profile='full',selected_case='redis-pod-restart')
            run.nodes=['node1','node2']
            events=[]
            def execute(node,*args,**kwargs):
                events.append(('execute',node,args[-1]))
                if args[-1]=='recovered-marker':
                    return '{"cases":[{"id":"sdk-route-restored","status":"passed"}]}'
                return ''
            run.execute=execute
            run.helper=lambda node,*args,**kwargs:events.append(('helper',node,args[0]))
            run.restart_redis_pod=lambda:events.append(('replace','redis'))
            checks=[]
            run.scenarios(checks,('redis-pod-restart',))
            self.assertEqual(checks,['redis-pod-restart'])
            self.assertEqual(run.case_results[0]['subcases'][0]['id'],'sdk-route-restored')
            self.assertLess(events.index(('helper','node1','redis-pod-before')),
                            events.index(('replace','redis')))
            self.assertLess(events.index(('replace','redis')),
                            events.index(('helper','node1','redis-pod-after')))
            self.assertIn(('helper','node2','unchanged'),events)
            self.assertIn(('execute','node1','recovered-marker'),events)
            self.assertIn(('execute','node1','cleanup-live-redis'),events)

    def test_redis_pod_case_is_kubernetes_full_only(self):
        self.assertEqual(self.module.selected_checks('full','redis-pod-restart'),
                         ('redis-pod-restart',))
        with self.assertRaisesRegex(ValueError,'full Kubernetes'):
            self.module.selected_checks('k8s-basic','redis-pod-restart')

    def test_sdk_result_is_read_from_pod_before_evidence_copy(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'))
            run.nodes=['node1','node2']
            calls=[]
            def execute(node,*args,**kwargs):
                calls.append((node,args))
                if args[-1]=='/evidence/sdk/sdk-result.json':
                    return json.dumps({'instances':['capsule-1']})
                if '/opt/adx/e2e/runtime_logs.py' in args:
                    return json.dumps({'runtime_ids':['capsule-1-backend']})
                if 'sdk' in args:
                    return json.dumps({'cases':[{'id':'sandbox.create-query-two-nodes','status':'passed','seconds':0}]})
                return ''
            run.execute=execute
            run.helper=lambda *args,**kwargs: ''
            checks=[]
            run.scenarios(checks,('sdk',))
            self.assertEqual(checks,['sdk'])
            self.assertIn(('node1',('cat','/evidence/sdk/sdk-result.json')),calls)
            self.assertFalse((Path(d)/'sdk/sdk-result.json').exists())

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
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'),profile='full')
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

    def test_targeted_restart_uses_basic_cleanup_without_stop_evidence(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'),
                                          profile='full',selected_case='restart')
            run.namespace_attempted=True;run.namespace_uid='owned';run.nodes=['node1']
            calls=[];deleted=False
            def kube(*args,**kwargs):
                nonlocal deleted
                if args[:2]==('get','namespace'):
                    return '' if deleted else json.dumps({'metadata':{'uid':'owned','labels':{'adx.e2e.run':run.id}}})
                if args[:2]==('delete','namespace'):deleted=True
                return ''
            run.kube=kube
            run.execute=lambda *args,**kwargs:calls.append(args)
            run.helper=lambda *args,**kwargs:None
            self.assertEqual(run.cleanup(),[])
            self.assertTrue(deleted)
            self.assertIn('node.py cleanup node1',calls[0][-1])

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

    def test_l0_cleanup_selects_basic_stop_and_keeps_namespace_removal(self):
        import json,tempfile
        with tempfile.TemporaryDirectory() as d:
            run=self.module.KubernetesRun(Path(d),Path('/fixture/kubeconfig'),profile='l0')
            run.namespace_attempted=True;run.namespace_uid='owned';run.nodes=['node1']
            calls=[];deleted=False
            def kube(*args,**kwargs):
                nonlocal deleted
                if args[:2]==('get','namespace'):
                    return '' if deleted else json.dumps({'metadata':{'uid':'owned','labels':{'adx.e2e.run':run.id}}})
                if args[:2]==('delete','namespace'):deleted=True
                return ''
            run.kube=kube
            run.execute=lambda *a,**k:calls.append(a)
            run.helper=lambda *a,**k:None
            self.assertEqual(run.cleanup(),[])
            self.assertTrue(deleted)
            self.assertIn('node.py cleanup node1', calls[0][-1])

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
            image_ids={'node':'node-id','execd':'execd-id','entrypoint':'entrypoint-id'}
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
        pipeline=(repository/'.buildkite/pipeline-full.yml').read_text()
        summary=(repository/'.buildkite/summary.py').read_text()
        self.assertIn('ADX_E2E_ARTIFACT_BUILD',script)
        self.assertIn('ADX_E2E_ARTIFACT_COMMIT',script)
        self.assertIn('--build "$artifact_build"',script)
        self.assertIn('build.env("ADX_E2E_ARTIFACT_BUILD") == null',pipeline)
        self.assertIn("commit = os.environ['BUILDKITE_COMMIT']",summary)
        self.assertIn("harness.get('product_commit')",summary)

    def test_targeted_case_uses_fresh_or_reused_images_and_is_reported_separately(self):
        script=(ROOT.parents[1]/'.buildkite/run-e2e.sh').read_text()
        runner=(ROOT/'kubernetes/run.py').read_text()
        self.assertIn('ADX_E2E_TARGET_CASE',script)
        self.assertIn('download_image_artifact',script)
        self.assertIn('args+=(--case "$ADX_E2E_TARGET_CASE")',script)
        self.assertIn("profile='targeted' if a.case else a.profile",runner)
        self.assertIn('selected_case=a.case',runner)

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
                                         'registry.example/execd@sha256:'+'c'*64)
            objects=k8s.resources('adx-e2e-test','registry.example/node@sha256:'+'a'*64,'amd64')
            pod=next(o for o in objects if o['kind']=='Pod')
            secret=next(v['secret'] for v in pod['spec']['volumes'] if v['name']=='credentials')
            self.assertTrue(all(item['key'] in data for item in secret['items']))
            self.assertIn({'key':'admin-key','path':'admin-key'}, secret['items'])
            self.assertNotIn('ca.key',data)
            self.assertNotEqual(data['admin-key'],data['api-key'])
            self.assertNotIn('ca.key',data)
            self.assertEqual(base64.b64decode(data['image']).decode(),'registry.example/user@sha256:'+'b'*64)
            self.assertEqual(base64.b64decode(data['runtime-image']).decode(),'registry.example/execd@sha256:'+'c'*64)
            password=base64.b64decode(data['redis-key']).decode()
            self.assertEqual(base64.b64decode(data['redis-acl']).decode(),
                             f'user default on >{password} ~* &* +@all\n')

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
