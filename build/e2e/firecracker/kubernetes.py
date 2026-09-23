#!/usr/bin/env python3
"""Independent Kubernetes FC/S3 public SDK acceptance; requires a verified FC kit."""
import argparse,base64,importlib.util,json,os,signal,sys
from pathlib import Path
HERE=Path(__file__).resolve().parent
sys.path.insert(0,str(HERE))
from acceptance import pod,verify,junit
spec=importlib.util.spec_from_file_location('adx_kube_runner',HERE.parent/'kubernetes/run.py')
kube=importlib.util.module_from_spec(spec);spec.loader.exec_module(kube)

class FirecrackerRun(kube.KubernetesRun):
 def cleanup(self):
  errors=[]
  if not self.namespace_attempted:return errors
  try:
   raw=self.kube('get','namespace',self.id,'--ignore-not-found','-o','json',timeout=30)
   if not raw.strip():return errors
   meta=json.loads(raw)['metadata']
   if meta.get('labels',{}).get(kube.LABEL)!=self.id or (self.namespace_uid and meta['uid']!=self.namespace_uid): raise RuntimeError('namespace ownership changed; refusing cleanup')
   for args in [('get','pods','-o','wide'),('get','events','--sort-by=.lastTimestamp')]:
    try:self.kube('-n',self.id,*args,timeout=20)
    except Exception as error:errors.append(str(error))
   if self.nodes:
    try:
     self.execute('fc-node','python3','/opt/adx/e2e/firecracker/collect.py','/var/lib/adx-fc-test/run',timeout=30)
     self.kube('-n',self.id,'cp','fc-node:/var/lib/adx-fc-test/run/export/.',str(self.output/'node'),'-c','platform',timeout=90)
    except Exception as error:errors.append('diagnostic collection: '+str(error))
   self.kube('delete','namespace',self.id,'--wait=true','--timeout=180s',timeout=200)
   if self.kube('get','namespace',self.id,'--ignore-not-found','-o','name').strip():raise RuntimeError('namespace remains')
  except Exception as error:errors.append(str(error))
  return errors

def main():
 p=argparse.ArgumentParser()
 for arg in ('bundle','registry-images','kubeconfig','output'):p.add_argument('--'+arg,type=Path,required=True)
 p.add_argument('--context');p.add_argument('--node-name',required=True,help='hostname label of an explicitly selected KVM-capable worker')
 p.add_argument('--registry-auth',type=Path);p.add_argument('--proxy-mode',choices=['embedded','standalone'],default='embedded')
 a=p.parse_args();a.output.mkdir(parents=True,exist_ok=False);run=FirecrackerRun(a.output,a.kubeconfig,a.context);error=None;records=[]
 def cancel(signum,frame):raise InterruptedError('canceled by signal '+str(signum))
 for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,cancel)
 try:
  m,refs=kube.identity(a.bundle,a.registry_images,os.getenv('BUILDKITE_COMMIT'),bool(os.getenv('BUILDKITE')))
  if not m.get('firecracker_kit'):raise ValueError('node image bundle lacks a verified Firecracker kit')
  (a.output/'bundle.json').write_text(json.dumps(m,indent=2))
  (a.output/'registry-images.json').write_text(json.dumps(refs,indent=2))
  run.kube('version','-o','json',timeout=30)
  run.namespace_attempted=True
  run.apply({'apiVersion':'v1','kind':'Namespace','metadata':{'name':run.id,'labels':{kube.LABEL:run.id,'pod-security.kubernetes.io/enforce':'privileged'}}})
  run.namespace_uid=json.loads(run.kube('get','namespace',run.id,'-o','json'))['metadata']['uid']
  if a.registry_auth:
   auth=json.loads(a.registry_auth.read_text())
   if not isinstance(auth.get('auths'),dict):raise ValueError('invalid registry auth')
   run.apply({'apiVersion':'v1','kind':'Secret','type':'kubernetes.io/dockerconfigjson','metadata':{'name':'adx-test-registry','namespace':run.id},'data':{'.dockerconfigjson':base64.b64encode(a.registry_auth.read_bytes()).decode()}})
  obj=pod(run.id,refs['references']['node'],m['architecture'],a.node_name,bool(a.registry_auth))
  (a.output/'resources.json').write_text(json.dumps(obj,indent=2));run.apply(obj);run.nodes=['fc-node']
  run.kube('-n',run.id,'wait','pod/fc-node','--for=condition=Ready','--timeout=300s',timeout=320)
  actual=json.loads(run.kube('-n',run.id,'get','pod/fc-node','-o','json'))
  (a.output/'placement.json').write_text(json.dumps({'pod':'fc-node','host':actual['spec']['nodeName'],'ip':actual['status']['podIP'],'deployment':'kubernetes','proxy_mode':a.proxy_mode},indent=2))
  run.event('[RUN] FC/S3 public SDK checkpoint and fault cases')
  run.execute('fc-node','env','ADX_FC_PROXY_MODE='+a.proxy_mode,'ADX_E2E_EXECD_IMAGE='+refs['references']['execd'],
   'ADX_E2E_ENTRYPOINT_IMAGE='+refs['references']['entrypoint'],
   'ADX_EXPECTED_COMMIT='+m['package']['commit'],'python3','-u','/opt/adx/e2e/firecracker/node.py','/var/lib/adx-fc-test/run',timeout=1500)
 except Exception as e:error=str(e);run.event('[FAIL] '+error)
 finally:
  for sig in (signal.SIGTERM,signal.SIGINT):signal.signal(sig,signal.SIG_IGN)
  cleanup_errors=run.cleanup()
 try:records=verify(a.output/'node/evidence')
 except Exception as e:error=error or str(e)
 report={'status':'passed' if not error and not cleanup_errors else 'failed','deployment':'kubernetes','run_id':run.id,'cases':records,'error':error,'cleanup_errors':cleanup_errors}
 (a.output/'result.json').write_text(json.dumps(report,indent=2));junit(a.output/'junit.xml',records,error,cleanup_errors)
 run.event('[RESULT] '+report['status']+'; successful cases='+str(len(records)))
 return 0 if report['status']=='passed' else 1
if __name__=='__main__':raise SystemExit(main())
