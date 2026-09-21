"""Strict FC evidence and isolated Kubernetes resource contracts."""
import json
import re
from pathlib import Path
import xml.etree.ElementTree as ET

CASES = {
 'sdk': {
  'create through Frontend and execute through Edge',
  'S3 rootfs and independent execution limits start through sandboxd',
  'S3 EROFS mount is visible inside the sandbox',
  'inherited image entrypoint reports structured exit status',
  'pause with persisted recovery point',
  'Node Manager restart while paused',
  'remote orphan GC preserves registered checkpoint',
  'SDK labels peer affinity and ordered weighted placement',
  'resume preserves process memory, PID and binary file',
  'reload restores the latest recovery point without a cold start',
  'runtime network policy replacement blocks egress and preserves RRT control',
  'creation network policy is enforced while the control route stays reachable',
  'explicit delete',
  'reusable snapshot preserves running source',
  'snapshot remains queryable after source deletion',
  'snapshot deletion accepted for artifact collection',
  'snapshot clones inherit geometry and preserve process memory',
  'snapshot clones have independent identity and writable files',
  'clones pause resume and delete after snapshot collection',
 },
 'lifecycle': {
  'unexpected backend exit restarts with a fresh execution',
  'Master outage uses SQLite for idle deletion while Redis remains stale',
  'Node Manager restart waits for Master without cleaning an owned runtime',
  'Master recovery fences an expired node session and reconciles stale runtimes',
  'expired resource observations close admission and recover without killing instances',
  'failover restores the latest checkpoint without a cold start',
  'failover without a checkpoint becomes failed without cold start',
 },
}

def verify(root):
 root=Path(root); records=[]
 summary=json.loads((root/'result.json').read_text())
 if summary.get('status')!='passed' or summary.get('stop_error'): raise ValueError('fixture or stop failed')
 for group,names in CASES.items():
  value=json.loads((root/group/'result.json').read_text()); cases=value.get('cases',[])
  if value.get('status')!='passed' or value.get('cleanup_error') or value.get('cleanup_errors'): raise ValueError(group+' failed cleanup or execution')
  if len(cases)!=len(names) or {c.get('name') for c in cases}!=names or any(c.get('passed') is not True for c in cases): raise ValueError(group+' required cases missing, duplicated or failed')
  records.extend(cases)
 collected=json.loads((root/'sdk/snapshot-collected-before-clone-resume.json').read_text())
 if not collected.get('snapshot_id') or collected.get('state')!='Deleted' or collected.get('references')!=[]: raise ValueError('source snapshot collection before clone resume is unproven')
 orphan=json.loads((root/'orphan-gc.json').read_text())
 if any(orphan.get(k) is not True for k in ('passed','current_session_preserved','retired_session_removed','foreign_preserved','unmarked_preserved')) or not orphan.get('registered_checkpoint_preserved'): raise ValueError('orphan GC preservation evidence missing')
 catalog=json.loads((root/'catalog-final.json').read_text())
 if not catalog or any(r.get('result',{}).get('state')!='Deleted' or r['result'].get('resources_held') is not False for r in catalog.values()): raise ValueError('Instance cleanup incomplete')
 snapshots=json.loads((root/'snapshots-final.json').read_text())
 if not snapshots or any(s.get('state')!='Deleted' or s.get('references') for s in snapshots.values()): raise ValueError('Snapshot cleanup incomplete')
 if ET.fromstring((root/'s3-final.xml').read_text()).findall('{*}Contents'): raise ValueError('S3 objects remain')
 if len((root/'inventory-final.txt').read_text().strip().splitlines())!=1: raise ValueError('backend instances remain')
 return records

def pod(namespace,image,architecture,node_name,registry_auth=False):
 if not re.fullmatch(r'adx-e2e-[a-z0-9-]{1,40}',namespace): raise ValueError('isolated namespace required')
 if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}',image): raise ValueError('immutable node image required')
 if architecture not in ('amd64','arm64') or not re.fullmatch(r'[a-z0-9][a-z0-9.-]{0,252}',node_name): raise ValueError('explicit architecture and KVM node required')
 volumes=[{'name':'state','emptyDir':{}},{'name':'kvm','hostPath':{'path':'/dev/kvm','type':'CharDevice'}}]
 mounts=[{'name':'state','mountPath':'/var/lib/adx-fc-test'},{'name':'kvm','mountPath':'/dev/kvm'}]
 spec={'restartPolicy':'Never','automountServiceAccountToken':False,'terminationGracePeriodSeconds':120,
  'nodeSelector':{'kubernetes.io/os':'linux','kubernetes.io/arch':architecture,'kubernetes.io/hostname':node_name},
  'volumes':volumes,'containers':[{'name':'platform','image':image,'command':['sleep','infinity'],
   'securityContext':{'privileged':True,'runAsUser':0},'resources':{'requests':{'cpu':'4','memory':'6Gi'},'limits':{'cpu':'4','memory':'6Gi'}},
   'env':[{'name':'ADX_E2E_KUBERNETES','value':'1'}],'volumeMounts':mounts}]}
 if registry_auth:
  spec['imagePullSecrets']=[{'name':'adx-test-registry'}]
  volumes.append({'name':'registry-auth','secret':{'secretName':'adx-test-registry'}})
  mounts.append({'name':'registry-auth','mountPath':'/registry-auth','readOnly':True})
 return {'apiVersion':'v1','kind':'Pod','metadata':{'name':'fc-node','namespace':namespace,'labels':{'adx.e2e.run':namespace}},'spec':spec}

def junit(path,records,error,cleanup_errors):
 suite=ET.Element('testsuite',name='adx-firecracker-kubernetes')
 passed={c['name'] for c in records}
 for group,names in CASES.items():
  for name in sorted(names):
   case=ET.SubElement(suite,'testcase',classname=group,name=name)
   if name not in passed: ET.SubElement(case,'failure').text=error or 'Missing successful evidence'
 case=ET.SubElement(suite,'testcase',name='namespace cleanup')
 if cleanup_errors: ET.SubElement(case,'failure').text='; '.join(cleanup_errors)
 if error and len(passed)==sum(map(len,CASES.values())): ET.SubElement(ET.SubElement(suite,'testcase',name='acceptance'),'failure').text=error
 suite.set('tests',str(len(suite)));suite.set('failures',str(len(suite.findall('testcase/failure'))))
 ET.ElementTree(suite).write(path,encoding='utf-8',xml_declaration=True)
