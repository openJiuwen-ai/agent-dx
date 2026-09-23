#!/usr/bin/env python3
import json, os, pathlib, subprocess, sys, time, signal, shutil, socket
BASE=pathlib.Path(os.environ.get('ADX_FC_BASE', '/opt/adx')); RUN=pathlib.Path(sys.argv[1]); E=RUN/'evidence'
env={**os.environ,'PATH':f'/opt/adx-fc/bin:{BASE}/tools:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin','ADX_FC_RUN_ROOT':str(RUN)}
# Fail before deployment when the selected Pod/VM cannot run KVM.
import fcntl, platform
if platform.system() != 'Linux': raise RuntimeError('Linux KVM runtime required')
with open('/dev/kvm','rb',buffering=0) as kvm:
 if fcntl.ioctl(kvm.fileno(),0xAE00,0) != 12: raise RuntimeError('unsupported KVM API')
if os.environ.get('ADX_FC_PROXY_MODE','embedded') not in ('embedded','standalone'): raise ValueError('invalid proxy mode')
subprocess.run(['python3',str(BASE/'e2e/firecracker/configure.py'),'node1'],env=env,check=True)
env.update({'MINIO_ROOT_USER':(RUN/'secrets/s3-user').read_text().strip(),'MINIO_ROOT_PASSWORD':(RUN/'secrets/s3-key').read_text().strip()})
# Validate the package before launching any control-plane process.
subprocess.run(['python3',str(BASE/'e2e/package.py'),'verify',str(BASE/'package')],check=True)
manifest=json.loads((BASE/'package/manifest.json').read_text())
if os.environ.get('ADX_EXPECTED_COMMIT') and (manifest.get('dirty') or manifest.get('commit') != os.environ['ADX_EXPECTED_COMMIT']): raise ValueError('deployed package does not match CI commit')
children=[]
probe_namespace=None
def spawn(name,args):
 f=(E/(name+'.log')).open('w');p=subprocess.Popen(list(map(str,args)),env=env,stdout=f,stderr=subprocess.STDOUT);children.append((name,p));return p

def run(args):return subprocess.check_output(list(map(str,args)),env=env,text=True,timeout=30).strip()
def wait(test,seconds=120):
 end=time.monotonic()+seconds
 while True:
  for name,proc in children:
   if proc.poll() is not None:raise RuntimeError(name+' exited; see '+str(E/(name+'.log')))
  try:
   result=test()
   if result:return result
  except (OSError,subprocess.SubprocessError,KeyError,ValueError):pass
  if time.monotonic()>end:raise TimeoutError('readiness timeout')
  time.sleep(.5)

summary={'status':'failed','root':str(RUN)}
try:
 # Use a separate network namespace as the deterministic egress peer. Traffic
 # from a guest to a service in sandboxd's host namespace traverses INPUT and
 # is intentionally not equivalent to ordinary egress through the ACL hooks.
 suffix=str(os.getpid()%100000)
 probe_namespace='adx-probe-'+suffix
 host_veth='axph'+suffix
 peer_veth='axpn'+suffix
 subprocess.run(['ip','netns','add',probe_namespace],check=True)
 subprocess.run(['ip','link','add',host_veth,'type','veth','peer','name',peer_veth],check=True)
 subprocess.run(['ip','link','set',peer_veth,'netns',probe_namespace],check=True)
 subprocess.run(['ip','address','add','198.18.0.1/30','dev',host_veth],check=True)
 subprocess.run(['ip','link','set',host_veth,'up'],check=True)
 subprocess.run(['ip','netns','exec',probe_namespace,'ip','link','set','lo','up'],check=True)
 subprocess.run(['ip','netns','exec',probe_namespace,'ip','address','add','198.18.0.2/30','dev',peer_veth],check=True)
 subprocess.run(['ip','netns','exec',probe_namespace,'ip','link','set',peer_veth,'up'],check=True)
 subprocess.run(['ip','netns','exec',probe_namespace,'ip','route','add','default','via','198.18.0.1'],check=True)
 env['ADX_FC_EGRESS_PROBE_HOST']='198.18.0.2'
 env['ADX_FC_EGRESS_PROBE_PORT']='19092'
 spawn('egress-probe',['ip','netns','exec',probe_namespace,'python3','-m','http.server','19092','--bind','198.18.0.2'])
 wait(lambda:socket.create_connection(('198.18.0.2',19092),timeout=2).close() is None)
 (RUN/'objects').mkdir(parents=True)
 minio=spawn('minio',[BASE/'tools/minio','server',RUN/'objects','--address','0.0.0.0:19090','--console-address','127.0.0.1:19091'])
 import urllib.request
 wait(lambda:urllib.request.urlopen('http://127.0.0.1:19090/minio/health/ready',timeout=2).status==200)
 subprocess.run(['python3',str(BASE/'e2e/firecracker/s3_probe.py'),str(RUN),'--create'],env=env,check=True)
 image=os.environ.get('ADX_E2E_EXECD_IMAGE')
 entrypoint_image=os.environ.get('ADX_E2E_ENTRYPOINT_IMAGE')
 if not image:
  registry=spawn('registry',['docker-registry','serve',RUN/'registry.yaml'])
  sys.path.insert(0,str(BASE/'e2e'))
  import publish
  digest=publish.publish(str(BASE/'execd.tar'))
  image='127.0.0.1:5000/adx-execd@'+digest
  entrypoint_digest=publish.publish(str(BASE/'entrypoint.tar'),'adx-entrypoint')
  entrypoint_image='127.0.0.1:5000/adx-entrypoint@'+entrypoint_digest
 if not entrypoint_image: raise ValueError('entrypoint test image is required')
 (E/'execd-image.json').write_text(json.dumps({'image':image}))
 (E/'entrypoint-image.json').write_text(json.dumps({'image':entrypoint_image}))
 sandboxd=spawn('sandboxd',['sandboxd','--root',RUN/'sandboxd/root','--config',RUN/'sandboxd/config.toml','--socket',RUN/'sandboxd/sandboxd.sock','--http-address','127.0.0.1:18081','--pprof-address','127.0.0.1:16061','--log-file',E/'sandboxd-service.log'])
 wait(lambda:(RUN/'sandboxd/sandboxd.sock').exists())
 supervisor=spawn('supervisor',[BASE/'package/bin/adxctl','run','--config',RUN/'deployment.yaml'])
 authenv={**env,'REDISCLI_AUTH':(RUN/'secrets/redis-key').read_text().strip()}
 def catalog():return json.loads(subprocess.check_output(['redis-cli','--json','HGETALL','adx:{acceptance}:control:v1'],env=authenv,text=True))
 def ready():
  c=catalog();n=json.loads(c.get('node:node1','{}'));return n.get('session',{}).get('routable') and n.get('node',{}).get('available')
 wait(ready)
 (E/'ready.json').write_text(json.dumps({'ready':True}))
 (E/'egress-probe.json').write_text(json.dumps({'host':env['ADX_FC_EGRESS_PROBE_HOST'],'port':int(env['ADX_FC_EGRESS_PROBE_PORT']),'namespace':probe_namespace}))
 print('PLATFORM READY',RUN,flush=True)
 subprocess.run([str(BASE/'client/bin/python'),'-u',str(BASE/'e2e/firecracker/sdk_checkpoint.py'),'--endpoint','127.0.0.1:8443','--token-file',str(RUN/'secrets/api-key'),'--ca',str(RUN/'secrets/tls/ca.pem'),'--image',image,'--entrypoint-image',entrypoint_image,'--package',str(BASE/'package'),'--output',str(E/'sdk'),'--restart-command','python3',str(BASE/'e2e/firecracker/restart_paused.py'),str(RUN)],env=env,check=True,timeout=1500)
 subprocess.run([str(BASE/'client/bin/python'),'-u',str(BASE/'e2e/firecracker/sdk_node_lifecycle.py'),'--run-root',str(RUN),'--image',image,'--package',str(BASE/'package'),'--tools',str(BASE/'tools')],env=env,check=True,timeout=600)
 def snapshots_collected():
  raw=json.loads(subprocess.check_output(['redis-cli','--json','HGETALL','adx:{acceptance}:control:v1:snapshots'],env=authenv,text=True))
  snapshots={k:json.loads(v) for k,v in raw.items()}
  if snapshots and all(s['state']=='Deleted' and not s['references'] for s in snapshots.values()):
   (E/'snapshots-final.json').write_text(json.dumps(snapshots,indent=2)); return True
  return False
 wait(snapshots_collected)
 saved={k:json.loads(v) for k,v in catalog().items() if k.startswith('environment:')}
 (E/'catalog-final.json').write_text(json.dumps(saved,indent=2))
 assert saved and all(i['result']['state']=='Deleted' and not i['result']['resources_held'] for i in saved.values()),saved
 inventory=run(['sbox','-a',RUN/'sandboxd/sandboxd.sock','list'])
 assert len(inventory.splitlines())==1,inventory
 (E/'inventory-final.txt').write_text(inventory)
 assert not [p for p in (RUN/'checkpoints').rglob('*') if p.is_file()],'checkpoint artifacts leaked'
 subprocess.run(['python3',str(BASE/'e2e/firecracker/s3_probe.py'),str(RUN)],env=env,check=True)
 summary['status']='passed'
except BaseException as error:
 summary['error']=repr(error);raise
finally:
 try:
  if any(n=='supervisor' and p.poll() is None for n,p in children):
   print(run([BASE/'package/bin/adxctl','stop','--config',RUN/'deployment.yaml']),flush=True)
 except Exception as error:summary['stop_error']=repr(error);summary['status']='failed'
 for name,proc in reversed(children):
  if proc.poll() is None:
   proc.terminate()
   try:proc.wait(timeout=20)
   except subprocess.TimeoutExpired:proc.kill();proc.wait()
 if probe_namespace:
  subprocess.run(['ip','netns','delete',probe_namespace],check=False)
 (E/'result.json').write_text(json.dumps(summary,indent=2)+'\n')

if summary['status'] != 'passed': sys.exit(1)
