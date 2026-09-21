#!/usr/bin/env python3
import pathlib,sys,subprocess,json,os,signal,time
P=pathlib.Path(sys.argv[1]);INSTANCE_ID=sys.argv[2];BASE=pathlib.Path(os.environ.get('ADX_FC_BASE','/opt/adx'));B=BASE/'package/bin';E=P/'evidence'
env={**os.environ,'REDISCLI_AUTH':(P/'secrets/redis-key').read_text().strip()}
def catalog():return json.loads(subprocess.check_output([str(BASE/'tools/redis-cli'),'--json','HGETALL','adx:{acceptance}:control:v1'],env=env,text=True,timeout=5))
c=catalog();key='instance:'+INSTANCE_ID
assert key in c, 'paused Instance is absent from the authoritative catalog'
records={key:json.loads(c[key])}
assert records[key]['result']['state']=='Paused' and not records[key]['result']['resources_held']
(E/'catalog-paused.json').write_text(json.dumps(records,indent=2))
for r in records.values():
 artifact=r['result']['checkpoint']['artifact']
 assert artifact['storage']=='shared',artifact
 subprocess.run(['python3',str(pathlib.Path(os.environ.get('ADX_FC_BASE','/opt/adx'))/'e2e/firecracker/s3_probe.py'),str(P),'--artifact',artifact['location']],check=True)
 assert not [p for p in (P/'checkpoints').rglob('*') if p.is_file()], 'pause left local data; remote-only recovery was not established'
inventory=subprocess.check_output(['/opt/adx-fc/bin/sbox','-a',str(P/'sandboxd/sandboxd.sock'),'list'],text=True)
assert len(inventory.strip().splitlines())==1,inventory
(E/'inventory-paused.txt').write_text(inventory)
status=json.loads(subprocess.check_output([str(B/'adxctl'),'status','--config',str(P/'deployment.yaml')],text=True))
node=[s for s in status['services'] if s['role']=='node-manager'];assert len(node)==1
session=json.loads(c['node:node1'])['session']['id']
from orphan_fixture import OrphanFixture
orphan = OrphanFixture(P, artifact["location"], session)
os.kill(node[0]['pid'],signal.SIGKILL)
end=time.monotonic()+90
while time.monotonic()<end:
 c=catalog();n=json.loads(c['node:node1'])
 if n['session']['id']!=session and n['session']['routable'] and n['node']['available']:
  print('PASS Node Manager restarted and reconciled persisted Paused instance',flush=True);break
 time.sleep(.5)
else:raise TimeoutError('Node Manager did not reconcile after restart')

orphan.verify()
