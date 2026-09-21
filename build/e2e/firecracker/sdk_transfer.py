#!/usr/bin/env python3
"""Real SDK checkpoint + heartbeat-fault transfer; run in the fixture host."""
import json, os, pathlib, signal, subprocess, sys, time
from transfer_contract import CASES, recovered
from transfer_config import ADDRESSES
root=pathlib.Path(sys.argv[1]); image=sys.argv[2]
base=pathlib.Path(os.environ['ADX_FC_BASE']); output=root/'evidence/sdk-transfer.json'
os.environ['SSL_CERT_FILE']=str(root/'secrets/tls/ca.pem')
from adx_sandbox import Sandbox, ConnectionConfig
connection=ConnectionConfig(server_address='127.0.0.1:8443',token=(root/'secrets/api-key').read_text().strip(),use_tls=True,verify_tls=True)
env={**os.environ,'REDISCLI_AUTH':(root/'secrets/redis-key').read_text().strip()}
result={'status':'failed','cases':[], 'interruption_requested':os.getenv('ADX_TRANSFER_INTERRUPT_MASTER')=='1', 'node_interruption_requested':os.getenv('ADX_TRANSFER_INTERRUPT_NODE')=='1'}; sandbox=None; stopped=None; blocked_rpc=None; blocked_rrt=None

def catalog():
    return {k:json.loads(v) for k,v in json.loads(subprocess.check_output([str(base/'tools/redis-cli'),'--json','HGETALL','adx:{acceptance}:control:v1'],env=env,text=True,timeout=10)).items()}
def wait(predicate,seconds=180):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        value=predicate()
        if value:return value
        time.sleep(1)
    raise TimeoutError('transfer condition timed out')
def command(text):
    r=sandbox.commands.run(text);assert r.exit_code==0,r
    return r.stdout.strip()
def passed(index,**details):
    item={'name':CASES[index],'passed':True,**details};result['cases'].append(item)
    print('PASS',json.dumps(item),flush=True)
def pid(folder,role):
    status=json.loads(subprocess.check_output([str(base/'package/bin/adxctl'),'status','--config',str(folder/'deployment.yaml')],text=True))
    return next(s['pid'] for s in status['services'] if s['role']==role)
def inventory(node):
    raw=subprocess.check_output(['/opt/adx-fc/bin/sbox','-a',str(root/node/'sandboxd/sandboxd.sock'),'list'],text=True,timeout=10)
    return raw.strip().splitlines()[1:]
try:
    sandbox=Sandbox(image=image,runtime='firecracker',cpu=1000,memory=512,idle_timeout=0,connection=connection,create_timeout=180)
    command("sh -c 'echo $$ >/tmp/counter.pid; n=0; while :; do n=$((n+1)); echo $n >/tmp/counter; sleep 0.1; done' >/tmp/counter.log 2>&1 </dev/null &")
    time.sleep(1)
    before=int(command('cat /tmp/counter')); saved_pid=command('cat /tmp/counter.pid')
    payload=b'cross-node-checkpoint\x00\xff'*4096
    sandbox.files.write('/tmp/transfer.bin',payload)
    paused=sandbox.pause(ttl_seconds=900,timeout_seconds=120)
    assert paused.size>0
    sandbox.resume()
    old=catalog()['capsule:'+sandbox.id]
    source=old['assignment']['node_id']; target='node2' if source=='node1' else 'node1'
    assert old['result']['checkpoint']['artifact']['storage']=='shared'
    passed(0,instance_id=sandbox.id,source=source,checkpoint=paused.snapshot_id)
    if result['interruption_requested']:
        blocked_rpc=['OUTPUT','-d',ADDRESSES[target],'-p','tcp','--dport','17001','-m','comment','--comment',root.name,'-j','DROP']
        subprocess.run(['iptables','-I',*blocked_rpc],check=True)
    if result['node_interruption_requested']:
        ns=json.loads((root/'evidence/topology.json').read_text())['namespaces'][target]
        blocked_rrt=['nsenter','--net=/var/run/netns/'+ns,'iptables']
        rrt_rule=['OUTPUT','-p','tcp','--dport','50090','-m','comment','--comment',root.name,'-j','DROP']
        subprocess.run([*blocked_rrt,'-I',*rrt_rule],check=True)
    stopped=pid(root/source,'node-manager'); os.kill(stopped,signal.SIGSTOP)
    (root/'evidence/source-stopped.json').write_text(json.dumps({'pid':stopped,'node_id':source}))
    if result['interruption_requested']:
        def pending():
            r=catalog()['capsule:'+sandbox.id]
            return r if r['assignment']['node_id']==target and r.get('recovery',{}).get('pending') else None
        planned=wait(pending)
        epoch=catalog()['header']['epoch']
        os.kill(pid(root,'master'),signal.SIGKILL)
        wait(lambda:catalog()['header']['epoch']>epoch)
        kept=catalog()['capsule:'+sandbox.id]
        assert kept['assignment']==planned['assignment'] and kept['recovery']['pending']
        result['mid_recovery_restart']={'epoch_before':epoch,'epoch_after':catalog()['header']['epoch'], 'assignment':kept['assignment'],'plan_preserved':True}
        (root/'evidence/mid-recovery-master-restart.json').write_text(json.dumps(result['mid_recovery_restart'],indent=2))
        subprocess.run(['iptables','-D',*blocked_rpc],check=True);blocked_rpc=None
        print('MASTER RESTARTED WITH DURABLE RECOVERY PLAN',flush=True)
    if result['node_interruption_requested']:
        def uncommitted_backend():
            r=catalog()['capsule:'+sandbox.id]
            backends=inventory(target)
            if (r['assignment']['node_id']==target and r.get('recovery',{}).get('pending')
                    and r['result']['state']=='Paused' and len(backends)==1 and 'SANDBOX_STATE_RUNNING' in backends[0]):
                return r,backends[0].split()[0]
            return None
        planned,physical=wait(uncommitted_backend)
        session=catalog()['node:'+target]['session']['id']
        result['node_recovery_restart']={'session_before':session,'backend_before':physical,
            'state_at_crash':planned['result']['state'],'pending_at_crash':planned['recovery']['pending']}
        (root/'evidence/node-recovery-before-crash.json').write_text(json.dumps({'record':planned,'backend':physical,'session':session},indent=2))
        os.kill(pid(root/target,'node-manager'),signal.SIGKILL)
        subprocess.run([*blocked_rrt,'-D',*rrt_rule],check=True);blocked_rrt=None
        wait(lambda:catalog()['node:'+target]['session']['id']!=session)
        print('TARGET NODE MANAGER RESTARTED WITH UNCOMMITTED BACKEND',physical,flush=True)
    new=wait(lambda: (lambda r:r if recovered(old,r) else None)(catalog()['capsule:'+sandbox.id]))
    if result['node_interruption_requested']:
        backends=inventory(target)
        assert len(backends)==1
        fault=result['node_recovery_restart']
        fault.update(session_after=catalog()['node:'+target]['session']['id'],backend_after=backends[0].split()[0],
            assignment_preserved=new['assignment']==planned['assignment'],
            old_backend_removed=all(fault['backend_before'] not in line for line in backends))
        assert fault['assignment_preserved'] and fault['old_backend_removed']
        (root/'evidence/node-recovery-restart.json').write_text(json.dumps(fault,indent=2))
    assert new['assignment']['node_id']==target
    passed(1,source=old['assignment'],target=new['assignment'])
    # Directory publication precedes Edge streaming delivery; wait for the view only.
    time.sleep(3)
    after=int(command('cat /tmp/counter'))
    assert after>before,(before,after)
    assert command('cat /tmp/counter.pid')==saved_pid
    assert sandbox.files.read('/tmp/transfer.bin',format='bytes')==payload
    passed(2,before=before,after=after,pid=saved_pid)
    os.kill(stopped,signal.SIGCONT);stopped=None
    wait(lambda: not inventory(source) and catalog()['node:'+source]['session']['routable'])
    assert len(inventory(target))==1
    subprocess.run(['python3',str(base/'e2e/firecracker/s3_probe.py'),str(root),'--artifact',new['result']['checkpoint']['artifact']['location']],env=env,check=True)
    passed(3,source_inventory=inventory(source),target_inventory=inventory(target))
    old_runtime=new['result']['runtime_id']; master_pid=pid(root,'master'); master_epoch=catalog()['header']['epoch']
    os.kill(master_pid,signal.SIGKILL)
    wait(lambda: catalog()['header']['epoch']>master_epoch and all(catalog()['node:'+n]['session']['routable'] for n in ('node1','node2')))
    same=catalog()['capsule:'+sandbox.id]
    assert same['assignment']==new['assignment'] and same['result']['runtime_id']==old_runtime
    assert command('cat /tmp/counter.pid')==saved_pid
    passed(4,generation=same['assignment']['generation'],runtime_id=old_runtime)
    sandbox.kill();sandbox.close();sandbox=None
    wait(lambda: not inventory('node1') and not inventory('node2'))
    records={k:v for k,v in catalog().items() if k.startswith('capsule:')}
    assert records and all(v['result']['state']=='Deleted' and not v['result']['resources_held'] for v in records.values())
    (root/'evidence/catalog-final.json').write_text(json.dumps(records,indent=2))
    passed(5)
    result['status']='passed'
except BaseException as error:
    result['error']=repr(error);raise
finally:
    if blocked_rrt:
        subprocess.run([*blocked_rrt,'-D',*rrt_rule],check=True)
    if blocked_rpc:
        subprocess.run(['iptables','-D',*blocked_rpc],check=True)
    if stopped:
        os.kill(stopped,signal.SIGCONT)
    if sandbox:
        try:sandbox.kill();sandbox.close()
        except Exception as error:result['delete_error']=repr(error)
    output.write_text(json.dumps(result,indent=2))
