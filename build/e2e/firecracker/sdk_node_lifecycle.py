#!/usr/bin/env python3
"""Public SDK cases plus explicitly scoped faults in an ADX FC fixture.

Run as the fixture owner in its Linux VM. Redis/backend reads are test oracles;
all ordinary create/execute/delete requests use the installed public SDK.
"""
import argparse
import json
import os
from pathlib import Path
import signal
import sqlite3
import subprocess
import time
import traceback
import urllib.request

p = argparse.ArgumentParser()
p.add_argument('--run-root', required=True, type=Path)
p.add_argument('--image', required=True)
p.add_argument('--endpoint', default='127.0.0.1:8443')
p.add_argument('--package', default='/opt/adx-pause/package', type=Path)
p.add_argument('--tools', default='/opt/adx-pause/tools', type=Path)
p.add_argument('--sbox', default='/opt/adx-fc/bin/sbox', type=Path)
a = p.parse_args()
root = a.run_root.resolve()
assert (root/'deployment.yaml').is_file(), 'explicit deployed fixture required'
config = json.loads((root/'deployment.yaml').read_text())
assert Path(config['package_dir']).resolve() == a.package.resolve()
os.environ['SSL_CERT_FILE'] = str(root/'secrets/tls/ca.pem')
from adx_sandbox import Sandbox, RestartPolicy, ConnectionConfig
connection = ConnectionConfig(server_address=a.endpoint, token=(root/'secrets/api-key').read_text().strip(), use_tls=True, verify_tls=True)
env = {**os.environ, 'REDISCLI_AUTH': (root/'secrets/redis-key').read_text().strip()}
out = root/'evidence/lifecycle'
out.mkdir(exist_ok=False)
result = {'status':'failed','cases':[]}
instances = []
stopped = set()
hidden_socket = root/'resource-hidden.sock'
resource_socket = root/'resource.sock'

def command(args):
    return subprocess.check_output(list(map(str,args)), text=True, env=env, timeout=30).strip()

def catalog():
    value = json.loads(command([a.tools/'redis-cli','--json','HGETALL','adx:{acceptance}:control:v1']))
    return {k:json.loads(v) for k,v in value.items() if k.startswith(('instance:','node:'))}

def record(id):
    return catalog().get('instance:'+id,{}).get('result')

def services():
    return json.loads(command([a.package/'bin/adxctl','status','--config',root/'deployment.yaml']))['services']

def pid(role):
    matches = [s['pid'] for s in services() if s['role'] == role]
    assert len(matches) == 1, (role, matches)
    return matches[0]

def wait(test, seconds=90):
    end = time.monotonic()+seconds
    last = None
    while time.monotonic() < end:
        try:
            value = test()
            if value: return value
        except Exception as error: last = repr(error)
        time.sleep(.5)
    raise TimeoutError(f'condition did not become true; last error={last}')

def passed(name, **details):
    print('PASS', name, json.dumps(details), flush=True)
    result['cases'].append({'name':name,'passed':True,**details})

def create(**kwargs):
    s = Sandbox(image=a.image,runtime='firecracker',cpu=1000,memory=512,connection=connection,create_timeout=180,**kwargs)
    instances.append(s)
    assert s.commands.run('printf lifecycle-ready').stdout.strip() == 'lifecycle-ready'
    return s

def physical(id):
    lines = command([a.sbox,'-a',root/'sandboxd/sandboxd.sock','list','--label','adx.instance_id='+id]).splitlines()
    return [line.split()[0] for line in lines[1:] if line.strip()]

try:
    s = create(idle_timeout=0,restart_policy=RestartPolicy(max_attempts=2,initial_backoff_seconds=1,max_backoff_seconds=4))
    first = record(s.id)
    old = physical(s.id)
    assert len(old) == 1, old
    command([a.sbox,'-a',root/'sandboxd/sandboxd.sock','delete',old[0]])
    restarted = wait(lambda: (r if (r:=record(s.id)) and r['state']=='Running' and r['runtime_id']!=first['runtime_id'] else None))
    assert restarted['restart_attempts'] == 1
    assert s.commands.run('printf restarted').stdout.strip() == 'restarted'
    passed('unexpected backend exit restarts with a fresh execution', instance_id=s.id, old_runtime=first['runtime_id'], new_runtime=restarted['runtime_id'])
    def sampled_stats():
        text = urllib.request.urlopen('http://127.0.0.1:17003/metrics',timeout=5).read().decode()
        return text if 'adx_instance_memory_usage_bytes' in text and s.id in text else None
    stats = wait(sampled_stats, 30)
    (out/'metrics.txt').write_text(stats)
    assert 'adx_node_reserved_cpu_millis' in stats
    s.kill()

    keep = create(idle_timeout=0)
    idle = create(idle_timeout=8)
    master_pid = pid('master')
    os.kill(master_pid,signal.SIGSTOP); stopped.add(master_pid)
    db_path = root/'degraded/results.sqlite'
    def journaled_delete():
        if not db_path.exists(): return False
        with sqlite3.connect('file:'+str(db_path)+'?mode=ro',uri=True,timeout=2) as db:
            return next((json.loads(row[0]) for row in db.execute('select payload from pending') if json.loads(row[0])['spec']['id']==idle.id and json.loads(row[0])['state']=='Deleted'),None)
    deleted = wait(journaled_delete,60)
    assert record(idle.id)['state']=='Running', 'Redis should still contain the pre-outage record'
    assert not physical(idle.id), 'idle backend was not cleaned locally'
    (out/'journaled-delete.json').write_text(json.dumps(deleted,indent=2))
    passed('Master outage uses SQLite for idle deletion while Redis remains stale', instance_id=idle.id)

    node_pid = pid('node-manager')
    kept_runtime = record(keep.id)['runtime_id']
    os.kill(node_pid,signal.SIGKILL)
    wait(lambda:pid('node-manager') != node_pid,30)
    time.sleep(4)
    assert len(physical(keep.id))==1
    assert record(keep.id)['runtime_id']==kept_runtime
    assert journaled_delete()
    passed('Node Manager restart waits for Master without cleaning an owned runtime')
    os.kill(master_pid,signal.SIGCONT); stopped.remove(master_pid)
    wait(lambda:record(idle.id)['state']=='Deleted',90)
    wait(lambda:catalog()['node:node1']['session']['routable'],90)
    def drained():
        with sqlite3.connect(db_path) as db:return db.execute('select count(*) from pending').fetchone()[0]==0
    wait(drained)
    assert keep.commands.run('printf recovered').stdout.strip()=='recovered'
    passed('Master recovery replays journal and reconciles the retained runtime')

    resource_socket.rename(hidden_socket)
    wait(lambda:not catalog()['node:node1']['node']['available'],30)
    assert len(physical(keep.id))==1
    hidden_socket.rename(resource_socket)
    wait(lambda:catalog()['node:node1']['node']['available'],30)
    passed('expired resource observations close admission and recover without killing instances')
    keep.kill()
    wait(lambda:all(v.get('result',{}).get('state')=='Deleted' for k,v in catalog().items() if k.startswith('instance:')))
    result['status']='passed'
except Exception as error:
    result['error']=f'{type(error).__name__}: {error}'
    traceback.print_exc()
finally:
    for stopped_pid in stopped:
        try:os.kill(stopped_pid,signal.SIGCONT)
        except ProcessLookupError:pass
    if hidden_socket.exists(): hidden_socket.rename(resource_socket)
    for s in instances:
        try:s.kill()
        except Exception as error:result.setdefault('cleanup_errors',[]).append(str(error))
        s.close()
    (out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result),flush=True)
raise SystemExit(0 if result['status']=='passed' and not result.get('cleanup_errors') else 1)
