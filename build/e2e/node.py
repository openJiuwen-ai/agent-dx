#!/usr/bin/env python3
"""Node-side fixture operations; SDK business assertions live in scenarios.py."""
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import sys
import time

P=Path('/tmp/adx-e2e');E=Path('/evidence');S=Path('/secrets');A=Path('/opt/adx');H=A/'e2e'

def command(args,timeout=180):return subprocess.check_output(list(map(str,args)),text=True,timeout=timeout)
def catalog():
    env={**os.environ,'REDISCLI_AUTH':(S/'redis-key').read_text().strip()}
    return json.loads(subprocess.check_output(['redis-cli','--json','HGETALL','adx:{acceptance}:control:v1'],env=env,text=True,timeout=5))
def nodes():
    c=catalog();return [json.loads(c['node:'+node]) for node in ('node1','node2')]
def backend():
    lines=command(['sbox','-a',P/'sandboxd/sandboxd.sock','list']).splitlines()
    return sorted(line.split()[0] for line in lines[1:] if line.strip())
def supervisor(action):return json.loads(command([A/'package/bin/adxctl',action,'--config',P/'deployment.json']))
def collect(node):
    dest=E/f'logs-{node}';dest.mkdir(exist_ok=True)
    secrets=[p.read_bytes().strip() for p in S.glob('*key') if p.is_file()]
    for path in (P/'state/logs').glob('*.log'):
        data=path.read_bytes()
        for secret in secrets:
            if secret:data=data.replace(secret,b'REDACTED')
        (dest/path.name).write_bytes(data)

def main():
    action=sys.argv[1];node=sys.argv[2] if len(sys.argv)>2 else ''
    if action=='setup':subprocess.run(['python3',str(H/'configure.py'),node],check=True)
    elif action=='services':
        jobs=[['python3',str(H/'observe.py')],['sandboxd','--root',str(P/'sandboxd/root'),'--config',str(P/'sandboxd/config.toml'),'--socket',str(P/'sandboxd/sandboxd.sock'),'--http-address','127.0.0.1:18081','--pprof-address','127.0.0.1:16061','--log-file',str(E/f'sandboxd-{node}.log')]]
        if not os.getenv('ADX_E2E_KUBERNETES'):
            jobs += [['docker-registry','serve',str(H/'registry.yaml')]] if node=='node1' else [['python3',str(H/'registry-relay.py')]]
        children=[subprocess.Popen(c) for c in jobs]
        (P/'sandboxd.pid').write_text(str(children[1].pid))
        while True:
            if any(c.poll() is not None for c in children):raise RuntimeError('fixture service exited')
            time.sleep(1)
    elif action=='backend-ready':
        end=time.monotonic()+30
        while True:
            try:backend();break
            except (OSError,subprocess.SubprocessError):
                if time.monotonic()>end:raise TimeoutError('sandboxd not ready')
                time.sleep(.2)
    elif action=='ready':
        previous=json.loads((E/'previous-sessions.json').read_text()) if node=='restart' else {}
        end=time.monotonic()+100
        while True:
            try:
                current=nodes()
                if all(n['node']['available'] and n['session']['routable'] and n['session']['id']!=previous.get(n['node']['id']) for n in current):
                    (E/'ready-nodes.json').write_text(json.dumps(current,indent=2));break
            except (KeyError,ValueError,subprocess.SubprocessError):pass
            if time.monotonic()>end:raise TimeoutError('both nodes did not become ready')
            time.sleep(1)
    elif action=='postcheck':
        c=catalog();r=json.loads((E/'sdk/sdk-result.json').read_text());assert r['status']=='passed'
        records=[json.loads(c['instance:'+i]) for i in r['instances']]
        assert {r['assignment']['node_id'] for r in records}=={'node1','node2'}
        assert all(r['result']['state']=='Deleted' and not r['result']['resources_held'] for r in records)
        (E/'catalog-after-delete.json').write_text(json.dumps(records,indent=2))
    elif action=='sessions':
        (E/'previous-sessions.json').write_text(json.dumps({n['node']['id']:n['session']['id'] for n in nodes()}))
    elif action=='restart':
        before=backend();assert len(before)==1
        (E/f'backend-before-{node}.json').write_text(json.dumps(before))
        current=supervisor('status');manager=[s for s in current['services'] if s['role']=='node-manager'];assert len(manager)==1 and manager[0]['pid']
        os.kill(manager[0]['pid'],signal.SIGKILL)
    elif action=='unchanged':
        after=backend();assert after==json.loads((E/f'backend-before-{node}.json').read_text())
        (E/f'backend-after-{node}.json').write_text(json.dumps(after))
    elif action=='stop':
        assert supervisor('stop')['ok'];collect(node)
        # sandboxd is independent of the supervisor and still responds here.
        assert not backend()
        os.kill(int((P/'sandboxd.pid').read_text()),signal.SIGTERM)
    elif action=='empty':
        # Stop checks absence before terminating sandboxd; SDK checks query it live.
        if not (E/f'stop-{node}.json').exists():
            assert not backend()
        (E/f'backend-empty-{node}.json').write_text(json.dumps({'empty':True}))
    elif action=='collect':collect(node)
    else:raise ValueError('unknown action')
    if action=='stop':(E/f'stop-{node}.json').write_text(json.dumps({'ok':True,'backend_empty':True}))
    print(action,node,'passed',flush=True)
if __name__=='__main__':main()
