#!/usr/bin/env python3
"""Node-side fixture operations; SDK business assertions live in scenarios.py."""
import json
import gzip
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
def supervisor(action):return json.loads(command([A/'package/bin/adxctl',action,'--config',P/'deployment.yaml']))
def collect(node):
    dest=E/f'logs-{node}';dest.mkdir(exist_ok=True)
    secrets=[p.read_bytes().strip() for p in S.glob('*key') if p.is_file()]
    for path in (P/'state/logs').glob('*.log*'):
        if path.name.endswith('.tmp') or not path.is_file():continue
        try:data=gzip.decompress(path.read_bytes()) if path.suffix=='.gz' else path.read_bytes()
        except FileNotFoundError:continue
        for secret in secrets:
            if secret:data=data.replace(secret,b'REDACTED')
        (dest/path.name).write_bytes(gzip.compress(data) if path.suffix=='.gz' else data)

def main():
    action=sys.argv[1];node=sys.argv[2] if len(sys.argv)>2 else ''
    if action=='setup':
        subprocess.run(['python3',str(H/'configure.py'),node],check=True)
        subprocess.run(['python3',str(H/'telemetry.py'),'setup',node],check=True)
    elif action=='services':
        jobs=[['python3',str(H/'observe.py')],['sandboxd','--root',str(P/'sandboxd/root'),'--config',str(P/'sandboxd/config.toml'),'--socket',str(P/'sandboxd/sandboxd.sock'),'--http-address','127.0.0.1:18081','--pprof-address','127.0.0.1:16061','--log-file',str(E/f'sandboxd-{node}.log')]]
        if not os.getenv('ADX_E2E_KUBERNETES'):
            jobs += [['docker-registry','serve',str(H/'registry.yaml')]] if node=='node1' else [['python3',str(H/'registry-relay.py')]]
        if not os.getenv('ADX_E2E_KUBERNETES'):jobs.append(['python3',str(H/'telemetry.py'),'run'])
        children=[subprocess.Popen(c) for c in jobs]
        (P/'sandboxd.pid').write_text(str(children[1].pid))
        while True:
            for index, child in enumerate(children):
                if child.poll() is None:continue
                marker=P/'restart-sandboxd-request'
                if index==1 and marker.exists():
                    children[1]=subprocess.Popen(jobs[1])
                    (P/'sandboxd.pid').write_text(str(children[1].pid))
                    marker.unlink()
                    continue
                raise RuntimeError('fixture service exited')
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
        result_name=node or 'sdk'
        c=catalog();r=json.loads((E/result_name/'sdk-result.json').read_text());assert r['status']=='passed'
        records=[json.loads(c['environment:'+i]) for i in r['instances']]
        assignments={record['assignment']['node_id'] for record in records}
        assert assignments and assignments <= {'node1','node2'}
        if result_name=='sdk':assert assignments=={'node1','node2'}
        assert all(r['result']['state']=='Deleted' and not r['result']['resources_held'] for r in records)
        output='catalog-after-delete.json' if result_name=='sdk' else f'catalog-after-delete-{result_name}.json'
        (E/output).write_text(json.dumps(records,indent=2))
    elif action=='sessions':
        (E/'previous-sessions.json').write_text(json.dumps({n['node']['id']:n['session']['id'] for n in nodes()}))
    elif action=='freeze':
        current=supervisor('status');manager=next(s for s in current['services'] if s['role']=='adxlet')
        assert manager['pid'] and len(backend())==1
        (P/'frozen-manager.pid').write_text(str(manager['pid']))
        os.kill(manager['pid'],signal.SIGSTOP)
        print('Adxlet heartbeat suspended; runtime remains externally hosted',flush=True)
    elif action=='thaw':
        path=P/'frozen-manager.pid'
        if path.exists():
            pid=int(path.read_text());current=supervisor('status')
            assert any(s['role']=='adxlet' and s['pid']==pid for s in current['services'])
            os.kill(pid,signal.SIGCONT);path.unlink()
            print('Adxlet resumed; waiting for authoritative cleanup',flush=True)
    elif action=='failure-observed':
        end=time.monotonic()+65
        while True:
            c=catalog();live=json.loads((E/'live-instances.json').read_text())
            records=[json.loads(c['environment:'+sid]) for sid in live]
            failed=[r for r in records if r['assignment']['node_id']=='node2']
            healthy=[r for r in records if r['assignment']['node_id']=='node1']
            assert len(failed)==len(healthy)==1
            node_record=json.loads(c['node:node2'])
            if failed[0].get('invalidated'):
                assert failed[0]['result']['state']=='Failed' and not failed[0]['result']['resources_held']
                assert not node_record['node']['available'] and not node_record['session']['routable']
                assert healthy[0]['result']['state']=='Running'
                (E/'node-failure-observed.json').write_text(json.dumps({'failed_id':failed[0]['spec']['id'],'healthy_id':healthy[0]['spec']['id'],'invalidated':True,'node_unavailable':True,'route_unpublished':True},indent=2))
                print('PASS: heartbeat timeout invalidated old execution; healthy node unaffected',flush=True)
                break
            if time.monotonic()>end:raise TimeoutError('expired execution was not invalidated')
            time.sleep(.5)
    elif action=='create-mode':
        assert node in ('central','local_first')
        current=supervisor('status')
        api=next(s for s in current['services'] if s['role']=='apiserver')
        pid=api['pid'];assert pid
        args=Path(f'/proc/{pid}/cmdline').read_bytes().decode().split('\0')
        path=Path(args[args.index('--config')+1])
        assert path.resolve().is_relative_to((P/'state').resolve()), path
        config=json.loads(path.read_text());config['create_mode']=node
        temporary=path.with_suffix('.new');temporary.write_text(json.dumps(config));temporary.chmod(0o600)
        temporary.replace(path)
        os.kill(pid,signal.SIGTERM)
        end=time.monotonic()+30
        import ssl
        import urllib.error
        import urllib.request
        context=ssl.create_default_context(cafile=str(S/'tls/ca.pem'))
        request=urllib.request.Request('https://127.0.0.1:8443/api/sandbox/v1/resources',headers={
            'Authorization':'Bearer '+(S/'api-key').read_text().strip(),
        })
        while True:
            api=next(s for s in supervisor('status')['services'] if s['role']=='apiserver')
            if api['pid'] and api['pid']!=pid:
                try:
                    with urllib.request.urlopen(request,context=context,timeout=1) as response:
                        if response.status==200:break
                except (OSError,urllib.error.URLError):pass
            if time.monotonic()>end:raise TimeoutError('API Server mode switch did not become ready')
            time.sleep(.2)
        time.sleep(1.2) # Receive the fresh full node directory before creating.
        (E/f'create-mode-{node}.json').write_text(json.dumps({'mode':node,'previous_pid':pid,'pid':api['pid']}))
        print('PASS API Server mode: '+node,flush=True)
    elif action=='restart':
        before=backend();assert len(before)==1
        (E/f'backend-before-{node}.json').write_text(json.dumps(before))
        current=supervisor('status');manager=[s for s in current['services'] if s['role']=='adxlet'];assert len(manager)==1 and manager[0]['pid']
        os.kill(manager[0]['pid'],signal.SIGKILL)
    elif action=='restart-sandboxd':
        before=backend();assert len(before)==1
        previous_pid=int((P/'sandboxd.pid').read_text())
        marker=P/'restart-sandboxd-request'
        marker.touch()
        os.kill(previous_pid,signal.SIGTERM)
        deadline=time.monotonic()+65
        while True:
            try:
                current_pid=int((P/'sandboxd.pid').read_text())
                if current_pid!=previous_pid:
                    after=backend()
                    assert after==before,'backend identity changed across sandboxd restart'
                    (E/f'sandboxd-restart-{node}.json').write_text(json.dumps({
                        'previous_pid':previous_pid,'pid':current_pid,
                        'backend_ids_before':before,'backend_ids_after':after,
                    },indent=2))
                    break
            except (OSError,subprocess.SubprocessError):pass
            if time.monotonic()>deadline:raise TimeoutError('sandboxd did not restart with existing backend IDs')
            time.sleep(.2)
    elif action=='unchanged':
        after=backend()
        (E/f'backend-after-{node}.json').write_text(json.dumps(after))
        assert after==json.loads((E/f'backend-before-{node}.json').read_text()), 'backend identity changed across Adxlet restart'
    elif action in ('stop','cleanup'):
        if action=='stop':subprocess.run(['python3',str(H/'telemetry.py'),'metrics',node],check=True)
        stopped=supervisor('stop');assert stopped['ok'];collect(node)
        if action=='stop':
            subprocess.run(['python3',str(H/'telemetry.py'),'validate',node],check=True)
            logs=list((P/'state/logs').glob('*.gz'));assert logs,'no compressed component logs'
            assert not list((P/'state/logs').glob('*.tmp')),'incomplete compression after stop'
            for path in logs:gzip.decompress(path.read_bytes())
            health=[s['logging'] for s in stopped['services']];assert all(h is not None and h['error'] is None and h['failed_bytes']==0 for h in health),health
            (E/f'logging-{node}.json').write_text(json.dumps({'status':'passed','gzip_files':len(logs),'no_temporary_files':True,'services':stopped['services']},indent=2))
            print(f'[LOGGING PASS] {node}: {len(logs)} gzip archives, all readable; no I/O loss or temporary residue',flush=True)
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
    if action in ('stop','cleanup'):(E/f'stop-{node}.json').write_text(json.dumps({'ok':True,'backend_empty':True}))
    print(action,node,'passed',flush=True)
if __name__=='__main__':main()
