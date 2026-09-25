#!/usr/bin/env python3
"""Node-side fixture operations; SDK business assertions live in scenarios.py."""
import json
import gzip
import os
from pathlib import Path
import shutil
import signal
import socket
import sqlite3
import subprocess
import sys
import time

P=Path('/tmp/adx-e2e');E=Path('/evidence');S=Path('/secrets');A=Path('/opt/adx');H=A/'e2e'

def command(args,timeout=180):return subprocess.check_output(list(map(str,args)),text=True,timeout=timeout)
def catalog():
    env={**os.environ,'REDISCLI_AUTH':(S/'redis-key').read_text().strip()}
    host=os.getenv('ADX_E2E_REDIS_HOST','coordinator')
    return json.loads(subprocess.check_output(['redis-cli','-h',host,'--json','HGETALL','adx:{acceptance}:control:v1'],env=env,text=True,timeout=5))
def persisted_runtime_id(result):
    return result['runtime']['id']
def nodes():
    c=catalog();return [json.loads(c['node:'+node]) for node in ('node1','node2')]
def backend():
    lines=command(['sbox','-a',P/'sandboxd/sandboxd.sock','list']).splitlines()
    return sorted(line.split()[0] for line in lines[1:] if line.strip())
def labeled_backend(instance_id):
    lines=command(['sbox','-a',P/'sandboxd/sandboxd.sock','list',
                   '--label','adx.environment_id='+instance_id]).splitlines()
    return sorted(line.split()[0] for line in lines[1:] if line.strip())
def journal_pending():
    path=P/'degraded/results.sqlite'
    if not path.is_file():return None
    with sqlite3.connect('file:'+str(path)+'?mode=ro',uri=True,timeout=2) as db:
        return [json.loads(payload) for (payload,) in db.execute('SELECT payload FROM pending ORDER BY sequence')]
def partition_rule(address):
    return ['-p','tcp','-d',address,'--dport','17000',
            '-m','comment','--comment','adx-e2e-network-partition','-j','DROP']
def heal_partition(marker, require_packets):
    address=marker.read_text().strip()
    rules=subprocess.check_output(['iptables-save','-c'],text=True,timeout=5)
    matching=[line for line in rules.splitlines()
              if 'adx-e2e-network-partition' in line and '--dport 17000' in line
              and address in line]
    assert len(matching)==1,matching
    counters=matching[0].split(']',1)[0].lstrip('[').split(':',1)
    packets=int(counters[0])
    subprocess.run(['iptables','-D','OUTPUT',*partition_rule(address)],check=True,timeout=5)
    marker.unlink()
    if require_packets:assert packets>0,'partition rule blocked no Coordinator packets'
    return {'coordinator_ip':address,'blocked_packets':packets}
def returning_node_status(records,current):
    failed=[]
    for key,value in records.items():
        if not key.startswith('environment:'):continue
        record=json.loads(value)
        if (record.get('assignment') or {}).get('node_id')=='node2' \
                and record.get('invalidated'):
            failed.append(record)
    assert len(failed)==1,failed
    node_record=json.loads(records['node:node2'])
    available=node_record['node']['available']
    if available and current:
        raise AssertionError('node reopened admission before stale backend cleanup')
    return failed[0]['spec']['id'],available,node_record['session']['routable']
def supervisor(action):return json.loads(command([A/'package/bin/adxctl',action,'--config',P/'deployment.yaml']))
def persisted_ownership(records,ids):
    ownership={}
    for sid in ids:
        record=json.loads(records['environment:'+sid])
        result=record['result']
        assert result['state']=='Running' and result['resources_held'],sid
        assignment=record['assignment']
        ownership[sid]={'node_id':assignment['node_id'],'generation':assignment['generation']}
    return ownership
def validated_coordinator_recovery(before,after,ids):
    old_epoch=json.loads(before['header'])['epoch']
    new_epoch=json.loads(after['header'])['epoch']
    assert new_epoch>old_epoch,(old_epoch,new_epoch)
    ownership=persisted_ownership(before,ids)
    assert persisted_ownership(after,ids)==ownership,'instance ownership changed across Coordinator restart'
    for node_id in ('node1','node2'):
        record=json.loads(after['node:'+node_id])
        assert record['node']['available'] and record['session']['routable'],node_id
    return {'epoch_before':old_epoch,'epoch_after':new_epoch,'ownership':ownership}
def validated_gateway_recovery(before,after,ids):
    old_epoch=json.loads(before['header'])['epoch']
    assert json.loads(after['header'])['epoch']==old_epoch,'Coordinator epoch changed during gateway restart'
    ownership=persisted_ownership(before,ids)
    assert persisted_ownership(after,ids)==ownership,'instance ownership changed during gateway restart'
    return {'coordinator_epoch':old_epoch,'ownership':ownership}
def redis_info():
    env={**os.environ,'REDISCLI_AUTH':(S/'redis-key').read_text().strip()}
    host=os.getenv('ADX_E2E_REDIS_HOST','coordinator')
    lines=subprocess.check_output(['redis-cli','-h',host,'--raw','INFO','persistence'],env=env,text=True,timeout=5).splitlines()
    return dict(line.split(':',1) for line in lines if ':' in line)
def stopped_with_pending_signal(status, signum):
    fields=dict(line.split(':',1) for line in status.splitlines() if ':' in line)
    state=fields.get('State','').strip()
    mask=1 << (signum-1)
    return state.startswith(('T ', 't ')) and any(
        int(fields.get(key,'0').strip(),16) & mask
        for key in ('SigPnd','ShdPnd')
    )
def process_status(pid):
    return (Path('/proc')/str(pid)/'status').read_text()
def process_start_ticks(pid):
    # Field 22 is stable across PID reuse; the command field may contain spaces.
    return int((Path('/proc')/str(pid)/'stat').read_text().rsplit(') ',1)[1].split()[19])
def stale_runtime_marker():
    return P/'frozen-stale-runtime.json'
def thaw_stale_runtime():
    marker=stale_runtime_marker()
    if not marker.exists():return
    runtime=json.loads(marker.read_text())
    try:
        if process_start_ticks(runtime['pid'])==runtime['start_ticks']:
            os.kill(runtime['pid'],signal.SIGCONT)
    except (FileNotFoundError,ProcessLookupError):pass
    marker.unlink()
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
        (P/'observer.pid').write_text(str(children[0].pid))
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
    elif action=='relay-separate':
        import urllib.error
        import urllib.request
        deployment=json.loads((P/'deployment.yaml').read_text())
        manager_config=next(s['config'] for s in deployment['services'] if s['role']=='adxlet')
        assert manager_config['proxy_mode']=='standalone',manager_config['proxy_mode']
        current=supervisor('status')['services']
        manager=[s for s in current if s['role']=='adxlet']
        relay=[s for s in current if s['role']=='relay']
        assert len(manager)==len(relay)==1 and manager[0]['pid'] and relay[0]['pid'],current
        assert manager[0]['pid']!=relay[0]['pid'],current
        binary=(Path('/proc')/str(relay[0]['pid'])/'exe').resolve()
        assert binary.name=='adx-relay',binary
        deadline=time.monotonic()+30
        while True:
            try:
                with urllib.request.urlopen('http://127.0.0.1:19443/readyz',timeout=2) as response:
                    if response.status==200:break
            except (OSError,urllib.error.URLError):pass
            if time.monotonic()>deadline:raise TimeoutError('standalone Relay did not become ready')
            time.sleep(.2)
        evidence={'node_id':node,'manager_pid':manager[0]['pid'],
                  'relay_pid':relay[0]['pid'],'relay_binary':str(binary)}
        (E/f'relay-separate-{node}.json').write_text(json.dumps(evidence,indent=2))
        print('PASS standalone Relay: '+node,flush=True)
    elif action=='coordinator-suspend':
        marker=P/'frozen-coordinator.pid'
        assert not marker.exists(),'Coordinator already suspended'
        services=[s for s in supervisor('status')['services'] if s['role']=='coordinator']
        assert len(services)==1 and services[0]['pid'],services
        pid=services[0]['pid'];marker.write_text(str(pid))
        os.kill(pid,signal.SIGSTOP)
        print('Coordinator suspended while managed Redis remains available',flush=True)
    elif action=='observer-freeze':
        marker=P/'frozen-observer.pid'
        assert not marker.exists(),'resource observer already suspended'
        pid=int((P/'observer.pid').read_text())
        os.kill(pid,0)
        marker.write_text(str(pid))
        os.kill(pid,signal.SIGSTOP)
        print('Resource observer suspended without stopping adxlet heartbeats',flush=True)
    elif action=='observer-resume':
        marker=P/'frozen-observer.pid'
        assert marker.is_file(),'resource observer suspension marker missing'
        os.kill(int(marker.read_text()),signal.SIGCONT)
        marker.unlink()
        print('Resource observer resumed',flush=True)
    elif action=='network-partition':
        assert node=='node2','network fault targets node2 only'
        marker=P/'network-partition.ip'
        assert not marker.exists(),'network partition already active'
        address=socket.gethostbyname('coordinator')
        subprocess.run(['iptables','-I','OUTPUT','1',*partition_rule(address)],
                       check=True,timeout=5)
        marker.write_text(address+'\n')
        print('Node2 Coordinator traffic blocked at the network boundary',flush=True)
    elif action=='network-heal':
        assert node=='node2','network recovery targets node2 only'
        marker=P/'network-partition.ip'
        assert marker.is_file(),'network partition marker missing'
        evidence=heal_partition(marker,require_packets=True)
        (E/'network-partition-rule.json').write_text(json.dumps(evidence,indent=2)+'\n')
        print('PASS Coordinator network partition had blocked packets and was removed',flush=True)
    elif action=='coordinator-resume':
        marker=P/'frozen-coordinator.pid'
        assert marker.is_file(),'Coordinator suspension marker missing'
        os.kill(int(marker.read_text()),signal.SIGCONT)
        marker.unlink()
        print('Coordinator resumed for journal reconciliation',flush=True)
    elif action=='sqlite-journaled':
        live=json.loads((E/'sqlite-live.json').read_text())
        started=time.monotonic()
        deadline=time.monotonic()+55
        last=None
        while True:
            try:
                pending=journal_pending()
                local=next((r for r in pending or [] if r['spec']['id']==live['idle_id']
                            and r['state']=='Deleted'),None)
                records=catalog()
                idle=json.loads(records['environment:'+live['idle_id']])['result']
                keep=json.loads(records['environment:'+live['keep_id']])['result']
                keep_backend=labeled_backend(live['keep_id'])
                idle_backend=labeled_backend(live['idle_id'])
                if local and idle['state']=='Running' and keep['state']=='Running' \
                        and len(keep_backend)==1 and not idle_backend:
                    evidence={'idle_id':live['idle_id'],'keep_id':live['keep_id'],
                              'journal_state':local['state'],'redis_idle_state':idle['state'],
                              'keep_runtime_id':persisted_runtime_id(keep),
                              'keep_backend':keep_backend[0],'idle_backend':idle_backend,
                              'pending_records':len(pending),
                              'seconds':round(time.monotonic()-started,3)}
                    (E/'sqlite-journaled.json').write_text(json.dumps(evidence,indent=2))
                    print('PASS local SQLite journaled idle deletion during Coordinator outage',flush=True)
                    break
                last={'pending':len(pending or []),'idle':idle['state'],'keep':keep['state'],
                      'keep_backend':keep_backend,'idle_backend':idle_backend}
            except (OSError,sqlite3.Error,KeyError,ValueError,subprocess.SubprocessError) as error:
                last=str(error)
            if time.monotonic()>deadline:raise TimeoutError(f'SQLite fallback missing: {last}')
            time.sleep(.5)
    elif action=='sqlite-reconciled':
        before=json.loads((E/'sqlite-journaled.json').read_text())
        started=time.monotonic()
        deadline=time.monotonic()+85
        last=None
        while True:
            try:
                pending=journal_pending()
                records=catalog()
                idle=json.loads(records['environment:'+before['idle_id']])['result']
                keep=json.loads(records['environment:'+before['keep_id']])['result']
                session=json.loads(records['node:node1'])['session']
                keep_backend=labeled_backend(before['keep_id'])
                idle_backend=labeled_backend(before['idle_id'])
                if pending==[] and idle['state']=='Deleted' and not idle['resources_held'] \
                        and keep['state']=='Running' and persisted_runtime_id(keep)==before['keep_runtime_id'] \
                        and keep_backend==[before['keep_backend']] and not idle_backend \
                        and session['routable']:
                    evidence={'idle_state':idle['state'],'idle_resources_held':False,
                              'keep_runtime_id':persisted_runtime_id(keep),'keep_backend':keep_backend,
                              'pending_records':0,'node_routable':True,
                              'seconds':round(time.monotonic()-started,3)}
                    (E/'sqlite-reconciled.json').write_text(json.dumps(evidence,indent=2))
                    print('PASS SQLite journal replay and retained backend after recovery',flush=True)
                    break
                last={'pending':None if pending is None else len(pending),'idle':idle['state'],
                      'keep':keep['state'],'session':session.get('routable'),
                      'keep_backend':keep_backend,'idle_backend':idle_backend}
            except (OSError,sqlite3.Error,KeyError,ValueError,subprocess.SubprocessError) as error:
                last=str(error)
            if time.monotonic()>deadline:raise TimeoutError(f'SQLite replay missing: {last}')
            time.sleep(.5)
    elif action in ('resource-stale','resource-fresh'):
        live=json.loads((E/'resource-live.json').read_text())
        expect_available=action=='resource-fresh'
        started=time.monotonic()
        deadline=started+30
        last=None
        while True:
            try:
                records=catalog()
                node1=json.loads(records['node:node1'])
                node2=json.loads(records['node:node2'])
                result=json.loads(records['environment:'+live['instance_id']])['result']
                backends=labeled_backend(live['instance_id'])
                if node1['node']['available']==expect_available \
                        and node1['session']['routable'] \
                        and node1['session']['id']==live['session_id'] \
                        and node2['node']['available'] \
                        and result['state']=='Running' \
                        and persisted_runtime_id(result)==live['runtime_id'] \
                        and backends==[live['backend']]:
                    evidence={'node1_available':expect_available,'node2_available':True,
                              'session_id':live['session_id'],'instance_id':live['instance_id'],
                              'backend':live['backend'],
                              'seconds':round(time.monotonic()-started,3)}
                    (E/f'{action}.json').write_text(json.dumps(evidence,indent=2))
                    print('PASS resource observation '+action,flush=True)
                    break
                last={'node1_available':node1['node']['available'],
                      'node1_routable':node1['session']['routable'],
                      'node2_available':node2['node']['available'],
                      'session_id':node1['session']['id'],
                      'runtime_id':persisted_runtime_id(result),'backends':backends}
            except (OSError,KeyError,ValueError,subprocess.SubprocessError) as error:
                last=str(error)
            if time.monotonic()>deadline:raise TimeoutError(f'{action} not observed: {last}')
            time.sleep(.5)
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
    elif action=='freeze-stale-runtime':
        assert node=='node2','stale runtime fault targets node2 only'
        marker=stale_runtime_marker()
        assert not marker.exists(),'stale runtime is already frozen'
        runtimes=backend();assert len(runtimes)==1,runtimes
        runtime_id=runtimes[0]
        state=json.loads(command(['runc','--root',P/'sandboxd/runc','state',runtime_id],timeout=5))
        assert state['id']==runtime_id and state['status']=='running',state
        pid=state['pid'];assert isinstance(pid,int) and pid>1,state
        marker.write_text(json.dumps({
            'runtime_id':runtime_id,'pid':pid,'start_ticks':process_start_ticks(pid),
        }))
        os.kill(pid,signal.SIGSTOP)
        deadline=time.monotonic()+5
        while True:
            status=process_status(pid)
            if status.split('State:',1)[1].strip().startswith(('T ', 't ')):break
            if time.monotonic()>deadline:raise TimeoutError('stale runc init did not stop')
            time.sleep(.1)
        print('Stale runc init suspended before Adxlet reconciliation',flush=True)
    elif action=='thaw-stale-runtime':
        thaw_stale_runtime()
    elif action=='reconcile-delete-blocked':
        assert node=='node2','interrupted reconciliation targets node2 only'
        runtime=json.loads(stale_runtime_marker().read_text())
        deadline=time.monotonic()+18
        last='waiting for sandboxd Delete to send SIGTERM'
        while True:
            try:
                records=catalog()
                runtimes=backend()
                failed_id,available,routable=returning_node_status(records,runtimes)
                record=json.loads(records['node:node2'])
                service=next(s for s in supervisor('status')['services'] if s['role']=='adxlet')
                assert process_start_ticks(runtime['pid'])==runtime['start_ticks'], \
                    'stale runc init PID was replaced before cleanup'
                status=process_status(runtime['pid'])
                failed=json.loads(records['environment:'+failed_id])
                if (service['pid'] and not routable and not available
                        and failed.get('invalidated') and failed['result']['state']=='Failed'
                        and runtime['runtime_id'] in runtimes
                        and stopped_with_pending_signal(status,signal.SIGTERM)):
                    evidence={'old_manager_pid':service['pid'],
                              'old_session_id':record['session']['id'],
                              'failed_id':failed_id,
                              'runtime_id':runtime['runtime_id'],
                              'runtime_pid':runtime['pid'],
                              'delete_signal_pending':True,
                              'admission_closed':not record['node']['available']}
                    assert evidence['admission_closed'],'node admitted while cleanup was blocked'
                    (E/'reconcile-delete-blocked.json').write_text(json.dumps(evidence,indent=2))
                    os.kill(service['pid'],signal.SIGKILL)
                    print('Adxlet killed while sandboxd Delete waited for stopped runc init',flush=True)
                    break
                last={'session':record['session'],'runtime_status':status.splitlines()[2:5]}
            except (OSError,KeyError,ValueError,subprocess.SubprocessError) as error:
                last=str(error)
            if time.monotonic()>deadline:raise TimeoutError(f'reconciliation did not enter physical cleanup: {last}')
            time.sleep(.1)
    elif action=='reconcile-recovered':
        assert node=='node2','reconciliation recovery targets node2 only'
        before=json.loads((E/'reconcile-delete-blocked.json').read_text())
        deadline=time.monotonic()+80
        last='new Adxlet has not reconciled the stale runtime'
        while True:
            try:
                service=next(s for s in supervisor('status')['services'] if s['role']=='adxlet')
                records=catalog()
                node_record=json.loads(records['node:node2'])
                session=node_record['session']
                failed=json.loads(records['environment:'+before['failed_id']])
                runtimes=backend()
                assert not node_record['node']['available'] or not runtimes, \
                    'node reopened admission before stale backend cleanup'
                if (service['pid'] and service['pid']!=before['old_manager_pid']
                        and session['id']!=before['old_session_id']
                        and node_record['node']['available'] and session['routable']
                        and not runtimes
                        and failed.get('invalidated') and failed['result']['state']=='Failed'
                        and not failed['result']['resources_held']):
                    (E/'reconcile-recovered.json').write_text(json.dumps({
                        **before,'new_manager_pid':service['pid'],
                        'new_session_id':session['id'],'stale_backend_empty':True,
                        'admission_reopened':True,
                    },indent=2))
                    print('PASS second Adxlet restart completed stale backend cleanup',flush=True)
                    break
                last={'manager_pid':service['pid'],'session':session,'backend':runtimes}
            except (OSError,KeyError,ValueError,subprocess.SubprocessError) as error:
                last=str(error)
            if time.monotonic()>deadline:raise TimeoutError(f'Adxlet did not finish interrupted reconciliation: {last}')
            time.sleep(.2)
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
                assert not labeled_backend(failed[0]['spec']['id']), \
                    'failed instance was recreated on the healthy node'
                (E/'node-failure-observed.json').write_text(json.dumps({'failed_id':failed[0]['spec']['id'],'healthy_id':healthy[0]['spec']['id'],'invalidated':True,'node_unavailable':True,'route_unpublished':True,'not_rescheduled':True},indent=2))
                print('PASS: heartbeat timeout invalidated old execution; healthy node unaffected',flush=True)
                break
            if time.monotonic()>end:raise TimeoutError('expired execution was not invalidated')
            time.sleep(.5)
    elif action=='network-recovery':
        assert node=='node2','network recovery proof targets node2'
        started=time.monotonic()
        deadline=started+85
        last=None
        while True:
            try:
                records=catalog()
                current=backend()
                failed_id,available,routable=returning_node_status(records,current)
                old_backend=labeled_backend(failed_id)
                if available and routable and not old_backend and not current:
                    evidence={'node_id':'node2','failed_id':failed_id,
                              'available_after_cleanup':True,'backend_empty':True,
                              'seconds':round(time.monotonic()-started,3)}
                    (E/'network-recovery.json').write_text(json.dumps(evidence,indent=2)+'\n')
                    print('PASS returning node cleaned old execution before admission',flush=True)
                    break
                last={'available':available,'routable':routable,
                      'old_backend':old_backend,'current':current}
            except (KeyError,ValueError,subprocess.SubprocessError) as error:
                last=str(error)
            if time.monotonic()>deadline:raise TimeoutError(f'network recovery missing: {last}')
            time.sleep(.2)
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
    elif action=='capture-backend':
        before=backend();assert len(before)==1
        (E/f'backend-before-{node}.json').write_text(json.dumps(before))
    elif action=='redis-pod-before':
        ids=json.loads((E/'live-instances.json').read_text())
        ownership=persisted_ownership(catalog(),ids)
        info=redis_info()
        assert info.get('aof_enabled','').strip()=='1','persistent Redis AOF is not enabled'
        (E/'redis-pod-before.json').write_text(json.dumps({
            'instance_ids':ids,'ownership':ownership,
            'aof_current_size':int(info['aof_current_size']),
        },indent=2))
    elif action=='redis-pod-after':
        before=json.loads((E/'redis-pod-before.json').read_text())
        deadline=time.monotonic()+30
        last_error='Redis AOF state has not loaded'
        while True:
            try:
                ownership=persisted_ownership(catalog(),before['instance_ids'])
                info=redis_info()
                assert info.get('aof_enabled','').strip()=='1','persistent Redis AOF is not enabled'
                assert ownership==before['ownership'],(before['ownership'],ownership)
                (E/'redis-pod-after.json').write_text(json.dumps({
                    'instance_ids':before['instance_ids'],
                    'ownership_before':before['ownership'],'ownership_after':ownership,
                    'aof_current_size':int(info['aof_current_size']),
                },indent=2))
                break
            except (OSError,subprocess.SubprocessError,KeyError,ValueError,AssertionError) as error:
                last_error=str(error)
            if time.monotonic()>deadline:raise TimeoutError('Redis Pod recovery did not preserve ownership: '+last_error)
            time.sleep(.2)
    elif action=='redis-restart':
        ids=json.loads((E/'live-instances.json').read_text())
        before=persisted_ownership(catalog(),ids)
        assert redis_info().get('aof_enabled','').strip()=='1','Redis AOF is not enabled'
        services=[s for s in supervisor('status')['services'] if s['role']=='redis']
        assert len(services)==1 and services[0]['pid'],'supervised Redis is unavailable'
        previous_pid=services[0]['pid']
        os.kill(previous_pid,signal.SIGKILL)
        deadline=time.monotonic()+60
        while True:
            try:
                services=[s for s in supervisor('status')['services'] if s['role']=='redis']
                current_pid=services[0]['pid']
                if current_pid and current_pid!=previous_pid:
                    after=persisted_ownership(catalog(),ids)
                    assert after==before,(before,after)
                    assert redis_info().get('aof_enabled','').strip()=='1'
                    (E/'redis-restart.json').write_text(json.dumps({
                        'previous_pid':previous_pid,'pid':current_pid,
                        'ownership_before':before,'ownership_after':after,
                        'aof_enabled':True,
                    },indent=2))
                    break
            except (OSError,subprocess.SubprocessError,IndexError,KeyError,ValueError):pass
            if time.monotonic()>deadline:raise TimeoutError('AOF Redis restart did not preserve instance ownership')
            time.sleep(.2)
    elif action=='coordinator-restart':
        ids=json.loads((E/'live-instances.json').read_text())
        before=catalog()
        persisted_ownership(before,ids)
        services=[s for s in supervisor('status')['services'] if s['role']=='coordinator']
        assert len(services)==1 and services[0]['pid'],'supervised Coordinator is unavailable'
        previous_pid=services[0]['pid']
        os.kill(previous_pid,signal.SIGKILL)
        deadline=time.monotonic()+60
        last_error='new Coordinator process not yet available'
        while True:
            try:
                services=[s for s in supervisor('status')['services'] if s['role']=='coordinator']
                current_pid=services[0]['pid']
                if current_pid and current_pid!=previous_pid:
                    evidence=validated_coordinator_recovery(before,catalog(),ids)
                    (E/'coordinator-restart.json').write_text(json.dumps({
                        'previous_pid':previous_pid,'pid':current_pid,**evidence,
                    },indent=2))
                    break
            except (OSError,subprocess.SubprocessError,IndexError,KeyError,ValueError,AssertionError) as error:
                last_error=str(error)
            if time.monotonic()>deadline:
                raise TimeoutError('Coordinator restart did not reconcile: '+last_error)
            time.sleep(.2)
    elif action=='gateway-restart':
        assert node in ('apiserver','ingress'),node
        import ssl
        import urllib.error
        import urllib.request
        ids=json.loads((E/'live-instances.json').read_text())
        before=catalog()
        persisted_ownership(before,ids)
        services=[s for s in supervisor('status')['services'] if s['role']==node]
        assert len(services)==1 and services[0]['pid'],node+' is unavailable'
        previous_pid=services[0]['pid']
        os.kill(previous_pid,signal.SIGKILL)
        context=ssl.create_default_context(cafile=str(S/'tls/ca.pem'))
        request=urllib.request.Request('https://127.0.0.1:8443/api/sandbox/v1/resources',headers={
            'Authorization':'Bearer '+(S/'api-key').read_text().strip(),
        })
        deadline=time.monotonic()+60
        last_error='new process not yet available'
        while True:
            try:
                services=[s for s in supervisor('status')['services'] if s['role']==node]
                current_pid=services[0]['pid']
                if current_pid and current_pid!=previous_pid:
                    evidence=validated_gateway_recovery(before,catalog(),ids)
                    with urllib.request.urlopen(request,context=context,timeout=2) as response:
                        assert response.status==200,response.status
                    (E/f'{node}-restart.json').write_text(json.dumps({
                        'previous_pid':previous_pid,'pid':current_pid,
                        'public_api_ready':True,**evidence,
                    },indent=2))
                    break
            except (OSError,subprocess.SubprocessError,IndexError,KeyError,ValueError,AssertionError,urllib.error.URLError) as error:
                last_error=str(error)
            if time.monotonic()>deadline:
                raise TimeoutError(node+' restart did not restore public API: '+last_error)
            time.sleep(.2)
    elif action=='restart-sandboxd':
        before=backend();assert len(before)==1
        previous_pid=int((P/'sandboxd.pid').read_text())
        marker=P/'restart-sandboxd-request'
        marker.touch()
        os.kill(previous_pid,signal.SIGKILL)
        deadline=time.monotonic()+65
        last_after=None
        while True:
            try:
                current_pid=int((P/'sandboxd.pid').read_text())
                if current_pid!=previous_pid:
                    after=backend()
                    last_after=after
                    if after==before:
                        (E/f'sandboxd-restart-{node}.json').write_text(json.dumps({
                            'previous_pid':previous_pid,'pid':current_pid,
                            'backend_ids_before':before,'backend_ids_after':after,
                        },indent=2))
                        break
            except (OSError,subprocess.SubprocessError):pass
            if time.monotonic()>deadline:
                raise TimeoutError(f'sandboxd backend IDs did not converge: before={before!r}, after={last_after!r}')
            time.sleep(.2)
    elif action=='unchanged':
        after=backend()
        (E/f'backend-after-{node}.json').write_text(json.dumps(after))
        assert after==json.loads((E/f'backend-before-{node}.json').read_text()), 'backend identity changed across Adxlet restart'
    elif action=='occupied':
        current=backend()
        assert current, f'{node} has no running backend before stop'
        (E/f'backend-occupied-{node}.json').write_text(json.dumps(current))
    elif action in ('stop','cleanup'):
        stop_errors=[]
        thaw_stale_runtime()
        partition=P/'network-partition.ip'
        if partition.exists():
            try:heal_partition(partition,require_packets=False)
            except (AssertionError,OSError,subprocess.SubprocessError):pass
        observer=P/'frozen-observer.pid'
        if observer.exists():
            try:os.kill(int(observer.read_text()),signal.SIGCONT)
            except ProcessLookupError:pass
            observer.unlink()
        marker=P/'frozen-coordinator.pid'
        if marker.exists():
            try:os.kill(int(marker.read_text()),signal.SIGCONT)
            except ProcessLookupError:pass
            marker.unlink()
        if action=='stop':
            try:subprocess.run(['python3',str(H/'telemetry.py'),'metrics',node],check=True)
            except (AssertionError,subprocess.CalledProcessError) as error:stop_errors.append(str(error))
        stopped=supervisor('stop');assert stopped['ok'];collect(node)
        if action=='stop':
            try:subprocess.run(['python3',str(H/'telemetry.py'),'validate',node],check=True)
            except (AssertionError,subprocess.CalledProcessError) as error:stop_errors.append(str(error))
            try:
                logs=list((P/'state/logs').glob('*.gz'));assert logs,'no compressed component logs'
                assert not list((P/'state/logs').glob('*.tmp')),'incomplete compression after stop'
                for path in logs:gzip.decompress(path.read_bytes())
                health=[s['logging'] for s in stopped['services']];assert all(h is not None and h['error'] is None and h['failed_bytes']==0 for h in health),health
                (E/f'logging-{node}.json').write_text(json.dumps({'status':'passed','gzip_files':len(logs),'no_temporary_files':True,'services':stopped['services']},indent=2))
                print(f'[LOGGING PASS] {node}: {len(logs)} gzip archives, all readable; no I/O loss or temporary residue',flush=True)
            except (AssertionError,EOFError,OSError) as error:stop_errors.append(str(error))
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
    if action=='stop' and stop_errors:raise AssertionError('; '.join(stop_errors))
    print(action,node,'passed',flush=True)
if __name__=='__main__':main()
