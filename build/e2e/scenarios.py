#!/usr/bin/env python3
"""Business flows use only the installed public SDK, never source imports."""
import json
import os
from pathlib import Path
import subprocess
import sys
import time
E=Path('/evidence');S=Path('/secrets')
os.environ['SSL_CERT_FILE']=str(S/'tls/ca.pem')
from adx_sandbox import Sandbox,ConnectionConfig,SandboxError
connection=ConnectionConfig(server_address='127.0.0.1:8443',token=(S/'api-key').read_text().strip(),use_tls=True,verify_tls=True)
image=(S/'image').read_text().strip()
def event(message):print(message,flush=True)
event('[SCENARIO] '+sys.argv[1])
if sys.argv[1]=='l0':
    subprocess.run([sys.executable,'-u','/opt/adx/e2e/sdk_smoke.py','--endpoint','127.0.0.1:8443','--token-file',str(S/'api-key'),'--ca',str(S/'tls/ca.pem'),'--image',image,'--output',str(E/'l0')],check=True)
elif sys.argv[1]=='sdk':
    sdk_cases=[]
    if Path('/opt/adx/package/runtime/adx-runtime-rootfs.img').is_file():
        from runtime_profile import run
        runtime_result=E/'runtime-environment-result.json'
        run(connection,image,runtime_result)
        runtime_payload=json.loads(runtime_result.read_text())
        sdk_cases.extend({
            'id':'runtime-environment.'+check['mode'],
            'status':'passed',
            'seconds':0,
        } for check in runtime_payload['checks'])
    subprocess.run([sys.executable,'-u','/opt/adx/e2e/sdk_smoke.py','--endpoint','127.0.0.1:8443','--token-file',str(S/'api-key'),'--ca',str(S/'tls/ca.pem'),'--image',image,'--output',str(E/'sdk')],check=True)
    sdk_cases.extend(json.loads((E/'sdk/sdk-result.json').read_text())['cases'])
    print(json.dumps({'status':'passed','cases':sdk_cases}),flush=True)
elif sys.argv[1]=='data-plane':
    from functional_data_plane import run
    run(connection,image,E/'data-plane-result.json',S/'tls/ca.pem')
elif sys.argv[1]=='lifecycle':
    from functional_lifecycle import run
    run(connection,image,E/'lifecycle-result.json')
elif sys.argv[1]=='local-first':
    from local_first import run
    run(connection,image,E/'local-first-result.json')
elif sys.argv[1]=='placement':
    from placement import run
    placement_result=E/'placement-result.json'
    run(connection,image,placement_result)
    print(json.dumps(json.loads(placement_result.read_text())),flush=True)
elif sys.argv[1]=='runtime-affinity':
    from runtime_affinity import run
    result=E/'runtime-affinity-result.json'
    run(connection,image,result)
    print(json.dumps(json.loads(result.read_text())),flush=True)
elif sys.argv[1]=='idle-active':
    from idle_active import run
    result=E/'idle-active-result.json'
    run(connection,image,result)
    print(json.dumps(json.loads(result.read_text())),flush=True)
elif sys.argv[1]=='runtime-exit':
    from runtime_exit import run
    result=E/'runtime-exit-result.json'
    run(connection,image,result)
    print(json.dumps(json.loads(result.read_text())),flush=True)
elif sys.argv[1]=='sqlite-create':
    from sqlite_fallback import create
    create(connection,image,E/'sqlite-live.json')
    print('Created retained and idle instances before Coordinator suspension',flush=True)
elif sys.argv[1]=='sqlite-verify':
    from sqlite_fallback import verify
    report=verify(connection,E,E/'sqlite-fallback-result.json')
    print(json.dumps(report),flush=True)
elif sys.argv[1]=='auth':
    from adx_sandbox import PermissionDenied, SandboxError
    s=Sandbox(image=image,runtime='runc',cpu=500,memory=512,idle_timeout=0,connection=connection,create_timeout=150)
    try:
        event('Created instance for authentication checks: '+s.id)
        for credential,token in [('invalid API key','invalid-acceptance-key'),('other tenant',(S/'other-key').read_text().strip())]:
            event('Checking denied read/delete: '+credential)
            denied=ConnectionConfig(server_address='127.0.0.1:8443',token=token,use_tls=True,verify_tls=True)
            try:
                handle=Sandbox.from_id(s.id,connection=denied);handle.close()
            except PermissionDenied:pass
            except SandboxError as error:
                assert 'HTTP 401' in str(error) or 'HTTP 403' in str(error),str(error)
            else:raise AssertionError('unauthorized read accepted')
            try:Sandbox.delete(s.id,connection=denied)
            except SandboxError as error:
                assert '403' in str(error) or '401' in str(error),str(error)
            else:raise AssertionError('unauthorized delete accepted')
        assert s.is_running()
        event('Authentication checks passed; owner instance remains running')
        from credentials import check_management
        management=check_management(S,event)
        (E/'auth-result.json').write_text(json.dumps({'status':'passed','invalid_key':True,'tenant_read_isolation':True,'tenant_delete_isolation':True,'key_management':management}))
    finally:s.kill();s.close()
elif sys.argv[1]=='capacity':
    from metrics import check as check_metrics
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from node import catalog
    instances=[]
    def create():return Sandbox(image=image,runtime='runc',cpu=2000,memory=512,idle_timeout=0,connection=connection,create_timeout=150)
    try:
        event('Filling node1 and node2 resource capacity')
        instances=[create(),create()]
        event('Capacity holders: '+', '.join(s.id for s in instances))
        records=[json.loads(v) for k,v in catalog().items() if k.startswith('environment:')]
        held=[r for r in records if r.get('result') and r['result']['resources_held']]
        assert len(held)==2 and {r['assignment']['node_id'] for r in held}=={'node1','node2'}
        check_metrics('allocated',running=2,reserved=4000,pending=0)
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(create)
            try:
                extra=pending.result(timeout=2);instances.append(extra)
            except TimeoutError:event('PASS: third create is waiting while both nodes are full')
            else:raise AssertionError('overcommitted full nodes')
            check_metrics('queued',running=2,reserved=4000,pending=1)
            event('Releasing capacity by deleting '+instances[0].id)
            instances[0].kill()
            resumed=pending.result(timeout=120);instances.append(resumed)
            assert resumed.is_running()
            event('Pending create resumed: '+resumed.id)
            r=resumed.commands.run("printf 'capacity-released'")
            assert r.exit_code==0 and r.stdout=='capacity-released'
        (E/'capacity-result.json').write_text(json.dumps({'status':'passed','not_overcommitted':True,'pending_create_resumed':True}))
    finally:
        for s in instances:s.kill();s.close()
    check_metrics('released',running=0,reserved=0,pending=0)
elif sys.argv[1] in ('create','create-marker','create-stop'):
    instances=[]
    try:
        for index in range(2):
            # Fault cases require one actual backend on each node. Placement
            # policy itself is exercised independently by the placement group.
            options={'node_id':f'node{index+1}'}
            if sys.argv[1]=='create-stop':options['port_forwardings']=[18081]
            s=Sandbox(image=image,runtime='runc',cpu=500,memory=512,idle_timeout=0,connection=connection,create_timeout=150,**options)
            instances.append(s);assert s.is_running()
            if sys.argv[1]=='create-stop':
                from functional_data_plane import SERVER_COMMAND, EXPECTED_BODY, _fetch_forwarded
                s.commands.run(SERVER_COMMAND,background=True,command_id=f'stop-http-{index}')
                assert _fetch_forwarded(s,S/'tls/ca.pem')==EXPECTED_BODY
                # Keep s running for supervisor cleanup while an independent
                # deletion supplies the log/trace contract on each node.
                temporary=Sandbox(image=image,runtime='runc',node_id=f'node{index+1}',
                                  cpu=250,memory=256,idle_timeout=0,connection=connection,create_timeout=150)
                try:temporary.kill()
                finally:temporary.close()
            if sys.argv[1]=='create-marker':
                deadline=time.monotonic()+10
                retries=0
                while True:
                    try:
                        s.files.write('/tmp/adx-e2e-restart-marker',s.id)
                        break
                    except SandboxError as error:
                        if 'route point-get is temporarily unavailable' not in str(error) or time.monotonic()>deadline:raise
                        retries+=1
                        time.sleep(.2)
                event('Marker written after route synchronization; retries='+str(retries))
            event('Created live instance: '+s.id)
        (E/'live-instances.json').write_text(json.dumps([s.id for s in instances]))
    finally:
        for s in instances:s.close()
elif sys.argv[1]=='failure-cleanup':
    from node import catalog
    observed=json.loads((E/'node-failure-observed.json').read_text())
    healthy=Sandbox.from_id(observed['healthy_id'],connection=connection)
    try:
        result=healthy.commands.run('printf unaffected')
        assert result.exit_code==0 and result.stdout=='unaffected'
    finally:healthy.close()
    for sid in json.loads((E/'live-instances.json').read_text()):Sandbox.delete(sid,connection=connection)
    records=catalog()
    for sid in json.loads((E/'live-instances.json').read_text()):
        result=json.loads(records['environment:'+sid])['result']
        assert result['state']=='Deleted' and not result['resources_held']
    (E/'node-failure-result.json').write_text(json.dumps({'status':'passed',**observed,'reconnected_backend_empty':True,'cleanup_committed':True},indent=2))
    event('PASS: reconnected node cleaned old execution; healthy instance still executes; final deletion committed')
elif sys.argv[1] in ('cleanup-live','cleanup-live-redis','cleanup-live-control','cleanup-live-gateway'):
    from node import catalog
    ids=json.loads((E/'live-instances.json').read_text())
    for sid in ids:Sandbox.delete(sid,connection=connection)
    records=catalog()
    for sid in ids:
        result=json.loads(records['environment:'+sid])['result']
        assert result['state']=='Deleted' and not result['resources_held'],sid
    result_name={
        'cleanup-live':'sandboxd-restart-result.json',
        'cleanup-live-redis':'redis-restart-cleanup.json',
        'cleanup-live-control':'coordinator-restart-cleanup.json',
    }.get(sys.argv[1])
    if result_name is None:
        assert sys.argv[2] in ('apiserver','ingress')
        result_name=sys.argv[2]+'-restart-cleanup.json'
    (E/result_name).write_text(json.dumps({'status':'passed','instance_ids':ids,'resources_released':True},indent=2))
elif sys.argv[1] in ('recovered','recovered-marker'):
    checks=[]
    for sid in json.loads((E/'live-instances.json').read_text()):
        s=Sandbox.from_id(sid,connection=connection)
        try:
            assert s.is_running()
            r=s.commands.run("printf 'recovered-generated-id'")
            assert r.stdout=='recovered-generated-id' and r.exit_code==0
            if sys.argv[1]=='recovered-marker':assert s.files.read('/tmp/adx-e2e-restart-marker')==sid
            checks.append({'id':sid,'query':True,'command':True,'file':sys.argv[1]=='recovered-marker'})
            event('PASS: preserved instance '+sid+' is queryable and executes commands after restart')
        finally:s.close()
    (E/'restart-result.json').write_text(json.dumps({'status':'passed','checks':checks},indent=2))
else:raise ValueError('unknown scenario')

event('[SCENARIO COMPLETE] '+sys.argv[1])
