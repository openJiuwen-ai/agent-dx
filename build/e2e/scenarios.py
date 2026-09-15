#!/usr/bin/env python3
"""Business flows use only the installed public SDK, never source imports."""
import json
import os
from pathlib import Path
import subprocess
import sys
E=Path('/evidence');S=Path('/secrets')
os.environ['SSL_CERT_FILE']=str(S/'tls/ca.pem')
from adx_sandbox import Sandbox,ConnectionConfig
connection=ConnectionConfig(server_address='127.0.0.1:8443',token=(S/'api-key').read_text().strip(),use_tls=True,verify_tls=True)
image=(S/'image').read_text().strip()
if sys.argv[1]=='sdk':
    subprocess.run([sys.executable,'/opt/adx/e2e/sdk_smoke.py','--endpoint','127.0.0.1:8443','--token-file',str(S/'api-key'),'--ca',str(S/'tls/ca.pem'),'--image',image,'--output',str(E/'sdk')],check=True)
elif sys.argv[1]=='auth':
    from adx_sandbox import PermissionDenied, SandboxError
    s=Sandbox(image=image,runtime='runc',cpu=500,memory=512,idle_timeout=0,connection=connection,create_timeout=150)
    try:
        for token in ['invalid-acceptance-key',(S/'other-key').read_text().strip()]:
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
        (E/'auth-result.json').write_text(json.dumps({'status':'passed','invalid_key':True,'tenant_read_isolation':True,'tenant_delete_isolation':True}))
    finally:s.kill();s.close()
elif sys.argv[1]=='capacity':
    from concurrent.futures import ThreadPoolExecutor, TimeoutError
    from node import catalog
    instances=[]
    def create():return Sandbox(image=image,runtime='runc',cpu=2000,memory=512,idle_timeout=0,connection=connection,create_timeout=150)
    try:
        instances=[create(),create()]
        records=[json.loads(v) for k,v in catalog().items() if k.startswith('instance:')]
        held=[r for r in records if r.get('result') and r['result']['resources_held']]
        assert len(held)==2 and {r['assignment']['node_id'] for r in held}=={'node1','node2'}
        with ThreadPoolExecutor(max_workers=1) as pool:
            pending=pool.submit(create)
            try:
                extra=pending.result(timeout=2);instances.append(extra)
            except TimeoutError:pass
            else:raise AssertionError('overcommitted full nodes')
            instances[0].kill()
            resumed=pending.result(timeout=120);instances.append(resumed)
            assert resumed.is_running()
            r=resumed.commands.run("printf 'capacity-released'")
            assert r.exit_code==0 and r.stdout=='capacity-released'
        (E/'capacity-result.json').write_text(json.dumps({'status':'passed','not_overcommitted':True,'pending_create_resumed':True}))
    finally:
        for s in instances:s.kill();s.close()
elif sys.argv[1]=='create':
    instances=[]
    try:
        for _ in range(2):
            s=Sandbox(image=image,runtime='runc',cpu=500,memory=512,idle_timeout=0,connection=connection,create_timeout=150)
            instances.append(s);assert s.is_running()
        (E/'live-instances.json').write_text(json.dumps([s.id for s in instances]))
    finally:
        for s in instances:s.close()
elif sys.argv[1]=='recovered':
    checks=[]
    for sid in json.loads((E/'live-instances.json').read_text()):
        s=Sandbox.from_id(sid,connection=connection)
        try:
            assert s.is_running()
            r=s.commands.run("printf 'recovered-generated-id'")
            assert r.stdout=='recovered-generated-id' and r.exit_code==0
            checks.append({'id':sid,'query':True,'command':True})
        finally:s.close()
    (E/'restart-result.json').write_text(json.dumps({'status':'passed','checks':checks},indent=2))
else:raise ValueError('unknown scenario')
