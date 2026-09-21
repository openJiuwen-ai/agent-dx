"""Best-effort network metadata on failure; never export Redis credentials/env."""
import json
import os
import pathlib
import socket
import subprocess
import sys
import time
run = pathlib.Path(sys.argv[1])
base = pathlib.Path(os.environ.get('ADX_FC_BASE','/opt/adx'))
result = {'captured_at':time.time()}
for name, command in [('neighbors',['ip','-j','neigh','show']),('fdb',['bridge','-j','fdb','show']),('links',['ip','-j','link','show']),('routes',['ip','-j','route','show'])]:
    try:
        result[name]=json.loads(subprocess.check_output(command,timeout=3,text=True))
    except Exception as error:
        result[name]={'error':str(error)}
try:
    env={**os.environ,'REDISCLI_AUTH':(run/'secrets/redis-key').read_text().strip()}
    catalog=json.loads(subprocess.check_output([str(base/'tools/redis-cli'),'--json','HGETALL','adx:{acceptance}:control:v1'],env=env,text=True,timeout=3))
    probes=[]
    for key,value in catalog.items():
        if not key.startswith('capsule:'):continue
        record=json.loads(value);state=record.get('result',{});ip=state.get('runtime_ip')
        if state.get('state')!='Running' or not ip:continue
        probe={'instance':key,'runtime_id':state.get('runtime_id'),'ip':ip,'tcp':[]}
        for _ in range(3):
            start=time.monotonic()
            try:
                with socket.create_connection((ip,50090),timeout=2): pass
                probe['tcp'].append({'connected':True,'seconds':time.monotonic()-start})
            except OSError as error:
                probe['tcp'].append({'connected':False,'error':str(error)})
        probes.append(probe)
    result['probes']=probes
except Exception as error:
    result['probe_error']=str(error)
(run/'evidence/network-failure.json').write_text(json.dumps(result,indent=2))
print('Network failure metadata saved',flush=True)
