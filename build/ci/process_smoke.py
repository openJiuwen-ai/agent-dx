#!/usr/bin/env python3
"""Real Redis/Master supervision check. No Instance or platform E2E claim."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import sys
import tempfile
import time

ROOT=Path(__file__).resolve().parents[2]
def port():
    with socket.socket() as s:
        s.bind(('127.0.0.1',0));return s.getsockname()[1]
def wait(fn):
    end=time.monotonic()+20
    while time.monotonic()<end:
        try:
            value=fn()
            if value:return value
        except (OSError,ValueError,subprocess.SubprocessError):pass
        time.sleep(.1)
    raise TimeoutError('process condition not satisfied')
def endpoint(port_number):
    with socket.create_connection(('127.0.0.1',port_number),timeout=1) as s:
        key=b'adx:{process-smoke}:master:v1'
        s.sendall(b'*2\r\n$3\r\nGET\r\n$'+str(len(key)).encode()+b'\r\n'+key+b'\r\n')
        stream=s.makefile('rb');header=stream.readline()
        if not header.startswith(b'$') or int(header[1:])<0:return None
        return json.loads(stream.read(int(header[1:])))
def main():
    p=argparse.ArgumentParser();p.add_argument('--package',type=Path,required=True);p.add_argument('--output',type=Path,required=True);a=p.parse_args()
    package=a.package.resolve();out=a.output.resolve();out.mkdir(parents=True,exist_ok=False)
    subprocess.run([sys.executable,str(ROOT/'build/release/package.py'),'verify',str(package)],check=True)
    subprocess.run([sys.executable,str(ROOT/'build/ci/rpc_certificates.py'),str(out/'tls')],check=True)
    tls=out/'tls';result={'status':'failed','scope':'real Redis and Master supervision','instance_e2e':False}
    try:
        with tempfile.TemporaryDirectory(prefix='adx-proc-',dir='/tmp') as t:
            root=Path(t);redis_port=port();master_port=port()
            data=out/'redis';data.mkdir()

            d={'schema_version':1,'package_dir':str(package),'state_dir':str(root/'state'),'redis_url':f'redis://127.0.0.1:{redis_port}/','namespace':'process-smoke','restart_limit':4,'restart_delay_ms':200,'stop_timeout_seconds':5,'services':[
                {'id':'redis','role':'redis','config':{'bind':'127.0.0.1','port':redis_port,'data_dir':str(data),'appendfsync':'always'}},
                {'id':'master','role':'master','config':{'listen':f'127.0.0.1:{master_port}','advertised_address':f'https://127.0.0.1:{master_port}','scheduler_shards':1,'placement':'pack','rpc_timeout_seconds':2,'tls':{'ca':str(tls/'ca.pem'),'certificate':str(tls/'master.pem'),'private_key':str(tls/'master.key'),'server_name':'localhost','peers':{'api-server':str(tls/'api-server.der')}},'bootstrap_credentials':[]}}]}
            config=root/'deployment.yaml';config.write_text(json.dumps(d));config.chmod(0o600)
            command=[str(package/'bin/adxctl')]
            def control(action):
                return json.loads(subprocess.check_output(command+[action,'--config',str(config)],stderr=subprocess.DEVNULL,text=True,timeout=15))
            with (out/'supervisor.log').open('w') as log:
                process=subprocess.Popen(command+['run','--config',str(config)],stdout=log,stderr=subprocess.STDOUT)
                try:
                    first=wait(lambda:endpoint(redis_port));before=control('status');master=next(s for s in before['services'] if s['id']=='master')
                    # This PID is a currently owned child reported by this run's supervisor.
                    os.kill(master['pid'],signal.SIGKILL)
                    recovered=wait(lambda:(v if v and v['epoch']>first['epoch'] else None) if (v:=endpoint(redis_port)) is not None else None)
                    after=control('status');new=next(s for s in after['services'] if s['id']=='master')
                    assert new['pid']!=master['pid'] and new['restarts']>master['restarts']
                    control('stop');assert process.wait(timeout=10)==0
                    result.update(status='passed',initial_epoch=first['epoch'],recovered_epoch=recovered['epoch'],restarts=new['restarts'],clean_stop=True)
                finally:
                    if process.poll() is None:
                        try:control('stop')
                        except Exception:process.terminate()
                        process.wait(timeout=15)
                    import shutil
                    if (root/'state/logs').exists():shutil.copytree(root/'state/logs',out/'logs')
    finally:(out/'result.json').write_text(json.dumps(result,indent=2)+'\n')
    print(json.dumps(result))
if __name__=='__main__':main()
