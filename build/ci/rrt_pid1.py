#!/usr/bin/env python3
"""Linux PID-namespace acceptance; needs CAP_SYS_ADMIN and a real RRT binary."""
import argparse
import json
import os
from pathlib import Path
import signal
import socket
import subprocess
import time
import urllib.request


def run(binary):
    with socket.socket() as s:
        s.bind(('127.0.0.1',0)); port=s.getsockname()[1]
    env={**os.environ,'RRT_HTTP_ONLY':'1','RRT_HTTP_PORT':str(port)}
    process=subprocess.Popen(['unshare','--fork','--pid','--mount-proc',str(binary)],env=env)
    init_pid=None
    try:
        end=time.monotonic()+15
        while True:
            children=Path(f'/proc/{process.pid}/task/{process.pid}/children').read_text().split()
            if children:init_pid=int(children[0])
            try:
                urllib.request.urlopen(f'http://127.0.0.1:{port}/healthz',timeout=.5).close()
                if init_pid:
                    break
                if time.monotonic()>end:
                    raise RuntimeError('namespace init PID was not observed')
                time.sleep(.1)
            except OSError:
                if process.poll() is not None or time.monotonic()>end:raise RuntimeError('RRT failed to start')
                time.sleep(.1)
        def execute(command):
            request=urllib.request.Request(f'http://127.0.0.1:{port}/invoke',data=json.dumps({'action':'process.exec','args':{'command':command}}).encode(),headers={'Content-Type':'application/json'})
            return json.load(urllib.request.urlopen(request,timeout=15))
        managed=execute('printf managed; printf error >&2; exit 7')
        assert managed['stdout']=='managed' and managed['stderr']=='error' and managed['exit_code']==7,managed
        reaped=execute("i=0; while [ $i -lt 30 ]; do sh -c 'sleep 0.1 &' ; i=$((i+1)); done; sleep 1; ps -eo stat=")
        zombies=[line for line in reaped['stdout'].splitlines() if line.strip().startswith('Z')]
        assert not zombies, ('unreaped orphans',len(zombies))
        assert init_pid
        os.kill(init_pid,signal.SIGTERM)
        code=process.wait(timeout=10)
        assert code==128+signal.SIGTERM,code
        print(json.dumps({'status':'passed','orphans':30,'zombies':0,'managed_exit_code':7,'sigterm_exit_code':code}),flush=True)
    finally:
        if process.poll() is None:
            if init_pid:
                try:os.kill(init_pid,signal.SIGKILL)
                except ProcessLookupError:pass
            process.kill();process.wait()


if __name__=='__main__':
    p=argparse.ArgumentParser();p.add_argument('binary',type=Path);a=p.parse_args();run(a.binary)
