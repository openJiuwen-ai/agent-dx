#!/usr/bin/env python3
"""Two real FC backend nodes in network namespaces on one dedicated KVM host.

This fixture owns only its explicit new run directory and named namespaces. It
is local multi-process evidence, not independent-host or Kubernetes acceptance.
"""
import fcntl
import hashlib
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import time
import urllib.request
from transfer_config import ADDRESSES, HOST, configure
from transfer_contract import verify


def main():
    root=Path(sys.argv[1]).resolve()
    base=Path(os.environ.get('ADX_FC_BASE','/opt/adx'))
    if root.exists(): raise ValueError('run directory must be new')
    with open('/dev/kvm','rb',buffering=0) as kvm:
        if fcntl.ioctl(kvm.fileno(),0xAE00,0)!=12:raise RuntimeError('KVM API 12 required')
        vm=fcntl.ioctl(kvm.fileno(),0xAE01,0)
        try:
            vcpu=fcntl.ioctl(vm,0xAE41,0);os.close(vcpu)
        finally:os.close(vm)
    if shutil.disk_usage(root.parent).free < 8*1024**3:raise RuntimeError('8 GiB free required')
    if subprocess.check_output(['ip','route','show','10.240.0.0/24'],text=True).strip():
        raise RuntimeError('fixture network already in use')
    env={**os.environ,'PATH':f'/opt/adx-fc/bin:{base}/tools:'+os.environ['PATH'],
         'ADX_FC_BASE':str(base),'ADX_FC_RUN_ROOT':str(root),'ADX_FC_PROXY_MODE':'embedded'}
    bypass=','.join(filter(None,[env.get('NO_PROXY',''),'127.0.0.1,localhost,10.240.0.1,10.240.0.11,10.240.0.12,10.240.0.0/24,10.231.16.0/20,10.231.32.0/20']))
    env.update(NO_PROXY=bypass,no_proxy=bypass)
    subprocess.run(['python3',str(base/'e2e/firecracker/configure.py'),'node1'],env=env,check=True)
    configure(root)
    evidence=root/'evidence'
    def call(args,**kw):return subprocess.run(list(map(str,args)),env=env,check=True,**kw)
    call(['python3',base/'e2e/package.py','verify',base/'package'])
    shutil.copyfile(base/'package/manifest.json',evidence/'package-manifest.json')
    (evidence/'runtime-hashes.json').write_text(json.dumps({str(p):hashlib.file_digest(p.open('rb'),'sha256').hexdigest() for p in [Path('/opt/adx-fc/bin/sandboxd'),Path('/opt/adx-fc/bin/firecracker'),base/'package/runtime/rrt-runtime']},indent=2))
    tag=hashlib.sha256(str(root).encode()).hexdigest()[:6]
    bridge='axt'+tag
    namespaces={n:'axt-'+tag+'-'+n for n in ADDRESSES}
    children=[];backend_pids={};started_nodes=[];created_namespaces=[];bridge_created=False
    result={'status':'failed','cases':[],'cleanup_errors':[],'inventories':{}}
    env.update(MINIO_ROOT_USER=(root/'secrets/s3-user').read_text().strip(),MINIO_ROOT_PASSWORD=(root/'secrets/s3-key').read_text().strip())
    def spawn(name,args):
        log=(evidence/(name+'.log')).open('w')
        p=subprocess.Popen(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT)
        log.close();children.append((name,p));return p
    def wait(test,seconds=120):
        end=time.monotonic()+seconds
        while time.monotonic()<end:
            for name,p in children:
                if p.poll() is not None:raise RuntimeError(name+' exited; inspect logs')
            try:
                if test():return
            except (OSError,subprocess.SubprocessError,ValueError,KeyError):pass
            time.sleep(.5)
        raise TimeoutError('fixture readiness timeout')
    def inventory(name):
        raw=subprocess.check_output(['sbox','-a',str(root/name/'sandboxd/sandboxd.sock'),'list'],env=env,text=True,timeout=10)
        (evidence/(name+'-inventory-final.txt')).write_text(raw)
        return len(raw.strip().splitlines()[1:])
    try:
        call(['ip','link','add',bridge,'type','bridge']);bridge_created=True
        call(['ip','addr','add',HOST+'/24','dev',bridge]);call(['ip','link','set',bridge,'up'])
        for i,(node,address) in enumerate(ADDRESSES.items()):
            ns=namespaces[node];call(['ip','netns','add',ns]);created_namespaces.append(ns)
            hostdev=f'ax{tag}{i}';peer=f'ay{tag}{i}'
            call(['ip','link','add',hostdev,'type','veth','peer','name',peer])
            call(['ip','link','set',hostdev,'master',bridge]);call(['ip','link','set',hostdev,'up'])
            call(['ip','link','set',peer,'netns',ns])
            for args in (['link','set','lo','up'],['link','set',peer,'name','eth0'],['addr','add',address+'/24','dev','eth0'],['link','set','eth0','up'],['route','add','default','via',HOST]):
                call(['ip','-n',ns,*args])
            call(['ip','netns','exec',ns,'sysctl','-qw','net.ipv4.ip_forward=1','net.ipv4.conf.all.rp_filter=0','net.ipv4.conf.default.rp_filter=0'])
            subnet=f'10.231.{16 if node=="node1" else 32}.0/20'
            call(['ip','route','add',subnet,'via',address])
        (evidence/'topology.json').write_text(json.dumps({'kind':'single-kvm-host-two-network-namespaces','bridge':bridge,'namespaces':namespaces,'addresses':ADDRESSES,'backend_peer_filesystem_hidden':True},indent=2))
        spawn('minio',[base/'tools/minio','server',root/'objects','--address','0.0.0.0:19090','--console-address','127.0.0.1:19091'])
        wait(lambda:urllib.request.urlopen('http://127.0.0.1:19090/minio/health/ready',timeout=2).status==200)
        call(['python3',base/'e2e/firecracker/s3_probe.py',root,'--create'])
        spawn('registry',['docker-registry','serve',root/'registry.yaml'])
        sys.path.insert(0,str(base/'e2e'))
        import publish
        digest=publish.publish(str(base/'rrt.tar'))
        image=f'{HOST}:5000/adx-rrt@{digest}'
        (evidence/'rrt-image.json').write_text(json.dumps({'image':image}))
        spawn('control',[base/'package/bin/adxctl','run','--config',root/'deployment.yaml'])
        for node,ns in namespaces.items():
            folder=root/node
            mask=folder/'peer-mask';mask.mkdir()
            peer=root/('node2' if node=='node1' else 'node1')
            backend_pids[node]=spawn(node+'-sandboxd',['nsenter','--net=/var/run/netns/'+ns,'unshare','--mount','--propagation','private','sh','-ec','mount --bind "$1" "$2"; shift 2; exec "$@"','sh',mask,peer,'sandboxd','--root',folder/'sandboxd/root','--config',folder/'sandboxd/config.toml','--socket',folder/'sandboxd/sandboxd.sock','--http-address','127.0.0.1:18081','--pprof-address','127.0.0.1:16061','--log-file',evidence/(node+'-sandboxd-service.log')]).pid
            wait(lambda:(folder/'sandboxd/sandboxd.sock').exists())
            spawn(node,['nsenter','--net=/var/run/netns/'+ns,base/'package/bin/adxctl','run','--config',folder/'deployment.yaml']);started_nodes.append(node)
        authenv={**env,'REDISCLI_AUTH':(root/'secrets/redis-key').read_text().strip()}
        def ready():
            c=json.loads(subprocess.check_output(['redis-cli','--json','HGETALL','adx:{acceptance}:control:v1'],env=authenv,text=True,timeout=10))
            return all(json.loads(c['node:'+n])['session']['routable'] and json.loads(c['node:'+n])['node']['available'] for n in namespaces)
        wait(ready)
        (root/'backend-pids.json').write_text(json.dumps(backend_pids))
        print('TWO FIRECRACKER NODES READY',flush=True)
        call([base/'client/bin/python','-u',base/'e2e/firecracker/sdk_transfer.py',root,image],timeout=900)
        result.update(json.loads((evidence/'sdk-transfer.json').read_text()))
        for name in namespaces:result['inventories'][name]=inventory(name)
        call(['python3',base/'e2e/firecracker/s3_probe.py',root])
    except BaseException as error:
        sdk_result=evidence/'sdk-transfer.json'
        if sdk_result.exists():result.update(json.loads(sdk_result.read_text()))
        result['error']=repr(error);result['status']='failed'
        print('FAIL',repr(error),flush=True)
    finally:
        for address in ADDRESSES.values():
            rule=['OUTPUT','-d',address,'-p','tcp','--dport','17001','-m','comment','--comment',root.name,'-j','DROP']
            if subprocess.run(['iptables','-C',*rule],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
                call(['iptables','-D',*rule])
        for ns in created_namespaces:
            command=['nsenter','--net=/var/run/netns/'+ns,'iptables']
            rule=['OUTPUT','-p','tcp','--dport','50090','-m','comment','--comment',root.name,'-j','DROP']
            if subprocess.run([*command,'-C',*rule],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL).returncode==0:
                call([*command,'-D',*rule])
        # Nodes drain while their backend and the control node remain alive.
        for node in reversed(started_nodes):
            try:call([base/'package/bin/adxctl','stop','--config',root/node/'deployment.yaml'],timeout=90)
            except Exception as error:result['cleanup_errors'].append(f'{node}: {error}')
        try:call([base/'package/bin/adxctl','stop','--config',root/'deployment.yaml'],timeout=90)
        except Exception as error:result['cleanup_errors'].append(f'control: {error}')
        for name,p in reversed(children):
            if p.poll() is None:
                p.terminate()
                try:p.wait(timeout=20)
                except subprocess.TimeoutExpired:p.kill();p.wait()
        for ns in reversed(created_namespaces):
            pids=subprocess.check_output(['ip','netns','pids',ns],text=True).split()
            if pids:
                result['cleanup_errors'].append(f'{ns}: remaining processes {pids}')
                for pid in pids:
                    try:os.kill(int(pid),9)
                    except ProcessLookupError:pass
            call(['ip','netns','del',ns])
        if bridge_created:call(['ip','link','del',bridge])
        if result['cleanup_errors']:result['status']='failed'
        try:verify(result)
        except ValueError:result['status']='failed'
        (evidence/'result.json').write_text(json.dumps(result,indent=2))
        # Export only logs and public evidence, with credentials redacted.
        for node in ADDRESSES:
            logs=root/node/'state/logs'
            if logs.exists():shutil.copytree(logs,evidence/(node+'-logs'),dirs_exist_ok=True)
        call(['python3',base/'e2e/firecracker/collect.py',root])
    return 0 if result['status']=='passed' else 1

if __name__=='__main__':sys.exit(main())
