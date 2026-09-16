#!/usr/bin/env python3
"""Run the complete installed example on the selected dedicated KVM test host.

Owns /opt/adx, /etc/adx, /var/lib/adx and /run/adx only when absent at start.
External sandboxd/Redis fixtures are deliberately outside the ADX supervisor.
"""
import hashlib,json,os,pathlib,secrets,shutil,ssl,subprocess,sys,time,traceback,urllib.request
from contract import CASES,verify
BASE=pathlib.Path(os.environ['ADX_EXAMPLE_BASE']);ROOT=pathlib.Path(sys.argv[1]).resolve()
INSTALL=[pathlib.Path(p) for p in ('/opt/adx','/etc/adx','/var/lib/adx','/run/adx')]
SOCKET=pathlib.Path('/run/sandboxd/sandboxd.sock')
if ROOT.exists() or any(p.exists() or p.is_symlink() for p in INSTALL) or SOCKET.exists():
    raise RuntimeError('example paths must be unused; refusing to overwrite existing deployment')
env={**os.environ,'PATH':f'/opt/adx-fc/bin:{BASE}/tools:'+os.environ['PATH'],
     'NO_PROXY':'127.0.0.1,localhost,10.88.0.0/16','no_proxy':'127.0.0.1,localhost,10.88.0.0/16'}
ROOT.mkdir(parents=True);E=ROOT/'evidence';E.mkdir();children=[];created=[]
result={'status':'failed','cases':[],'cleanup_errors':[]}
def event(index,**data):
    result['cases'].append({'name':CASES[index],'passed':True,**data});print('PASS',CASES[index],flush=True)
def call(args,**kw):return subprocess.run(list(map(str,args)),env=env,check=True,**kw)
def output(args):return subprocess.check_output(list(map(str,args)),env=env,text=True,timeout=15)
def spawn(name,args):
    with (E/(name+'.log')).open('w') as log:p=subprocess.Popen(list(map(str,args)),env=env,stdout=log,stderr=subprocess.STDOUT)
    children.append((name,p));return p
def wait(test,seconds=120):
    end=time.monotonic()+seconds
    while time.monotonic()<end:
        for name,p in children:
            if p.poll() is not None:raise RuntimeError(name+' exited')
        try:
            r=test()
            if r:return r
        except (OSError,subprocess.SubprocessError,KeyError,ValueError):pass
        time.sleep(.5)
    raise TimeoutError('example readiness timeout')
def catalog():return {k:json.loads(v) for k,v in json.loads(output(['redis-cli','--json','HGETALL','adx:{adx}:control:v1'])).items()}
def cli(command,*args):return ['/opt/adx/bin/adxctl',command,'--config','/etc/adx/deployment.json',*args]
def inventory():return output(['sbox','-a',SOCKET,'list']).strip().splitlines()[1:]
try:
    # Reuse only the external sandboxd prerequisites from the FC fixture helper.
    preparation=ROOT/'prerequisites'
    prep_env={**env,'ADX_FC_BASE':str(BASE),'ADX_FC_RUN_ROOT':str(preparation)}
    subprocess.run(['python3',str(BASE/'e2e/firecracker/configure.py'),'node1'],env=prep_env,check=True)
    for path in INSTALL:path.mkdir(mode=0o700,parents=True);created.append(path)
    shutil.copytree(BASE/'package','/opt/adx',dirs_exist_ok=True)
    call(['python3',BASE/'e2e/package.py','verify','/opt/adx'])
    shutil.copyfile('/opt/adx/manifest.json',E/'package-manifest.json')
    example=pathlib.Path('/opt/adx/etc/examples/deployment.json')
    installed=pathlib.Path('/etc/adx/deployment.json');shutil.copyfile(example,installed);installed.chmod(0o600)
    result['example_sha256']=hashlib.sha256(example.read_bytes()).hexdigest()
    result['installed_sha256']=hashlib.sha256(installed.read_bytes()).hexdigest()
    tls=pathlib.Path('/etc/adx/tls');tls.mkdir(mode=0o700)
    def openssl(*args):call(['openssl',*args],stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
    openssl('req','-x509','-newkey','rsa:2048','-nodes','-keyout',tls/'ca.key','-out',tls/'ca.pem','-days','2','-subj','/CN=ADX example test CA')
    extensions=ROOT/'extensions.cnf';extensions.write_text('basicConstraints=CA:FALSE\nkeyUsage=digitalSignature,keyEncipherment\nextendedKeyUsage=serverAuth,clientAuth\nsubjectAltName=DNS:adx.internal,DNS:localhost,IP:127.0.0.1\n')
    for name in ('master','node-1','frontend','edge','edge-public'):
        openssl('req','-newkey','rsa:2048','-nodes','-keyout',tls/(name+'.key'),'-out',ROOT/(name+'.csr'),'-subj','/CN=ADX example '+name)
        openssl('x509','-req','-in',ROOT/(name+'.csr'),'-CA',tls/'ca.pem','-CAkey',tls/'ca.key','-CAcreateserial','-out',tls/(name+'.pem'),'-days','2','-extfile',extensions)
        openssl('x509','-in',tls/(name+'.pem'),'-outform','DER','-out',tls/(name+'.der'))
    for path in tls.glob('*.key'):path.chmod(0o600)
    shutil.copyfile(tls/'ca.pem',tls/'public-ca.pem')
    private=pathlib.Path('/etc/adx/secrets');private.mkdir(mode=0o700)
    key=private/'admin-key';key.write_text(secrets.token_hex(32));key.chmod(0o600)
    # Keep a private copy for evidence redaction, never exported itself.
    (ROOT/'secrets').mkdir(mode=0o700);shutil.copyfile(key,ROOT/'secrets/admin-key')
    backend=preparation/'sandboxd/config.toml'
    backend.write_text(backend.read_text().replace('10.231.16.0/20','10.88.0.0/16'))
    registry=spawn('registry',['docker-registry','serve',preparation/'registry.yaml'])
    sys.path.insert(0,str(BASE/'e2e'));import publish
    image='127.0.0.1:5000/adx-rrt@'+publish.publish(str(BASE/'rrt.tar'))
    (E/'image.json').write_text(json.dumps({'image':image}))
    redis=spawn('external-redis',['/opt/adx/bin/redis-server','--bind','127.0.0.1','--port','6379','--appendonly','yes','--appendfsync','always','--dir',preparation/'redis','--save',''])
    wait(lambda:output(['redis-cli','ping'])=='PONG\n')
    SOCKET.parent.mkdir(mode=0o700,parents=True,exist_ok=True)
    sandboxd=spawn('external-sandboxd',['sandboxd','--root',preparation/'sandboxd/root','--config',backend,'--socket',SOCKET,'--http-address','127.0.0.1:18081','--pprof-address','127.0.0.1:16061','--log-file',E/'sandboxd-service.log'])
    wait(lambda:SOCKET.exists())
    for args,name in [(cli('validate'),'validate'),(cli('render','--output','/run/adx/config-review'),'render')]:
        with (E/(name+'.log')).open('w') as log:call(args,stdout=log,stderr=subprocess.STDOUT)
    event(0)
    supervisor=spawn('supervisor',cli('run'))
    wait(lambda:catalog()['node:node-1']['session']['routable'] and catalog()['node:node-1']['node']['available'])
    status=json.loads(output(cli('status')))
    (E/'status-running.json').write_text(json.dumps(status,indent=2))
    assert len(status['services'])==5 and all(s['pid'] and not s['failed'] for s in status['services']), 'five live supervisor roles required; inspect status-running.json and component-logs'
    node=catalog()['node:node-1'];(E/'node-ready.json').write_text(json.dumps(node,indent=2))
    event(1)
    context=ssl.create_default_context(cafile='/etc/adx/tls/public-ca.pem')
    def request(method,path,data=None):
        req=urllib.request.Request('https://localhost:8443'+path,data=json.dumps(data).encode() if data else None,method=method,headers={'Authorization':'Bearer '+key.read_text().strip(),'Content-Type':'application/json'})
        with urllib.request.urlopen(req,context=context,timeout=20) as response:return json.load(response)
    wait(lambda:request('GET','/api/admin/v1/keys') is not None)
    credential=request('POST','/api/admin/v1/keys',{'tenantId':'example'})
    tenant=private/'tenant-key';tenant.write_text(credential['apiKey']);tenant.chmod(0o600)
    shutil.copyfile(tenant,ROOT/'secrets/tenant-key')
    event(2)
    call([BASE/'client/bin/python','-m','pip','install','--no-index','--no-deps','--force-reinstall',next(pathlib.Path('/opt/adx/sdk').glob('*.whl'))],stdout=subprocess.DEVNULL)
    call([BASE/'client/bin/python','-u',BASE/'e2e/example/sdk.py',ROOT,image],timeout=300)
    event(3);event(4)
    assert len(inventory())==1
    call(cli('stop'),timeout=120);supervisor.wait(timeout=30)
    records={k:v for k,v in catalog().items() if k.startswith('instance:')}
    assert len(records)==2 and all(r['result']['state']=='Deleted' and not r['result']['resources_held'] for r in records.values())
    result['backend_count']=len(inventory());assert result['backend_count']==0
    result['external_dependencies_alive_after_stop']=redis.poll() is None and sandboxd.poll() is None
    assert result['external_dependencies_alive_after_stop']
    (E/'catalog-final.json').write_text(json.dumps(records,indent=2));event(5)
    result['status']='passed'
except BaseException as error:
    result['error']=repr(error);traceback.print_exc();(E/'failure.txt').write_text(traceback.format_exc());print('FAIL',repr(error),flush=True)
finally:
    if any(n=='supervisor' and p.poll() is None for n,p in children):
        try:call(cli('stop'),timeout=120)
        except Exception as error:result['cleanup_errors'].append(str(error))
    logs=pathlib.Path('/run/adx/control/logs')
    if logs.exists():shutil.copytree(logs,E/'component-logs')
    for name,proc in reversed(children):
        if proc.poll() is None:
            proc.terminate()
            try:proc.wait(timeout=20)
            except subprocess.TimeoutExpired:proc.kill();proc.wait();result['cleanup_errors'].append(name+' forced kill')
    # Remove only installation paths exclusively created by this run.
    for path in reversed(created):shutil.rmtree(path)
    if SOCKET.exists():SOCKET.unlink()
    try:verify(result)
    except ValueError:result['status']='failed'
    (E/'result.json').write_text(json.dumps(result,indent=2))
    hidden=[p.read_bytes().strip() for p in (ROOT/'secrets').glob('*') if p.is_file()]
    export=ROOT/'export/evidence';export.mkdir(parents=True)
    for path in E.rglob('*'):
        if not path.is_file():continue
        data=path.read_bytes()
        for token in hidden:
            if token:data=data.replace(token,b'[REDACTED]')
        target=export/path.relative_to(E);target.parent.mkdir(parents=True,exist_ok=True);target.write_bytes(data)
sys.exit(0 if result['status']=='passed' else 1)
