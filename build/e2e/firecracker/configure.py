#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import secrets
import subprocess

from fc_environment_spec import resolve as resolve_environment_spec


def parse_args():
    parser = argparse.ArgumentParser(
        description="Render one ADX Firecracker E2E node configuration"
    )
    parser.add_argument("node", choices=("node1", "node2"))
    return parser.parse_args()


BASE=pathlib.Path(os.environ.get('ADX_FC_BASE', '/opt/adx')); RUN=pathlib.Path(os.environ['ADX_FC_RUN_ROOT']); PRIVATE=RUN/'secrets'; EVIDENCE=RUN/'evidence'; PRIVATE.mkdir(parents=True); EVIDENCE.mkdir();
P=RUN; P.mkdir(exist_ok=True)
node=parse_args().node; T=PRIVATE/'tls'
for name in ('s3-user','s3-key'):
 path=PRIVATE/name; path.write_text(secrets.token_hex(32)); path.chmod(0o600)
if not T.exists():
 subprocess.run(['python3',str(BASE/'e2e/rpc_certificates.py'),str(T)],check=True)
 subprocess.run(['openssl','req','-newkey','rsa:2048','-nodes','-keyout',str(T/'node2.key'),'-out',str(T/'node2.csr'),'-subj','/CN=ADX test node2'],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 subprocess.run(['openssl','x509','-req','-in',str(T/'node2.csr'),'-CA',str(T/'ca.pem'),'-CAkey',str(T/'ca.key'),'-CAcreateserial','-out',str(T/'node2.pem'),'-days','2','-extfile',str(T/'extensions.cnf')],check=True,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL)
 subprocess.run(['openssl','x509','-in',str(T/'node2.pem'),'-outform','DER','-out',str(T/'node2.der')],check=True)
 (T/'node2.key').chmod(0o600)
redis_key=PRIVATE/'redis-key'
if not redis_key.exists(): redis_key.write_text(secrets.token_hex(32)); redis_key.chmod(0o600)
key=PRIVATE/'api-key'
if not key.exists(): key.write_text(secrets.token_hex(32)); key.chmod(0o600)
other=PRIVATE/'other-key'
if not other.exists(): other.write_text(secrets.token_hex(32)); other.chmod(0o600)
def tls(n,peers):return {'ca':str(T/'ca.pem'),'certificate':str(T/f'{n}.pem'),'private_key':str(T/f'{n}.key'),'server_name':'localhost','peers':{k:str(T/f'{v}.der') for k,v in peers.items()}}
R=P/'sandboxd'; R.mkdir(exist_ok=True)
CGROUP='adx-fc-'+secrets.token_hex(8)
for f,data in [('oss.json',{'oss':{},'type':'oss'}),('registry.json',{'registry':{'scheme':'https' if os.getenv('ADX_E2E_KUBERNETES') else 'http','skip_verify':False if os.getenv('ADX_E2E_KUBERNETES') else True},'type':'registry'}),('oss_auths.json',{}),('registry_auths.json',{'auths':{}})]: (R/f).write_text(json.dumps(data))
registry_auth=pathlib.Path('/registry-auth/.dockerconfigjson')
if registry_auth.exists(): (R/'registry_auths.json').write_bytes(registry_auth.read_bytes()); (R/'registry_auths.json').chmod(0o600)
(R/'config.toml').write_text(f'''rootDir = "{R}/root"
storeDir = "{R}/store"
[plugin.network]
ip_range = "10.231.{16 if node=='node1' else 32}.0/20"
nat_backend = "iptables"
enable_local_dnat = true
enable_network_acl = true
[plugin.resource]
disable_cgroup = false
cpu_limit_mode = "quota"
cgroup_cache_size = 1
interface_cache_size = 2
cgroup_root_name = "/{CGROUP}"
max_instance_num = 8
pids_max = 256
[plugin.runtime]
image_lib_dir = "{R}/images"
filestore_dir = "{R}/filestore"
filestore_dir_size = ""
loop_device_dir = "/dev"
overlay_tmpfs_size = "64M"
[plugin.runtime.runc]
state_root = "{R}/runc"
shim_binary = "/usr/local/bin/runc-shim"
[plugin.runtime.basic_spec]
runc = ""
[plugin.runtime.runtime_binary]
runc = "/usr/local/bin/runc"
[plugin.image]
root = "{R}/image_manager/data"
distill_fs_bin = "{BASE}/tools/distill_fs"
oss_template = "{R}/oss.json"
nydus_template = "{R}/registry.json"
nydus_suffix = "_nydus_v3"
oss_auths_path = "{R}/oss_auths.json"
registry_auths_path = "{R}/registry_auths.json"
cgroup_memory_limit = "0"
''')
(P/'proxy').mkdir(exist_ok=True,mode=0o700)
# Observe the limits visible inside the node, supporting cgroup v1 and v2.
import os
cg=pathlib.Path('/sys/fs/cgroup')
capacity={'cpu_millis':4000,'memory_bytes':3*1024**3,'disk_bytes':4*1024**3}
(P/'capacity-base.json').write_text(json.dumps(capacity))
services=[]
def add(service_id, role, config=None, env=None):
 services.append({
  'id': service_id,
  'role': role,
  'config': {} if config is None else config,
  'env': {} if env is None else env,
 })
if node=='node1':
 (P/'redis').mkdir(exist_ok=True)
 add('redis','redis',{'bind':'0.0.0.0','port':6379,'data_dir':str(P/'redis'),'appendfsync':'always','password_file':str(redis_key)})
 add('master','master',{'listen':'0.0.0.0:17000','advertised_address':'https://127.0.0.1:17000','scheduler_shards':1,'placement':'spread','rpc_timeout_seconds':120,'tls':tls('master',{'api-server':'api-server','edge':'edge','node:node1':'node','node:node2':'node2'}),'bootstrap_credentials':[{'key_file':str(key),'tenant_id':'e2e','administrator':False,'expires_at_unix_seconds':0},{'key_file':str(other),'tenant_id':'e2e-other','administrator':False,'expires_at_unix_seconds':0}]})
edge_peer=os.getenv('ADX_E2E_EDGE_IP')
edge_cidrs=(edge_peer+('/128' if ':' in edge_peer else '/32')+',127.0.0.1/32') if edge_peer else '127.0.0.1/32'
common={'ADX_DATA_PLANE_EDGE_FRONTEND_NODE_SECURITY_MODE':'mtls','RUST_LOG':'info'}
np={**common,'ADX_DATA_PLANE_NODE_PROXY_BIND':'0.0.0.0:18443','ADX_DATA_PLANE_NODE_PROXY_HEALTH_BIND':'127.0.0.1:19443','ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS':f'10.231.{16 if node=="node1" else 32}.0/20','ADX_DATA_PLANE_ALLOWED_EDGE_CIDRS':edge_cidrs,'ADX_DATA_PLANE_NODE_PROXY_TLS_CERT':str(T/('node.pem' if node=='node1' else 'node2.pem')),'ADX_DATA_PLANE_NODE_PROXY_TLS_KEY':str(T/('node.key' if node=='node1' else 'node2.key')),'ADX_DATA_PLANE_NODE_PROXY_MTLS_CLIENT_CA':str(T/'ca.pem'),'ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR':str(P/'proxy')}
add('proxy','node-proxy',env=np)
host=os.getenv('ADX_E2E_NODE_IP') or ('127.0.0.1' if node=='node1' else 'node2')
if ':' in host: host='['+host+']'
add(node,'node-manager',{'node_id':node,'listen':'0.0.0.0:17001','advertised_address':f'{host}:17001','proxy_address':f'{host}:18443','tls':tls('node' if node=='node1' else 'node2',{'master':'master','api-server':'api-server'}),'sandboxd_socket':str(R/'sandboxd.sock'),'proxy_socket':str(P/'proxy/route.sock'),'resource_source':{'kind':'sandboxd','socket':str(P/'resource.sock'),'valid_for_seconds':6},'degradation_journal':str(P/'degraded/results.sqlite'),'metrics_listen':'127.0.0.1:17003','checkpoint_dir':str(P/'checkpoints'),'report_interval_seconds':2,'rpc_timeout_seconds':20,'rrt_port':50090,'rrt_command':['/usr/local/bin/rrt-runtime'],'rrt_env':{}})
if node=='node1':
 add('api','api-server',{'listen':'127.0.0.1:8888','loopback_http':True,'discovery':{'poll_seconds':1},'ca':str(T/'ca.pem'),'certificate':str(T/'api-server.pem'),'private_key':str(T/'api-server.key'),'server_name':'localhost','rpc_timeout_seconds':120,'cache_entries':1000,'auth_cache_ttl_seconds':10})
 ee={**common,'ADX_DATA_PLANE_EDGE_FRONTEND_TLS_BIND':'0.0.0.0:8443','ADX_DATA_PLANE_EDGE_FRONTEND_PLAIN_BIND':'127.0.0.1:8080','ADX_DATA_PLANE_EDGE_FRONTEND_HEALTH_BIND':'127.0.0.1:18080','ADX_DATA_PLANE_EDGE_FRONTEND_TLS_CERT':str(T/'edge.pem'),'ADX_DATA_PLANE_EDGE_FRONTEND_TLS_KEY':str(T/'edge.key'),'ADX_DATA_PLANE_EDGE_FRONTEND_ALLOWED_CLIENT_CIDRS':'127.0.0.1/32','ADX_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_CA':str(T/'ca.pem'),'ADX_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_SERVER_NAME':'localhost','ADX_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_CLIENT_CERT':str(T/'edge.pem'),'ADX_DATA_PLANE_EDGE_FRONTEND_NODE_TLS_CLIENT_KEY':str(T/'edge.key'),'ADX_DATA_PLANE_EDGE_FRONTEND_CONTROL_PLANE_ADDRESS':'127.0.0.1:8888'}
 add('edge','edge',{'tls':tls('edge',{'master':'master'}),'rpc_timeout_seconds':5,'refresh_seconds':1,'auth_cache_seconds':10,'auth_cache_entries':1000},ee)
d={'schema_version':1,'package_dir':str(BASE/'package'),'state_dir':str(P/'state'),'redis_url':f'redis://:{redis_key.read_text().strip()}@127.0.0.1:6379/','namespace':'acceptance','restart_limit':3,'restart_delay_ms':1000,'stop_timeout_seconds':30,'services':services}
d['environment'] = resolve_environment_spec(
    BASE / 'package',
    bool(os.getenv('ADX_E2E_KUBERNETES')),
    PRIVATE / 'runtime-image',
    os.getenv('ADX_E2E_IMAGE_PROCESS_CONFIG', '/etc/adx-image-process.json'),
)
(P/'deployment.yaml').write_text(json.dumps(d));(P/'deployment.yaml').chmod(0o600)

print('configured',node,capacity)

(P/'capacity.json').write_text(json.dumps({'capacity':capacity,'devices':[],'valid_until_unix_seconds':int(__import__('time').time())+7200}))
sandbox_config=(R/'config.toml').read_text()
a=sandbox_config.index('[plugin.runtime.runc]'); b=sandbox_config.index('[plugin.image]',a)
sandbox_config=sandbox_config[:a]+f"""[plugin.node_resource]
provider = "cgroup"
sock_path = "{P}/resource.sock"
[plugin.runtime.firecracker]
kernel_image_path = "/opt/adx-fc/artifacts/Image"
initrd_path = "/opt/adx-fc/artifacts/initrd.img"
kernel_args = "console=ttyS0 reboot=k panic=1 pci=off init=/init random.trust_cpu=on"
kvm_device = "/dev/kvm"
default_vcpu_count = 1
default_memory_mib = 512
default_overlay_size_bytes = 536870912
checkpoint_mode = "full"
virtiofs_enabled = true
virtiofsd_path = "{BASE}/tools/virtiofsd"
[plugin.runtime.basic_spec]
firecracker = ""
[plugin.runtime.runtime_binary]
firecracker = "/opt/adx-fc/bin/firecracker"
"""+sandbox_config[b:]
(R/'config.toml').write_text(sandbox_config)
(P/'registry.yaml').write_text(f"version: 0.1\nlog:\n  level: warn\nstorage:\n  filesystem:\n    rootdirectory: {P}/registry\nhttp:\n  addr: 127.0.0.1:5000\n")

config_path=RUN/'deployment.yaml'
deployment=json.loads(config_path.read_text())
for service in deployment['services']:
 if service['role']=='node-manager':
  c=service['config']; c.pop('checkpoint_dir',None)
  c['checkpoint_storage']={'kind':'s3','alias':'shared','bucket':'checkpoints','region':'us-east-1','endpoint':'http://127.0.0.1:19090','allow_http':True,'prefix':'adx','root':str(RUN/'checkpoints'),'cache_budget_bytes':4*1024**3}
  c['checkpoint_gc']={'enabled':True,'min_age_seconds':0,'interval_seconds':1,'max_artifacts':100}
  service['env'].update({'AWS_ACCESS_KEY_ID':(PRIVATE/'s3-user').read_text().strip(),'AWS_SECRET_ACCESS_KEY':(PRIVATE/'s3-key').read_text().strip()})
config_path.write_text(json.dumps(deployment,indent=2))

if os.environ.get('ADX_FC_PROXY_MODE','embedded') == 'embedded':
 deployment=json.loads(config_path.read_text())
 proxy=next(s for s in deployment['services'] if s['role']=='node-proxy')
 node=next(s for s in deployment['services'] if s['role']=='node-manager')
 node['env'].update(proxy['env'])
 node['config']['proxy_mode']='embedded'
 deployment['services']=[s for s in deployment['services'] if s['role']!='node-proxy']
 config_path.write_text(json.dumps(deployment,indent=2))
 print('embedded Node Proxy enabled in Node Manager',flush=True)
else:
 deployment=json.loads(config_path.read_text())
 node=next(s for s in deployment['services'] if s['role']=='node-manager')
 node['config']['proxy_mode']='standalone'
 config_path.write_text(json.dumps(deployment,indent=2))
 print('standalone Node Proxy explicitly enabled',flush=True)
run = RUN

d = json.loads((run / 'deployment.yaml').read_text())
allowed = {'node_id', 'listen', 'proxy_mode', 'proxy_socket', 'proxy_address', 'checkpoint_storage', 'checkpoint_gc'}
result = {'schema_version': d['schema_version'], 'package_dir': d['package_dir'], 'state_dir': d['state_dir'], 'environment': d['environment'], 'services': [
    {'id': s['id'], 'role': s['role'], 'config': {k: v for k, v in s.get('config', {}).items() if k in allowed}, 'environment_names': sorted(s.get('env', {}))} for s in d['services']]}
(EVIDENCE/'deployment-final.json').write_text(json.dumps(result, indent=2))
