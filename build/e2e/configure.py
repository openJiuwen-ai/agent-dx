#!/usr/bin/env python3
import argparse
import json
import os
import pathlib
import secrets
import shutil
import socket
import subprocess
import re
from cgroup_limits import v2_directory


def parse_args():
    parser = argparse.ArgumentParser(description="Render one ADX E2E node configuration")
    parser.add_argument("node", choices=("node1", "node2"))
    return parser.parse_args()


BASE=pathlib.Path('/opt/adx'); PRIVATE=pathlib.Path('/secrets'); EVIDENCE=pathlib.Path('/evidence')
P=pathlib.Path('/tmp/adx-e2e'); P.mkdir(exist_ok=True)
node=parse_args().node; T=PRIVATE/'tls'
if not T.exists():
 subprocess.run(['python3','/opt/adx/e2e/rpc_certificates.py',str(T)],check=True)
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
admin=PRIVATE/'admin-key'
if not admin.exists(): admin.write_text(secrets.token_hex(32)); admin.chmod(0o600)
def tls(n,peers):return {'ca':str(T/'ca.pem'),'certificate':str(T/f'{n}.pem'),'private_key':str(T/f'{n}.key'),'server_name':'localhost','peers':{k:str(T/f'{v}.der') for k,v in peers.items()}}
cgroup_root=os.getenv('ADX_E2E_CGROUP_ROOT',f'adx-e2e-{node}')
if not re.fullmatch(r'adx-e2e-[A-Za-z0-9_-]+',cgroup_root):raise ValueError('invalid test cgroup root')
R=P/'sandboxd'; R.mkdir(exist_ok=True)
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
cgroup_root_name = "/{cgroup_root}"
max_instance_num = 8
pids_max = 256
[plugin.runtime]
image_lib_dir = "{R}/images"
filestore_dir = "{R}/filestore"
filestore_dir_size = "1G"
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
distill_fs_bin = "/usr/local/bin/distill_fs"
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
if (cg/'cgroup.controllers').exists():
 cg=v2_directory(cg,pathlib.Path('/proc/self/cgroup').read_text())
 quota,period=(cg/'cpu.max').read_text().split()
 cpus=int(quota)*1000//int(period) if quota!='max' else os.cpu_count()*1000
 limit=(cg/'memory.max').read_text().strip()
 memory=int(limit) if limit!='max' else os.sysconf('SC_PAGE_SIZE')*os.sysconf('SC_PHYS_PAGES')
else:
 quota=int((cg/'cpu/cpu.cfs_quota_us').read_text());period=int((cg/'cpu/cpu.cfs_period_us').read_text())
 cpus=quota*1000//period if quota>0 else os.cpu_count()*1000
 memory=int((cg/'memory/memory.limit_in_bytes').read_text())
capacity={'cpu_millis':min(cpus,2000),'memory_bytes':min(memory//2,1536*1024**2),'disk_bytes':min(shutil.disk_usage(P).free//4,2*1024**3)}
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
 add('coordinator','coordinator',{'listen':'0.0.0.0:17000','metrics_listen':'127.0.0.1:17090','advertised_address':'https://coordinator:17000','scheduler_shards':1,'placement':'spread','rpc_timeout_seconds':120,'tls':tls('coordinator',{'apiserver':'apiserver','ingress':'ingress','node:node1':'node','node:node2':'node2'}),'bootstrap_credentials':[{'key_file':str(admin),'tenant_id':'admin','administrator':True,'expires_at_unix_seconds':0},{'key_file':str(key),'tenant_id':'e2e','administrator':False,'expires_at_unix_seconds':0},{'key_file':str(other),'tenant_id':'e2e-other','administrator':False,'expires_at_unix_seconds':0}]})
ingress_peer=os.getenv('ADX_E2E_INGRESS_IP')
ingress_cidrs=(ingress_peer+('/128' if ':' in ingress_peer else '/32')+',127.0.0.1/32') if ingress_peer else '172.16.0.0/12,127.0.0.1/32'
common={'ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE':'mtls','RUST_LOG':'info','ADX_LOG_FORMAT':'json'}
np={**common,'ADX_DATA_PLANE_RELAY_BIND':'0.0.0.0:18443','ADX_DATA_PLANE_RELAY_HEALTH_BIND':'127.0.0.1:19443','ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS':f'10.231.{16 if node=="node1" else 32}.0/20','ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS':ingress_cidrs,'ADX_DATA_PLANE_RELAY_TLS_CERT':str(T/('node.pem' if node=='node1' else 'node2.pem')),'ADX_DATA_PLANE_RELAY_TLS_KEY':str(T/('node.key' if node=='node1' else 'node2.key')),'ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA':str(T/'ca.pem'),'ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR':str(P/'proxy')}
host=os.getenv('ADX_E2E_NODE_IP') or ('coordinator' if node=='node1' else 'node2')
if ':' in host: host='['+host+']'
add(node,'adxlet',{'node_id':node,'listen':'0.0.0.0:17001','metrics_listen':'0.0.0.0:17091','advertised_address':f'{host}:17001','proxy_address':f'{host}:18443','tls':tls('node' if node=='node1' else 'node2',{'coordinator':'coordinator','apiserver':'apiserver'}),'sandboxd_socket':str(R/'sandboxd.sock'),'proxy_socket':str(P/'proxy/route.sock'),'capacity_file':str(P/'capacity.json'),'report_interval_seconds':2,'rpc_timeout_seconds':120,'execd_port':50090,'execd_command':['/usr/local/bin/adx-execd'],'execd_env':{'ADX_TRACE_ENABLED':'true','OTEL_EXPORTER_OTLP_TRACES_ENDPOINT':f'http://{socket.gethostbyname(host)}:14317/v1/traces','OTEL_BSP_SCHEDULE_DELAY':'200'}},np)
if node=='node1':
 add('api','apiserver',{'listen':'127.0.0.1:8888','loopback_http':True,'discovery':{'poll_seconds':1},'ca':str(T/'ca.pem'),'certificate':str(T/'apiserver.pem'),'private_key':str(T/'apiserver.key'),'server_name':'localhost','rpc_timeout_seconds':120,'cache_entries':1000,'auth_cache_ttl_seconds':10})
 ee={**common,'ADX_DATA_PLANE_INGRESS_TLS_BIND':'0.0.0.0:8443','ADX_DATA_PLANE_INGRESS_PLAIN_BIND':'127.0.0.1:8080','ADX_DATA_PLANE_INGRESS_HEALTH_BIND':'127.0.0.1:18080','ADX_DATA_PLANE_INGRESS_TLS_CERT':str(T/'ingress.pem'),'ADX_DATA_PLANE_INGRESS_TLS_KEY':str(T/'ingress.key'),'ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS':'127.0.0.1/32','ADX_DATA_PLANE_INGRESS_NODE_TLS_CA':str(T/'ca.pem'),'ADX_DATA_PLANE_INGRESS_NODE_TLS_SERVER_NAME':'localhost','ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_CERT':str(T/'ingress.pem'),'ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_KEY':str(T/'ingress.key'),'ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS':'127.0.0.1:8888'}
 add('ingress','ingress',{'tls':tls('ingress',{'coordinator':'coordinator'}),'rpc_timeout_seconds':5,'refresh_seconds':1,'auth_cache_seconds':10,'auth_cache_entries':1000},ee)
for service in services:
 service.setdefault('env',{}).update({'ADX_LOG_FORMAT':'json','ADX_TRACE_ENABLED':'true','OTEL_EXPORTER_OTLP_TRACES_ENDPOINT':'http://127.0.0.1:14317/v1/traces','OTEL_BSP_SCHEDULE_DELAY':'200'})
d={'schema_version':1,'logging':{'enabled':True,'max_file_bytes':4096,'rotate_seconds':1,'compress':True,'line_records':True,'max_record_bytes':65536,'compress_after_seconds':1,'max_files':100,'max_age_seconds':3600,'max_total_bytes':1048576},'package_dir':str(BASE/'package'),'state_dir':str(P/'state'),'redis_url':f'redis://:{redis_key.read_text().strip()}@coordinator:6379/','namespace':'acceptance','restart_limit':3,'restart_delay_ms':1000,'stop_timeout_seconds':30,'services':services}
runtime_artifact=BASE/'package/runtime/adx-runtime-rootfs.img'
runtime_image=PRIVATE/'runtime-image'
if os.getenv('ADX_E2E_KUBERNETES'):
 if not runtime_image.is_file(): raise RuntimeError('Kubernetes OCI runtime image is missing')
 image=runtime_image.read_text().strip()
 if '@sha256:' not in image: raise RuntimeError('Kubernetes OCI runtime image must be digest pinned')
 d['runtime_profile']={
  'rootfs':{'runtime_class':'runc','type':'image','image':image,'readonly':False},
  'bootstrap':{'type':'image','image':image,'target':'/__adx',
    'entrypoint':['/__adx/usr/local/bin/adx-execd']},
  'env':{'PATH':'/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'}}
elif runtime_artifact.is_file():
 d['runtime_profile']={
  'rootfs':{'runtime_class':'runc','type':'local','path':str(runtime_artifact),'readonly':False},
  'bootstrap':{'type':'erofs','root':str(runtime_artifact),'target':'/__adx',
    'entrypoint':['/__adx/usr/local/bin/adx-execd']},
  'env':{'PATH':'/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'}}
(P/'deployment.yaml').write_text(json.dumps(d));(P/'deployment.yaml').chmod(0o600)
(EVIDENCE/f'deployment-{node}.json').write_text(json.dumps({**d,'redis_url':'redis://:REDACTED@coordinator:6379/'},indent=2))
print('configured',node,capacity)
