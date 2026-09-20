"""Split an existing FC fixture into control and two isolated process nodes."""
import copy
import json
from pathlib import Path

HOST = '10.240.0.1'
ADDRESSES = {'node1':'10.240.0.11','node2':'10.240.0.12'}

def configure(root):
    root = Path(root)
    original = json.loads((root/'deployment.yaml').read_text())
    template = next(s for s in original['services'] if s['role']=='node-manager')
    if template['config'].get('proxy_mode') != 'embedded':
        raise ValueError('transfer fixture requires embedded Node Proxy')
    for name, address in ADDRESSES.items():
        folder = root/name
        folder.mkdir()
        def relocate(value):
            if isinstance(value, dict): return {k:relocate(v) for k,v in value.items()}
            if isinstance(value, list): return [relocate(v) for v in value]
            if isinstance(value, str) and str(root) in value and str(root/'secrets') not in value:
                return value.replace(str(root), str(folder))
            return value
        service = relocate(copy.deepcopy(template))
        service['id'] = name
        c = service['config']
        c.update(node_id=name, advertised_address=f'{address}:17001', proxy_address=f'{address}:18443')
        cert = 'node' if name=='node1' else 'node2'
        for key, suffix in [('certificate','pem'),('private_key','key')]:
            c['tls'][key] = str(root/'secrets/tls'/f'{cert}.{suffix}')
        c['checkpoint_storage']['endpoint'] = f'http://{HOST}:19090'
        c['checkpoint_gc']['min_age_seconds'] = 3600
        env = service['env']
        env['ADX_DATA_PLANE_NODE_PROXY_TLS_CERT'] = c['tls']['certificate']
        env['ADX_DATA_PLANE_NODE_PROXY_TLS_KEY'] = c['tls']['private_key']
        env['ADX_DATA_PLANE_ALLOWED_EDGE_CIDRS'] = f'{HOST}/32'
        env['ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS'] = f'10.231.{16 if name=="node1" else 32}.0/20'
        d = {**original, 'state_dir':str(folder/'state'), 'services':[service],
             'redis_url':original['redis_url'].replace('127.0.0.1',HOST)}
        path = folder/'deployment.yaml'
        path.write_text(json.dumps(d,indent=2)); path.chmod(0o600)
        backend = folder/'sandboxd'; backend.mkdir()
        for filename in ('oss.json','registry.json','oss_auths.json','registry_auths.json'):
            (backend/filename).write_bytes((root/'sandboxd'/filename).read_bytes())
        text = (root/'sandboxd/config.toml').read_text().replace(str(root),str(folder))
        if name=='node2': text=text.replace('10.231.16.0/20','10.231.32.0/20')
        text=text.replace('cgroup_root_name = "/adx-fc-',f'cgroup_root_name = "/{name}-adx-fc-')
        (backend/'config.toml').write_text(text)
    control = copy.deepcopy(original)
    control['services'] = [s for s in control['services'] if s['role'] not in ('node-manager','node-proxy')]
    master = next(s for s in control['services'] if s['role']=='master')['config']
    master.update(advertised_address=f'https://{HOST}:17000', heartbeat_timeout_seconds=12)
    (root/'deployment.yaml').write_text(json.dumps(control,indent=2))
    path=root/'registry.yaml';path.write_text(path.read_text().replace('127.0.0.1:5000','0.0.0.0:5000'))
