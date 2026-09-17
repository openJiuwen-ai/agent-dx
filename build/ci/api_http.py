#!/usr/bin/env python3
"""Public Rust HTTPS -> Rust RPC regression; called by the isolated RPC fixture."""
import base64
import json
import os
from pathlib import Path
import ssl
import time
import urllib.error
import urllib.request

endpoint = os.environ['ADX_TEST_API_ENDPOINT']
context = ssl.create_default_context(cafile=str(Path(os.environ['ADX_TEST_TLS_DIR']) / 'ca.pem'))

def call(method, path, body=None, key='a' * 40, extra=None):
    headers = {'X-Auth': key, 'Content-Type': 'application/json'}
    headers.update(extra or {})
    req = urllib.request.Request(endpoint + path, data=None if body is None else json.dumps(body).encode(), headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=context, timeout=10) as response:
            payload = response.read()
            return response.status, json.loads(payload) if payload else None
    except urllib.error.HTTPError as error:
        return error.code, error.read()

for _ in range(100):
    try:
        code, _ = call('DELETE', '/api/sandbox/absent', key='wrong')
        assert code == 401
        break
    except urllib.error.URLError:
        time.sleep(.05)
else:
    raise AssertionError('Rust API Server did not listen')
for invalid_key in ('z' * 40, 'c' * 40):
    code, _ = call('DELETE', '/api/sandbox/absent', key=invalid_key)
    assert code == 401, code
code, body = call('POST', '/api/sandbox/v1/sandboxes', {
    'name': 'http-case', 'namespace': 't', 'image': 'image', 'cpu': 100, 'memory': 1,
    'tenant': 'spoofed', 'env': {'USER_VALUE': 'propagated'},
    'scheduleAffinities': [{'kind': 0, 'affinity': 2, 'labelOps': [
        {'type': 0, 'labelKey': 'NODE_ID', 'labelValues': ['node']}
    ]}],
}, extra={'X-Tenant-Id': 'spoofed'})
assert code == 200, (code, body)
payload = json.loads(base64.b64decode(body['data']))
assert payload['sandboxId'] == 't-http-case', payload
code, _ = call('DELETE', '/api/sandbox/t-http-case', key='b' * 40)
assert code == 403, code
# Target Node rechecks tenant ownership; valid deletion then succeeds twice.
for _ in range(2):
    code, body = call('DELETE', '/api/sandbox/t-http-case')
    assert code == 200, (code, body)
code, body = call('POST', '/api/sandbox/v1/sandboxes/t-http-case/pause', {}, extra={'X-ADX-Request-ID':'pause-http-case'})
assert code == 409, (code, body)
print('Public HTTPS create, credential/tenant checks, repeated delete, pause on deleted instance rejected')

# Real HTTPS -> authenticated API Server -> mTLS Master -> Redis key lifecycle.
admin = 'd' * 40
code, _ = call('POST', '/api/admin/v1/keys', {'tenantId': 'managed'})
assert code == 403, code
code, body = call('POST', '/api/admin/v1/keys', {'tenantId': 'managed'}, key=admin)
assert code == 201, code
managed_key, key_id = body['apiKey'], body['key']['id']
assert managed_key != key_id
code, _ = call('GET', '/api/admin/v1/keys', key=managed_key)
assert code == 403, code  # Valid tenant identity, no administrator permission.
code, page = call('GET', '/api/admin/v1/keys?tenantId=managed', key=admin)
assert code == 200 and len(page['items']) == 1
assert page['items'][0]['id'] == key_id and 'apiKey' not in page['items'][0]
for _ in range(2):
    code, _ = call('DELETE', '/api/admin/v1/keys/' + key_id, key=admin)
    assert code == 204, code
# Existing ingress cache entries expire naturally; fixture TTL is one second.
for _ in range(40):
    code, _ = call('GET', '/api/admin/v1/keys', key=managed_key)
    if code == 401:
        break
    assert code == 403, code
    time.sleep(.05)
else:
    raise AssertionError('revoked key remained valid beyond cache TTL')
code, _ = call('POST', '/api/admin/v1/keys', {'tenantId': 'managed', 'expiresAtUnixSeconds': 1}, key=admin)
assert code == 400, code
print('Public HTTPS tenant key creation, admin-only listing/revocation, repeated revoke, cache expiry and invalid expiry passed')


# Public affinity groups traverse HTTP -> Instance RPC -> Master -> Redis. Backend
# execution remains the RPC fixture; multi-node decisions have native tests.
def create_placement(name, labels=None, affinities=None):
    code, body = call('POST','/api/sandbox/v1/sandboxes', {
        'name':name,'namespace':'t','image':'image','cpu':100,'memory':1,
        'labels':labels or {},'scheduleAffinities':affinities or [],
    })
    assert code == 200, (name,code,body)
    return json.loads(base64.b64decode(body['data']))['sandboxId']
def affinity(kind, mode, key, value, **extra):
    return {'kind':kind,'affinity':mode,'labelOps':[{'type':0,'labelKey':key,'labelValues':[value]}],**extra}
peer=create_placement('http-peer',{'app':'db'})
client=create_placement('http-affinity',{'app':'client'},[
    affinity(1,2,'app','absent'),affinity(1,2,'app','db'),
    affinity(0,0,'NODE_ID','node',preferredPriority=True),
    affinity(0,0,'NODE_ID','absent',preferredPriority=True),
    affinity(1,1,'app','forbidden',weight=7),
])
for identity in (client,peer):
    code,body=call('DELETE','/api/sandbox/'+identity)
    assert code==200,(code,body)
code,body=call('POST','/api/sandbox/v1/sandboxes',{'name':'bad-weight','image':'image','scheduleAffinities':[affinity(0,0,'NODE_ID','node',weight=-1)]})
assert code==400,(code,body)
print('Public HTTPS peer OR, labels, weighted anti-affinity, ordered node preferences and validation passed')

# JSON and SSE share the same HTTP compatibility contract, including replay.
request_id='http-replay-fixed'
request={'name':'http-replay','namespace':'t','image':'image','cpu':100,'memory':1}
created=[]
for _ in range(2):
    code,value=call('POST','/api/sandbox/v1/sandboxes',request,extra={'X-Request-Id':request_id})
    assert code==200,(code,value)
    created.append(json.loads(base64.b64decode(value['data']))['sandboxId'])
assert created[0]==created[1]
code,_=call('POST','/api/sandbox/v1/sandboxes',dict(request,name='changed'),extra={'X-Request-Id':request_id})
assert code==409,code
code,_=call('POST','/api/sandbox/v1/sandboxes',request,extra={'X-Request-Id':'another-request'})
assert code==409,code
assert call('DELETE','/api/sandbox/'+created[0])[0]==200

request=urllib.request.Request(endpoint+'/api/sandbox/v1/sandboxes',data=json.dumps({'name':'http-stream','namespace':'t','image':'image','cpu':100,'memory':1}).encode(),headers={'X-Auth':'a'*40,'Content-Type':'application/json','Accept':'text/event-stream'},method='POST')
with urllib.request.urlopen(request,context=context,timeout=10) as result:
    assert result.headers['Content-Type'].startswith('text/event-stream')
    events=result.read().decode()
assert events.count('event: accepted')==1 and events.count('event: final')==1,events
assert '"status":"running"' in events,events
assert call('DELETE','/api/sandbox/t-http-stream')[0]==200

# Legacy public path retains its response field, without legacy internal messages.
code,value=call('POST','/api/sandbox/create',{'name':'http-old-path','namespace':'t','runtime':'rust','rootfs':'image','cpu':100,'memory':1})
assert code==200,(code,value)
identity=json.loads(base64.b64decode(value['data']))['instance_id']
assert identity=='t-http-old-path'
assert call('DELETE','/api/sandbox/'+identity)[0]==200
print('HTTP request replay/conflict, named duplicate rejection, SSE and legacy path response passed')

# Real HTTPS lifecycle round trips with local checkpoint bytes and Redis metadata.
identity=create_placement('http-lifecycle')
pause='/api/sandbox/v1/sandboxes/'+identity+'/pause'
for request_id, value in [('pause-invalid', {'timeoutSeconds':-1}), ('pause-invalid-name', {'name':'   '})]:
    assert call('POST',pause,value,extra={'X-ADX-Request-ID':request_id})[0]==400
for _ in range(2):
    code,value=call('POST',pause,{'ttlSeconds':120,'timeoutSeconds':10},extra={'X-ADX-Request-ID':'pause-http-positive'})
    assert code==200,(code,value)
    value=json.loads(base64.b64decode(value['data']))
    assert value['state']=='paused' and value['snapshotId']=='pause-http-positive',value
for _ in range(2):
    code,value=call('POST','/api/sandbox/v1/sandboxes/'+identity+'/resume',{},extra={'X-ADX-Request-ID':'resume-http-positive'})
    assert code==200,(code,value)
    value=json.loads(base64.b64decode(value['data']))
    assert value['state']=='running' and value['nodeId']=='node',value
code,value=call('POST','/api/sandbox/v1/sandboxes/'+identity+'/snapshots',{'name':'http-saved','timeoutSeconds':10},extra={'X-ADX-Request-ID':'snapshot-http-positive'})
assert code==200,(code,value)
snapshot=json.loads(base64.b64decode(value['data']))['snapshotId']
code,value=call('GET','/api/sandbox/v1/snapshots/'+snapshot)
assert code==200 and json.loads(base64.b64decode(value['data']))['names']==['http-saved'],(code,value)
assert call('GET','/api/sandbox/v1/snapshots/'+snapshot,key='b'*40)[0]==403
code,value=call('GET','/api/sandbox/v1/snapshots?name=http-saved')
assert code==200 and len(json.loads(base64.b64decode(value['data']))['items'])==1,(code,value)
code,value=call('POST','/api/sandbox/v1/sandboxes',{'name':'http-clone','namespace':'t','snapshotId':snapshot})
assert code==200,(code,value)
clone=json.loads(base64.b64decode(value['data']))['sandboxId']
assert clone!=identity
assert call('DELETE','/api/sandbox/'+clone)[0]==200
assert call('DELETE','/api/sandbox/'+identity)[0]==200
assert call('DELETE','/api/sandbox/v1/snapshots/'+snapshot,extra={'X-ADX-Request-ID':'delete-snapshot-http-positive'})[0]==200
print('HTTPS pause/resume retries, snapshot create/get/list/delete, tenant isolation and clone passed')

# Agent streaming remains an upper-layer service, reached through the API boundary.
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread
class AgentHandler(BaseHTTPRequestHandler):
    protocol_version='HTTP/1.1'
    def log_message(self,*args): pass
    def handle_request(self):
        data=b''
        if self.headers.get('Transfer-Encoding','').lower()=='chunked':
            while True:
                size=int(self.rfile.readline().strip(),16)
                if size==0:
                    self.rfile.readline()
                    break
                data+=self.rfile.read(size)
                assert self.rfile.read(2)==b'\r\n'
        else:
            data=self.rfile.read(int(self.headers.get('Content-Length',0)))
        result=json.dumps({'tenant':self.headers.get('X-Tenant-Id'),'path':self.path,'method':self.command,'body':data.decode(),'role':self.headers.get('X-ADX-Role')}).encode()
        self.send_response(200)
        self.send_header('Content-Type','application/json')
        self.send_header('Transfer-Encoding','chunked')
        self.end_headers()
        for chunk in [result[:5],result[5:]]:
            self.wfile.write(('%x\r\n'%len(chunk)).encode()+chunk+b'\r\n')
            self.wfile.flush()
        self.wfile.write(b'0\r\n\r\n')
    do_GET=do_POST=do_DELETE=handle_request
server=ThreadingHTTPServer(('127.0.0.1',int(os.environ['ADX_TEST_AGENT_PORT'])),AgentHandler)
Thread(target=server.serve_forever,daemon=True).start()
try:
    for method,path in [('GET','/api/agent'),('POST','/api/agent'),('GET','/api/agent/a'),('DELETE','/api/agent/a'),('POST','/api/agent/a/invoke'),('POST','/api/agent/a/files/upload'),('POST','/api/agent/a/files/mkdir'),('GET','/api/agent/a/files/download'),('GET','/api/agent/a/files/list')]:
        code,value=call(method,path+'?case=forward',{'input':'unchanged'} if method=='POST' else None,extra={'X-Tenant-Id':'forged','X-ADX-Role':'admin'})
        assert code==200 and value['tenant']=='tenant' and value['role'] is None,(code,value)
        assert value['method']==method and value['path']==path+'?case=forward',value
        if method=='POST': assert json.loads(value['body'])=={'input':'unchanged'},value
    print('Nine Agent routes: chunked forwarding, verified tenant, method/query/body preservation passed')
finally:
    server.shutdown()
    server.server_close()

# HTTP paths decode escaped instance IDs exactly once.
from urllib.parse import quote
identity=create_placement('http space+percent%')
code,value=call('DELETE','/api/sandbox/'+quote(identity,safe=''))
assert code==200,(code,value)
print('Escaped instance ID deletion passed')
