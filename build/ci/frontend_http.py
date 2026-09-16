#!/usr/bin/env python3
"""Public Go HTTPS -> Rust RPC regression; called by the isolated RPC fixture."""
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
    raise AssertionError('Go API did not listen')
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

# Real HTTPS -> authenticated Frontend -> mTLS Master -> Redis key lifecycle.
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


# Public affinity groups traverse Go -> protobuf -> Master -> Redis. Backend
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
