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
            return response.status, json.load(response)
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
assert code == 501, (code, body)
print('Public HTTPS create, credential/tenant checks, repeated delete, unsupported pause passed')
