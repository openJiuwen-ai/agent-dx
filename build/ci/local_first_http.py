#!/usr/bin/env python3
"""Real API HTTPS/local Node RPC contract; backend supplied by the RPC fixture."""
import base64
from concurrent.futures import ThreadPoolExecutor
import json
import os
from pathlib import Path
import ssl
import time
import urllib.error
import urllib.request

endpoint = os.environ['ADX_TEST_API_ENDPOINT']
context = ssl.create_default_context(cafile=str(Path(os.environ['ADX_TEST_TLS_DIR']) / 'ca.pem'))

def call(method, path, body=None, request_id='probe', key='a' * 40):
    headers = {'X-Auth': key, 'Content-Type': 'application/json', 'X-Request-Id': request_id}
    request = urllib.request.Request(endpoint + path, method=method, headers=headers,
        data=None if body is None else json.dumps(body).encode())
    try:
        with urllib.request.urlopen(request, context=context, timeout=10) as response:
            return response.status, json.loads(response.read())
    except urllib.error.HTTPError as error:
        return error.code, error.read().decode()

def create(name, request_id, **extra):
    return call('POST', '/api/sandbox/v1/sandboxes', {
        'name': name, 'namespace': 'lf', 'image': 'image', 'cpu': 100, 'memory': 1, **extra
    }, request_id)

def identity(result):
    status, value = result
    assert status == 200, result
    return json.loads(base64.b64decode(value['data']))['sandboxId']

for _ in range(100):
    try:
        assert call('DELETE', '/api/sandbox/absent', key='wrong')[0] == 401
        break
    except urllib.error.URLError:
        time.sleep(.05)
else:
    raise AssertionError('API Server did not listen')
# Readiness requires both the Environment directory and the local-first node view.
for _ in range(100):
    if call('DELETE', '/api/sandbox/absent')[0] == 404:
        break
    time.sleep(.05)
else:
    raise AssertionError('API Server directories did not synchronize')
# Rust additionally checks owners: Pack would select a twice, whereas local
# round-robin selects b.
time.sleep(1.1)
first = identity(create('first', 'first'))
second = identity(create('second', 'second'))
with ThreadPoolExecutor(max_workers=4) as executor:
    race = list(executor.map(lambda n: identity(create('race', 'race-' + str(n))), range(4)))
assert set(race) == {'lf-race'}, race
assert identity(create('race', 'another-client')) == 'lf-race'
assert create('race', 'changed', image='other-image')[0] == 409
assert call('POST', '/api/sandbox/v1/sandboxes', {
    'name': 'race', 'namespace': 'lf', 'image': 'image', 'cpu': 100, 'memory': 1,
}, 'other-tenant', key='b' * 40)[0] == 409
for instance in (first, second, 'lf-race'):
    assert call('DELETE', '/api/sandbox/' + instance, request_id='delete-' + instance)[0] == 200
print('PASS HTTPS local-first: round-robin, four concurrent creates, same-ID replay, spec/tenant conflicts, deletion')
