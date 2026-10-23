#!/usr/bin/env python3
"""Public Rust HTTPS -> Rust RPC regression; called by the isolated RPC fixture."""

import base64
import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

endpoint = os.environ['ADX_TEST_API_ENDPOINT']
context = ssl.create_default_context(cafile=str(Path(os.environ['ADX_TEST_TLS_DIR']) / 'ca.pem'))


def call(method_local, path_local, body_local=None, key='a' * 40, extra=None):
    headers = {'X-Auth': key, 'Content-Type': 'application/json'}
    headers.update(extra or {})
    req = urllib.request.Request(
        urllib.parse.urljoin(endpoint, path_local),
        data=None if body_local is None else json.dumps(body_local).encode(),
        headers=headers,
        method=method_local,
    )
    try:
        with urllib.request.urlopen(req, context=context, timeout=10) as response:
            payload_local = response.read()
            return response.status, json.loads(payload_local) if payload_local else None
    except urllib.error.HTTPError as error:
        return error.code, error.read()


for _ in range(100):
    try:
        code, _ = call('DELETE', '/api/environment/absent', key='wrong')
        if not (code == 401):
            raise AssertionError()
        break
    except urllib.error.URLError:
        time.sleep(0.05)
else:
    raise AssertionError('Rust API Server did not listen')
for _ in range(100):
    if call('DELETE', '/api/environment/absent')[0] == 404:
        break
    time.sleep(0.05)
else:
    raise AssertionError('Rust API Server Environment directory did not synchronize')
for invalid_key in ('z' * 40, 'c' * 40):
    code, _ = call('DELETE', '/api/environment/absent', key=invalid_key)
    if not (code == 401):
        raise AssertionError(code)
code, body = call(
    'POST',
    '/api/environment/v1/environments',
    {
        'name': 'http-case',
        'namespace': 't',
        'image': 'image',
        'cpu': 100,
        'memory': 1,
        'tenant': 'spoofed',
        'env': {'USER_VALUE': 'propagated'},
        'scheduleAffinities': [
            {'kind': 0, 'affinity': 2, 'labelOps': [{'type': 0, 'labelKey': 'NODE_ID', 'labelValues': ['node']}]}
        ],
    },
    extra={'X-Tenant-Id': 'spoofed'},
)
if not (code == 200):
    raise AssertionError((code, body))
payload = json.loads(base64.b64decode(body['data']))
if not (payload['environmentId'] == 't-http-case'):
    raise AssertionError(payload)
code, _ = call('DELETE', '/api/environment/t-http-case', key='b' * 40)
if not (code == 403):
    raise AssertionError(code)
# Target Node rechecks tenant ownership. A successful delete retires the public
# directory entry, so later requests observe absence.
code, body = call('DELETE', '/api/environment/t-http-case')
if code != 200:
    raise AssertionError((code, body))
code, body = call('DELETE', '/api/environment/t-http-case')
if code != 404:
    raise AssertionError((code, body))
code, body = call(
    'POST', '/api/environment/v1/environments/t-http-case/pause', {}, extra={'X-ADX-Request-ID': 'pause-http-case'}
)
if not (code == 404):
    raise AssertionError((code, body))
print(('Public HTTPS create, credential/tenant checks, deletion and retired-instance absence'))

# Real HTTPS -> authenticated API Server -> mTLS Coordinator -> Redis key lifecycle.
admin = 'd' * 40
code, current = call('GET', '/api/environment/v1/resources')
if not (code == 200 and current['items'][0]['status'] == 0):
    raise AssertionError((code, current))
code, compatible = call('GET', '/global-scheduler/resources')
if not (code == 200):
    raise AssertionError((code, compatible))
fragment = compatible['resource']['fragment']['node']
if not (fragment['status'] == 0 and fragment['capacity']['resources']['CPU']['scalar']['value'] > 0):
    raise AssertionError(compatible)
if not (call('GET', '/global-scheduler/scheduling_queue')[0] == 403):
    raise AssertionError()
code, waiting = call('GET', '/global-scheduler/scheduling_queue', key=admin)
if not (code == 200 and waiting == {'count': 0, 'instanceInfos': []}):
    raise AssertionError((code, waiting))
code, state = call('POST', '/global-scheduler/node/localschedulingstatus?node_id=node', key=admin)
if not (code == 200 and state == {'status': 'evicting', 'message': 'success'}):
    raise AssertionError((code, state))
for _ in range(40):
    code, paused = call('GET', '/global-scheduler/resources', key=admin)
    if code == 200 and paused['resource']['fragment']['node']['status'] == 1:
        break
    time.sleep(0.05)
else:
    raise AssertionError(('paused node did not remain in the resource directory', code, paused))
code, state = call('DELETE', '/global-scheduler/node/localschedulingstatus?node_id=node', key=admin)
if not (code == 200 and state == {'status': 'normal', 'message': 'success'}):
    raise AssertionError((code, state))
for _ in range(40):
    code, resumed = call('GET', '/global-scheduler/resources', key=admin)
    if code == 200 and resumed['resource']['fragment']['node']['status'] == 0:
        break
    time.sleep(0.05)
else:
    raise AssertionError(('resumed node did not reopen admission', code, resumed))
print('Scheduler resource alias, admin queue query and persistent node pause/resume passed')

code, _ = call('POST', '/api/admin/v1/keys', {'tenantId': 'managed'})
if not (code == 403):
    raise AssertionError(code)
code, body = call('POST', '/api/admin/v1/keys', {'tenantId': 'managed'}, key=admin)
if not (code == 201):
    raise AssertionError(code)
managed_key, key_id = body['apiKey'], body['key']['id']
if not (managed_key != key_id):
    raise AssertionError()
code, _ = call('GET', '/api/admin/v1/keys', key=managed_key)
if not (code == 403):
    raise AssertionError(code)  # Valid tenant identity, no administrator permission.
code, page = call('GET', '/api/admin/v1/keys?tenantId=managed', key=admin)
if not (code == 200 and len(page['items']) == 1):
    raise AssertionError()
if not (page['items'][0]['id'] == key_id and 'apiKey' not in page['items'][0]):
    raise AssertionError()
for _ in range(2):
    code, _ = call('DELETE', '/api/admin/v1/keys/' + key_id, key=admin)
    if not (code == 204):
        raise AssertionError(code)
# Existing ingress cache entries expire naturally; fixture TTL is one second.
for _ in range(40):
    code, _ = call('GET', '/api/admin/v1/keys', key=managed_key)
    if code == 401:
        break
    if not (code == 403):
        raise AssertionError(code)
    time.sleep(0.05)
else:
    raise AssertionError('revoked key remained valid beyond cache TTL')
code, _ = call('POST', '/api/admin/v1/keys', {'tenantId': 'managed', 'expiresAtUnixSeconds': 1}, key=admin)
if not (code == 400):
    raise AssertionError(code)
print(
    (
        'Public HTTPS tenant key creation, admin-only listing/revocat'
        'ion, repeated revoke, cache expiry and invalid expiry passed'
    )
)


# Public affinity groups traverse HTTP -> Environment RPC -> Coordinator -> Redis. Backend
# execution remains the RPC fixture; multi-node decisions have native tests.
def create_placement(name, labels=None, affinities=None):
    code_local, body_local = call(
        'POST',
        '/api/environment/v1/environments',
        {
            'name': name,
            'namespace': 't',
            'image': 'image',
            'cpu': 100,
            'memory': 1,
            'labels': labels or {},
            'scheduleAffinities': affinities or [],
        },
    )
    if not (code_local == 200):
        raise AssertionError((name, code_local, body_local))
    return json.loads(base64.b64decode(body_local['data']))['environmentId']


def affinity(kind, mode, key, value_local, **extra):
    return {
        'kind': kind,
        'affinity': mode,
        'labelOps': [{'type': 0, 'labelKey': key, 'labelValues': [value_local]}],
        **extra,
    }


peer = create_placement('http-peer', {'app': 'db'})
client = create_placement(
    'http-affinity',
    {'app': 'client'},
    [
        affinity(1, 2, 'app', 'absent'),
        affinity(1, 2, 'app', 'db'),
        affinity(0, 0, 'NODE_ID', 'node', preferredPriority=True),
        affinity(0, 0, 'NODE_ID', 'absent', preferredPriority=True),
        affinity(1, 1, 'app', 'forbidden', weight=7),
    ],
)
for identity in (client, peer):
    code, body = call('DELETE', '/api/environment/' + identity)
    if not (code == 200):
        raise AssertionError((code, body))
code, body = call(
    'POST',
    '/api/environment/v1/environments',
    {'name': 'bad-weight', 'image': 'image', 'scheduleAffinities': [affinity(0, 0, 'NODE_ID', 'node', weight=-1)]},
)
if not (code == 400):
    raise AssertionError((code, body))
print(('Public HTTPS peer OR, labels, weighted anti-affinity, ordered node preferences and validation passed'))

# JSON and SSE share the same HTTP compatibility contract, including replay.
request_id = 'http-replay-fixed'
request = {'name': 'http-replay', 'namespace': 't', 'image': 'image', 'cpu': 100, 'memory': 1}
created = []
for _ in range(2):
    code, value = call('POST', '/api/environment/v1/environments', request, extra={'X-Request-Id': request_id})
    if not (code == 200):
        raise AssertionError((code, value))
    created.append(json.loads(base64.b64decode(value['data']))['environmentId'])
if not (created[0] == created[1]):
    raise AssertionError()
code, _ = call(
    'POST',
    '/api/environment/v1/environments',
    dict(request, name='changed'),
    extra={'X-Request-Id': request_id},
)
if not (code == 409):
    raise AssertionError(code)
code, value = call('POST', '/api/environment/v1/environments', request, extra={'X-Request-Id': 'another-request'})
if not (code == 200):
    raise AssertionError((code, value))
if not (json.loads(base64.b64decode(value['data']))['environmentId'] == created[0]):
    raise AssertionError()
if not (call('DELETE', '/api/environment/' + created[0])[0] == 200):
    raise AssertionError()

request = urllib.request.Request(
    endpoint + '/api/environment/v1/environments',
    data=json.dumps({'name': 'http-stream', 'namespace': 't', 'image': 'image', 'cpu': 100, 'memory': 1}).encode(),
    headers={'X-Auth': 'a' * 40, 'Content-Type': 'application/json', 'Accept': 'text/event-stream'},
    method='POST',
)
with urllib.request.urlopen(request, context=context, timeout=10) as result:
    if not (result.headers['Content-Type'].startswith('text/event-stream')):
        raise AssertionError()
    events = result.read().decode()
if not (events.count('event: accepted') == 1 and events.count('event: final') == 1):
    raise AssertionError(events)
if '"status":"running"' not in events:
    raise AssertionError(events)
if not (call('DELETE', '/api/environment/t-http-stream')[0] == 200):
    raise AssertionError()

# Legacy public path retains its response field, without legacy internal messages.
code, value = call(
    'POST',
    '/api/environment/create',
    {'name': 'http-old-path', 'namespace': 't', 'runtime': 'rust', 'rootfs': 'image', 'cpu': 100, 'memory': 1},
)
if not (code == 200):
    raise AssertionError((code, value))
identity = json.loads(base64.b64decode(value['data']))['instance_id']
if not (identity == 't-http-old-path'):
    raise AssertionError()
if not (call('DELETE', '/api/environment/' + identity)[0] == 200):
    raise AssertionError()
print('HTTP request replay/conflict, named duplicate rejection, SSE and legacy path response passed')

# Real HTTPS lifecycle round trips with local checkpoint bytes and Redis metadata.
identity = create_placement('http-lifecycle')
pause = '/api/environment/v1/environments/' + identity + '/pause'
for request_id, value in [('pause-invalid', {'timeoutSeconds': -1}), ('pause-invalid-name', {'name': '   '})]:
    if not (call('POST', pause, value, extra={'X-ADX-Request-ID': request_id})[0] == 400):
        raise AssertionError()
for _ in range(2):
    code, value = call(
        'POST',
        pause,
        {'retention': {'kind': 'ttl', 'ttlSeconds': 120}, 'timeoutSeconds': 10},
        extra={'X-ADX-Request-ID': 'pause-http-positive'},
    )
    if not (code == 200):
        raise AssertionError((code, value))
    value = json.loads(base64.b64decode(value['data']))
    if not (value['state'] == 'paused' and value['snapshotId'] == 'pause-http-positive'):
        raise AssertionError(value)
for _ in range(2):
    code, value = call(
        'POST',
        '/api/environment/v1/environments/' + identity + '/resume',
        {},
        extra={'X-ADX-Request-ID': 'resume-http-positive'},
    )
    if not (code == 200):
        raise AssertionError((code, value))
    value = json.loads(base64.b64decode(value['data']))
    if not (value['state'] == 'running' and value['nodeId'] == 'node'):
        raise AssertionError(value)
code, value = call(
    'POST',
    '/api/environment/v1/environments/' + identity + '/snapshots',
    {'name': 'http-saved', 'timeoutSeconds': 10},
    extra={'X-ADX-Request-ID': 'snapshot-http-positive'},
)
if not (code == 200):
    raise AssertionError((code, value))
snapshot = json.loads(base64.b64decode(value['data']))['snapshotId']
code, value = call('GET', '/api/environment/v1/snapshots/' + snapshot)
if not (code == 200 and json.loads(base64.b64decode(value['data']))['names'] == ['http-saved']):
    raise AssertionError((code, value))
if not (call('GET', '/api/environment/v1/snapshots/' + snapshot, key='b' * 40)[0] == 403):
    raise AssertionError()
code, value = call('GET', '/api/environment/v1/snapshots?name=http-saved')
if not (code == 200 and len(json.loads(base64.b64decode(value['data']))['items']) == 1):
    raise AssertionError((code, value))
code, value = call(
    'POST', '/api/environment/v1/environments', {'name': 'http-clone', 'namespace': 't', 'snapshotId': snapshot}
)
if not (code == 200):
    raise AssertionError((code, value))
clone = json.loads(base64.b64decode(value['data']))['environmentId']
if not (clone != identity):
    raise AssertionError()
if not (call('DELETE', '/api/environment/' + clone)[0] == 200):
    raise AssertionError()
if not (call('DELETE', '/api/environment/' + identity)[0] == 200):
    raise AssertionError()
if not (
    call(
        'DELETE',
        f'/api/environment/v1/snapshots/{snapshot}',
        extra={'X-ADX-Request-ID': 'delete-snapshot-http-positive'},
    )[0]
    == 200
):
    raise AssertionError()
print(('HTTPS pause/resume retries, snapshot create/get/list/delete, tenant isolation and clone passed'))

# Agent streaming remains an upper-layer service, reached through the API boundary.
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from threading import Thread


class AgentHandler(BaseHTTPRequestHandler):
    protocol_version = 'HTTP/1.1'

    def log_message(self, *args):
        pass

    def handle_request(self):
        data = b''
        if self.headers.get('Transfer-Encoding', '').lower() == 'chunked':
            while True:
                size = int(self.rfile.readline().strip(), 16)
                if size == 0:
                    self.rfile.readline()
                    break
                data += self.rfile.read(size)
                if not (self.rfile.read(2) == b'\r\n'):
                    raise AssertionError()
        else:
            data = self.rfile.read(int(self.headers.get('Content-Length', 0)))
        result_local = json.dumps(
            {
                'tenant': self.headers.get('X-Tenant-Id'),
                'path': self.path,
                'method': self.command,
                'body': data.decode(),
                'role': self.headers.get('X-ADX-Role'),
            }
        ).encode()
        self.send_response(200)
        self.send_header('Content-Type', 'application/json')
        self.send_header('Transfer-Encoding', 'chunked')
        self.end_headers()
        for chunk in [result_local[:5], result_local[5:]]:
            self.wfile.write(('%x\r\n' % len(chunk)).encode() + chunk + b'\r\n')
            self.wfile.flush()
        self.wfile.write(b'0\r\n\r\n')

    do_GET = do_POST = do_DELETE = handle_request


server = ThreadingHTTPServer(('127.0.0.1', int(os.environ['ADX_TEST_AGENT_PORT'])), AgentHandler)
Thread(target=server.serve_forever, daemon=True).start()
try:
    for method, path in [
        ('GET', '/api/agent'),
        ('POST', '/api/agent'),
        ('GET', '/api/agent/a'),
        ('DELETE', '/api/agent/a'),
        ('POST', '/api/agent/a/invoke'),
        ('POST', '/api/agent/a/files/upload'),
        ('POST', '/api/agent/a/files/mkdir'),
        ('GET', '/api/agent/a/files/download'),
        ('GET', '/api/agent/a/files/list'),
    ]:
        code, value = call(
            method,
            path + '?case=forward',
            {'input': 'unchanged'} if method == 'POST' else None,
            extra={'X-Tenant-Id': 'forged', 'X-ADX-Role': 'admin'},
        )
        if not (code == 200 and value['tenant'] == 'tenant' and value['role'] is None):
            raise AssertionError((code, value))
        if not (value['method'] == method and value['path'] == path + '?case=forward'):
            raise AssertionError(value)
        if method == 'POST':
            if not (json.loads(value['body']) == {'input': 'unchanged'}):
                raise AssertionError(value)
    print('Nine Agent routes: chunked forwarding, verified tenant, method/query/body preservation passed')
finally:
    server.shutdown()
    server.server_close()

# HTTP paths decode escaped instance IDs exactly once.
from urllib.parse import quote

identity = create_placement('http space+percent%')
code, value = call('DELETE', '/api/environment/' + quote(identity, safe=''))
if not (code == 200):
    raise AssertionError((code, value))
print('Escaped instance ID deletion passed')

# Exercise the public wire protocols against the real Coordinator/Redis fixture.
import uuid

code, baseline = call('GET', '/api/environment/v1/resources')
if code != 200:
    raise AssertionError((code, baseline))
workers = baseline['items']
max_cpu = max(worker['capacity']['CPU'] for worker in workers)
max_memory = max(worker['capacity']['Memory'] for worker in workers)
for resource, reason in [('cpu', 'insufficient_cpu'), ('memory', 'insufficient_memory')]:
    for sse in (False, True):
        case_name = 'http-' + resource + ('-sse' if sse else '-json')
        request_id = 'create-' + str(uuid.uuid4())
        request_body = {
            'namespace': 't', 'name': case_name, 'image': 'image',
            'cpu': max_cpu + 1 if resource == 'cpu' else 100,
            'memory': max_memory + 1 if resource == 'memory' else 1,
            'scheduleTimeoutSeconds': 1, 'createTimeoutSeconds': 61,
        }
        request_headers = {'Accept': 'text/event-stream' if sse else 'application/json',
                           'X-Request-Id': request_id}
        started = time.monotonic()
        if sse:
            request = urllib.request.Request(
                urllib.parse.urljoin(endpoint, '/api/environment/v1/environments'),
                data=json.dumps(request_body).encode(), method='POST',
                headers={**request_headers, 'Content-Type': 'application/json', 'X-Auth': 'a' * 40},
            )
            with urllib.request.urlopen(request, context=context, timeout=10) as stream_response:
                if stream_response.status != 200 or 'text/event-stream' not in stream_response.headers['Content-Type']:
                    raise AssertionError((stream_response.status, stream_response.headers))
                stream = stream_response.read().decode()
            finals = [frame for frame in stream.split('\n\n') if frame.startswith('event: final\n')]
            if len(finals) != 1:
                raise AssertionError(stream)
            payload = json.loads(finals[0].split('data: ', 1)[1])
            if payload['errorCode'] != 504 or payload['status'] != 'timeout':
                raise AssertionError(payload)
        else:
            code, payload = call('POST', '/api/environment/v1/environments', request_body, extra=request_headers)
            if code != 504:
                raise AssertionError((code, payload))
            payload = json.loads(payload)
        elapsed = time.monotonic() - started
        if not 0.9 <= elapsed < 6:
            raise AssertionError(('queue wait duration', elapsed))
        detail = payload['error']
        if (detail['code'], detail['retry'], detail['outcome']) != ('DEADLINE_EXCEEDED', 'never', 'not_started'):
            raise AssertionError(payload)
        if detail['requestId'] != request_id or detail['environmentId'] != 't-' + case_name:
            raise AssertionError(payload)
        diagnosis = detail['schedulingFailure']
        if diagnosis['rejections'].get(reason, 0) < 1 or diagnosis['eligibleNodes'] != 0:
            raise AssertionError(payload)
        if diagnosis['maxCapacity']['cpu_millis'] != max_cpu:
            raise AssertionError(payload)
code, after = call('GET', '/api/environment/v1/resources')
if code != 200 or after != baseline:
    raise AssertionError(('resource baseline changed', baseline, after))
code, queue = call('GET', '/global-scheduler/scheduling_queue', key=admin)
if code != 200 or queue != {'count': 0, 'instanceInfos': []}:
    raise AssertionError((code, queue))
print('Raw JSON/SSE CPU and memory non-fit: bounded wait, diagnosis, known cancellation and resource baseline passed')
