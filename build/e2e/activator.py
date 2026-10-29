#!/usr/bin/env python3
"""Real Redis/Platform/sandboxd acceptance through two public Agent entrypoints."""

import concurrent.futures
import json
import os
import shutil
import ssl
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

CASES = (
    'publish-template',
    'tenant-isolation',
    'concurrent-first-traffic',
    'persisted-single-execution',
    'execd-command-and-files',
    'resolve-and-refresh',
    'restart-preserves-execution',
    'delete-and-recreate',
    'physical-and-metadata-cleanup',
)


def verify(result):
    ids = result.get('environment_ids', [])
    if result.get('status') != 'passed' or result.get('mode') not in ('standalone', 'embedded'):
        raise ValueError('incomplete Activator deployment evidence: execution status')
    cases = result.get('cases', [])
    if [case.get('id') for case in cases] != list(CASES) or any(case.get('status') != 'passed' for case in cases):
        raise ValueError('incomplete Activator deployment evidence: required cases')
    if len(ids) != 2 or len(set(ids)) != 2:
        raise ValueError('incomplete Activator deployment evidence: distinct environments')
    if result.get('cleanup_errors') != [] or result.get('backend_counts') != [0, 0]:
        raise ValueError('incomplete Activator deployment evidence: physical cleanup')
    if result.get('remaining_bindings') != 0 or result.get('remaining_environments') != 0:
        raise ValueError('incomplete Activator deployment evidence: metadata cleanup')


class Acceptance:
    def __init__(self, mode):
        self.mode = mode
        self.evidence, self.private = Path('/evidence'), Path('/secrets')
        self.path = self.evidence / 'activator-result.json'
        self.result = (
            json.loads(self.path.read_text())
            if self.path.exists()
            else {
                'mode': mode,
                'status': 'running',
                'cases': [],
                'environment_ids': [],
                'cleanup_errors': [],
            }
        )
        self.token = (self.private / 'api-key').read_text().strip()
        self.other = (self.private / 'other-key').read_text().strip()
        self.context = ssl.create_default_context(cafile=str(self.private / 'tls/ca.pem'))
        self.opener = urllib.request.build_opener(
            urllib.request.ProxyHandler({}), urllib.request.HTTPSHandler(context=self.context)
        )
        self.endpoints = ('https://127.0.0.1:8443', 'https://node2:8443')
        version = self.result.get('template_version', '1')
        self.base = f'/api/agent/v2/templates/activator-e2e/versions/{version}/bindings'
        self.binding = self.base + '/shared'
        self.data_path = '/agent/http/echo?' + urllib.parse.urlencode(
            {'target': f'urn:adx:binding:activator-e2e:{version}:shared'}
        )

    def save(self):
        self.path.write_text(json.dumps(self.result, indent=2) + '\n')

    def passed(self, name, **details):
        self.result['cases'].append({'id': name, 'status': 'passed', **details})
        self.save()
        print('[ACTIVATOR PASS]', name, flush=True)

    def request(self, endpoint, path, method='GET', payload=None, token=None):
        request = urllib.request.Request(
            endpoint + path,
            method=method,
            data=None if payload is None else json.dumps(payload).encode(),
            headers={
                'Authorization': 'Bearer ' + (self.token if token is None else token),
                'Content-Type': 'application/json',
            },
        )
        try:
            response = self.opener.open(request, timeout=130)
        except urllib.error.HTTPError as error:
            response = error
        with response:
            body = response.read()
            try:
                value = json.loads(body) if body else None
            except json.JSONDecodeError:
                value = body.decode('utf-8', errors='replace')
            return response.status, dict(response.headers), value

    def success(self, endpoint, path, method='GET', payload=None):
        code, headers, body = self.request(endpoint, path, method, payload)
        if code != 200:
            raise AssertionError(f'{method} {path}: HTTP {code}, {body}')
        return headers, body

    def wait(self, check, seconds=150):
        deadline = time.monotonic() + seconds
        last = None
        while time.monotonic() < deadline:
            try:
                value = check()
                if value:
                    return value
            except (OSError, AssertionError, KeyError, ValueError) as error:
                last = str(error)
            time.sleep(0.5)
        raise TimeoutError(f'Activator acceptance timed out: {last}')

    def redis(self, *args):
        env = {**os.environ, 'REDISCLI_AUTH': (self.private / 'redis-key').read_text().strip()}
        executable = shutil.which('redis-cli')
        if executable is None:
            raise RuntimeError('redis-cli is required for Activator acceptance')
        return json.loads(
            subprocess.check_output([executable, '-h', 'coordinator', '--json', *args], env=env, text=True, timeout=10)
        )

    def records(self):
        catalog = self.redis('HGETALL', 'adx:{acceptance}:control:v1')
        return {
            key.removeprefix('environment:'): json.loads(value)
            for key, value in catalog.items()
            if key.startswith('environment:')
        }

    def bindings(self):
        return self.redis('KEYS', 'adx:v2:acceptance-agent:binding:*')

    def traffic(self, endpoint):
        headers, body = self.wait(lambda: self.success(endpoint, self.data_path))
        if body['marker'] != 'real-activator-workload' or body['path'] != '/echo':
            raise AssertionError(body)
        normalized = {key.lower(): value for key, value in headers.items()}
        if normalized.get('x-adx-binding-id') != 'shared':
            raise AssertionError('missing binding response header')
        return body

    def concurrent(self):
        with concurrent.futures.ThreadPoolExecutor(max_workers=4) as pool:
            return list(pool.map(self.traffic, self.endpoints * 2))

    def capture(self, body):
        binding = self.success(self.endpoints[0], self.binding)[1]['binding']
        other = self.success(self.endpoints[1], self.binding)[1]['binding']
        if binding != other or len(self.bindings()) != 1:
            raise AssertionError('Agent replicas did not share one durable binding')
        durable = json.loads(self.redis('GET', self.bindings()[0]))['value']
        if durable != binding:
            raise AssertionError('Redis AgentBinding differs from public binding')
        records = self.records()
        if list(records) != [binding['current']['environment_id']]:
            raise AssertionError('Platform did not persist exactly one Environment')
        record = records[binding['current']['environment_id']]
        if record['result']['state'] != 'Running':
            raise AssertionError('Platform Environment not Running')
        return {
            'binding': binding,
            'boot_id': body['boot_id'],
            'assignment': record['assignment'],
            'runtime_id': record['result']['runtime']['id'],
        }

    def create(self):
        template = {
            'name': 'activator-e2e',
            'image': (self.private / 'activator-image').read_text().strip(),
            'isolation_runtime': 'runc',
            'resources': {'cpu_millis': 500, 'memory_mib': 256},
            'env': {'AGENT_E2E_MARKER': 'real-activator-workload'},
            'service': [{'protocol': 'http', 'port': 18092}],
        }
        published = self.success(self.endpoints[0], '/api/agent/v2/templates', 'POST', template)[1]['template']
        version = published['version']
        self.result['template_version'] = version
        self.base = f'/api/agent/v2/templates/activator-e2e/versions/{version}/bindings'
        self.binding = self.base + '/shared'
        self.data_path = '/agent/http/echo?' + urllib.parse.urlencode(
            {'target': f'urn:adx:binding:activator-e2e:{version}:shared'}
        )
        observed = self.success(self.endpoints[1], f'/api/agent/v2/templates/activator-e2e/versions/{version}')[1][
            'template'
        ]
        if observed != published or published != {**template, 'version': version}:
            raise AssertionError('template not visible across entrypoints')
        self.passed('publish-template')
        for endpoint in self.endpoints:
            for token in ('invalid-e2e-key', self.other):
                code, _, _ = self.request(endpoint, self.data_path, token=token)
                if code not in (401, 403, 404):
                    raise AssertionError(f'foreign traffic HTTP {code}')
        if self.bindings() or self.records():
            raise AssertionError('unauthorized traffic created state')
        self.passed('tenant-isolation')
        bodies = self.concurrent()
        if len({body['boot_id'] for body in bodies}) != 1:
            raise AssertionError('concurrent traffic selected multiple workloads')
        self.result['original'] = self.capture(bodies[0])
        identity = self.result['original']['binding']['current']['environment_id']
        self.result['environment_ids'].append(identity)
        (self.evidence / 'live-instances.json').write_text(json.dumps([identity]))
        self.passed('concurrent-first-traffic', requests=len(bodies), environment_id=identity)

    def inventory(self, prefix):
        rows = [
            json.loads((self.evidence / f'activator-{prefix}-{node}.json').read_text()) for node in ('node1', 'node2')
        ]
        if sum(len(row['backends']) for row in rows) != 1 or any(
            set(row['backends']) != set(row['labeled']) for row in rows
        ):
            raise AssertionError('expected exactly one real sandboxd backend with the Environment label')
        return rows

    def physical(self):
        self.result['physical_before'] = self.inventory('before')
        self.passed('persisted-single-execution', inventories=self.result['physical_before'])
        identity = self.result['original']['binding']['current']['environment_id']
        from adx_environment import ConnectionConfig, Environment

        os.environ['SSL_CERT_FILE'] = str(self.private / 'tls/ca.pem')
        connection = ConnectionConfig(server_address='127.0.0.1:8443', token=self.token, use_tls=True, verify_tls=True)
        handle = Environment.from_id(identity, connection=connection)
        try:
            command = handle.commands.run("printf 'activator-execd-ok'; printf 'activator-stderr' >&2; exit 7")
            if (command.stdout, command.stderr, command.exit_code) != ('activator-execd-ok', 'activator-stderr', 7):
                raise AssertionError('real Execd command output differs')
            payload = b'ADX-Activator\x00\xff\n' * 1024
            handle.files.write('/tmp/activator-e2e.bin', payload)
            if handle.files.read('/tmp/activator-e2e.bin', format='bytes') != payload:
                raise AssertionError('real Execd binary file round trip differs')
        finally:
            handle.close()
        self.passed('execd-command-and-files', bytes=len(payload))
        for endpoint in self.endpoints:
            for refresh in (False, True):
                resolved = self.success(
                    endpoint, self.binding + '/resolve', 'POST', {'protocol': 'http', 'bypasscache': refresh}
                )[1]
                if resolved['environment_id'] != identity or resolved['port'] != 18092:
                    raise AssertionError('resolve changed the binding identity')
        if self.mode == 'standalone':
            scope = {
                'tenant': 'e2e',
                'template': 'activator-e2e',
                'template_ref': {'version': self.result['template_version']},
                'binding_id': 'shared',
            }
            for endpoint in ('http://coordinator:18091', 'http://node2:18091'):
                code, _, target = self.request(
                    endpoint,
                    '/internal/adx/v1/bindings/activate',
                    'POST',
                    {'scope': scope, 'bypasscache': True},
                    token=(self.private / 'activator-key').read_text().strip(),
                )
                if code != 200 or target['binding'] != self.result['original']['binding']:
                    raise AssertionError('independent Activators disagree')
        self.passed('resolve-and-refresh')

    def recover(self):
        bodies = self.concurrent()
        original = self.result['original']
        current = self.capture(bodies[0])
        if (
            any(body['boot_id'] != original['boot_id'] for body in bodies)
            or current != original
            or self.inventory('restarted') != self.result['physical_before']
        ):
            raise AssertionError('Activator restart replaced the Environment or physical execution')
        restarts = [
            json.loads(
                (
                    self.evidence / f'{"activator" if self.mode == "standalone" else "apiserver"}-restart-{node}.json'
                ).read_text()
            )
            for node in ('node1', 'node2')
        ]
        if any(row['pid'] == row['previous_pid'] for row in restarts):
            raise AssertionError('Activator did not restart')
        self.passed('restart-preserves-execution', restarts=restarts)
        self.success(self.endpoints[1], self.binding, 'DELETE')
        self.wait(lambda: not self.records() and not self.bindings())
        if self.request(self.endpoints[0], self.binding)[0] != 404:
            raise AssertionError('deleted binding remains visible')

    def recreate(self):
        bodies = self.concurrent()
        if len({body['boot_id'] for body in bodies}) != 1:
            raise AssertionError('recreated binding selected multiple workloads')
        fresh = self.capture(bodies[0])
        old = self.result['original']
        if (
            fresh['binding']['current']['environment_id'] == old['binding']['current']['environment_id']
            or fresh['binding']['generation'] == old['binding']['generation']
            or fresh['boot_id'] == old['boot_id']
        ):
            raise AssertionError('recreation reused old identity or workload')
        self.result['environment_ids'].append(fresh['binding']['current']['environment_id'])
        self.result['fresh'] = fresh
        (self.evidence / 'live-instances.json').write_text(json.dumps([fresh['binding']['current']['environment_id']]))
        self.passed('delete-and-recreate', binding=fresh['binding'])

    def delete(self):
        self.result['physical_new'] = self.inventory('new')
        self.success(self.endpoints[0], self.binding, 'DELETE')
        self.wait(lambda: not self.records() and not self.bindings())
        self.result['remaining_bindings'] = len(self.bindings())
        self.result['remaining_environments'] = len(self.records())
        self.save()

    def finish(self):
        rows = [json.loads((self.evidence / f'activator-empty-{node}.json').read_text()) for node in ('node1', 'node2')]
        self.result['backend_counts'] = [len(row['backends']) for row in rows]
        self.result['status'] = 'passed'
        self.passed('physical-and-metadata-cleanup', inventories=rows)
        verify(self.result)
        print(json.dumps(self.result), flush=True)


def main():
    mode, phase = sys.argv[1:3]
    acceptance = Acceptance(mode)
    try:
        getattr(acceptance, phase)()
    except Exception as error:
        acceptance.result.update(status='failed', error=f'{type(error).__name__}: {error}')
        acceptance.save()
        raise


if __name__ == '__main__':
    main()
