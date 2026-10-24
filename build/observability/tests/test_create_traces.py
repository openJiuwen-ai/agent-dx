"""Validate creation rows against an isolated Tempo 2.7.2 (requires Docker)."""
import json
import shutil
import subprocess
import tempfile
import time
import unittest
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]


class CreateTraceTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        executable = shutil.which('docker')
        if executable is None:
            raise FileNotFoundError('Docker is required for the Tempo regression')
        cls.docker = str(Path(executable).resolve())
        cls.name = 'adx-create-traces-' + uuid.uuid4().hex[:12]
        directory = tempfile.TemporaryDirectory(prefix='adx-create-traces-')
        cls.addClassCleanup(directory.cleanup)
        config = Path(directory.name) / 'tempo.json'
        config.write_text(json.dumps({
            'server': {'http_listen_port': 3200},
            'distributor': {'receivers': {'otlp': {'protocols': {'http': {'endpoint': '0.0.0.0:4318'}}}}},
            'storage': {'trace': {'backend': 'local', 'wal': {'path': '/tmp/tempo/wal'},
                                  'local': {'path': '/tmp/tempo/blocks'}}},
        }))
        subprocess.run([
            cls.docker, 'run', '--pull', 'never', '--rm', '-d', '--name', cls.name,
            '-p', '127.0.0.1::3200', '-p', '127.0.0.1::4318',
            '--mount', f'type=bind,source={config},target=/etc/tempo.json,readonly',
            'grafana/tempo:2.7.2', '-config.file=/etc/tempo.json',
        ], check=True, capture_output=True, text=True)
        cls.addClassCleanup(subprocess.run, [cls.docker, 'rm', '-f', cls.name],
                            check=True, capture_output=True, text=True)
        cls.http = cls.port('3200/tcp')
        cls.otlp = cls.port('4318/tcp')
        deadline = time.monotonic() + 45
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(cls.url(cls.http, '/ready'), timeout=2) as response:
                    if response.status == 200:
                        return
            except OSError:
                time.sleep(0.2)
        raise TimeoutError('Tempo did not become ready')

    @classmethod
    def port(cls, port):
        return subprocess.check_output([cls.docker, 'port', cls.name, port], text=True).strip()

    @staticmethod
    def url(address, path, query=''):
        return urllib.parse.urlunsplit(('http', address, path, query, ''))

    def test_explore_shows_total_create_and_rejections(self):
        queries = []
        for name in ('adx-schedule.json', 'adx-environment-logs.json'):
            dashboard = json.loads((ROOT / 'build/observability/grafana/dashboards' / name).read_text())
            link = next(item for item in dashboard['links'] if item['title'] == 'Explore Requests')
            args = urllib.parse.parse_qs(urllib.parse.urlsplit(link['url']).query)
            query = json.loads(args['panes'][0])['a']['queries'][0]
            self.assertEqual(query['tableType'], 'spans')
            self.assertEqual(query['limit'], 50)
            self.assertEqual(query['spss'], 3)
            queries.append(query['query'].replace('${adx_env:percentencode}', 'trace-timing-regression'))
        self.assertEqual(queries[0], queries[1])
        now = time.time_ns() - 1_000_000_000
        expected = set()
        resources = []
        for name, route, wanted, environment, status in [
            ('environment.create', None, True, 'trace-timing-regression', 200),
            ('POST /api/environment/v1/environments', '/api/environment/v1/environments',
             False, 'trace-timing-regression', 200),
            ('POST /api/environment/create', '/api/environment/create', False, 'trace-timing-regression', 200),
            ('GET /api/environment/v1/environments/{id}', '/api/environment/v1/environments/{id}',
             True, 'trace-timing-regression', 200),
            ('POST /api/environment/v1/environments', '/api/environment/v1/environments',
             True, 'trace-timing-regression', 401),
            ('POST /api/environment/create', '/api/environment/create', True, 'trace-timing-regression', 400),
            ('POST /api/environment/v1/environments', '/api/environment/v1/environments',
             True, 'trace-timing-regression', 500),
            ('GET /healthz', '/healthz', False, 'trace-timing-regression', 200),
            ('GET /unmatched', '/unmatched', False, 'trace-timing-regression', 200),
            ('environment.create', None, False, 'different-cluster', 200),
        ]:
            trace_id = uuid.uuid4().hex
            if wanted:
                expected.add(trace_id)
            attributes = [] if route is None else [
                {'key': 'http.route', 'value': {'stringValue': route}},
                {'key': 'http.response.status_code', 'value': {'stringValue': str(status)}},
            ]
            resources.append({
                'resource': {'attributes': [
                    {'key': 'adx_env', 'value': {'stringValue': environment}},
                    {'key': 'service.name', 'value': {'stringValue': 'adx-apiserver'}},
                ]},
                'scopeSpans': [{'spans': [{
                    'traceId': trace_id, 'spanId': uuid.uuid4().hex[:16], 'name': name,
                    'startTimeUnixNano': str(now), 'endTimeUnixNano': str(now + 200_000_000),
                    'attributes': attributes,
                }]}],
            })
        request = urllib.request.Request(self.url(self.otlp, '/v1/traces'),
                                         data=json.dumps({'resourceSpans': resources}).encode(),
                                         headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=10) as response:
            self.assertEqual(response.status, 200)
        args = urllib.parse.urlencode({'q': queries[0], 'limit': 50, 'spss': 3})
        deadline = time.monotonic() + 20
        result = {}
        while time.monotonic() < deadline:
            with urllib.request.urlopen(self.url(self.http, '/api/search', args), timeout=10) as response:
                result = json.load(response)
            traces = {item['traceID'] for item in result.get('traces', [])}
            if traces == expected:
                break
            time.sleep(0.2)
        self.assertEqual({item['traceID'] for item in result.get('traces', [])}, expected, result)


if __name__ == '__main__':
    unittest.main()
