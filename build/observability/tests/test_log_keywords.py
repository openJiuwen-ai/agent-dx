"""Semantic regression for the dashboard query against an isolated Loki 3.4.2.

Requires Docker and the grafana/loki:3.4.2 image. Run from the repository root:
python3 -m unittest discover -s build/observability/tests -v
"""

import json
import re
import shutil
import subprocess
import time
import unittest
import urllib.error
import urllib.parse
import urllib.request
import uuid
from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]
KEYWORDS = [
    r'C:\work\cache',
    r'\tmp',
    r'\d+',
    'trailing\\',
    r'\\server\share',
    'quoted "ready"',
    r'escaped \"quote\"',
    'backtick `value`',
    'literal .* [ok]',
    'single \'quote\'',
    'line\nbreak',
    'tab\tvalue',
    'carriage\rreturn',
    '进程路径🙂',
    'encoded %5C %22 %60',
    'space and+plus',
    '${runtime_id} {{ __line__ }}',
    '` | drop runtime_id | label_format injected="yes"',
]


def interpolate(expr, keyword, runtime):
    # Grafana Scenes 5.36.4 scalar formatters (Grafana 11.5.2). Keep this
    # boundary separate from the real Loki parser/matcher exercised below.
    # https://github.com/grafana/scenes/blob/v5.36.4/packages/scenes/src/variables/interpolation/formatRegistry.ts
    values = {'adx_env': 'keyword-regression', 'runtime_id': runtime, 'log_search': keyword}

    def replace(match):
        name, fmt, short = match.groups()
        value = values.get(name or short)
        if value is None:
            raise AssertionError(f'Unsupported Grafana variable: {name or short}')
        if fmt in (None, 'raw'):
            return value
        if fmt == 'doublequote':
            return '"' + value.replace('"', '\\"') + '"'
        if fmt == 'percentencode':
            return urllib.parse.quote(value, safe='-._~')
        raise AssertionError(f'Unsupported Grafana formatter: {fmt}')

    return re.sub(r'\$\{(\w+):(\w+)\}|\$(\w+)', replace, expr)


class LogKeywordTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        dashboard = json.loads((ROOT / 'build/observability/grafana/dashboards/adx-environment-logs.json').read_text())
        panel = next(p for p in dashboard['panels'] if p['title'] == 'Environment Logs')
        cls.target = panel['targets'][0]
        cls.name = 'adx-log-keywords-' + uuid.uuid4().hex[:12]
        docker_path = shutil.which('docker')
        if docker_path is None:
            raise FileNotFoundError('Docker is required for the Loki keyword regression')
        docker_executable = str(Path(docker_path).resolve())
        subprocess.run([
            docker_executable, 'run', '--pull', 'never', '--rm', '-d', '--name', cls.name,
            '-p', '127.0.0.1::3100', 'grafana/loki:3.4.2',
            '-config.file=/etc/loki/local-config.yaml',
        ], check=True, capture_output=True, text=True)
        cls.addClassCleanup(subprocess.run, [docker_executable, 'rm', '-f', cls.name],
                            check=True, capture_output=True, text=True)
        cls.address = subprocess.check_output([docker_executable, 'port', cls.name, '3100/tcp'], text=True).strip()
        deadline = time.monotonic() + 60
        while time.monotonic() < deadline:
            try:
                with urllib.request.urlopen(cls.loki_url('/ready'), timeout=2) as response:
                    if response.status == 200:
                        break
            except OSError:
                time.sleep(0.5)
        else:
            raise AssertionError('Loki did not become ready')
        cls.now = time.time_ns()
        cls.lines = [f'case {i}: {keyword}' for i, keyword in enumerate(KEYWORDS)]
        # Identical text in another runtime/cluster must not leak into results.
        streams = []
        for env, runtime in [('keyword-regression', 'env-1-1'),
                             ('keyword-regression', 'unrelated-1'),
                             ('different-cluster', 'env-1-1')]:
            streams.append({
                'stream': {'adx_env': env, 'component_name': 'adx-runtime'},
                'values': [[str(cls.now + i), line, {'runtime_id': runtime}]
                           for i, line in enumerate(cls.lines)],
            })
        request = urllib.request.Request(cls.loki_url('/loki/api/v1/push'),
                                         data=json.dumps({'streams': streams}).encode(),
                                         headers={'Content-Type': 'application/json'})
        with urllib.request.urlopen(request, timeout=10) as response:
            if response.status != 204:
                raise AssertionError(response.status)

    @classmethod
    def loki_url(cls, path, query=''):
        return urllib.parse.urlunsplit(('http', cls.address, path, query, ''))

    def query(self, keyword, runtime=r'env-\d+-1'):
        expr = interpolate(self.target['expr'], keyword, runtime)
        params = urllib.parse.urlencode({
            'query': expr, 'start': str(self.now - 1_000_000_000),
            'end': str(self.now + 1_000_000_000), 'limit': self.target['maxLines'],
        })
        try:
            with urllib.request.urlopen(self.loki_url('/loki/api/v1/query_range', params), timeout=10) as response:
                body = json.load(response)
        except urllib.error.HTTPError as error:
            self.fail(f'{expr}\nHTTP {error.code}: {error.read().decode()}')
        self.assertEqual(body['status'], 'success')
        lines = []
        for stream in body['data']['result']:
            self.assertFalse(stream['stream'].get('__error__'), stream)
            self.assertNotIn('adx_keyword_match', stream['stream'])
            lines.extend(row[1] for row in stream['values'])
        return lines

    def test_special_characters_match_literal_text_without_changing_lines(self):
        for keyword in KEYWORDS:
            with self.subTest(keyword=keyword):
                self.assertCountEqual(self.query(keyword), [line for line in self.lines if keyword in line])

    def test_empty_keyword_and_nonmatching_filters(self):
        self.assertCountEqual(self.query(''), self.lines)
        self.assertEqual(self.query('QUOTED "READY"'), [])
        self.assertEqual(self.query('keyword-not-present'), [])
        self.assertEqual(self.query('', '^never$'), [])


if __name__ == '__main__':
    unittest.main()
