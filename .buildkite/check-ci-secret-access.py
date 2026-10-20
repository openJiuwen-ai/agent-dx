#!/usr/bin/env python3
"""Read-only check of the current CI namespace's publication-secret access."""
import json
import os
from pathlib import Path
import subprocess
import tempfile

service = Path('/var/run/secrets/kubernetes.io/serviceaccount')
namespace = (service / 'namespace').read_text().strip()
server = 'https://' + os.environ['KUBERNETES_SERVICE_HOST'] + ':' + os.environ.get('KUBERNETES_SERVICE_PORT_HTTPS', '443')
with tempfile.TemporaryDirectory(prefix='adx-ci-access-') as private:
    config = Path(private) / 'config.json'
    config.write_text(json.dumps({
        'apiVersion': 'v1', 'kind': 'Config',
        'clusters': [{'name': 'ci', 'cluster': {'server': server, 'certificate-authority': str(service / 'ca.crt')}}],
        'users': [{'name': 'ci', 'user': {'tokenFile': str(service / 'token')}}],
        'contexts': [{'name': 'ci', 'context': {'cluster': 'ci', 'user': 'ci', 'namespace': namespace}}],
        'current-context': 'ci',
    }))
    config.chmod(0o600)
    results = {'namespace': namespace, 'server': server}
    for name, verb, resource in [('patch_publication_secret', 'patch', 'secret/adx-pypi-credentials'), ('create_secret', 'create', 'secrets')]:
        check = subprocess.run(['kubectl', '--kubeconfig', str(config), '--request-timeout=10s', 'auth', 'can-i', verb, resource, '-n', namespace], text=True, capture_output=True, timeout=30)
        results[name] = {'status': check.returncode, 'result': check.stdout.strip(), 'error': check.stderr[-500:]}
    output = Path('out/buildkite/ci-secret-access.json')
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(results, indent=2) + '\n')
    print(json.dumps(results, indent=2))
