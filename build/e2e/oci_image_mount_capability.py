"""Exercise verified read-only OCI image volume placement with two real workers."""

import argparse
import json
import os
from pathlib import Path
import shlex

from adx_environment import ConnectionConfig, Environment, EnvironmentError, Mount
from node import catalog, live_catalog
from runtime_inventory import require_unassigned_create


CAPABILITY = 'oci-image-mount-readonly'
LABEL = 'adx.internal/node-capability/runsc/' + CAPABILITY
TARGET = '/mnt/adx-test-oci'


def worker_capability(records):
    qualified = []
    for node_id in ('node1', 'node2'):
        record = json.loads(records['node:' + node_id])
        node = record['node']
        if not node['available'] or not record['session']['routable'] or 'runsc' not in node['runtime_classes']:
            raise AssertionError(f'{node_id} must be a live runsc worker')
        if node['labels'].get(LABEL) == 'true':
            qualified.append(node_id)
    if len(qualified) != 1:
        raise AssertionError(f'expected one OCI image mount worker, got {qualified}')
    capable = qualified[0]
    return capable, 'node2' if capable == 'node1' else 'node1'


def assigned_worker(environment):
    return json.loads(catalog()['environment:' + environment.id])['assignment']['node_id']


def run(connection, image, marker, output, mode='central'):
    capable, incapable = worker_capability(live_catalog())
    common = {'runtime': 'runsc', 'connection': connection, 'cpu': 250, 'memory': 256, 'create_timeout': 60}
    report = {'capable': capable, 'incapable': incapable, 'cases': []}

    ordinary = Environment(**common, node_id=incapable)
    try:
        if assigned_worker(ordinary) != incapable:
            raise AssertionError('ordinary creation did not land on the incapable worker')
        report['cases'].append('ordinary-placement')
    finally:
        ordinary.kill()
        ordinary.close()

    constrained = {
        **common,
        'required_node_capabilities': [CAPABILITY],
        'mounts': [Mount(target=TARGET, image_url=image)],
    }
    try:
        unexpected = Environment(**common, node_id=incapable, mounts=constrained['mounts'])
    except EnvironmentError as error:
        if 'OCI image volumes are disabled on this worker' not in str(error):
            raise
        report['cases'].append('worker-rejects-disabled-mount')
    else:
        try:
            unexpected.kill()
        finally:
            unexpected.close()
        raise AssertionError('an unqualified request mounted an OCI image on the disabled worker')

    environment = Environment(**constrained)
    try:
        if assigned_worker(environment) != capable:
            raise AssertionError('OCI image mounted on the wrong worker')
        path = shlex.quote(TARGET + '/' + marker)
        read = environment.commands.run(f'test -s {path}')
        if read.exit_code != 0:
            raise AssertionError('OCI image marker is missing')
        write = environment.commands.run(f'touch {TARGET}/should-not-exist')
        if write.exit_code == 0 or 'Read-only file system' not in write.stderr:
            raise AssertionError(f'OCI image mount did not reject a write: {write}')
        report['cases'].append('mounted-read-only')
    finally:
        environment.kill()
        environment.close()

    try:
        unexpected = Environment(
            **constrained, name='oci-mount-rejected', node_id=incapable, schedule_timeout=2
        )
    except EnvironmentError as error:
        reasons = (error.scheduling_failure or {}).get('rejections', {})
        if error.outcome != 'not_started' or not reasons.get('node_capability'):
            raise AssertionError(f'expected capability scheduling rejection, got {error}') from error
        require_unassigned_create(catalog(), 'default-oci-mount-rejected')
        report['cases'].append('incompatible-capability-rejected')
    else:
        try:
            unexpected.kill()
        finally:
            unexpected.close()
        raise AssertionError('an OCI mount was scheduled onto the incapable worker')
    report['cases'] = [{'id': f'{mode}.{case}', 'status': 'passed'} for case in report['cases']]
    output.write_text(json.dumps(report, indent=2) + '\n')


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--mode', required=True, choices=('central', 'local_first'))
    parser.add_argument('--marker', default='usr/local/bin/adx-execd')
    parser.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    secrets = Path('/secrets')
    os.environ.setdefault('SSL_CERT_FILE', str(secrets / 'tls/ca.pem'))
    connection = ConnectionConfig(
        '127.0.0.1:8443', (secrets / 'api-key').read_text().strip(), verify_tls=True
    )
    run(connection, (secrets / 'image').read_text().strip(), args.marker, args.output, mode=args.mode)


if __name__ == '__main__':
    main()
