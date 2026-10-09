"""Live 64-operation ledger boundary; unchanged physical-identity oracle is reused."""
import importlib.util
import os
from pathlib import Path


LIMIT = 64
SENTINEL = 'capacity-denied-proof'
ERRORS = {16: 'Device or resource busy (os error 16)',
          28: 'No space left on device (os error 28)'}


def verify_errno(response, code):
    if (response.get('status') != 'ERROR' or response.get('production_ready') is not False
            or response.get('error') != ERRORS[code]):
        raise ValueError('expected exact Linux errno ' + str(code))


def verify_fill(initial, records):
    if (initial['request'] != {'operation': 'start', 'id': 'first', 'workspace': 'workspace'}
            or initial['response'].get('state') != 'FinalVerified'
            or initial['response'].get('production_ready') is not False or len(records) != LIMIT - 1):
        raise ValueError('ledger requires one verified Start and exactly 63 busy Starts')
    ids = {initial['request']['id']}
    for index, entry in enumerate(records):
        request = entry['request']
        if request != {'operation': 'start', 'id': f'capacity-busy-{index:02d}', 'workspace': 'workspace'}:
            raise ValueError('busy Start must be otherwise valid, distinct and ordered')
        if request['id'] in ids:
            raise ValueError('duplicate operation ID cannot fill ledger')
        ids.add(request['id'])
        verify_errno(entry['response'], 16)
        if entry['response'].get('active') != initial['response']:
            raise ValueError('busy Start changed the active grant')
    if len(ids) != LIMIT:
        raise ValueError('wrong number of distinct cached operations')


def verify_absent(observations):
    if (set(observations) != {'fuse', 'native'}
            or any(value is not False for value in observations.values())):
        raise ValueError('denied Exec must not create the sentinel in either view')


def verify_full(before, after, response, observations, statuses, identity):
    verify_errno(response, 28)
    identity.verify_unchanged(before, after)
    verify_absent(observations)
    if len(statuses) != 2 or any(value != before['status'] for value in statuses):
        raise ValueError('two distinct Status calls must preserve the full verified grant')


def load(path, name):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def execute(run, start, workspace):
    identity = load(Path(__file__).with_name('native_source_rejection.py'), 'capacity_identity')
    client = load(run.args.controller, 'capacity_client')
    initial = {'request': {'operation': 'start', 'id': 'first', 'workspace': 'workspace'}, 'response': start}
    before = identity.snapshot(run, 'capacity-before', start['container'])
    sentinel = workspace / SENTINEL
    native_sentinel = Path(f"/proc/{before['process']['pid']}/root/workspace") / SENTINEL
    absent = {'fuse': os.path.lexists(sentinel), 'native': os.path.lexists(native_sentinel)}
    run.save('control-capacity-sentinel-before.json', absent)
    verify_absent(absent)
    records = []
    for index in range(LIMIT - 1):
        request = {'operation': 'start', 'id': f'capacity-busy-{index:02d}', 'workspace': 'workspace'}
        run.save(f'control-capacity-busy-{index:02d}-request.json', request)
        response = client.exchange(run.root / 'control/control.sock', request)
        run.save(f'control-capacity-busy-{index:02d}-response.json', response)
        records.append({'request': request, 'response': response})
        verify_errno(response, 16)
    verify_fill(initial, records)
    after_fill = identity.snapshot(run, 'capacity-after-fill', start['container'])
    identity.verify_unchanged(before, after_fill)
    request = {'operation': 'exec', 'id': 'capacity-denied-exec',
               'argv': ['/bin/sh', '-ec', "printf 'must-not-run\\n' > /workspace/" + SENTINEL]}
    run.save('control-capacity-denied-request.json', request)
    response = client.exchange(run.root / 'control/control.sock', request)
    run.save('control-capacity-denied-response.json', response)
    statuses = [run.native(f'capacity-full-status-{i}', 'status') for i in range(2)]
    after_full = identity.snapshot(run, 'capacity-after-full', start['container'])
    observations = {'fuse': os.path.lexists(sentinel), 'native': os.path.lexists(native_sentinel)}
    run.save('control-capacity-sentinel-after.json', observations)
    verify_full(before, after_full, response, observations, statuses, identity)
    run.check('control-capacity-ENOSPC-no-side-effects', True, response)
    run.check('control-capacity-status-available', True, statuses)
    run.save('control-capacity-result.json', {'status': 'PASS', 'cached_operations': LIMIT,
             'initial': initial, 'busy': records, 'denied_request': request, 'denied_response': response,
             'before': before, 'after_fill': after_fill, 'after_full': after_full,
             'sentinel_before': absent, 'sentinel_after': observations, 'status_after_full': statuses,
             'scope': 'capacity rejection and Status only; parent driver records public Stop/closure'})
