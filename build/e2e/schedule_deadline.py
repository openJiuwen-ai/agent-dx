"""Verify a real central queue deadline leaves no assignment or waiting entry."""

from concurrent.futures import ThreadPoolExecutor
import json
import ssl
import time
import urllib.request
import uuid

from adx_sandbox import Sandbox, SandboxError
from node import catalog
from runtime_inventory import require_runc_only_inventory, require_unassigned_create


def pending_ids(endpoint, token, ca):
    request = urllib.request.Request(
        'https://' + endpoint + '/global-scheduler/scheduling_queue',
        headers={'Authorization': 'Bearer ' + token},
    )
    with urllib.request.urlopen(
        request, context=ssl.create_default_context(cafile=str(ca)), timeout=5,
    ) as response:
        body = json.load(response)
    entries = body['instanceInfos']
    if body['count'] != len(entries):
        raise AssertionError('scheduler queue count differs from its entries')
    return {item['instanceID'] for item in entries}


def run(connection, image, output, ca, admin_key, *, queue_reader=pending_ids,
        clock=time.monotonic, sleep=time.sleep):
    """Use the installed SDK while observing the admin queue and Redis catalog."""
    require_runc_only_inventory(catalog())
    name = 'queue-deadline-' + uuid.uuid4().hex[:12]
    instance_id = 'default-' + name
    report = {'status': 'failed', 'cases': [], 'cleanup_errors': [],
              'queue_observed': False, 'queue_drained': False,
              'instance_id': instance_id}
    admin_token = admin_key.read_text().strip()
    started = clock()

    def create():
        return Sandbox(
            name=name, image=image, runtime='runsc', node_id='node1',
            cpu=250, memory=256, idle_timeout=0,
            connection=connection, schedule_timeout=3, create_timeout=40,
        )

    try:
        with ThreadPoolExecutor(max_workers=1) as executor:
            future = executor.submit(create)
            try:
                probe_deadline = clock() + 20
                while clock() < probe_deadline:
                    if instance_id in queue_reader(connection.server_address,
                                                   admin_token, ca):
                        report['queue_observed'] = True
                        break
                    if future.done():
                        break
                    sleep(.05)
                try:
                    unexpected = future.result(timeout=90)
                except SandboxError as error:
                    if ('central scheduling queue deadline exceeded' not in str(error)
                            or error.code != 'OUTCOME_UNKNOWN'
                            or error.retry != 'same_operation'
                            or error.outcome != 'unknown'
                            or error.instance_id != instance_id):
                        raise AssertionError(f'wrong scheduling deadline contract: {error}') from error
                    report['error_contract'] = {
                        'code': error.code, 'retry': error.retry,
                        'outcome': error.outcome, 'instance_id': error.instance_id,
                    }
                else:
                    try:
                        unexpected.kill()
                    finally:
                        unexpected.close()
                    raise AssertionError('unsupported runtime was assigned after queue deadline')
            finally:
                if not future.done():
                    # Do not leave a create running after a failed observation.
                    try:
                        unexpected = future.result(timeout=90)
                    except SandboxError:
                        pass
                    except Exception as error:
                        report['cleanup_errors'].append(f'create did not settle: {error}')
                    else:
                        try:
                            unexpected.kill()
                        finally:
                            unexpected.close()

        if not report['queue_observed']:
            raise AssertionError('request never appeared in the central waiting queue')
        drain_deadline = clock() + 10
        while clock() < drain_deadline:
            if instance_id not in queue_reader(connection.server_address,
                                               admin_token, ca):
                report['queue_drained'] = True
                break
            sleep(.1)
        if not report['queue_drained']:
            raise AssertionError('timed-out request remained in the scheduler queue')
        require_unassigned_create(catalog(), instance_id)
        report['status'] = 'passed'
        report['cases'].append({
            'id': 'reliability.central-queue-deadline', 'status': 'passed',
            'seconds': round(clock() - started, 3),
            'instance_id': instance_id,
        })
        return report
    except Exception as error:
        report['error'] = str(error)
        raise
    finally:
        if report['cleanup_errors']:
            report['status'] = 'failed'
        output.write_text(json.dumps(report, indent=2) + '\n')
