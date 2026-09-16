"""Strict evidence contract for the isolated two-node Firecracker fixture."""
CASES = (
    'create and checkpoint through public SDK',
    'node failure restores same instance on another node',
    'restored memory PID and files preserved',
    'returning source cleans old execution',
    'master restart preserves recovered execution',
    'explicit delete releases both nodes',
)

def recovered(old, new):
    return (new.get('spec', {}).get('id') == old['spec']['id']
            and new.get('assignment', {}).get('node_id') != old['assignment']['node_id']
            and new.get('assignment', {}).get('generation', 0) > old['assignment']['generation']
            and new.get('result', {}).get('state') == 'Running'
            and new['result'].get('resources_held') is True
            and new.get('recovery', {}).get('pending') is False)

def verify(result):
    if result.get('node_interruption_requested'):
        fault = result.get('node_recovery_restart', {})
        if (fault.get('state_at_crash') != 'Paused' or fault.get('pending_at_crash') is not True
                or fault.get('assignment_preserved') is not True or fault.get('old_backend_removed') is not True
                or not fault.get('session_before') or not fault.get('session_after')
                or fault['session_before'] == fault['session_after']
                or not fault.get('backend_before') or not fault.get('backend_after')
                or fault['backend_before'] == fault['backend_after']):
            raise ValueError('missing uncommitted recovery cleanup evidence')
    if result.get('interruption_requested'):
        fault = result.get('mid_recovery_restart', {})
        if (fault.get('plan_preserved') is not True
                or fault.get('epoch_after', 0) <= fault.get('epoch_before', 0)):
            raise ValueError('missing in-flight Master restart evidence')
    cases = result.get('cases', [])
    if (result.get('status') != 'passed' or result.get('cleanup_errors') != []
            or result.get('inventories') != {'node1':0,'node2':0}
            or [c.get('name') for c in cases] != list(CASES)
            or not all(c.get('passed') is True for c in cases)):
        raise ValueError('incomplete Firecracker transfer evidence')
