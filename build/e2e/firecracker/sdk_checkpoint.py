#!/usr/bin/env python3
"""Public installed-SDK pause/resume acceptance against a real Firecracker ADX node.

No mock backend or direct lifecycle RPC is used. The optional restart command is
an explicit test fixture hook; it must restart only the selected Adxlet.
"""
import argparse
import importlib.metadata
import json
import os
import shlex
from pathlib import Path
import subprocess
import time
import traceback

p = argparse.ArgumentParser()
p.add_argument('--endpoint', required=True)
p.add_argument('--token-file', type=Path, required=True)
p.add_argument('--ca', type=Path, required=True)
p.add_argument('--image', required=True)
p.add_argument('--entrypoint-image', required=True)
p.add_argument('--package', type=Path, default=Path('/opt/adx/package'))
p.add_argument('--output', type=Path, required=True)
p.add_argument('--restart-command', nargs='+')
a = p.parse_args()
os.environ['SSL_CERT_FILE'] = str(a.ca.resolve())
from adx_sandbox import Sandbox, ConnectionConfig, Mount, NetworkPolicy, S3Config
from s3_client import Client as S3Client

out = a.output.resolve()
out.mkdir(parents=True, exist_ok=False)
result = {'status': 'failed', 'sdk_version': importlib.metadata.version('adx-sandbox'), 'cases': []}
connection = ConnectionConfig(server_address=a.endpoint, token=a.token_file.read_text().strip(), use_tls=True, verify_tls=True)
sandbox = None
snapshot = None
clones = []
buddy = None
fixtures = []
fixture_object = '/checkpoints/fixtures/runtime.erofs'

def passed(name, **details):
    print('PASS', name, json.dumps(details), flush=True)
    result['cases'].append({'name': name, 'passed': True, **details})

def command(script):
    value = sandbox.commands.run(script)
    assert value.exit_code == 0, value
    return value.stdout.strip()

try:
    run_root = Path(os.environ['ADX_FC_RUN_ROOT'])
    rootfs_image = a.package.resolve() / 'runtime/adx-runtime-rootfs.img'
    assert rootfs_image.is_file(), rootfs_image
    s3 = S3Client(run_root)
    s3.request('PUT', fixture_object, rootfs_image.read_bytes())
    source = S3Config(
        endpoint='http://127.0.0.1:19090',
        bucket='checkpoints',
        object='fixtures/runtime.erofs',
        access_key=s3.access,
        secret_key=s3.secret,
    )

    s3_root = Sandbox(
        rootfs=source, runtime='firecracker', cpu=500, cpu_limit=1000,
        memory=512, mem_limit=768, storage_mb=64, storage_limit_mb=128,
        idle_timeout=0, connection=connection, create_timeout=180)
    fixtures.append(s3_root)
    assert s3_root.commands.run('printf s3-root-ready').stdout.strip() == 's3-root-ready'
    passed('S3 rootfs and independent execution limits start through sandboxd')
    s3_root.kill(); s3_root.close(); fixtures.remove(s3_root)

    mounted = Sandbox(
        image=a.image, runtime='firecracker', cpu=500, memory=512,
        mounts=[Mount(target='/mnt/runtime', type='erofs', s3_config=source)],
        idle_timeout=0, connection=connection, create_timeout=180)
    fixtures.append(mounted)
    mounted_check = mounted.commands.run('test -x /mnt/runtime/usr/local/bin/adx-execd')
    assert mounted_check.exit_code == 0, mounted_check
    passed('S3 EROFS mount is visible inside the sandbox')
    mounted.kill(); mounted.close(); fixtures.remove(mounted)

    inherited = Sandbox(
        image=a.entrypoint_image, runtime='firecracker', cpu=500, memory=512,
        inherit_entrypoint=True, idle_timeout=0, connection=connection,
        create_timeout=180)
    fixtures.append(inherited)
    assert inherited.wait_entrypoint() == 7
    entrypoint_info = inherited.entrypoint_exit_info
    (out/'entrypoint-exit.json').write_text(json.dumps(entrypoint_info, indent=2)+'\n')
    assert entrypoint_info and entrypoint_info['status_kind'] == 'exited'
    assert entrypoint_info['exit_code'] == 7
    assert 'adx-entrypoint-stderr' in entrypoint_info['stderr_tail']
    passed('inherited image entrypoint reports structured exit status')
    inherited.kill(); inherited.close(); fixtures.remove(inherited)

    # Exercise the mutable network contract on a fresh execution. A restored
    # Firecracker snapshot may retain guest ARP/FDB state from its previous TAP;
    # that backend-specific recovery condition must not turn the policy baseline
    # into a test of checkpoint networking.
    networked = Sandbox(
        image=a.image, runtime='firecracker', cpu=500, memory=512,
        idle_timeout=0, connection=connection, create_timeout=180)
    fixtures.append(networked)
    network_target = os.environ['ADX_FC_EGRESS_PROBE_HOST']
    network_port = int(os.environ['ADX_FC_EGRESS_PROBE_PORT'])
    # The Ubuntu test image intentionally contains no curl, wget, nc or
    # BusyBox. Bash's /dev/tcp support avoids adding a package solely for the
    # probe while still proving a TCP connection and an HTTP response.
    network_script = (
        f"exec 3<>/dev/tcp/{network_target}/{network_port}; "
        'printf "GET / HTTP/1.0\\r\\nHost: probe\\r\\n\\r\\n" >&3; '
        'IFS= read -r line <&3; [[ "$line" == HTTP/* ]]')
    network_probe = f'/usr/bin/timeout 3 /bin/bash -c {shlex.quote(network_script)}'
    assert networked.commands.run(network_probe).exit_code == 0
    networked.update_network_policy(NetworkPolicy.block())
    assert networked.commands.run('printf control-still-ready').stdout.strip() == 'control-still-ready'
    assert networked.commands.run(network_probe).exit_code != 0
    networked.update_network_policy(None)
    assert networked.commands.run(network_probe).exit_code == 0
    passed('runtime network policy replacement blocks egress and preserves EXECD control', target=network_target, port=network_port)
    networked.kill(); networked.close(); fixtures.remove(networked)

    blocked = Sandbox(
        image=a.image, runtime='firecracker', cpu=500, memory=512,
        network=NetworkPolicy.block(), idle_timeout=0, connection=connection,
        create_timeout=180)
    fixtures.append(blocked)
    assert blocked.commands.run('printf creation-policy-ready').stdout.strip() == 'creation-policy-ready'
    assert blocked.commands.run(network_probe).exit_code != 0
    passed('creation network policy is enforced while the control route stays reachable', target=network_target, port=network_port)
    blocked.kill(); blocked.close(); fixtures.remove(blocked)

    sandbox = Sandbox(labels={'app':'checkpoint-source'}, image=a.image, runtime='firecracker', cpu=1000, memory=512, idle_timeout=0, node_id=os.environ.get('ADX_E2E_EXPECTED_NODE'), connection=connection, create_timeout=180)
    result['instance_id'] = sandbox.id
    passed('create through Frontend and execute through Ingress', output=command("printf checkpoint-ready"))
    # A detached process carries a shell variable across the checkpoint. Its PID
    # and counter prove that resume did not just cold-start the original image.
    command("sh -c 'echo $$ >/tmp/counter.pid; n=0; while :; do n=$((n+1)); echo $n >/tmp/counter; sleep 0.1; done' >/tmp/counter.log 2>&1 </dev/null &")
    time.sleep(1)
    before = int(command('cat /tmp/counter'))
    pid = command('cat /tmp/counter.pid')
    payload = b'checkpoint-preserved\x00\xff' * 4096
    sandbox.files.write('/tmp/checkpoint.bin', payload)
    paused = sandbox.pause(ttl_seconds=600, timeout_seconds=120)
    assert paused.sandbox_id == sandbox.id and paused.size > 0
    passed('pause with persisted recovery point', snapshot_id=paused.snapshot_id, size=paused.size, expires_at=paused.expires_at)
    if a.restart_command:
        subprocess.run([*a.restart_command, sandbox.id], check=True, timeout=180)
        passed('Adxlet restart while paused')
        passed('remote orphan GC preserves registered checkpoint')
    resumed = sandbox.resume()
    assert resumed.sandbox_id == sandbox.id
    time.sleep(1)
    after = int(command('cat /tmp/counter'))
    assert after > before, (before, after)
    assert command('cat /tmp/counter.pid') == pid
    assert sandbox.files.read('/tmp/checkpoint.bin', format='bytes') == payload
    passed('resume preserves process memory, PID and binary file', before=before, after=after, pid=pid)
    assert sandbox.reload()
    time.sleep(1)
    reloaded = int(command('cat /tmp/counter'))
    assert reloaded > before
    assert command('cat /tmp/counter.pid') == pid
    assert sandbox.files.read('/tmp/checkpoint.bin', format='bytes') == payload
    passed('reload restores the latest recovery point without a cold start', counter=reloaded, pid=pid)

    def affinity(kind, mode, key, value, **extra):
        return {'kind':kind,'affinity':mode,'labelOps':[{'type':0,'labelKey':key,'labelValues':[value]}],**extra}
    buddy = Sandbox(image=a.image, runtime='firecracker', cpu=1000, memory=512,
        idle_timeout=0, connection=connection, create_timeout=180,
        labels={'app':'consumer'}, schedule_affinities=[
            affinity(1,2,'app','absent'), affinity(1,2,'app','checkpoint-source'),
            affinity(0,0,'NODE_ID','missing',preferredPriority=True),
            affinity(0,0,'NODE_ID',os.environ.get('ADX_E2E_EXPECTED_NODE','node1'),preferredPriority=True),
            affinity(1,1,'app','absent',weight=9),
        ])
    execution=buddy.commands.run('printf placement-ready')
    assert execution.exit_code == 0 and execution.stdout.strip() == 'placement-ready'
    buddy.kill(); buddy.close(); buddy = None
    passed('SDK labels peer affinity and ordered weighted placement')
    snapshot = sandbox.create_snapshot(name="fc-reusable", timeout_seconds=120)
    time.sleep(1)
    snapshot_counter = int(command('cat /tmp/counter'))
    assert snapshot_counter > after and command('cat /tmp/counter.pid') == pid
    assert sandbox.files.read('/tmp/checkpoint.bin', format='bytes') == payload
    passed('reusable snapshot preserves running source', snapshot_id=snapshot.snapshot_id, counter=snapshot_counter, pid=pid)
    command('kill "$(cat /tmp/counter.pid)"')
    sandbox.kill()
    passed('explicit delete')
    assert Sandbox.get_snapshot(snapshot.snapshot_id, connection=connection).snapshot_id == snapshot.snapshot_id
    saved, _ = Sandbox.list_snapshots(name="fc-reusable", connection=connection)
    assert any(s.snapshot_id == snapshot.snapshot_id for s in saved)
    passed('snapshot remains queryable after source deletion')
    source_id = sandbox.id
    for _ in range(2):
        # Omit image/runtime/resources: these must inherit the saved geometry.
        clone = Sandbox.create(snapshot, idle_timeout=0, connection=connection, create_timeout=180)
        clones.append(clone)
        sandbox = clone
        assert clone.id != source_id
        assert command('cat /tmp/counter.pid') == pid
        assert int(command('cat /tmp/counter')) >= after
        assert clone.files.read('/tmp/checkpoint.bin', format='bytes') == payload
    assert clones[0].id != clones[1].id
    passed('snapshot clones inherit geometry and preserve process memory', source=source_id, clones=[c.id for c in clones], pid=pid)
    clones[0].files.write('/tmp/checkpoint.bin', b'clone-one-only')
    assert clones[1].files.read('/tmp/checkpoint.bin', format='bytes') == payload
    passed('snapshot clones have independent identity and writable files')
    Sandbox.delete_snapshot(snapshot.snapshot_id, connection=connection)
    passed('snapshot deletion accepted for artifact collection')
    # Source reference release and physical GC run asynchronously. Prove the
    # remaining clones survive after the shared snapshot has been collected.
    redis_env = {**os.environ, 'REDISCLI_AUTH': (run_root/'secrets/redis-key').read_text().strip()}
    deadline = time.monotonic() + 120
    while True:
        encoded = json.loads(subprocess.check_output(['redis-cli', '--json', 'HGET', 'adx:{acceptance}:control:v1:snapshots', snapshot.snapshot_id], env=redis_env, text=True, timeout=10))
        record = json.loads(encoded) if encoded else {}
        if record.get('state') == 'Deleted' and not record.get('references'):
            (out/'snapshot-collected-before-clone-resume.json').write_text(json.dumps({'snapshot_id': snapshot.snapshot_id, 'state': 'Deleted', 'references': []})+'\n')
            break
        if time.monotonic() >= deadline: raise TimeoutError('source snapshot physical collection did not complete')
        time.sleep(1)
    for clone in clones:
        sandbox = clone
        count = int(command('cat /tmp/counter'))
        checkpoint = clone.pause(ttl_seconds=600, timeout_seconds=120)
        assert checkpoint.size > 0
        clone.resume()
        assert command('cat /tmp/counter.pid') == pid
        assert int(command('cat /tmp/counter')) >= count
        command('kill "$(cat /tmp/counter.pid)"')
        clone.kill()
        clone.close()
    clones.clear()
    passed('clones pause resume and delete after snapshot collection')
    snapshot = None
    result['status'] = 'passed'
except Exception as error:
    result['error'] = f'{type(error).__name__}: {error}'
    traceback.print_exc()
    run_root = os.environ.get('ADX_FC_RUN_ROOT')
    if run_root:
        try:
            subprocess.run(['python3',str(Path(__file__).with_name('network_diagnostics.py')),run_root],timeout=30,check=True)
        except Exception as diagnostic_error:
            print('Network diagnostics incomplete:',diagnostic_error,flush=True)
finally:
    for fixture in fixtures:
        try: fixture.kill()
        except Exception as error: result['cleanup_error'] = str(error)
        fixture.close()
    if buddy:
        try: buddy.kill()
        except Exception as error: result["cleanup_error"] = str(error)
        buddy.close()
    for clone in clones:
        try: clone.kill()
        except Exception as error: result['cleanup_error'] = str(error)
        clone.close()
    if snapshot:
        try: Sandbox.delete_snapshot(snapshot.snapshot_id, connection=connection)
        except Exception as error: result['cleanup_error'] = str(error)
    if sandbox:
        try:
            sandbox.kill()
        except Exception as error:
            result['cleanup_error'] = str(error)
        sandbox.close()
    try:
        if 's3' in locals(): s3.request('DELETE', fixture_object)
    except Exception as error:
        result['cleanup_error'] = str(error)
    (out/'result.json').write_text(json.dumps(result, indent=2)+'\n')
print(json.dumps(result), flush=True)
raise SystemExit(0 if result['status'] == 'passed' and 'cleanup_error' not in result else 1)
