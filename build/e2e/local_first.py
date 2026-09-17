"""Real SDK -> Edge -> local-first API -> Node -> sandboxd/RRT acceptance."""
from concurrent.futures import ThreadPoolExecutor
import json
import gzip
from pathlib import Path
import time
import uuid
from adx_sandbox import Sandbox, SandboxError
from node import catalog


def run(connection, image, output):
    instances = []
    prefix = 'lf-' + uuid.uuid4().hex[:10]
    def create(name, **extra):
        return Sandbox(name=name, image=image, runtime='runc', cpu=500, memory=512,
                       idle_timeout=0, connection=connection, create_timeout=150, **extra)
    def record(instance):
        return json.loads(catalog()['instance:' + instance.id])
    try:
        first = create(prefix + '-a'); instances.append(first)
        second = create(prefix + '-b'); instances.append(second)
        owners = [record(s)['assignment']['node_id'] for s in instances]
        assert owners == ['node1','node2'], owners
        # Duplicate SDK objects use independent HTTP requests but the same name.
        with ThreadPoolExecutor(max_workers=2) as pool:
            copies = list(pool.map(lambda _: create(prefix + '-race'), range(2)))
        instances.extend(copies)
        assert copies[0].id == copies[1].id
        for instance in (first, second, copies[0]):
            result = instance.commands.run("printf 'local-first-ready'")
            assert result.exit_code == 0 and result.stdout == 'local-first-ready'
        try:
            changed = Sandbox(name=prefix+'-race', image=image, runtime='runc', cpu=600,
                memory=512, idle_timeout=0, connection=connection, create_timeout=150)
        except SandboxError as error:
            assert '409' in str(error), str(error)
        else:
            instances.append(changed)
            raise AssertionError('changed specification unexpectedly accepted')
        records = [record(s) for s in (first,second,copies[0])]
        assert len({r['spec']['id'] for r in records}) == 3
        assert all(r['result']['resources_held'] for r in records)
        # Require evidence of the actual local-claim path, not just placement
        # that could also have resulted from the default center Spread policy.
        expected={r['spec']['id'] for r in records}
        end=time.monotonic()+10
        while True:
            claimed=set()
            for path in Path('/tmp/adx-e2e/state/logs').glob('master*.log*'):
                if path.name.endswith('.tmp'):continue
                try:
                    data=gzip.decompress(path.read_bytes()) if path.suffix=='.gz' else path.read_bytes()
                    for line in data.decode(errors='replace').splitlines():
                        if 'local_instance_claim' in line:
                            claimed.update(identity for identity in expected if identity in line)
                except (FileNotFoundError,EOFError,OSError):continue
            if claimed==expected:break
            if time.monotonic()>end:raise AssertionError(('missing local claim evidence',expected-claimed))
            time.sleep(.1)
        output.write_text(json.dumps({'status':'passed','local_claims':sorted(claimed),'entry_owners':owners,
            'unique_instances':3,'duplicate_converged':True,'conflict_rejected':True,
            'commands_passed':3,'assignments':[r['assignment'] for r in records]},indent=2))
        print('PASS local-first: two-node rotation, same-ID convergence, conflict and three real RRT commands',flush=True)
    finally:
        removed=set()
        for instance in instances:
            try:
                if instance.id not in removed:
                    instance.kill();removed.add(instance.id)
            finally:instance.close()
