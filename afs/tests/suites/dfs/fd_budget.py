"""Physical reader-cache bound and eviction on real DFS mounts."""

import json
import math
from pathlib import Path

from fixtures.common import write_json
from tools import fd_inventory

CAPACITY = 256
CYCLES = 768
BATCH = 128
PROBE = (Path(__file__).resolve().parents[2] / 'tools/fd_inventory.py').read_text()
PUBLISH = '''
for index in range(int(sys.argv[2]),int(sys.argv[3])):
 data=hashlib.sha256(('dfs-fd-budget-'+str(index)).encode()).digest()*512
 with open(sys.argv[1],'wb') as stream:
  stream.write(data);stream.flush();os.fsync(stream.fileno())
 fd=os.open(sys.argv[1],os.O_RDONLY)
 try:
  os.posix_fadvise(fd,0,0,os.POSIX_FADV_DONTNEED)
  with os.fdopen(os.dup(fd),'rb') as stream:got=stream.read()
  assert got==data,(index,len(got))
 finally:os.close(fd)
print(json.dumps({'cycles':int(sys.argv[3])-int(sys.argv[2]),'bytes_per_cycle':16384}))
'''
REUSE = '''
expected=hashlib.sha256(b'dfs-fd-budget-767').digest()*512
for _ in range(100):
 fd=os.open(sys.argv[1],os.O_RDONLY)
 try:
  os.posix_fadvise(fd,0,0,os.POSIX_FADV_DONTNEED)
  with os.fdopen(os.dup(fd),'rb') as stream:got=stream.read()
  assert got==expected
 finally:os.close(fd)
'''


def local_probes(text):
    values = [float(line.rsplit(' ', 1)[1]) for line in text.splitlines()
              if line.startswith('afs_dfs_read_checks_total{stage="local_probe"} ')]
    if len(values) != 1 or not math.isfinite(values[0]) or values[0] < 0 or not values[0].is_integer():
        raise AssertionError('one exact nonnegative local-probe counter is required')
    return int(values[0])


def validate(observations, probes_before, probes_after):
    """Use raw FD device/inode identities, never just the reported total curve."""
    assert len(observations) == 8, 'baseline, six batches and same-object checkpoint required'
    initial = observations[0]['nodes']
    tables = []
    for index, row in enumerate(observations):
        assert set(row['nodes']) == {'A', 'B'}
        for role, census in row['nodes'].items():
            assert all(census[key] == initial[role][key] for key in ('pid', 'start_ticks', 'cache_root'))
            fd_inventory.classify(census, CAPACITY)
        table = fd_inventory.classify(row['nodes']['A'], CAPACITY)
        tables.append(table)
        assert table['duplicates'] == table['excess'] == 0, 'duplicate or excess physical chunk FD'
        assert table['noncache'] <= tables[0]['noncache'] + 4, 'non-cache FD growth during closed checkpoints'
        assert fd_inventory.classify(row['nodes']['B'], CAPACITY)['eligible'] == 0
        if 1 <= index <= 6:
            assert row['verified_cycles'] == index * BATCH
            assert table['eligible'] == min(index * BATCH, CAPACITY), 'reader cache did not reach its declared bound'
    sets = [set(row['targets']) for row in tables[1:7]]
    assert len(set.union(*sets)) == CYCLES, 'different content did not exercise distinct physical readers'
    assert not sets[0] & sets[-1], 'oldest readers were retained'
    assert all(len(sets[i] - sets[i + 1]) == BATCH for i in range(1, 5)), 'eviction did not release old descriptors'
    assert tables[-1]['targets'] == tables[-2]['targets'], 'same-object reuse changed cached FD ownership'
    assert tables[-1]['noncache'] <= tables[-2]['noncache'] + 4, 'same-object reads accumulated other FDs'
    assert probes_after - probes_before >= CYCLES + 100, 'kernel cache hid the AFS read path'
    return {'capacity': CAPACITY, 'verified_cycles': CYCLES, 'bytes_per_cycle': 16384,
            'local_probe_growth': probes_after - probes_before,
            'chunk_fd_counts': [row['eligible'] for row in tables[1:7]],
            'distinct_observed_chunk_files': len(set.union(*sets)), 'oldest_readers_retired': True,
            'same_object_reads': 100, 'performance_acceptance': 'NOT_EVALUATED'}


def run(cluster):
    observations = []
    def observe(verified_cycles):
        nodes = {}
        for role in ('A', 'B'):
            state = cluster.channel(role).call('status')[role]
            assert state['returncode'] is None, 'owned AFS Node is not running'
            result = cluster.command(role, ['python3', '-c', PROBE, '--pid', str(state['pid']),
                                          '--cache-root', cluster.role_dir(role) + '/state/dfs/chunks'])
            nodes[role] = json.loads(result['stdout'])
        value = {'verified_cycles': verified_cycles, 'nodes': nodes}
        observations.append(value)
        write_json(cluster.output / 'fd-observations.json', observations)
        cluster.record('fd.checkpoint.' + str(len(observations)),
                       {'cycles': verified_cycles, 'A': fd_inventory.classify(nodes['A'], CAPACITY),
                        'B': fd_inventory.classify(nodes['B'], CAPACITY)})
    before = cluster.health('A', '/metrics')
    (cluster.output / 'fd-metrics-before.txt').write_text(before)
    observe(0)
    path = cluster.workspace('A') + '/reader-fd-budget.bin'
    for end in range(BATCH, CYCLES + 1, BATCH):
        result = cluster.python('A', PUBLISH, path, str(end - BATCH), str(end), timeout=180)
        assert json.loads(result['stdout']) == {'cycles': BATCH, 'bytes_per_cycle': 16384}
        observe(end)
    cluster.python('A', REUSE, path, timeout=60)
    observe(CYCLES)
    after = cluster.health('A', '/metrics')
    (cluster.output / 'fd-metrics-after.txt').write_text(after)
    cluster.record('fd.reader_budget', validate(observations, local_probes(before), local_probes(after)))
