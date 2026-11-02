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
  if got != data:raise AssertionError((index,len(got)))
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
  if got != expected:raise AssertionError('same-object content changed')
 finally:os.close(fd)
'''


def local_probes(text):
    values = [float(line.rsplit(' ', 1)[1]) for line in text.splitlines()
              if line.startswith('afs_dfs_read_checks_total{stage="local_probe"} ')]
    if (len(values) != 1
            or not math.isfinite(values[0])
            or values[0] < 0):
        raise AssertionError('one exact nonnegative local-probe counter is required')
    if (not values[0].is_integer()):
        raise AssertionError('one exact nonnegative local-probe counter is required')
    return int(values[0])


def validate(observations, probes_before, probes_after):
    """Use raw FD device/inode identities, never just the reported total curve."""
    if not (len(observations) == 8):
        raise AssertionError('baseline, six batches and same-object checkpoint required')
    initial = observations[0]['nodes']
    tables = []
    for index, row in enumerate(observations):
        if not (set(row['nodes']) == {'A', 'B'}):
            raise AssertionError('required test condition failed')
        for role, census in row['nodes'].items():
            if not (all(census[key] == initial[role][key] for key in ('pid', 'start_ticks', 'cache_root'))):
                raise AssertionError('required test condition failed')
            fd_inventory.classify(census, CAPACITY)
        table = fd_inventory.classify(row['nodes']['A'], CAPACITY)
        tables.append(table)
        if not (table['duplicates'] == table['excess'] == 0):
            raise AssertionError('duplicate or excess physical chunk FD')
        if not (table['noncache'] <= tables[0]['noncache'] + 4):
            raise AssertionError('non-cache FD growth during closed checkpoints')
        if not (fd_inventory.classify(row['nodes']['B'], CAPACITY)['eligible'] == 0):
            raise AssertionError('required test condition failed')
        if 1 <= index <= 6:
            if not (row['verified_cycles'] == index * BATCH):
                raise AssertionError('required test condition failed')
            if not (table['eligible'] == min(index * BATCH, CAPACITY)):
                raise AssertionError('reader cache did not reach its declared bound')
    sets = [set(row['targets']) for row in tables[1:7]]
    if not (len(set.union(*sets)) == CYCLES):
        raise AssertionError('different content did not exercise distinct physical readers')
    if not (not sets[0] & sets[-1]):
        raise AssertionError('oldest readers were retained')
    if not (all(len(sets[i] - sets[i + 1]) == BATCH for i in range(1, 5))):
        raise AssertionError('eviction did not release old descriptors')
    if not (tables[-1]['targets'] == tables[-2]['targets']):
        raise AssertionError('same-object reuse changed cached FD ownership')
    if not (tables[-1]['noncache'] <= tables[-2]['noncache'] + 4):
        raise AssertionError('same-object reads accumulated other FDs')
    if not (probes_after - probes_before >= CYCLES + 100):
        raise AssertionError('kernel cache hid the AFS read path')
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
            if state['returncode'] is not None:
                raise AssertionError('owned AFS Node is not running')
            cache_root = str(Path(cluster.role_dir(role)) / 'state' / 'dfs' / 'chunks')
            result = cluster.command(role, ['python3', '-c', PROBE, '--pid', str(state['pid']),
                                          '--cache-root', cache_root])
            nodes[role] = json.loads(result['stdout'])
        value = {'verified_cycles': verified_cycles, 'nodes': nodes}
        observations.append(value)
        write_json(cluster.output / 'fd-observations.json', observations)
        cluster.record('fd.checkpoint.' + str(len(observations)),
                       {'cycles': verified_cycles, 'A': fd_inventory.classify(nodes.get('A'), CAPACITY),
                        'B': fd_inventory.classify(nodes.get('B'), CAPACITY)})
    before = cluster.health('A', '/metrics')
    (cluster.output / 'fd-metrics-before.txt').write_text(before)
    observe(0)
    path = str(Path(cluster.workspace('A')) / 'reader-fd-budget.bin')
    for end in range(BATCH, CYCLES + 1, BATCH):
        result = cluster.python('A', PUBLISH, path, str(end - BATCH), str(end), timeout=180)
        if not (json.loads(result['stdout']) == {'cycles': BATCH, 'bytes_per_cycle': 16384}):
            raise AssertionError('required test condition failed')
        observe(end)
    cluster.python('A', REUSE, path, timeout=60)
    observe(CYCLES)
    after = cluster.health('A', '/metrics')
    (cluster.output / 'fd-metrics-after.txt').write_text(after)
    cluster.record('fd.reader_budget', validate(observations, local_probes(before), local_probes(after)))
