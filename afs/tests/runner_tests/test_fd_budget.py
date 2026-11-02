"""Cache resource regressions require real progress, distinct objects and retirement."""
from copy import deepcopy
import unittest

from fixtures.common import discover, select
from suites.dfs.fd_budget import validate
from test_fd_inventory import snapshot


def observations():
    result = []
    for end in [0, 128, 256, 384, 512, 640, 768, 768]:
        value = snapshot()
        for index in range(max(0, end - 256), end):
            value['entries'].append({'fd': index + 3, 'target': '/owned/dfs/chunks/c-' + str(index),
                                     'kind': 32768, 'device': 9, 'inode': index + 10})
        result.append({'verified_cycles': end, 'nodes': {'A': value, 'B': snapshot()}})
    return result


class ReaderFdBudgetContracts(unittest.TestCase):
    def test_registered_selection_and_complete_cache_retirement(self):
        case = select(discover(), patterns=['dfs.resources.*'])
        self.assertEqual([c.id for c in case], ['dfs.resources.reader_fd_budget'])
        self.assertNotIn('ci-full', case[0].tags)
        self.assertNotIn('local-basic', case[0].tags)
        self.assertEqual(validate(observations(), 0, 868)['chunk_fd_counts'], [128, 256, 256, 256, 256, 256])

    def test_flat_counts_without_distinct_objects_or_actual_backend_reads_fail(self):
        for modify in ('no-eviction', 'kernel-cache', 'replaced-process', 'replaced-root', 'missing'):
            values = deepcopy(observations())
            probes = 868
            if modify == 'no-eviction':
                for row in values[3:]:
                    row['nodes']['A'] = deepcopy(values[2]['nodes']['A'])
            elif modify == 'kernel-cache':
                probes = 768
            elif modify == 'replaced-process':
                values[3]['nodes']['A']['start_ticks'] += 1
            elif modify == 'replaced-root':
                values[3]['nodes']['A']['cache_root']['inode'] += 1
            else:
                values.pop(2)
            with self.subTest(modify=modify), self.assertRaises(AssertionError):
                validate(values, 0, probes)
