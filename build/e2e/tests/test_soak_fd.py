"""Cache credit is explicit, bounded and independently recomputed from raw FDs."""
import importlib.util
from pathlib import Path
import stat
import unittest


def module():
    spec = importlib.util.spec_from_file_location('soak_fd_test', Path(__file__).resolve().parents[1] / 'soak_fd.py')
    value = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(value)
    return value


def observed(count=0, mode='dfs'):
    census = {'scope': 'afs-fd-census-v1', 'comm': 'afs-node', 'pid': 2, 'start_ticks': 100,
              'cache_root': {'path': '/owned/chunks', 'device': 9, 'inode': 2} if mode == 'dfs' else None,
              'entries': [{'fd': i, 'target': '/owned/chunks/' + str(i), 'device': 9,
                           'inode': i + 10, 'kind': stat.S_IFREG} for i in range(count)]}
    process = {'pid': 2, 'start_ticks': 100, 'fds': count, 'fd_census': census}
    return {'processes': {'afs-node:2': process}, 'anchor_runtime': None, 'fds': count}


class FdAccounting(unittest.TestCase):
    def test_credit_and_excess_are_separately_recorded(self):
        fd = module()
        value = observed(257)
        fd.attach(value, 'dfs')
        self.assertEqual(value['fds_noncache'], 1)
        self.assertEqual(value['processes']['afs-node:2']['fd_accounting']['excess'], 1)
        fd.verify(value, value, 'dfs', 128)

    def test_missing_forged_or_unbound_census_fails(self):
        fd = module()
        for field in ('fds_noncache', 'fd_accounting', 'fd_census', 'pid'):
            value = observed(256)
            fd.attach(value, 'dfs')
            if field == 'fds_noncache':
                value[field] = 1000
            else:
                value['processes']['afs-node:2'][field] = None
            with self.subTest(field=field), self.assertRaises(ValueError):
                fd.verify(value, value, 'dfs', 128)

    def test_ownerfs_never_receives_credit_and_cache_root_cannot_change(self):
        fd = module()
        value = observed(256, 'ownerfs')
        fd.attach(value, 'ownerfs')
        self.assertEqual(value['fds_noncache'], 256)
        with self.assertRaises(ValueError):
            fd.attach(observed(256), 'ownerfs')
        initial, changed = observed(0), observed(256)
        fd.attach(initial, 'dfs')
        changed['processes']['afs-node:2']['fd_census']['cache_root']['inode'] += 1
        fd.attach(changed, 'dfs')
        with self.assertRaises(ValueError):
            fd.verify(changed, initial, 'dfs', 128)

    def test_per_process_growth_cannot_be_hidden_by_other_process_retirement(self):
        fd = module()
        initial, changed = observed(0), observed(129)
        for value in (initial, changed):
            value['processes']['other:3'] = {'pid': 3, 'start_ticks': 200, 'fds': 200 if value is initial else 0}
            value['fds'] += value['processes']['other:3']['fds']
            fd.attach(value, 'dfs')
        # Eligible cache handles are intentional. Convert them to noncache targets.
        for entry in changed['processes']['afs-node:2']['fd_census']['entries']:
            entry['target'] = '/outside/' + str(entry['fd'])
        fd.attach(changed, 'dfs')
        with self.assertRaisesRegex(ValueError, 'process.*peak'):
            fd.verify(changed, initial, 'dfs', 128)


if __name__ == '__main__':
    unittest.main()
