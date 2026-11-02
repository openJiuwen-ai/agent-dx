"""FD classification must not disguise duplicates, deleted files or foreign paths."""
from copy import deepcopy
import stat
import unittest

from tools import fd_inventory


def snapshot(count=0):
    return {'scope': 'afs-fd-census-v1', 'pid': 20, 'start_ticks': 100, 'comm': 'afs-node',
            'cache_root': {'path': '/owned/dfs/chunks', 'device': 9, 'inode': 2},
            'entries': [{'fd': i + 3, 'target': '/owned/dfs/chunks/c-' + str(i),
                         'kind': stat.S_IFREG, 'device': 9, 'inode': i + 10} for i in range(count)]}


class FdInventoryContracts(unittest.TestCase):
    def test_only_256_unique_live_owned_files_receive_cache_allowance(self):
        result = fd_inventory.classify(snapshot(257), 256)
        self.assertEqual((result['eligible'], result['accounted'], result['excess'], result['noncache']),
                         (257, 256, 1, 1))

    def test_alias_deleted_foreign_device_and_nonregular_descriptors_remain_charged(self):
        value = snapshot(1)
        for change in ({'target': '/owned/dfs/chunks/c-1 (deleted)'},
                       {'target': '/owned/dfs/chunks-other/c-1'}, {'target': '/owned/dfs/chunks/sub/c-1'},
                       {'target': '/foreign/c-1'}, {'device': 10}, {'kind': stat.S_IFDIR}):
            entry = dict(value['entries'][0], fd=len(value['entries']) + 3, inode=100, **change)
            value['entries'].append(entry)
        value['entries'].append(dict(value['entries'][0], fd=99))
        result = fd_inventory.classify(value, 256)
        self.assertEqual(result['accounted'], 1)
        self.assertEqual(result['noncache'], len(value['entries']) - 1)
        self.assertEqual(result['duplicates'], 1)

    def test_no_root_has_no_cache_exemption(self):
        value = snapshot(2)
        value['cache_root'] = None
        self.assertEqual(fd_inventory.classify(value, 0)['noncache'], 2)

    def test_malformed_or_ambiguous_raw_evidence_cannot_be_normalized_away(self):
        for change in ({'fd': True}, {'inode': -1}, {'device': '9'}, {'target': None}):
            value = snapshot(1)
            value['entries'][0].update(change)
            with self.assertRaises(ValueError):
                fd_inventory.classify(value, 256)
        value = snapshot(1)
        value['entries'].append(deepcopy(value['entries'][0]))
        with self.assertRaises(ValueError):
            fd_inventory.classify(value, 256)
        value = snapshot(1)
        value['cache_root']['path'] = '/owned/../foreign'
        with self.assertRaises(ValueError):
            fd_inventory.classify(value, 256)
