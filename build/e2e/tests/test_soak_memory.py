"""Real service attribution must reject PID1 substitution and ambiguous children."""
import importlib.util
from pathlib import Path
import unittest
from copy import deepcopy
import os
import sys

SPEC = importlib.util.spec_from_file_location(
    'soak_memory_tests', Path(__file__).resolve().parents[1] / 'soak_memory.py')
M = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(M)


class MemoryAttribution(unittest.TestCase):
    def test_service_is_owned_same_executable_child_in_same_namespace(self):
        anchor = {'pid': 10, 'identity': {'exe_device': 1, 'exe_inode': 2, 'pid_namespace': 'pid:[3]'}}
        child = {'pid': 11, 'ppid': 10, 'identity': deepcopy(anchor['identity'])}
        self.assertEqual(M.service_child(anchor, [child])['pid'], 11)
        for rows in ([], [child, dict(child, pid=12)], [dict(child, ppid=9)],
                     [dict(child, identity=dict(child['identity'], pid_namespace='pid:[99]'))],
                     [dict(child, race='exited_during_census')]):
            with self.assertRaisesRegex(ValueError, 'missing or ambiguous'):
                M.service_child(anchor, rows)

    @unittest.skipUnless(sys.platform.startswith('linux'), 'actual procfs requires Linux')
    def test_real_linux_profile_contains_raw_and_attributed_anon_memory(self):
        profile = M.profile(os.getpid())
        self.assertEqual(profile['pid'], os.getpid())
        self.assertGreater(profile['pss_anon_bytes'], 0)
        self.assertEqual(profile['rss_bytes'], M.smaps(profile['smaps_rollup_raw'])['Rss'])
        self.assertTrue(profile['identity']['pid_namespace'].startswith('pid:['))


if __name__ == '__main__':
    unittest.main()
