import importlib.util
from pathlib import Path
import unittest
from types import SimpleNamespace

spec = importlib.util.spec_from_file_location('counts', Path(__file__).with_name('fuse_callback_counts.py'))
counts = importlib.util.module_from_spec(spec)
spec.loader.exec_module(counts)


def samples(read=0, write=0, filesystem='ownerfs'):
    return '\n'.join(f'afs_fuse_callbacks_total{{filesystem="{filesystem}",operation="{op}"}} {value}' for op, value in [('read', read), ('write', write)])


class CounterGuards(unittest.TestCase):
    @unittest.skipUnless((Path(__file__).parents[1] / "native-workspace-linux.py").exists(), "native-workspace-linux.py runner not migrated in this slice")
    def test_metadata_selector_requires_counter_mode(self):
        spec = importlib.util.spec_from_file_location('native_guard', Path(__file__).parents[1] / 'native-workspace-linux.py')
        native = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(native)
        run = object.__new__(native.Run)
        run.args = SimpleNamespace(fuse_counters_only=False, fuse_counter_case='metadata')
        with self.assertRaisesRegex(ValueError, 'requires fuse-counters-only'):
            run.preflight()

    def test_metadata_missing_samples_and_leaked_callbacks_are_not_bypass(self):
        before = {'read': 0, 'write': 0, 'create': 0, 'unlink': 0, 'mkdir': 0, 'rmdir': 0}
        counts.verify_metadata_window(before, before, native=True)
        for after in [dict(before, unlink=1), {k: v for k, v in before.items() if k != 'unlink'}]:
            with self.subTest(after=after), self.assertRaises(ValueError):
                counts.verify_metadata_window(before, after, native=True)
        missing = {'read': 0, 'write': 0}
        with self.assertRaises(ValueError):
            counts.verify_metadata_window(missing, missing, native=True)

    def test_metadata_positive_control_requires_all_selected_operations(self):
        before = {'read': 0, 'write': 0, 'create': 5, 'unlink': 7, 'mkdir': 2, 'rmdir': 2}
        after = dict(before, create=17, unlink=19, mkdir=3, rmdir=3)
        self.assertEqual(counts.verify_metadata_window(before, after, native=False)['unlink'], 12)
        for field in ('create', 'unlink', 'mkdir', 'rmdir'):
            wrong = dict(after, **{field: before[field]})
            with self.subTest(field=field), self.assertRaises(ValueError):
                counts.verify_metadata_window(before, wrong, native=False)

    @unittest.skipUnless((Path(__file__).parents[1] / "native-workspace-linux.py").exists(), "native-workspace-linux.py runner not migrated in this slice")
    def test_counter_mode_admission_is_not_self_excluding(self):
        spec = importlib.util.spec_from_file_location('native_guard', Path(__file__).parents[1] / 'native-workspace-linux.py')
        native = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(native)
        run = object.__new__(native.Run)
        run.args = SimpleNamespace(source_rejection_only=False, control_capacity_only=False,
            orderly_recovery_only=False, node_shutdown_only=False, semantics_only=False,
            semantics_probe=None, fuse_counters_only=True)
        def reached(name, *args):
            raise RuntimeError('reached-' + name)
        run.check = reached
        with self.assertRaisesRegex(RuntimeError, 'reached-Linux-root'):
            run.preflight()

    def test_missing_foreign_fraction_negative_duplicate_not_zero(self):
        for text in ['', samples(filesystem='dfs'), samples().splitlines()[0], samples() + '\n' + samples(), samples(read=-1), samples(read='0.5')]:
            with self.subTest(text=text), self.assertRaises(ValueError):
                counts.parse_counts(text)

    def test_integer_counts_preserve_precision_and_label_order(self):
        text = samples(read=2**54) + '\n' + samples(filesystem='dfs')
        self.assertEqual(counts.parse_counts(text), {'read': 2**54, 'write': 0})
        self.assertEqual(counts.parse_counts(text.replace('filesystem="ownerfs",operation="read"', 'operation="read",filesystem="ownerfs"'))['read'], 2**54)

    def test_delta_rejects_reset_missing_series_and_invalid_counts(self):
        before = {'read': 3, 'write': 4}
        for after in [{'read': 2, 'write': 4}, {'read': 3}, {'read': 3, 'write': -1}, {'read': 3, 'write': True}]:
            with self.subTest(after=after), self.assertRaises(ValueError):
                counts.delta(before, after)
        self.assertEqual(counts.delta(before, {'read': 8, 'write': 4}), {'read': 5, 'write': 0})
        self.assertEqual(before, {'read': 3, 'write': 4})


if __name__ == '__main__':
    unittest.main()
