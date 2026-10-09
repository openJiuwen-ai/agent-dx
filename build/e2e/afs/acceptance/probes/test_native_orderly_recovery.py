"""Meaningful Linux oracles for bounded managed orderly recovery."""
import copy
import importlib.util
from pathlib import Path
import platform
import tempfile
import os
import json
from types import SimpleNamespace
import unittest

spec = importlib.util.spec_from_file_location('orderly', Path(__file__).with_name('native_orderly_recovery.py'))
probe = importlib.util.module_from_spec(spec)
spec.loader.exec_module(probe)


@unittest.skipUnless(platform.system() == 'Linux', 'Linux-only qualification')
class OrderlyGuards(unittest.TestCase):
    def service(self, pid, tick, role='meta'):
        return {'pid': pid, 'starttick': tick, 'sha256': 'a' * 64, 'boot_id': 'boot1',
                'installed': {'path': '/opt/f/prefix/bin/afs-' + role, 'device': 10, 'inode': 11}}

    def snapshot(self, container='one', pid=101, tick=200):
        return {'status': {'root': 'root1', 'home_node': 'node-a', 'container': container},
                'source': {'dev': 8, 'ino': 9}, 'process': {'pid': pid, 'starttick': tick},
                'namespace': {'dev': 4, 'ino': 5}}

    def content(self):
        return {'bytes_hex': probe.PAYLOAD.hex(), 'size': 4096, 'sha256': probe.DIGEST,
                'eof': True, 'mode': 0o600, 'uid': 501, 'gid': 501}

    def test_complete_content_eof_and_metadata_are_required(self):
        probe.verify_content(self.content())
        for field, value in [('bytes_hex', '00' * 4096), ('size', 4095), ('sha256', '0' * 64),
                             ('eof', False), ('eof', 1), ('uid', 0), ('gid', 0), ('mode', 0o644)]:
            bad = self.content();bad[field] = value
            with self.subTest(field=field), self.assertRaises(ValueError):
                probe.verify_content(bad)

    def test_actual_container_stdout_binds_full_data_eof_stat_and_hash(self):
        output = probe.PAYLOAD.decode() + f'\nRECOVERY_META 4096 600 501 501\n{probe.DIGEST}  /workspace/{probe.PROOF}\nRECOVERY_EOF 0\n'
        probe.parse_container_content(output)
        for bad in [output[1:], output + 'extra', output.replace('EOF 0', 'EOF 1'),
                    output.replace('600 501 501', '644 0 0'), output.replace(probe.DIGEST, '0' * 64)]:
            with self.assertRaises(ValueError):probe.parse_container_content(bad)

    def test_private_oci_root_cannot_use_template_foreign_bundle_or_escape(self):
        template = Path('/opt/f/rootfs')
        runtime = {'id': 'one', 'bundle': '/opt/f/control/bundle-one', 'rootfs': '/opt/f/control/bundle-one/runtime-root'}
        config = {'root': {'path': runtime['rootfs'], 'readonly': True}}
        probe.verify_private_root(runtime, config, template)
        for root in [str(template), '/opt/foreign/root', '/opt/f/control/bundle-one/../root', runtime['bundle']]:
            bad = dict(runtime, rootfs=root)
            with self.subTest(root=root), self.assertRaises(ValueError):
                probe.verify_private_root(bad, {'root': {'path': root, 'readonly': True}}, template)
        for bad in [{'root': {'path': runtime['rootfs'], 'readonly': False}},
                    {'root': {'path': '/opt/f/control/bundle-one/wrong', 'readonly': True}}]:
            with self.assertRaises(ValueError):probe.verify_private_root(runtime, bad, template)

    def test_restart_requires_fresh_services_and_container_but_allows_namespace_reuse(self):
        old = {r: self.service(i, 100, r) for r, i in [('meta', 10), ('node', 11)]}
        new = {r: self.service(i, 200, r) for r, i in [('meta', 20), ('node', 21)]}
        first, second = self.snapshot(), self.snapshot('two', 102, 300)
        probe.verify_restart(old, new, first, second)
        for key, value in [('pid', 10), ('starttick', 100), ('sha256', 'b' * 64), ('installed', False)]:
            bad = copy.deepcopy(new);bad['meta'][key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):probe.verify_restart(old, bad, first, second)
        for key, value in [('source', {'dev': 8, 'ino': 10}), ('process', {'pid': 101, 'starttick': 300})]:
            bad = copy.deepcopy(second);bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):probe.verify_restart(old, new, first, bad)
        bad = copy.deepcopy(second);bad['status']['root'] = 'wrong'
        with self.assertRaises(ValueError):probe.verify_restart(old, new, first, bad)

    def test_exact_wait_receipt_rejects_stale_foreign_nonzero_or_live_supervisor(self):
        observed = self.service(10, 100)
        child = {'pid': '10', 'start_ticks': '100', 'boot_id': 'boot1', 'supervisor_pid': '9',
                 'exe': '/opt/f/prefix/bin/afs-meta', 'config': '/opt/f/etc/meta.toml',
                 'lifecycle': '/opt/f/run/meta.lifecycle.X'}
        receipt = dict(child, exit_code='0');ready = {'supervisor_pid': '9'}
        probe.verify_receipt(receipt, child, ready, observed, True)
        for key, value in [('exit_code', '1'), ('pid', '12'), ('start_ticks', '99'),
                           ('boot_id', 'wrong'), ('exe', '/opt/other/afs-meta')]:
            bad = dict(receipt);bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):probe.verify_receipt(bad, child, ready, observed, True)
        with self.assertRaises(ValueError):probe.verify_receipt(receipt, child, ready, observed, False)
        bad = dict(child, config='/opt/wrong.toml')
        with self.assertRaises(ValueError):probe.verify_receipt(dict(bad, exit_code='0'), bad, ready, observed, True)

    @unittest.skipUnless(os.geteuid() == 0, "trusted template guard requires root-owned fixture")
    def test_template_full_tree_rejects_symlink_and_metadata_content_mutation(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d);(root / 'file').write_bytes(b'trusted');(root / 'file').chmod(0o755)
            before = probe.trusted_tree(root)
            probe.verify_inputs({'template': before, 'config_prefix': {'config': 'a'}, 'meta_state_directory': {'ino': 1}},
                                {'template': before, 'config_prefix': {'config': 'a'}, 'meta_state_directory': {'ino': 1}})
            (root / 'file').write_bytes(b'changed')
            after = probe.trusted_tree(root)
            with self.assertRaises(ValueError):probe.verify_inputs(
                {'template': before, 'config_prefix': {'config': 'a'}, 'meta_state_directory': {'ino': 1}},
                {'template': after, 'config_prefix': {'config': 'a'}, 'meta_state_directory': {'ino': 1}})
            (root / 'link').symlink_to('file')
            with self.assertRaises(ValueError):probe.trusted_tree(root)

    def test_config_and_meta_state_directory_identity_cannot_be_recreated(self):
        before = {'template': {'.': {'ino': 1}}, 'config_prefix': {'config': 'a'}, 'meta_state_directory': {'ino': 1}}
        for key, value in [('template', {}), ('config_prefix', {'config': 'b'}), ('meta_state_directory', {'ino': 2})]:
            bad = copy.deepcopy(before);bad[key] = value
            with self.subTest(key=key), self.assertRaises(ValueError):probe.verify_inputs(before, bad)

    def test_archive_preserves_text_and_indexes_owned_runtime_special_files_without_copying_them(self):
        with tempfile.TemporaryDirectory() as d:
            fixture = Path(d) / 'fixture';bundle = fixture / 'control/bundle-one'
            root = bundle / 'runtime-root';root.mkdir(parents=True)
            (root / 'dev').mkdir();(root / 'dev/fd').symlink_to('/proc/self/fd')
            os.mkfifo(root / 'dev/fifo')
            (bundle / 'config.json').write_text(json.dumps({'root': {'path': str(root), 'readonly': True}}))
            (fixture / 'control/command-0001.stdout').write_text('real output')
            saved = {};run = SimpleNamespace(root=fixture, save=lambda name, value: saved.update({name: value}))
            destination = Path(d) / 'archived'
            probe.archive(run, 'control', destination, 'phase1')
            self.assertEqual((destination / 'command-0001.stdout').read_text(), 'real output')
            self.assertTrue((destination / 'bundle-one/config.json').is_file())
            self.assertFalse((destination / 'bundle-one/runtime-root').exists())
            index = saved['orderly-phase1-runtime-roots-local-only.json'][str(root)]
            self.assertEqual(index['dev/fd']['target'], '/proc/self/fd')
            self.assertIn('rdev', index['dev/fifo'])
            self.assertTrue((root / 'dev/fd').is_symlink())

    def test_current_container_read_cannot_reuse_stale_phase1_command_or_cached_response(self):
        with tempfile.TemporaryDirectory() as d:
            root = Path(d);(root / 'control').mkdir()
            shell = probe.read_shell()
            old = root / 'control/command-0010.command.json'
            old.write_text(json.dumps({'argv': ['exec', 'first', '/bin/sh', '-ec', shell]}))
            old.with_name('command-0010.stdout').write_text('stale wrong data')
            output = probe.PAYLOAD.decode() + f'\nRECOVERY_META 4096 600 501 501\n{probe.DIGEST}  /workspace/{probe.PROOF}\nRECOVERY_EOF 0\n'
            def native(*args):
                fresh = root / 'control/command-0007.command.json'
                fresh.write_text(json.dumps({'argv': ['exec', 'second', '/bin/sh', '-ec', shell]}))
                fresh.with_name('command-0007.stdout').write_text(output)
                fresh.with_name('command-0007.exit.json').write_text(json.dumps({'code': 0, 'reason': None, 'success': True}))
                return {'status': 'Executed'}
            run = SimpleNamespace(root=root, native=native, save=lambda *a: None)
            self.assertEqual(probe.container_read(run, 'second'), self.content())
            # The same returned cached success, without a new exact command, is insufficient.
            with self.assertRaises(ValueError):probe.container_read(run, 'second')

    @unittest.skipUnless((Path(__file__).parents[1] / "native-workspace-linux.py").exists(), "native-workspace-linux.py runner not migrated in this slice")
    def test_shared_final_archiver_is_used_by_every_native_mode_and_rejects_unknown_links_devices(self):
        spec = importlib.util.spec_from_file_location('shared_archive_driver', Path(__file__).parents[1] / 'native-workspace-linux.py')
        driver = importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
        with tempfile.TemporaryDirectory() as d:
            root = Path(d) / 'fixture';bundle = root / 'control/bundle-one'
            private = bundle / 'runtime-root';private.mkdir(parents=True)
            (private / 'dev').mkdir();(private / 'dev/fd').symlink_to('/proc/self/fd')
            os.mkfifo(private / 'dev/fifo')
            (bundle / 'config.json').write_text(json.dumps({'root': {'path': str(private), 'readonly': True}}))
            (root / 'control/command-0001.stdout').write_text('preserved')
            for mode in ('basic', 'semantics_only', 'source_rejection_only', 'control_capacity_only', 'orderly_recovery_only'):
                run = object.__new__(driver.Run);run.root = root;run.out = Path(d) / mode;run.out.mkdir()
                run.args = SimpleNamespace(orderly_recovery_only=mode == 'orderly_recovery_only')
                run.archive_artifacts('control', run.out / 'control')
                self.assertEqual((run.out / 'control/command-0001.stdout').read_text(), 'preserved')
                self.assertFalse((run.out / 'control/bundle-one/runtime-root').exists())
                self.assertTrue((run.out / 'artifact-archive-tool.json').is_file())
            (root / 'control/foreign').symlink_to(private)
            with self.assertRaisesRegex(ValueError, 'unknown linked/nonregular'):
                run.archive_artifacts('control', Path(d) / 'foreign-output')
            (root / 'control/foreign').unlink();os.mkfifo(root / 'control/unknown-device')
            with self.assertRaisesRegex(ValueError, 'unknown linked/nonregular'):
                run.archive_artifacts('control', Path(d) / 'device-output')

    @unittest.skipUnless((Path(__file__).parents[1] / "native-workspace-linux.py").exists(), "native-workspace-linux.py runner not migrated in this slice")
    def test_orderly_mode_is_mutually_exclusive_before_any_runtime(self):
        spec = importlib.util.spec_from_file_location('orderly_driver', Path(__file__).parents[1] / 'native-workspace-linux.py')
        driver = importlib.util.module_from_spec(spec);spec.loader.exec_module(driver)
        for field in ('source_rejection_only', 'control_capacity_only', 'semantics_only', 'semantics_probe'):
            args = SimpleNamespace(source_rejection_only=False, control_capacity_only=False,
                                   orderly_recovery_only=True, semantics_only=False, semantics_probe=None)
            setattr(args, field, True)
            run = object.__new__(driver.Run);run.args = args
            with self.subTest(field=field), self.assertRaisesRegex(ValueError, 'mutually exclusive'):
                run.preflight()


if __name__ == '__main__':unittest.main()
