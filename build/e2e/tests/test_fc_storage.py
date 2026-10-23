import copy
import json
import sys
import unittest
from pathlib import Path
from tempfile import TemporaryDirectory
from types import SimpleNamespace
from unittest.mock import Mock, patch
import subprocess

sys.path.append(str(Path(__file__).resolve().parents[1] / "firecracker"))


class StorageFixtureTests(unittest.TestCase):
    def test_juicefs_read_metrics_distinguish_empty_family_from_missing_evidence(self):
        from filesystem_metrics import juicefs_read_bytes

        family = "# TYPE juicefs_object_request_data_bytes counter\n"
        self.assertEqual(juicefs_read_bytes(family), 0)
        self.assertEqual(
            juicefs_read_bytes(
                "# TYPE juicefs_blockcache_miss counter\njuicefs_blockcache_miss 0\n"
            ),
            0,
        )
        self.assertEqual(
            juicefs_read_bytes(
                family
                + 'juicefs_object_request_data_bytes{method="PUT",storage_class=""} 99\n'
                + 'juicefs_object_request_data_bytes{method="GET",storage_class=""} 42\n'
                + 'juicefs_object_request_data_bytes{method="GET",storage_class="cold"} 5\n'
            ),
            47,
        )
        with self.assertRaisesRegex(ValueError, "unavailable"):
            juicefs_read_bytes("unrelated_metric 0\n")

    def test_juicefs_mounts_keep_adx_storage_configuration_generic(self):
        from juicefs_fixture import load_config, storage_config
        from filesystem_fixture import storage_config as select_storage

        with TemporaryDirectory() as directory:
            root = Path(directory)
            config = {
                "binary": "/opt/juicefs/juicefs",
                "metadata_url": "postgres://adx@127.0.0.1:15432/juicefs?sslmode=disable",
                "metadata_password_file": "/private/pg-password",
                "mount_root": str(root / "mounts"),
                "identity_file": "/evidence/storage-identities.json",
                "alias": "shared",
                "volume": "adx-test",
            }
            path = root / "juicefs.json"
            path.write_text(json.dumps(config))
            path.chmod(0o600)
            loaded = load_config(path)
            for role in ("node1", "node2", "coordinator"):
                expected = {
                    "kind": "distributed_fs",
                    "alias": "shared",
                    "mount_root": str(root / "mounts" / role),
                    "prefix": "adx/run-a",
                }
                self.assertEqual(storage_config(loaded, role, "run-a"), expected)
                with patch.dict("os.environ", {"ADX_FC_JUICEFS_CONFIG": str(path)}):
                    self.assertEqual(select_storage("/unused", role, "run-a"), expected)
            for role, run in [("node1", "../escape"), ("other", "run-a")]:
                with self.assertRaises(ValueError):
                    storage_config(loaded, role, run)
            config["metadata_url"] = "postgres://adx:secret@127.0.0.1/juicefs"
            path.write_text(json.dumps(config))
            with self.assertRaisesRegex(ValueError, "password_file"):
                load_config(path)

    def test_native_xfs_control_is_explicit_and_direct_only(self):
        from native_xfs import storage_config, verify_mount, verify_result

        with TemporaryDirectory() as directory:
            mount = Path(directory)
            self.assertEqual(storage_config(mount, "node2", "run-a")["mount_root"], str(mount))
            info = {"filesystems": [{"target": str(mount), "fstype": "xfs", "source": "/dev/test"}]}
            self.assertEqual(verify_mount(mount, info)["fstype"], "xfs")
            info["filesystems"][0]["fstype"] = "ext4"
            with self.assertRaises(ValueError):
                verify_mount(mount, info)
            with self.assertRaises(ValueError):
                storage_config(mount, "node1", "../escape")
        from storage_workload import identity

        work = identity({})
        snapshot_kinds = (
            ("Full", "expected_full_sha256"),
            ("Incremental", "expected_dirty_sha256"),
        )
        samples = [
            {
                "snapshot_type": kind,
                "sha256": work[digest],
                "checked_bytes": work["payload_bytes"],
                "continuity": True,
            }
            for kind, digest in snapshot_kinds
        ]
        result = {
            "native_control_only": True,
            "status": "passed",
            "inventories": {"node1": 0, "node2": 0},
            "cleanup_errors": [],
            "direct_baseline": {
                "status": "passed",
                "cleanup_errors": [],
                "workload": work,
                "samples": samples,
            },
        }
        verify_result(result)
        result["direct_baseline"]["samples"][1]["continuity"] = False
        with self.assertRaises(ValueError):
            verify_result(result)

    def test_juicefs_readahead_override_is_typed_and_recorded(self):
        from juicefs_fixture import mount_options

        self.assertEqual(mount_options({}), ([], True, 32))
        self.assertEqual(
            mount_options({"max_readahead_mib": 0}), (["--max-readahead", "0"], False, 0)
        )
        for value in [-1, 33, True, "0", 0.5]:
            with self.assertRaises(ValueError):
                mount_options({"max_readahead_mib": value})

    def test_comparison_never_pools_different_juicefs_geometry(self):
        from compare_storage import aggregate

        samples = [
            {
                "mode": "juicefs",
                "cache": cache,
                "request_count": 12,
                "restore_count": 1,
                "continuity": True,
                "artifact_bytes": 1,
                "pause_seconds": 1,
                "first_http_seconds": 1,
                "touch_seconds": 1,
            }
            for cache in ("cold", "warm")
            for _ in range(3)
        ]
        run = {
            "storage": {"kind": "juicefs", "alias": "shared"},
            "samples": samples,
            "repetitions": 3,
            "benchmark_only": True,
            "fixture_cleanup": {"exit_code": 0, "errors": []},
            "cases": [],
            "filesystem_parameters": {"block_size_kib": 4096, "max_readahead_mib": 4},
        }
        peer = copy.deepcopy(run)
        self.assertEqual(len(aggregate([(run, {}), (peer, {})])), 1)
        for key, value in [("block_size_kib", 1024), ("max_readahead_mib", 32)]:
            changed = copy.deepcopy(peer)
            changed["filesystem_parameters"][key] = value
            with self.assertRaisesRegex(ValueError, "filesystem parameters"):
                aggregate([(run, {}), (changed, {})])

    def test_gc_observation_tolerates_files_deleted_after_inventory(self):
        from storage_gc import file_usage

        with TemporaryDirectory() as directory:
            present = Path(directory) / "present"
            present.write_bytes(b"data")
            removed = Path(directory) / "removed"
            removed.write_bytes(b"old")
            inventory = [present, removed]
            removed.unlink()
            self.assertEqual(file_usage(inventory), {"files": 1, "bytes": 4})

    def test_fenced_fuse_close_is_never_retried_and_unknown_errors_are_preserved(self):
        import errno
        from storage_gc import close_fenced_descriptor

        for code in (errno.ENOTCONN, errno.EIO):
            with patch("storage_gc.os.close", side_effect=OSError(code, "fenced")) as close:
                close_fenced_descriptor(71)
                close.assert_called_once_with(71)
        with patch(
            "storage_gc.os.close", side_effect=OSError(errno.EBADF, "bad descriptor")
        ) as close:
            with self.assertRaises(OSError):
                close_fenced_descriptor(71)
            close.assert_called_once_with(71)

    def test_gc_plateau_checks_physical_blocks_and_meta_not_directory_names(self):
        from storage_gc import plateau, reclaimed

        before = {
            "physical": {"n": {"chunks": 10, "bytes": 1024, "catalog_bytes": 200}},
            "meta": {
                "entities": {"dfs_reference": 2, "dfs_file_version": 3},
                "revision": 100,
                "watch_floor": 84,
                "requests": 20,
            },
        }
        after = copy.deepcopy(before)
        self.assertTrue(plateau(before, after))
        after["physical"]["n"]["bytes"] = 10 * 1024**2
        self.assertFalse(plateau(before, after))
        after = copy.deepcopy(before)
        after["meta"]["entities"]["dfs_file_version"] = 100
        self.assertFalse(plateau(before, after))
        after = copy.deepcopy(before)
        after["meta"]["watch_floor"] = 0
        self.assertFalse(plateau(before, after))
        after = copy.deepcopy(before)
        after["meta"]["requests"] = 1000
        self.assertFalse(plateau(before, after))
        after = copy.deepcopy(before)
        self.assertTrue(reclaimed(before, after))
        after["meta"]["entities"]["dfs_reference"] = 3
        self.assertFalse(reclaimed(before, after))

    def test_gc_bootstrap_allows_only_store_identity_and_no_writer_lease(self):
        from storage_gc import bootstrap_only, reclaimed

        row = {
            "physical": {"n": {"chunks": 1, "bytes": 3, "catalog_bytes": 850}},
            "meta": {
                "entities": {"dfs_file_version": 1, "dfs_layout_root": 1},
                "revision": 100,
                "watch_floor": 84,
                "requests": 20,
            },
        }
        self.assertTrue(bootstrap_only(row))
        active = copy.deepcopy(row)
        active["meta"]["entities"]["dfs_write_lease"] = 1
        self.assertFalse(bootstrap_only(active))
        self.assertFalse(reclaimed(row, active))
        orphan = copy.deepcopy(row)
        orphan["physical"]["n"]["bytes"] = 1024**2
        self.assertFalse(bootstrap_only(orphan))
        leaked = copy.deepcopy(row)
        leaked["meta"]["entities"]["dfs_inode"] = 1
        self.assertFalse(reclaimed(row, leaked))

    def test_http_failure_evidence_survives_guest_probe_failure(self):
        from storage_faults import TrafficFailure, TrafficFailures

        with TemporaryDirectory() as directory:
            evidence = Path(directory)
            failures = TrafficFailures(evidence)
            probe = Mock(side_effect=TimeoutError("guest probe timed out"))
            failures.capture(
                TrafficFailure("env-a", "nonce-a", 1, 502, "connection reset", 1.2), probe
            )
            failures.capture(
                TrafficFailure("env-a", "nonce-b", 0, 502, "connection closed", 1.3), probe
            )
            self.assertEqual(probe.call_count, 1)
            self.assertEqual(
                json.loads((evidence / "http-failure-nonce-a.json").read_text())["body"],
                "connection reset",
            )
            self.assertEqual(
                json.loads((evidence / "http-failure-nonce-b.json").read_text())["status"], 502
            )
            self.assertIn(
                "guest probe timed out", (evidence / "http-failure-env-a-guest.json").read_text()
            )

    def test_restore_readiness_barrier_is_scoped_and_removed_after_failure(self):
        from storage_faults import ReadinessBarrier

        with TemporaryDirectory() as directory:
            root = Path(directory) / "owned-run"
            (root / "evidence").mkdir(parents=True)
            topology = root / "evidence/topology.json"
            topology.write_text(json.dumps({"namespaces": {"node2": "axt-deadbeef-node2"}}))
            with patch("storage_faults.subprocess.run") as run:
                with self.assertRaisesRegex(RuntimeError, "injected"):
                    with ReadinessBarrier(root, "node2"):
                        raise RuntimeError("injected")
                inserted, removed = [call.args[0] for call in run.call_args_list]
                self.assertEqual(inserted[1], "--net=/var/run/netns/axt-deadbeef-node2")
                self.assertEqual(inserted[3], "-I")
                self.assertEqual(removed[3], "-D")
                self.assertEqual(inserted[4:], removed[4:])
                self.assertIn("50090", inserted)
                topology.write_text(json.dumps({"namespaces": {"node2": "host"}}))
                with self.assertRaises(ValueError):
                    ReadinessBarrier(root, "node2")
                self.assertEqual(run.call_count, 2)

    def test_comparison_excludes_profiled_runs(self):
        from compare_storage import load

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "result.json").write_text(json.dumps({"meta_profile_enabled": True}))
            with (
                patch("compare_storage.verify"),
                self.assertRaisesRegex(ValueError, "profiling run"),
            ):
                load(root)

    def test_stage_profile_rejects_ambiguous_identity_and_counter_reset(self):
        from profile_storage import event, cow_delta

        line = 'event="phase" runtime_id=env-1 total_ms=12.5'
        self.assertEqual(event([line], "phase", "runtime_id", "env-1"), {"total_ms": 12.5})
        for lines in ([], [line, line], [line.replace("12.5", "-1")]):
            with self.assertRaises(ValueError):
                event(lines, "phase", "runtime_id", "env-1")
        before = dict(queue_sum=1, queue_count=2, execute_sum=3, execute_count=2)
        after = dict(queue_sum=2, queue_count=3, execute_sum=4, execute_count=3)
        self.assertEqual(cow_delta(before, after)["calls"], 1)
        with self.assertRaises(ValueError):
            cow_delta(after, before)
        with self.assertRaises(ValueError):
            cow_delta(before, {**after, "queue_count": 4})

    def test_fenced_restore_requires_old_process_exit_before_replacement_exec(self):
        from physical_restore import serialized_restores

        log = (
            "Type:create SandboxID:sbox-a env-a\nfirecracker: restored sandbox sbox-a pid=10\n"
            "Type:create SandboxID:sbox-b env-a\nfirecracker: restored sandbox sbox-b pid=20\n"
        )
        traces = {
            10: '1.000 execve("/bin/firecracker", []) = 0\n2.000 +++ killed by SIGKILL +++\n',
            20: '3.000 execve("/bin/firecracker", []) = 0\n',
        }
        self.assertEqual(len(serialized_restores(log, "env-a", traces)), 2)
        for invalid in (
            {10: traces[10].splitlines()[0], 20: traces[20]},
            {10: traces[10].replace("2.000", "4.000"), 20: traces[20]},
            {},
        ):
            with self.assertRaises(ValueError):
                serialized_restores(log, "env-a", invalid)

    def test_comparison_rejects_failed_fixture_cleanup(self):
        from compare_storage import load

        with TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "result.json").write_text("{}")
            (root / "storage-cleanup.json").write_text(
                json.dumps({"exit_code": 0, "errors": ["unmount failed"]})
            )
            with (
                patch("compare_storage.verify"),
                self.assertRaisesRegex(ValueError, "did not finish"),
            ):
                load(root)

    def test_lazy_restore_requires_cow_and_deferred_remote_data(self):
        from storage_contract import CASES, verify

        result = {
            "status": "passed",
            "cleanup_errors": [],
            "inventories": {"node1": 0, "node2": 0},
            "cases": [{"name": name, "passed": True} for name in CASES],
            "diagnostic_only": True,
            "storage": {"kind": "afs"},
            "repetitions": 1,
            "samples": [
                {"request_count": 12, "restore_count": 1, "continuity": True} for _ in range(2)
            ],
            "restore_trace": {
                "metadata_cow_confirmed": True,
                "full_source_read_before_first_touch": False,
                "late_touch_remote_read_bytes": 4096,
            },
        }
        verify(result)
        for field, value in [
            ("metadata_cow_confirmed", False),
            ("full_source_read_before_first_touch", True),
            ("late_touch_remote_read_bytes", 0),
        ]:
            invalid = copy.deepcopy(result)
            invalid["restore_trace"][field] = value
            with self.assertRaises(ValueError):
                verify(invalid)

    def test_failed_unmount_still_stops_all_owned_servers(self):
        from filesystem_fixture import FilesystemFixture, storage_config

        with TemporaryDirectory() as directory:
            root = Path(directory)
            fixture = object.__new__(FilesystemFixture)
            fixture.evidence = root
            run = SimpleNamespace(
                root=root,
                processes=dict.fromkeys(("meta", "node-a", "node-b", "node-c")),
                logs=[],
                stop=Mock(),
            )
            fixture.core = SimpleNamespace(run=run)
            mount = storage_config(root, "node1", "cleanup")["mount_root"]
            mountinfo = f"1 0 0:1 / {mount} rw - fuse.afs-dfs afs-dfs rw\n"
            with (
                patch.object(Path, "read_text", return_value=mountinfo),
                patch("filesystem_fixture.executable", return_value="/test/fusermount3"),
                patch(
                    "filesystem_fixture.subprocess.run",
                    side_effect=subprocess.TimeoutExpired("fusermount3", 30),
                ),
            ):
                errors = fixture.stop()
            self.assertTrue(any("TimeoutExpired" in error for error in errors))
            self.assertEqual(
                [call.args[0] for call in run.stop.call_args_list],
                ["node-c", "node-b", "node-a", "meta"],
            )

    def test_failure_observation_survives_fast_recovery(self):
        from storage_contract import source_invalidated

        self.assertFalse(
            source_invalidated({"invalidated": False, "assignment": {"node_id": "a"}}, "a")
        )
        self.assertTrue(
            source_invalidated({"invalidated": True, "assignment": {"node_id": "a"}}, "a")
        )
        self.assertTrue(
            source_invalidated({"invalidated": False, "assignment": {"node_id": "b"}}, "a")
        )

    def test_physical_proof_accepts_external_sandboxd_field_names(self):
        from physical_restore import restores

        for kind, field in [("sandbox", "SandboxID"), ("environment", "EnvironmentID")]:
            text = (
                f"firecracker: restored {kind} sbox-a pid=123 phases: load=2ms\n"
                f'Type:create {field}:sbox-a labels:{{key:"adx.environment_id" value:"env-a"}}\n'
            )
            self.assertEqual(restores(text, "env-a"), [{"backend": "sbox-a", "pid": 123}])
            self.assertEqual(restores(text, "env"), [])
            self.assertEqual(restores(text, "env-b"), [])

    def test_independent_mounts_share_only_the_namespace(self):
        from filesystem_fixture import storage_config

        a = storage_config(Path("/var/tmp/afs-run"), "node1", "run-01")
        b = storage_config(Path("/var/tmp/afs-run"), "node2", "run-01")
        c = storage_config(Path("/var/tmp/afs-run"), "coordinator", "run-01")
        self.assertEqual(a["kind"], "distributed_fs")
        self.assertEqual(len({a["mount_root"], b["mount_root"], c["mount_root"]}), 3)
        self.assertEqual(a["alias"], b["alias"])
        self.assertEqual(a["prefix"], c["prefix"])
        for bad in ("", "../bad", "a/b"):
            with self.assertRaises(ValueError):
                storage_config(Path("/var/tmp/afs-run"), "node1", bad)

    def test_performance_requires_all_samples_and_actual_traffic(self):
        from storage_contract import summarize

        samples = [
            {
                "mode": "afs",
                "cache": "cold",
                "pause_seconds": 2.0,
                "first_http_seconds": 1.0,
                "touch_seconds": 0.5,
                "artifact_bytes": 1024,
                "request_count": 12,
                "restore_count": 1,
                "continuity": True,
            }
            for _ in range(3)
        ]
        samples += [dict(s, cache="warm") for s in samples]
        self.assertEqual(summarize(samples, 3)["afs/cold"]["first_http_seconds"]["median"], 1.0)
        for bad in (
            samples[:-1],
            samples[:3],
            [dict(s, continuity=False) for s in samples],
            [dict(s, restore_count=2) for s in samples],
        ):
            with self.assertRaises(ValueError):
                summarize(bad, 3)
        invalid = copy.deepcopy(samples)
        invalid[0]["first_http_seconds"] = float("nan")
        with self.assertRaises(ValueError):
            summarize(invalid, 3)

    def test_comparison_requires_matching_runtime_and_workload(self):
        from compare_storage import comparable

        value = {
            "package": {"files": {"runtime/adx-execd": "hash"}},
            "image": {"image": "image@sha256:abc"},
            "workload": {"memory_mib": 256, "cpu_millis": 1000, "source_sha256": "guest"},
        }
        comparable(value, value)
        other = copy.deepcopy(value)
        other["workload"]["memory_mib"] = 512
        with self.assertRaises(ValueError):
            comparable(value, other)
