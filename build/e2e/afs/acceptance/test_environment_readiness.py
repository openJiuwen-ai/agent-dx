#!/usr/bin/env python3
import hashlib
import io
import json
import tarfile
import tempfile
import unittest
from pathlib import Path

import environment
import test_environment as fixtures


def write_json(path: Path, value: object) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def refs_for(root: Path, *paths: str) -> dict[str, str]:
    return {rel: sha(root / rel) for rel in paths}


def manifest(required: list[str], audit_rel: str | None = None) -> dict:
    files: dict[str, object] = {rel: {"sha256": "0" * 64, "bytes": 1} for rel in required}
    if audit_rel is not None:
        files[audit_rel] = {"sha256": "1" * 64, "bytes": 2}
    return {"files": files}


def write_file(path: Path, data: bytes = b"x") -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(data)


def write_tar(path: Path, members: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tarfile.open(path, "w:gz") as archive:
        for name, value in members.items():
            data = json.dumps(value, sort_keys=True).encode() if isinstance(value, dict) else bytes(value)
            info = tarfile.TarInfo(name)
            info.size = len(data)
            archive.addfile(info, io.BytesIO(data))


def finalize_manifest(root: Path, rels: list[str], audit_rel: str | None = None) -> dict:
    files: dict[str, object] = {}
    for rel in rels:
        path = root / rel
        if not path.exists():
            write_file(path)
        files[rel] = {"sha256": sha(path), "bytes": path.stat().st_size}
    if audit_rel is not None:
        path = root / audit_rel
        files[audit_rel] = {"sha256": sha(path), "bytes": path.stat().st_size}
    return {"files": files}


def boundary(action: str, result: dict | None = None) -> dict:
    return {
        "action": action,
        "status": "PASS",
        "formal_acceptance": "NOT_RUN",
        "environment": "PREPARING",
        "result": result or {},
    }


def identity(role: str = "node") -> dict:
    return {
        "pid": 123,
        "start_ticks": "456",
        "boot_id": "boot",
        "exe": f"/bin/afs-{role}",
        "sha256": "895f39fd660b7f7d9735eaaa4f3a082c3692a409d954c4aa8b5b0cc515a700ad" if role == "meta" else "a3fe6573fc5f5a41c30b823855cfe2fdd1428950f878f1e29756e7bfc0d7e9d9",
    }


def backend_audit() -> dict:
    guest = {"final_available_bytes": 5 * environment.GIB}
    retained = {
        "status": "PASS",
        "before": {"snapshot_bytes": 72821, "native_status": {"durability": {"appendfsync": "always", "maxmemory-policy": "noeviction"}}},
        "after": {"snapshot_bytes": 72821, "native_status": {"durability": {"appendfsync": "always", "maxmemory-policy": "noeviction"}}},
    }
    return {
        "status": "PASS",
        "scope": "RETAINED_SCOPED_NORMAL_BACKEND_INTEGRATION",
        "formal_acceptance": "NOT_RUN",
        "environment": "PREPARING",
        "checks": [{"status": "PASS"} for _ in range(501)],
        "validation": {"r2": {"tests": 17}},
        "lanes": {
            "etcd": {"retained_snapshot": retained, "guests": {"ctl": guest, "a": guest, "b": guest}},
            "redis": {"retained_snapshot": retained, "guests": {"ctl": guest, "a": guest, "b": guest}},
        },
        "preserved_failures": [{"classification": "ORIGINAL_COLLECTOR_FAILURE"}],
    }


def create_backend_raw(root: Path, audit: dict) -> list[str]:
    rels = [
        "README.md", "audit.py", "preparation/etcd-native.json", "preparation/redis-native.json",
        "preparation/original-serialization-failure.json",
    ]
    for rel in rels:
        write_file(root / "backend" / rel)
    for backend in ("etcd", "redis"):
        retained = audit["lanes"][backend]["retained_snapshot"]
        for role in ("ctl", "a", "b"):
            role_name = "meta" if role == "ctl" else "node"
            result = {"identity": identity(role_name)}
            stop_result = {"controller": {"exit": 0, "stdout": f"{role_name} stopped exit_code=0\n"}}
            members = {
                f"etc/{role_name}.toml": b"config",
                "evidence/start-1.json": boundary("start", result),
                "evidence/stop-1.json": boundary("stop", stop_result),
                "evidence/start-2.json": boundary("start", result),
                "evidence/stop-2.json": boundary("stop", stop_result),
                "evidence/cleanup.json": boundary("cleanup", {"stopped": True, "mounts_gone": True}),
            }
            if role == "ctl":
                members["evidence/retained-snapshot-restart.json"] = retained
            else:
                members["evidence/read.json"] = boundary("read", {"identity": identity(), "read": {"bytes": 4194321, "sha256": "b" * 64}, "physical": [{"chunk": {"bytes": 17}}, {"chunk": {"bytes": 4194304}}]})
            rel = f"{backend}/{role}/runtime-raw.tar.gz"
            write_tar(root / "backend" / rel, members)
            rels.append(rel)
    return rels


def moose_audit() -> dict:
    inc = {
        "before": [1, "10", "boot", "/opt/afs-moose-round3-v85/bin/mfsmount", "a" * 64],
        "after": [2, "20", "boot", "/opt/afs-moose-round3-v85/bin/mfsmount", "a" * 64],
    }
    return {
        "result": "PASS_ARCHIVED_ONE_COPY_POLICY_READ_AND_NORMAL_RESTART_CHECKS",
        "formal_acceptance": "NOT_RUN",
        "environment": "PREPARING",
        "strong_durable_write": "BLOCKED",
        "blocker": "B001",
        "fair_performance_comparison": "NOT_RUN",
        "length": 32 * 1024**2,
        "sha256": "626f47ade3da8112941c844a3a1d8a02b94c5e134a9c3391a3375aa22cd1dbc7",
        "ranges_per_command": 64,
        "goal1_policy": {"status": "QUALIFIED_FOR_NEW_V89_FIXTURE", "fields": {"create_labels": "*", "keep_labels": "*"}},
        "content_receipts": [
            {"role": "a", "phase": "write", "receipt": "round3-moose-policy-v89/evidence/write.json"},
            {"role": "a", "phase": "pre", "receipt": "round3-moose-policy-v89/evidence/a-pre.json"},
            {"role": "a", "phase": "post", "receipt": "round3-moose-policy-v89/evidence/a-post.json"},
            {"role": "b", "phase": "pre", "receipt": "round3-moose-policy-v89/evidence/b-pre.json"},
            {"role": "b", "phase": "post", "receipt": "round3-moose-policy-v89/evidence/b-post.json"},
        ],
        "final_normal_stop_receipts": {
            "ctl": {"receipt": "round3-moose-read-v85/evidence/ctl-stop.json"},
            "a": {"receipt": "round3-moose-read-v85/evidence/a-stop.json"},
            "b": {"receipt": "round3-moose-read-v85/evidence/b-stop.json"},
        },
        "process_incarnations": {
            "ctl/master": {**inc, "initial_receipt": "round3-moose-read-v85/evidence/ctl-start.json", "restart_receipt": "round3-moose-read-v85/evidence/ctl-restart.json"},
            "a/chunk": {**inc, "initial_receipt": "round3-moose-read-v85/evidence/a-chunk-start.json", "restart_receipt": "round3-moose-read-v85/evidence/a-chunk-restart.json"},
            "a/fuse": {**inc, "initial_receipt": "round3-moose-read-v85/evidence/a-fuse-start.json", "restart_receipt": "round3-moose-read-v85/evidence/a-fuse-restart.json"},
            "b/fuse": {**inc, "initial_receipt": "round3-moose-read-v85/evidence/b-fuse-start.json", "restart_receipt": "round3-moose-read-v85/evidence/b-fuse-restart.json"},
        },
    }


def create_moose_raw(root: Path, audit: dict) -> list[str]:
    rels = ["README.md", "audit.py", "probes/round3-moose-policy.py", "probes/r3/round3-moose-read.py", "probes/r3/test_round3_moose_read.py"]
    for rel in rels:
        write_file(root / "moose" / rel)
    content = {"formal_acceptance": "NOT_RUN", "environment": "PREPARING", "blocker": "B001", "length": 32 * 1024**2, "sha256": "626f47ade3da8112941c844a3a1d8a02b94c5e134a9c3391a3375aa22cd1dbc7", "ranges": [{} for _ in range(64)]}
    proc = {"identity": {"pid": 1, "start_ticks": "10", "boot_id": "boot", "exe": "/x", "exe_sha256": "a" * 64}}
    for role in ("ctl", "a", "b"):
        members = {}
        for item in audit["content_receipts"]:
            if item["role"] == role:
                members[item["receipt"]] = content
        stop = audit["final_normal_stop_receipts"][role]["receipt"]
        members[stop] = {"formal_acceptance": "NOT_RUN", "environment": "PREPARING"}
        for label, value in audit["process_incarnations"].items():
            if label.startswith(role + "/"):
                members[value["initial_receipt"]] = proc
                members[value["restart_receipt"]] = proc
        rel = f"{role}/runtime-raw.tar.gz"
        write_tar(root / "moose" / rel, members)
        rels.append(rel)
    return rels


def threefs_audit() -> dict:
    return {
        "status": "PASS",
        "scope": "patched-reference normal IO/physical copies/retained-state restart only",
        "formal_acceptance": "NOT_RUN",
        "environment": "PREPARING",
        "strong_durable_comparison": "BLOCKED",
        "physical_slots_before": 192,
        "physical_slots_after": 192,
        "range_checks": 192,
        "compiler_inputs_reused": 143,
        "checks": 914,
        "process_resources": {"ctl": {"fdb": {}}, "a": {"storage": {}}, "b": {"storage": {}}, "c": {"storage": {}}},
    }


def create_3fs_raw(root: Path) -> list[str]:
    rels = [
        "README.md", "a/runtime-raw.tar.gz", "b/runtime-raw.tar.gz", "c/runtime-raw.tar.gz", "ctl/runtime-raw.tar.gz",
        "a/v84-write-r2.json", "a/v84-read.json", "b/v84-read.json", "b/v84-read-after-restart.json",
        "preparation/physical-plan-before.json", "preparation/physical-plan-after-r3.json", "probes/audit.py",
        "probes/round3-3fs.py", "probes/round3-3fs-physical.py",
    ]
    data = environment.payload_3fs()
    digest = hashlib.sha256(data).hexdigest()
    write_json(root / "threefs/a/v84-write-r2.json", {"status": "PASS", "bytes": len(data), "observed_sha256": digest, "writes_1mib": [{} for _ in range(32)]})
    read = {"status": "PASS", "bytes": len(data), "sha256": digest, "fixed_ranges_exact": [{} for _ in range(64)]}
    write_json(root / "threefs/a/v84-read.json", read)
    write_json(root / "threefs/b/v84-read.json", read)
    write_json(root / "threefs/b/v84-read-after-restart.json", read)
    plan = {"roles": {"a": [{} for _ in range(64)], "b": [{} for _ in range(64)], "c": [{} for _ in range(64)]}}
    write_json(root / "threefs/preparation/physical-plan-before.json", plan)
    write_json(root / "threefs/preparation/physical-plan-after-r3.json", plan)
    for rel in ("README.md", "probes/audit.py", "probes/round3-3fs.py", "probes/round3-3fs-physical.py"):
        write_file(root / "threefs" / rel)
    for role in ("ctl", "a", "b", "c"):
        write_tar(root / "threefs" / role / "runtime-raw.tar.gz", {"run/evidence/dummy.json": {"status": "PASS"}})
    return rels


class ReadinessConsumerTests(unittest.TestCase):
    def test_missing_readiness_schema_keeps_existing_deferred_blocked(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = fixtures.EnvironmentEvaluatorTests().make_bundle(root, initial_available=120 * environment.GIB)
            report = environment.evaluate_environment(fixtures.lock(bundle["contract"]["sha256"]), bundle, root)
            names = {item["name"]: item["status"] for item in report["checks"]}
            self.assertEqual(names["durable-backend-restart"], "BLOCKED")
            self.assertEqual(names["actual-moosefs-mount-io"], "BLOCKED")
            self.assertEqual(names["actual-3fs-mount-io"], "BLOCKED")

    def test_durable_backend_restart_consumer_requires_hash_bound_audit_and_manifest(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            required = [
                "README.md", "audit.py", "etcd/ctl/runtime-raw.tar.gz", "etcd/a/runtime-raw.tar.gz", "etcd/b/runtime-raw.tar.gz",
                "redis/ctl/runtime-raw.tar.gz", "redis/a/runtime-raw.tar.gz", "redis/b/runtime-raw.tar.gz",
                "preparation/etcd-native.json", "preparation/redis-native.json", "preparation/original-serialization-failure.json",
            ]
            audit = backend_audit()
            write_json(root / "backend/audit.json", audit)
            required = create_backend_raw(root, audit)
            m = finalize_manifest(root / "backend", required, "audit.json")
            write_json(root / "backend/artifact-hashes.json", m)
            bundle = {"readiness_evidence": {"durable-backend-restart": {"audit": "backend/audit.json", "artifact_hashes": "backend/artifact-hashes.json"}}}
            refs = refs_for(root, "backend/audit.json", "backend/artifact-hashes.json")
            self.assertEqual(environment.evaluate_durable_backend_restart(bundle, root, refs)["status"], "PASS")
            write_json(root / "backend/audit.json", {**backend_audit(), "status": "FAIL"})
            self.assertEqual(environment.evaluate_durable_backend_restart(bundle, root, refs)["status"], "FAIL")
            write_json(root / "backend/audit.json", audit)
            (root / "backend/etcd/a/runtime-raw.tar.gz").unlink()
            refs["backend/audit.json"] = sha(root / "backend/audit.json")
            self.assertEqual(environment.evaluate_durable_backend_restart(bundle, root, refs)["status"], "BLOCKED")

    def test_moosefs_mount_consumer_preserves_b001_boundary(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            required = ["README.md", "audit.py", "ctl/runtime-raw.tar.gz", "a/runtime-raw.tar.gz", "b/runtime-raw.tar.gz", "probes/round3-moose-policy.py", "probes/r3/round3-moose-read.py", "probes/r3/test_round3_moose_read.py"]
            audit = moose_audit()
            write_json(root / "moose/audit.json", audit)
            required = create_moose_raw(root, audit)
            m = finalize_manifest(root / "moose", required, "audit.json")
            write_json(root / "moose/artifacts.json", m)
            bundle = {"readiness_evidence": {"actual-moosefs-mount-io": {"audit": "moose/audit.json", "artifact_hashes": "moose/artifacts.json"}}}
            refs = refs_for(root, "moose/audit.json", "moose/artifacts.json")
            self.assertEqual(environment.evaluate_actual_moosefs_mount_io(bundle, root, refs)["status"], "PASS")
            bad = moose_audit()
            bad["strong_durable_write"] = "PASS"
            write_json(root / "moose/audit.json", bad)
            refs["moose/audit.json"] = sha(root / "moose/audit.json")
            self.assertEqual(environment.evaluate_actual_moosefs_mount_io(bundle, root, refs)["status"], "FAIL")
            write_json(root / "moose/audit.json", audit)
            write_file(root / "moose/probes/round3-moose-policy.py", b"changed")
            refs["moose/audit.json"] = sha(root / "moose/audit.json")
            self.assertEqual(environment.evaluate_actual_moosefs_mount_io(bundle, root, refs)["status"], "FAIL")

    def test_3fs_mount_consumer_requires_physical_slots_and_boundaries(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            required = [
                "README.md", "a/runtime-raw.tar.gz", "b/runtime-raw.tar.gz", "c/runtime-raw.tar.gz", "ctl/runtime-raw.tar.gz",
                "a/v84-write-r2.json", "a/v84-read.json", "b/v84-read.json", "b/v84-read-after-restart.json",
                "preparation/physical-plan-before.json", "preparation/physical-plan-after-r3.json", "probes/audit.py",
                "probes/round3-3fs.py", "probes/round3-3fs-physical.py",
            ]
            write_json(root / "threefs/audit.json", threefs_audit())
            required = create_3fs_raw(root)
            write_json(root / "threefs/artifact-hashes.json", finalize_manifest(root / "threefs", required))
            bundle = {"readiness_evidence": {"actual-3fs-mount-io": {"audit": "threefs/audit.json", "artifact_hashes": "threefs/artifact-hashes.json"}}}
            refs = refs_for(root, "threefs/audit.json", "threefs/artifact-hashes.json")
            self.assertEqual(environment.evaluate_actual_3fs_mount_io(bundle, root, refs)["status"], "PASS")
            bad = threefs_audit()
            bad["physical_slots_after"] = 191
            write_json(root / "threefs/audit.json", bad)
            refs["threefs/audit.json"] = sha(root / "threefs/audit.json")
            self.assertEqual(environment.evaluate_actual_3fs_mount_io(bundle, root, refs)["status"], "FAIL")
            write_json(root / "threefs/audit.json", threefs_audit())
            tampered = json.loads((root / "threefs/a/v84-read.json").read_text())
            tampered["sha256"] = "0" * 64
            write_json(root / "threefs/a/v84-read.json", tampered)
            refs["threefs/audit.json"] = sha(root / "threefs/audit.json")
            self.assertEqual(environment.evaluate_actual_3fs_mount_io(bundle, root, refs)["status"], "FAIL")

    def test_manifest_rejects_missing_escape_symlink_and_byte_or_hash_mismatch(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            packet = root / "packet"
            write_file(packet / "ok.txt", b"ok")
            write_file(root / "outside.txt", b"outside")
            manifest = {
                "files": {
                    "ok.txt": {"sha256": sha(packet / "ok.txt"), "bytes": 2},
                    "missing.txt": {"sha256": "0" * 64, "bytes": 1},
                    "../outside.txt": {"sha256": sha(root / "outside.txt"), "bytes": 7},
                    "bad-bytes.txt": {"sha256": "0" * 64, "bytes": 99},
                }
            }
            write_file(packet / "bad-bytes.txt", b"x")
            (packet / "link.txt").symlink_to(root / "outside.txt")
            manifest["files"]["link.txt"] = {"sha256": sha(root / "outside.txt"), "bytes": 7}
            problems: list[dict[str, str]] = []
            environment.validate_manifest_files(packet, manifest, problems, "unit artifacts")
            details = "\n".join(problem["detail"] for problem in problems)
            self.assertIn("missing artifact missing.txt", details)
            self.assertIn("invalid artifact path", details)
            self.assertIn("byte count mismatch for bad-bytes.txt", details)
            self.assertIn("sha256 mismatch for bad-bytes.txt", details)
            self.assertIn("symlink rejected for link.txt", details)


if __name__ == "__main__":
    unittest.main()
