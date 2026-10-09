#!/usr/bin/env python3
import hashlib
import json
import tempfile
import unittest
from pathlib import Path
from unittest import mock

import environment


GIB = 1024**3


def write_json(path: Path, value: object) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def sha(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def observed(stdout: str, returncode: int = 0, status: str = "OBSERVED", stderr: str = "") -> dict:
    return {"argv": [], "returncode": returncode, "status": status, "stdout": stdout, "stderr": stderr}


def address(ip: str) -> dict:
    return observed(json.dumps([{"ifname": "eth0", "mtu": 1500, "addr_info": [{"family": "inet", "local": ip}]}]))


def block_layout(volume: str, size: int) -> dict:
    return observed(json.dumps({"blockdevices": [{"name": "vdb", "size": size, "children": [{"name": "vdb1", "fstype": "ext4", "size": size - 2 * 1024 * 1024, "mountpoints": [f"/mnt/lima-{volume}"]}]}]}))


def disk_space(volume: str, available: int, bad_fuse: bool = False) -> dict:
    return observed(
        "Filesystem Type 1B-blocks Used Avail Use% Mounted on\n"
        f"/dev/vdb1 ext4 {32 * GIB} 1 {available} 1% /mnt/lima-{volume}\n",
        returncode=1 if bad_fuse else 0,
        status="UNKNOWN" if bad_fuse else "OBSERVED",
        stderr="df: stale fuse: Transport endpoint is not connected\n" if bad_fuse else "",
    )


def inventory(host: str, ip: str, volume: str, mem_gib: int, volume_gib: int, available: int, *, arch: str = "aarch64", with_mem: bool = True) -> dict:
    inv = {
        "architecture": arch,
        "hostname": f"lima-{host}",
        "cpu_count": 2,
        "kernel": environment.EXPECTED_KERNEL,
        "os_release": 'NAME="Ubuntu"\nID=ubuntu\nVERSION_ID="24.04"\n',
        "swap": "Filename Type Size Used Priority\n",
        "addresses": address(ip),
        "block_layout": block_layout(volume, volume_gib * GIB),
        "disk_space": disk_space(volume, available, bad_fuse=host == "afs-accept-a"),
        "fuse_present": True,
        "rdma_device": observed("hca_id: rxe0\nGID[  1]: ::ffff:" + ip + ", RoCE v2\n"),
        "rdma_links": observed("link rxe0/1 state ACTIVE physical_state LINK_UP netdev eth0\n"),
    }
    if with_mem:
        inv["meminfo"] = f"MemTotal: {int(mem_gib * GIB / 1024)} kB\n"
    return inv


def lock(contract_sha: str | None = None) -> dict:
    data = {"state": "PREPARING", "image": {"url": "https://example.invalid/pinned-arm64.img"}}
    if contract_sha:
        data["contract"] = {"sha256": contract_sha}
    return data


class EnvironmentEvaluatorTests(unittest.TestCase):
    def test_failed_probes_and_wrong_volume_or_image_cannot_record_pass(self):
        cases = (
            ("address-failed", "afs-accept-a-ip-mtu"),
            ("block-failed", "afs-accept-a-guest-ext4-volume"),
            ("rdma-failed", "afs-accept-a-rxe-device-observed"),
            ("shadow-mount", "afs-accept-a-guest-ext4-volume"),
            ("df-wrong-fs", "afs-accept-a-data-reserve"),
            ("image-arch", "afs-accept-a-lima-config"),
            ("image-location", "afs-accept-a-lima-config"),
        )
        for mutation, check_name in cases:
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                path = root / "inventory-a.json"; inv = json.loads(path.read_text())
                if mutation in ("address-failed", "block-failed", "rdma-failed"):
                    key = {"address-failed": "addresses", "block-failed": "block_layout", "rdma-failed": "rdma_device"}[mutation]
                    inv[key]["returncode"] = 1; inv[key]["status"] = "UNKNOWN"
                elif mutation == "shadow-mount":
                    inv["block_layout"]["stdout"] = inv["block_layout"]["stdout"].replace("/mnt/lima-afsadata", "/mnt/lima-afsadata-shadow")
                elif mutation == "df-wrong-fs":
                    inv["disk_space"]["stdout"] = inv["disk_space"]["stdout"].replace(" ext4 ", " tmpfs ")
                else:
                    path = root / "lima-after.jsonl"
                    rows = [json.loads(line) for line in path.read_text().splitlines()]
                    row = next(row for row in rows if row["name"] == "afs-accept-a")
                    row["config"]["images"][0]["arch" if mutation == "image-arch" else "location"] = "x86_64" if mutation == "image-arch" else "file:///wrong/image"
                    path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                if path.name != "lima-after.jsonl": write_json(path, inv)
                bundle["artifact_references"][path.name] = sha(path)
                report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
                check = next(check for check in report["checks"] if check["name"] == check_name)
                self.assertNotEqual(check["status"], "PASS")

    def test_nested_malformed_evidence_returns_blockers_without_exception(self):
        for artifact, field, value in (
            ("host.json", "cpu_count", None),
            ("host.json", "ram_bytes", "32GiB"),
            ("inventory-a.json", "rdma_device", ["PASS"]),
        ):
            with self.subTest(artifact=artifact, field=field), tempfile.TemporaryDirectory() as td:
                root = Path(td)
                bundle = self.make_bundle(root)
                path = root / artifact
                data = json.loads(path.read_text()); data[field] = value
                write_json(path, data)
                bundle["artifact_references"][artifact] = sha(path)
                write_json(root / "bundle.json", bundle)
                lck = lock(bundle["contract"]["sha256"])
                lck["environment_evidence"] = {"path": "bundle.json", "sha256": sha(root / "bundle.json")}
                self.assertTrue(environment.qualification_errors(lck, root / "lock.json"))

    def test_contract_path_escape_returns_blocker_without_exception(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root)
            bundle["contract"]["path"] = "../acceptance.md"
            write_json(root / "bundle.json", bundle)
            lck = lock(bundle["contract"]["sha256"])
            lck["environment_evidence"] = {"path": "bundle.json", "sha256": sha(root / "bundle.json")}
            self.assertTrue(environment.qualification_errors(lck, root / "lock.json"))

    def make_bundle(self, root: Path, *, corrupt_a: bool = False, low_space: bool = False, wrong_image: bool = False, missing_ram: bool = False, initial_available=None) -> dict:
        contract = root / "acceptance.md"
        contract.write_text("contract\n", encoding="utf-8")
        refs = {"acceptance.md": sha(contract)}
        host = root / "host.json"
        write_json(host, {"arch": "arm64", "cpu_count": 10, "ram_bytes": 32 * GIB, "available_bytes": 45 * GIB, "initial_available_bytes": initial_available})
        refs["host.json"] = sha(host)
        specs = {
            "inventory-ctl.json": ("afs-accept-ctl", "192.168.109.11", "afsctlstate", 4, 8, 5 * GIB),
            "inventory-a.json": ("afs-accept-a", "192.168.109.12", "afsadata", 6, 32, 5 * GIB),
            "inventory-b.json": ("afs-accept-b", "192.168.109.13", "afsbdata", 6, 32, 5 * GIB),
            "inventory-c-rxe.json": ("afs-accept-c", "192.168.109.14", "afscdata", 6, 32, 5 * GIB),
        }
        for name, args in specs.items():
            inv = inventory(*args, with_mem=not (missing_ram and name == "inventory-a.json"))
            if low_space and name == "inventory-a.json":
                inv = inventory(*args[:-1], 1 * GIB)
            if corrupt_a and name == "inventory-a.json":
                inv["architecture"] = "x86_64"
            path = root / name
            write_json(path, inv)
            refs[name] = sha(path)
        rows = []
        for name, expected in environment.EXPECTED_VMS.items():
            digest = "0" * 64 if wrong_image and name == "afs-accept-a" else environment.EXPECTED_IMAGE_SHA
            rows.append({"name": name, "hostname": f"lima-{name}", "status": "Running", "arch": "aarch64", "cpus": expected["cpus"], "memory": expected["memory"], "disk": expected["disk"], "additionalDisks": [{"name": expected["volume"], "format": True, "fsType": "ext4"}], "config": {"images": [{"arch": "aarch64", "location": "https://example.invalid/pinned-arm64.img", "digest": f"sha256:{digest}"}]}})
        lima = root / "lima-after.jsonl"
        lima.write_text("".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")
        refs["lima-after.jsonl"] = sha(lima)
        bundle = {"artifact_references": refs, "contract": {"path": "acceptance.md", "sha256": refs["acceptance.md"]}}
        write_json(root / "bundle.json", bundle)
        return bundle

    def test_actual_like_bundle_is_blocked_by_deferred_semantics_and_missing_initial(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, initial_available=None)
            report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
            self.assertEqual(report["status"], "BLOCKED")
            self.assertTrue(any(c["name"] == "host-initial-reserve" and c["status"] == "BLOCKED" for c in report["checks"]))
            self.assertTrue(any(c["name"] == "cross-vm-verbs" and c["status"] == "BLOCKED" for c in report["checks"]))
            self.assertTrue(any(c["name"] == "afs-accept-a-data-reserve" and c["status"] == "PASS" for c in report["checks"]))

    def test_host_uses_hash_bound_json_not_lock(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, initial_available=120 * GIB)
            host = root / "host.json"
            write_json(host, {"arch": "x86_64", "cpu_count": 10, "ram_bytes": 32 * GIB, "available_bytes": 45 * GIB, "initial_available_bytes": 120 * GIB})
            bundle["artifact_references"]["host.json"] = sha(host)
            report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
            self.assertTrue(any(c["name"] == "host-actual-observed" and c["status"] == "FAIL" for c in report["checks"]))

    def test_wrong_actual_image_is_fail_even_if_lock_correct(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, wrong_image=True, initial_available=120 * GIB)
            report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
            self.assertTrue(any(c["name"] == "afs-accept-a-lima-config" and c["status"] == "FAIL" for c in report["checks"]))

    def test_missing_memtotal_is_blocked_not_pass(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, missing_ram=True, initial_available=120 * GIB)
            report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
            self.assertTrue(any(c["name"] == "afs-accept-a-guest-identity" and c["status"] == "BLOCKED" for c in report["checks"]))

    def test_mismatched_guest_identity_is_fail(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, corrupt_a=True, initial_available=120 * GIB)
            report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
            self.assertEqual(report["status"], "FAIL")
            self.assertTrue(any(c["name"] == "afs-accept-a-guest-identity" and c["status"] == "FAIL" for c in report["checks"]))

    def test_low_data_reserve_blocks_and_preserves_dead_fuse_context(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, low_space=True, initial_available=120 * GIB)
            report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
            reserve = [c for c in report["checks"] if c["name"] == "afs-accept-a-data-reserve"][0]
            self.assertEqual(reserve["status"], "BLOCKED")
            self.assertEqual(reserve["evidence"]["df_status"], "UNKNOWN")

    def test_generic_receipts_do_not_make_environment_pass(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, initial_available=120 * GIB)
            bundle["receipt"] = {"status": "PASS", "text": "all good"}
            report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
            self.assertEqual(report["status"], "BLOCKED")

    def test_qualification_errors_malformed_and_tampered_paths(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            lock_path = root / "acceptance.lock.json"
            lck = lock()
            write_json(lock_path, lck)
            self.assertIn("missing", environment.qualification_errors(lck, lock_path)[0])
            bundle = self.make_bundle(root, initial_available=120 * GIB)
            lck["environment_evidence"] = {"path": "bundle.json", "sha256": sha(root / "bundle.json")}
            self.assertTrue(any("cross-vm-verbs" in err for err in environment.qualification_errors(lck, lock_path)))
            for value in ([], 1, None):
                bad = root / "bad.json"
                write_json(bad, value)
                lck["environment_evidence"] = {"path": "bad.json", "sha256": sha(bad)}
                self.assertIn("malformed", environment.qualification_errors(lck, lock_path)[0])
            raw = root / "raw.bin"
            raw.write_bytes(b"\xff")
            lck["environment_evidence"] = {"path": "raw.bin", "sha256": sha(raw)}
            self.assertIn("malformed", environment.qualification_errors(lck, lock_path)[0])
            lck["environment_evidence"] = {"path": "../escape.json", "sha256": "0" * 64}
            self.assertIn("escapes", environment.qualification_errors(lck, lock_path)[0])
            lck["environment_evidence"] = {"path": "bundle.json", "sha256": "0" * 64}
            self.assertIn("sha256 mismatch", environment.qualification_errors(lck, lock_path)[0])

    def test_bad_artifact_references_are_invalid_input(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, initial_available=120 * GIB)
            for refs in ([], {"host.json": 3}, {"../host.json": "0" * 64}):
                bundle["artifact_references"] = refs
                report = environment.evaluate_environment(lock(bundle["contract"]["sha256"]), bundle, root)
                self.assertEqual(report["status"], "BLOCKED")
                self.assertEqual(report["checks"][0]["name"], "bundle-shape")

    def test_contract_hashes_actual_referenced_file(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, initial_available=120 * GIB)
            bundle["contract"]["sha256"] = "0" * 64
            report = environment.evaluate_environment(lock(sha(root / "acceptance.md")), bundle, root)
            self.assertTrue(any(c["name"] == "acceptance-contract-sha256" and c["status"] == "FAIL" for c in report["checks"]))

    def test_cli_guard_adds_blocked_on_non_linux_arm64(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            bundle = self.make_bundle(root, initial_available=120 * GIB)
            lock_path = root / "acceptance.lock.json"
            write_json(lock_path, lock(bundle["contract"]["sha256"]))
            out = root / "out.json"
            with mock.patch("environment.is_linux_arm64", return_value=False):
                rc = environment.main(["--lock", str(lock_path), "--bundle", str(root / "bundle.json"), "--output", str(out)])
            report = json.loads(out.read_text(encoding="utf-8"))
            self.assertEqual(rc, 2)
            self.assertTrue(any(c["name"] == "cli-linux-arm64-guard" for c in report["checks"]))


if __name__ == "__main__":
    unittest.main()
