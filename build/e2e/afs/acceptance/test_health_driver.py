#!/usr/bin/env python3
import json
import os
import subprocess
import sys
import tempfile
import shutil
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

DRIVER = Path(__file__).resolve().parent / "drivers" / "health.py"
CASES = Path(__file__).resolve().parent / "cases.json"


def sha256_file(path: Path) -> str:
    import hashlib

    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def current_process_target(backend="DFS", node_id="node-health-test", scope="configured_fs"):
    exe = Path(os.readlink(f"/proc/{os.getpid()}/exe"))
    return {
        "role": "node",
        "id": node_id,
        "scope": scope,
        "backend": backend,
        "process": {
            "pid": os.getpid(),
            "executable_name": exe.name,
            "exe_sha256": sha256_file(exe),
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip(),
        },
    }


def health_doc(status="ready", overrides=None, node_id="node-health-test", scope="configured_fs"):
    checks = {
        "meta_persistence": {"ready": True},
        "node_registration": {"ready": True},
        "data_device": {"ready": True},
        "mounts": {"ready": True, "dfs": {"configured": True}},
        "rdma": {"ready": True},
    }
    if overrides:
        checks.update(overrides)
    return {
        "status": status,
        "role": "node",
        "id": node_id,
        "scope": scope,
        "dfs": {"configured": True},
        "checks": checks,
    }


class Server:
    def __init__(self, routes):
        self.routes = routes
        parent = self

        class Handler(BaseHTTPRequestHandler):
            def log_message(self, fmt, *args):
                return

            def do_GET(self):
                status, doc = parent.routes.get(self.path, (404, {"error": "missing"}))
                body = json.dumps(doc).encode()
                self.send_response(status)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

        self.httpd = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        self.thread = threading.Thread(target=self.httpd.serve_forever, daemon=True)

    def __enter__(self):
        self.thread.start()
        host, port = self.httpd.server_address
        self.base = f"http://{host}:{port}"
        return self

    def __exit__(self, *_):
        self.httpd.shutdown()
        self.httpd.server_close()
        self.thread.join(timeout=2)


@unittest.skipUnless(sys.platform.startswith("linux"), "health acceptance driver is Linux-only")
class HealthDriverTests(unittest.TestCase):
    def run_driver(self, binding=None, profile="smoke"):
        root = Path(tempfile.mkdtemp(prefix="afs-health-driver-test-"))
        self.addCleanup(lambda: shutil.rmtree(root, ignore_errors=True))
        env = os.environ.copy()
        env.update({
            "AFS_ACCEPTANCE_CASE_ID": "OPS-01",
            "AFS_ACCEPTANCE_PROFILE": profile,
            "AFS_ACCEPTANCE_MATRIX": json.dumps({"backend": "DFS", "meta": "etcd"}),
            "AFS_ACCEPTANCE_RUN_DIR": str(root / "run"),
        })
        if binding is not None:
            path = root / "bindings.json"
            path.write_text(json.dumps(binding), encoding="utf-8")
            env["AFS_ACCEPTANCE_HEALTH_BINDINGS"] = str(path)
        proc = subprocess.run([sys.executable, str(DRIVER)], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, env=env, check=False)
        lines = [line for line in proc.stdout.splitlines() if line.strip()]
        self.assertTrue(lines, proc.stderr)
        return proc, json.loads(lines[-1]), root

    def binding(self, scenarios, target=None, **extra):
        record = {
            "case_id": "OPS-01",
            "profile": "smoke",
            "matrix": {"backend": "DFS", "meta": "etcd"},
            "target": target or current_process_target(),
            "scenarios": scenarios,
        }
        record.update(extra)
        return {"schema_version": 1, "bindings": [record]}

    def test_missing_binding_is_blocked(self):
        proc, proof, _root = self.run_driver()
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("AFS_ACCEPTANCE_HEALTH_BINDINGS", proof["reason"])

    def test_smoke_passes_with_identity_artifacts_and_one_degraded_component(self):
        with Server({
            "/healthy": (200, health_doc()),
            "/device-down": (503, health_doc("degraded", {"data_device": {"ready": False, "error": "read-only"}})),
        }) as server:
            proc, proof, root = self.run_driver(self.binding([
                {"name": "healthy", "component": "all", "url": server.base + "/healthy", "expected_ready": True},
                {"name": "device-down", "component": "device", "url": server.base + "/device-down", "expected_ready": False},
            ]))
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")
        for check in proof["checks"]:
            self.assertEqual(check["status"], "PASS")
            self.assertFalse(Path(check["artifact"]).is_absolute())
            self.assertGreater(check["artifact_bytes"], 0)
            self.assertRegex(check["artifact_sha256"], r"^[0-9a-f]{64}$")
            self.assertTrue((root / "run" / check["artifact"]).is_file())
        self.assertEqual(proof["coverage"]["axes"]["components"]["checks"]["device"], "device-down")

    def test_foundation_scope_degraded_mount_is_rejected_without_explicit_expected_scope(self):
        target = current_process_target(scope="configured_fs")
        with Server({
            "/mount-covered": (
                503,
                health_doc(
                    "degraded",
                    {"mounts": {"ready": False, "dfs": {"configured": True}, "error": "configured mount covered"}},
                    scope="foundation",
                ),
            ),
        }) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {"name": "mount-covered", "component": "mount", "url": server.base + "/mount-covered", "expected_ready": False},
            ], target=target))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        self.assertIn("health scope 'foundation' does not match expected scope 'configured_fs'", json.dumps(proof["checks"]))

    def test_expected_scope_foundation_allows_only_prebound_degraded_mount_scope(self):
        target = current_process_target(scope="configured_fs")
        foundation_mount = health_doc(
            "degraded",
            {"mounts": {"ready": False, "dfs": {"configured": True}, "error": "configured mount covered"}},
            scope="foundation",
        )
        configured_mount = health_doc(
            "degraded",
            {"mounts": {"ready": False, "dfs": {"configured": True}, "error": "configured mount covered"}},
            scope="configured_fs",
        )
        with Server({
            "/mount-covered": (503, foundation_mount),
            "/wrong-scope": (503, configured_mount),
        }) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {
                    "name": "mount-covered",
                    "component": "mount",
                    "url": server.base + "/mount-covered",
                    "expected_ready": False,
                    "expected_scope": "foundation",
                },
            ], target=target))
            wrong_proc, wrong_proof, _wrong_root = self.run_driver(self.binding([
                {
                    "name": "wrong-scope",
                    "component": "mount",
                    "url": server.base + "/wrong-scope",
                    "expected_ready": False,
                    "expected_scope": "foundation",
                },
            ], target=target))
        self.assertEqual(proc.returncode, 0, proof)
        self.assertEqual(proof["status"], "PASS")
        self.assertEqual(proof["checks"][1]["evidence"]["http_status"], 503)
        self.assertEqual(proof["checks"][1]["evidence"]["component"], "mount")
        self.assertEqual(proof["checks"][1]["evidence"]["component_ready"], False)
        self.assertNotEqual(wrong_proc.returncode, 0)
        self.assertEqual(wrong_proof["status"], "FAIL")
        self.assertIn("health scope 'configured_fs' does not match expected scope 'foundation'", json.dumps(wrong_proof["checks"]))

    def test_invalid_expected_scope_is_blocked(self):
        with Server({"/mount-covered": (503, health_doc("degraded", {"mounts": {"ready": False, "dfs": {"configured": True}}}, scope="foundation"))}) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {
                    "name": "mount-covered",
                    "component": "mount",
                    "url": server.base + "/mount-covered",
                    "expected_ready": False,
                    "expected_scope": "returned-by-server",
                },
            ]))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("expected_scope", proof["reason"])

    def test_unknown_binding_field_is_blocked(self):
        with Server({"/healthy": (200, health_doc()), "/device-down": (503, health_doc("degraded", {"data_device": {"ready": False}}))}) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {"name": "healthy", "component": "all", "url": server.base + "/healthy", "expected_ready": True},
                {"name": "device-down", "component": "device", "url": server.base + "/device-down", "expected_ready": False},
            ], unexpected="loose"))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("unsupported binding field", proof["reason"])

    def test_unknown_scenario_field_is_blocked(self):
        with Server({"/device-down": (503, health_doc("degraded", {"data_device": {"ready": False}}))}) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {"name": "device-down", "component": "device", "url": server.base + "/device-down", "expected_ready": False, "unexpected": "loose"},
            ]))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("unsupported field", proof["reason"])

    def test_duplicate_scenario_name_is_blocked(self):
        with Server({"/device-down": (503, health_doc("degraded", {"data_device": {"ready": False}}))}) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {"name": "dup", "component": "device", "url": server.base + "/device-down", "expected_ready": False},
                {"name": "dup", "component": "device", "url": server.base + "/device-down", "expected_ready": False},
            ]))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("duplicate scenario", proof["reason"])

    def test_endpoint_identity_mismatch_fails(self):
        target = current_process_target(node_id="expected-node")
        with Server({
            "/healthy": (200, health_doc(node_id="wrong-node")),
            "/device-down": (503, health_doc("degraded", {"data_device": {"ready": False}}, node_id="wrong-node")),
        }) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {"name": "healthy", "component": "all", "url": server.base + "/healthy", "expected_ready": True},
                {"name": "device-down", "component": "device", "url": server.base + "/device-down", "expected_ready": False},
            ], target=target))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        self.assertIn("identity_errors", json.dumps(proof["checks"]))

    def test_wrong_http_status_fails(self):
        with Server({"/device-down": (200, health_doc("degraded", {"data_device": {"ready": False}}))}) as server:
            proc, proof, _root = self.run_driver(self.binding([
                {"name": "device-down", "component": "device", "url": server.base + "/device-down", "expected_ready": False},
            ]))
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "FAIL")
        self.assertIn("expected_http_status", json.dumps(proof["checks"]))

    def test_full_requires_degraded_scenario_for_every_component(self):
        binding = {
            "schema_version": 1,
            "bindings": [{
                "case_id": "OPS-01",
                "profile": "full",
                "matrix": {"backend": "DFS", "meta": "etcd"},
                "target": current_process_target(),
                "scenarios": [{"name": "device-down", "component": "device", "url": "http://127.0.0.1:9/health", "expected_ready": False}],
            }],
        }
        proc, proof, _root = self.run_driver(binding, profile="full")
        self.assertNotEqual(proc.returncode, 0)
        self.assertEqual(proof["status"], "BLOCKED")
        self.assertIn("missing", proof["reason"])

    def test_manifest_registers_ops01_driver(self):
        cases = {case["id"]: case for case in json.loads(CASES.read_text())["cases"]}
        self.assertEqual(cases["OPS-01"]["driver"]["state"], "READY")
        self.assertEqual(cases["OPS-01"]["driver"]["command"], ["python3", "drivers/health.py"])


if __name__ == "__main__":
    unittest.main()
