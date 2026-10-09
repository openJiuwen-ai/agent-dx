import importlib.util
import json
import os
import unittest
from pathlib import Path

spec = importlib.util.spec_from_file_location("target_identity", Path(__file__).parent / "drivers/target_identity.py")
target = importlib.util.module_from_spec(spec)
spec.loader.exec_module(target)


def mount(source="afs-dfs", filesystem="fuse", target="/mount"):
    return {"returncode": 0, "stdout": json.dumps({"filesystems": [{"source": source, "fstype": filesystem, "target": target}]})}


def process(name):
    return {"exists": True, "exe": "/bin/" + name, "exe_sha256": "a" * 64, "cmdline": name + " --config /config"}


class TargetIdentityTest(unittest.TestCase):
    def checks(self, backend="DFS", observed=None, base=None, node=None, meta=None, system="Linux"):
        observed = observed or mount()
        return target.target_checks(system, backend, observed, base or observed, node, meta)

    def test_observed_product(self):
        self.assertTrue(all(self.checks(node=process("afs-node"), meta=process("afs-meta")).values()))

    def test_ext4_cannot_be_product(self):
        self.assertFalse(self.checks(observed=mount("/dev/vda1", "ext4"))["observed-target-backend"])

    def test_other_fuse_cannot_be_product(self):
        self.assertFalse(self.checks(observed=mount("other"))["observed-target-backend"])

    def test_backend_mismatch(self):
        self.assertFalse(self.checks(backend="OwnerFs")["observed-target-backend"])

    def test_reference_ext4(self):
        self.assertTrue(all(self.checks(backend="reference", observed=mount("/dev/vda1", "ext4")).values()))

    def test_unknown_label(self):
        self.assertFalse(self.checks(backend="something")["observed-target-backend"])

    def test_no_product_process(self):
        self.assertFalse(self.checks()["product-process-identity"])

    def test_wrong_process(self):
        self.assertFalse(self.checks(node=process("python3"), meta=process("afs-meta"))["product-process-identity"])

    def test_remote_host_qualified_product_accepts_node_only(self):
        old = os.environ.get("AFS_ACCEPTANCE_REMOTE_HOST_QUALIFIED")
        try:
            os.environ["AFS_ACCEPTANCE_REMOTE_HOST_QUALIFIED"] = "1"
            self.assertTrue(self.checks(node=process("afs-node"))["product-process-identity"])
            self.assertFalse(self.checks(node=process("python3"))["product-process-identity"])
        finally:
            if old is None:
                os.environ.pop("AFS_ACCEPTANCE_REMOTE_HOST_QUALIFIED", None)
            else:
                os.environ["AFS_ACCEPTANCE_REMOTE_HOST_QUALIFIED"] = old

    def test_nested_mount(self):
        self.assertFalse(self.checks(base=mount(target="/mount/nested"))["same-fixture-filesystem"])

    def test_non_linux(self):
        self.assertFalse(self.checks(system="Darwin")["linux-runtime"])

    def test_invalid_mount_json(self):
        self.assertFalse(self.checks(observed={"returncode": 0, "stdout": "broken"})["observed-target-backend"])


def strict_identity(role="node", exe="afs-node", sha="a" * 64, boot="boot-a", machine="machine-a", cfg_sha="c" * 64, endpoint="http://10.0.0.1:7400", listen="0.0.0.0:7400", tls=None, start=123):
    cfg = {"sha256": cfg_sha, "tls": tls or {}}
    if role == "node":
        cfg["meta_endpoint"] = endpoint
    else:
        cfg["grpc_listen"] = listen
    return {
        "role": role,
        "exists": True,
        "pid": 100 if role == "node" else 200,
        "boot_id": boot,
        "machine_id": machine,
        "exe_path": "/opt/afs/bin/" + exe,
        "exe_dev": 11,
        "exe_inode": 22 if role == "node" else 33,
        "start_ticks": start,
        "sha256": sha,
        "expected_sha256": sha,
        "sha256_ok": True,
        "config": cfg,
        "network": {"interface_ips": ["10.0.0.1" if role == "meta" else "10.0.0.2"], "listen_sockets": ([{"family": "tcp", "ip": "0.0.0.0", "port": 7400, "inode": "99"}] if role == "meta" else [])},
    }


class RemoteTargetIdentityTest(unittest.TestCase):
    def remote_checks(self, **overrides):
        node_sha = "a" * 64
        meta_sha = "b" * 64
        node = overrides.pop("node", strict_identity("node", "afs-node", node_sha, boot="boot-b", machine="machine-b"))
        meta = overrides.pop("meta", strict_identity("meta", "afs-meta", meta_sha, boot="boot-a", machine="machine-a"))
        node_after = overrides.pop("node_after", dict(node))
        meta_after = overrides.pop("meta_after", dict(meta))
        params = {
            "system": "Linux",
            "backend": "DFS",
            "mount": mount("afs-dfs", "fuse.afs", "/mnt/dfs"),
            "base_mount": mount("afs-dfs", "fuse.afs", "/mnt/dfs"),
            "node_before": node,
            "node_after": node_after,
            "meta_before": meta,
            "meta_after": meta_after,
            "expected_node_sha256": node_sha,
            "expected_meta_sha256": meta_sha,
            "expected_meta_endpoint": "http://10.0.0.1:7400",
            "require_cross_worker": True,
        }
        params.update(overrides)
        return target.remote_target_checks(**params)

    def test_remote_identity_strict_passes_without_tls(self):
        self.assertTrue(all(self.remote_checks().values()))

    def test_remote_identity_rejects_missing_config_digest(self):
        node = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", cfg_sha=None)
        checks = self.remote_checks(node=node, node_after=dict(node))
        self.assertFalse(checks["node-process-identity"])

    def test_remote_identity_rejects_wrong_expected_sha(self):
        checks = self.remote_checks(expected_node_sha256="d" * 64)
        self.assertFalse(checks["node-process-identity"])

    def test_remote_identity_rejects_unstable_process(self):
        node_after = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", start=999)
        checks = self.remote_checks(node_after=node_after)
        self.assertFalse(checks["node-process-stable"])

    def test_remote_identity_rejects_endpoint_mismatch(self):
        node = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", endpoint="http://10.0.0.2:7400")
        checks = self.remote_checks(node=node, node_after=dict(node))
        self.assertFalse(checks["node-meta-endpoint-bound"])

    def test_remote_identity_rejects_listen_port_mismatch(self):
        meta = strict_identity("meta", "afs-meta", "b" * 64, boot="boot-a", machine="machine-a", listen="0.0.0.0:7500")
        checks = self.remote_checks(meta=meta, meta_after=dict(meta))
        self.assertFalse(checks["meta-listen-port-bound"])

    def test_remote_identity_rejects_same_worker_when_cross_required(self):
        meta = strict_identity("meta", "afs-meta", "b" * 64, boot="boot-b", machine="machine-b")
        checks = self.remote_checks(meta=meta, meta_after=dict(meta))
        self.assertFalse(checks["cross-worker-identity"])

    def complete_tls(self):
        return {
            "tls_ca_certificate": {"path": "/secret/ca", "sha256": "c" * 64, "exists": True},
            "tls_identity_certificate": {"path": "/secret/cert", "sha256": "d" * 64, "exists": True},
            "tls_identity_private_key": {"path": "/secret/key", "sha256": "e" * 64, "exists": True},
        }

    def test_remote_identity_accepts_configured_tls_and_trusted_cert_digests(self):
        tls = self.complete_tls()
        node = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", tls=tls)
        node["config"]["tls_required"] = True
        node["config"]["trusted_node_certs"] = {"memory-node-a": {"path": "/secret/peer-a", "sha256": "f" * 64, "exists": True}}
        checks = self.remote_checks(node=node, node_after=dict(node))
        self.assertTrue(checks["tls-digests-recorded"])

    def test_remote_identity_rejects_configured_tls_without_digest(self):
        tls = {"tls_identity_private_key": {"path": "/secret/key", "exists": True}}
        node = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", tls=tls)
        node["config"]["tls_required"] = True
        checks = self.remote_checks(node=node, node_after=dict(node))
        self.assertFalse(checks["tls-digests-recorded"])

    def test_remote_identity_rejects_tls_required_empty_material(self):
        node = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b")
        node["config"]["tls_required"] = True
        checks = self.remote_checks(node=node, node_after=dict(node))
        self.assertFalse(checks["tls-digests-recorded"])

    def test_remote_identity_rejects_expected_endpoint_on_wrong_a_host(self):
        checks = self.remote_checks(expected_meta_endpoint="http://10.0.0.9:7400")
        self.assertFalse(checks["meta-listen-port-bound"])

    def test_remote_identity_rejects_loopback_cross_worker_endpoint(self):
        meta = strict_identity("meta", "afs-meta", "b" * 64, boot="boot-a", machine="machine-a")
        meta["network"]["interface_ips"].append("127.0.0.1")
        checks = self.remote_checks(meta=meta, meta_after=dict(meta), expected_meta_endpoint="http://127.0.0.1:7400")
        self.assertFalse(checks["meta-listen-port-bound"])

    def test_remote_identity_rejects_wrong_pid_listener(self):
        meta = strict_identity("meta", "afs-meta", "b" * 64, boot="boot-a", machine="machine-a")
        meta["network"]["listen_sockets"] = []
        checks = self.remote_checks(meta=meta, meta_after=dict(meta))
        self.assertFalse(checks["meta-listen-port-bound"])

    def test_remote_identity_rejects_nonwildcard_listen_different_from_served_ip(self):
        meta = strict_identity("meta", "afs-meta", "b" * 64, boot="boot-a", machine="machine-a", listen="10.0.0.9:7400")
        meta["network"]["listen_sockets"] = [{"family": "tcp", "ip": "10.0.0.9", "port": 7400, "inode": "99"}]
        checks = self.remote_checks(meta=meta, meta_after=dict(meta))
        self.assertFalse(checks["meta-listen-port-bound"])

    def test_remote_identity_rejects_missing_post_identity(self):
        node_after = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b")
        node_after["exists"] = False
        node_after["sha256_ok"] = False
        checks = self.remote_checks(node_after=node_after)
        self.assertFalse(checks["node-process-identity"])

    def test_remote_identity_rejects_changed_tls_material_after_pre(self):
        tls_pre = self.complete_tls()
        tls_post = self.complete_tls()
        tls_post["tls_identity_certificate"] = {"path": "/secret/cert", "sha256": "f" * 64, "exists": True}
        node = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", tls=tls_pre)
        node_after = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", tls=tls_post)
        node["config"]["tls_required"] = True
        node_after["config"]["tls_required"] = True
        checks = self.remote_checks(node=node, node_after=node_after)
        self.assertFalse(checks["tls-digests-recorded"])

    def test_remote_identity_rejects_changed_trusted_node_cert_after_pre(self):
        tls = self.complete_tls()
        node = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", tls=tls)
        node_after = strict_identity("node", "afs-node", "a" * 64, boot="boot-b", machine="machine-b", tls=tls)
        node["config"].update({"tls_required": True, "trusted_node_certs": {"peer": {"path": "/peer", "sha256": "1" * 64, "exists": True}}})
        node_after["config"].update({"tls_required": True, "trusted_node_certs": {"peer": {"path": "/peer", "sha256": "2" * 64, "exists": True}}})
        checks = self.remote_checks(node=node, node_after=node_after)
        self.assertFalse(checks["tls-digests-recorded"])


if __name__ == "__main__":
    unittest.main()
