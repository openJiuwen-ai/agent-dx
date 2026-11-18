"""Tool contracts for the hybrid topology and SSH remote entry prefixes.

A hybrid topology places roles on more than one Linux kernel (for example a
writer on one host and readers on another) while several roles may still share a
kernel in distinct namespaces. These tests pin its isolation rules; they make no
product or performance claim.
"""

from pathlib import Path
import shlex
import sys
import tempfile
import unittest
from unittest.mock import patch

TESTS = Path(__file__).resolve().parents[1]
sys.path.append(str(TESTS))
from fixtures.common import Case, Blocked  # noqa: E402

CLUSTER_CASE = Case("dfs.cluster.read_write", ("cluster-basic",), "dfs", ("MV04",), None)
PREFIX = ["sh", "-c", 'exec nsenter -t 42 -n -m -p -- "$@"', "enter-node"]


def hybrid_config():
    def host(target):
        return {"transport": "ssh", "ssh_target": target, "sudo": False,
                "remote_prefix": list(PREFIX), "base": "/mnt"}

    return {
        "kind": "hybrid", "uid": 10001, "gid": 10001, "other_uid": 10002, "other_gid": 10002,
        "roles": {
            "M": {"host": "h1-m", "address": "10.222.0.10"},
            "A": {"host": "h1-a", "address": "10.222.1.1"},
            "B": {"host": "h2-b", "address": "10.222.1.2"},
        },
        "hosts": {"h1-m": host("cloud1"), "h1-a": host("cloud1"), "h2-b": host("cloud2")},
    }


def inspected(boot, machine, number, pid_number=None):
    return {
        "system": "Linux", "machine": "x86_64", "uid": 0, "fuse": True, "filesystem": "ext4",
        "free": 2 * 1024**3, "tools": {"python3": "/usr/bin/python3"},
        "machine_id": machine, "boot_id": boot, "product_uuid": None,
        "network_namespace": "net-%d" % number,
        "pid_namespace": "pid-%d" % (number if pid_number is None else pid_number),
        "mount_namespace": "mnt-%d" % number,
    }


class ObservedWorker:
    def __init__(self, info):
        self.info = info

    def call(self, action, **_):
        if action == "inspect":
            return dict(self.info)
        if action == "status":
            return {}
        if action == "cleanup":
            return {"status": "PASS", "errors": [], "waits": []}
        raise AssertionError(action)

    def close(self):
        pass


def connect_with(infos):
    """Connect a hybrid cluster whose hosts report `infos` in sorted host order."""
    from fixtures.multivm import Cluster
    import fixtures.multivm as fixtures

    with tempfile.TemporaryDirectory() as directory:
        cluster = Cluster(hybrid_config(), directory, directory, CLUSTER_CASE, "test-token")
        with patch.object(fixtures.ssh, "connect", side_effect=[ObservedWorker(i) for i in infos]):
            try:
                return cluster.connect()
            finally:
                cluster.cleanup()


class HybridValidationTests(unittest.TestCase):
    def test_hybrid_kind_accepts_cluster_cases_with_distinct_role_hosts(self):
        from fixtures.multivm import validate

        self.assertEqual(validate(hybrid_config(), [CLUSTER_CASE]), ["h1-a", "h1-m", "h2-b"])

    def test_hybrid_rejects_local_single_vm_cases(self):
        from fixtures.multivm import validate

        case = Case("dfs.basic.read_write", ("local-basic",), "dfs", ("LB01",), None)
        with self.assertRaises(Blocked):
            validate(hybrid_config(), [case])

    def test_hybrid_still_requires_one_host_entry_per_role(self):
        from fixtures.multivm import validate

        cfg = hybrid_config()
        cfg["roles"]["B"]["host"] = "h1-a"
        with self.assertRaises(Blocked):
            validate(cfg, [CLUSTER_CASE])


class HybridIdentityTests(unittest.TestCase):
    # Sorted host order is h1-a, h1-m, h2-b.
    def test_two_kernels_with_distinct_namespaces_are_accepted(self):
        result = connect_with([
            inspected("boot-1", "machine-1", 1), inspected("boot-1", "machine-1", 2),
            inspected("boot-2", "machine-2", 3),
        ])
        self.assertEqual(len({v["boot_id"] for v in result.values()}), 2)

    def test_one_kernel_is_not_hybrid_evidence(self):
        with self.assertRaises(Blocked):
            connect_with([inspected("boot-1", "machine-1", n) for n in (1, 2, 3)])

    def test_roles_sharing_a_kernel_must_not_share_a_namespace(self):
        with self.assertRaises(Blocked):
            connect_with([
                inspected("boot-1", "machine-1", 1), inspected("boot-1", "machine-1", 2, pid_number=1),
                inspected("boot-2", "machine-2", 3),
            ])

    def test_distinct_kernels_need_distinguishable_machines(self):
        with self.assertRaises(Blocked):
            connect_with([
                inspected("boot-1", "clone", 1), inspected("boot-1", "clone", 2),
                inspected("boot-2", "clone", 3),
            ])

    def test_one_kernel_cannot_report_two_machine_identities(self):
        with self.assertRaises(Blocked):
            connect_with([
                inspected("boot-1", "machine-1", 1), inspected("boot-1", "machine-9", 2),
                inspected("boot-2", "machine-2", 3),
            ])


class SshRemotePrefixTests(unittest.TestCase):
    @staticmethod
    def remote_command(config):
        from fixtures import ssh

        captured = {}

        def channel(argv, **_):
            captured["argv"] = argv
            return object()

        with patch.object(ssh, "Channel", side_effect=channel):
            ssh.connect(config)
        return captured["argv"]

    def test_remote_prefix_runs_before_the_private_mount_worker_as_one_quoted_command(self):
        config = {"ssh_target": "cloud1", "sudo": False, "remote_prefix": list(PREFIX)}
        argv = self.remote_command(config)
        self.assertEqual(argv[-2], "cloud1")
        remote = shlex.split(argv[-1])
        self.assertEqual(remote[: len(PREFIX)], PREFIX)
        self.assertEqual(remote[len(PREFIX): len(PREFIX) + 4], ["unshare", "--mount", "--propagation", "private"])

    def test_idle_channels_send_keepalives(self):
        # Workers can sit idle for minutes while another role measures; NAT or jump hosts
        # must not silently drop the channel and leave cleanup unconfirmed.
        argv = self.remote_command({"ssh_target": "cloud1"})
        self.assertIn("ServerAliveInterval=15", argv)
        self.assertIn("ServerAliveCountMax=8", argv)

    def test_without_remote_prefix_the_command_is_unchanged(self):
        argv = self.remote_command({"ssh_target": "cloud1"})
        self.assertEqual(shlex.split(argv[-1])[:3], ["sudo", "-n", "unshare"])

    def test_remote_prefix_must_be_a_literal_argv(self):
        from fixtures import ssh

        for bad in ("sh -c x", [], ["ok", ""], ["line\nbreak"], [1]):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                ssh.connect({"ssh_target": "cloud1", "remote_prefix": bad})


class StagedArtifactTests(unittest.TestCase):
    """Verify that the fixture copies and hashes pre-staged binaries."""

    class Recorder:
        def __init__(self, digest):
            self.calls, self.digest = [], digest

        def call(self, action, **request):
            self.calls.append((action, request))
            if action == "command":
                return {"returncode": 0, "stdout": "", "stderr": ""}
            if action == "hash":
                return {"sha256": self.digest, "size": 1}
            raise AssertionError(action)

        @staticmethod
        def upload(*_):
            raise AssertionError("staged artifacts must not be transferred from the controller")

    def place(self, host, digest="d" * 64):
        from fixtures.multivm import Cluster

        with tempfile.TemporaryDirectory() as directory:
            cluster = Cluster(hybrid_config(), directory, directory, CLUSTER_CASE, "test-token")
            channel = self.Recorder(digest)
            return cluster.place_binary(channel, host, "afs-node", "/mnt/run/bin/afs-node"), channel.calls

    def test_staged_binary_is_copied_on_the_host_and_hashed_at_its_destination(self):
        host = {"artifact_dir": "/srv/frozen/bins"}
        digest, calls = self.place(host)
        self.assertEqual(digest, "d" * 64)
        self.assertEqual(calls[0][0], "command")
        self.assertEqual(calls[0][1]["argv"], ["install", "-D", "-m", "0755", "/srv/frozen/bins/afs-node",
                                               "/mnt/run/bin/afs-node"])
        self.assertEqual(calls[1], ("hash", {"path": "/mnt/run/bin/afs-node"}))

    def test_staged_artifact_dir_must_be_absolute(self):
        from fixtures.multivm import validate

        for bad in ("relative/bins", "/a/../b", "/x\ny"):
            cfg = hybrid_config()
            cfg["hosts"]["h1-a"]["artifact_dir"] = bad
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                validate(cfg, [CLUSTER_CASE])


if __name__ == "__main__":
    unittest.main()
