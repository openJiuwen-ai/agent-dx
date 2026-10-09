import importlib.util
import json
import tempfile
import unittest
from argparse import Namespace
from pathlib import Path
from unittest import mock


HERE = Path(__file__).resolve().parent
SPEC = importlib.util.spec_from_file_location("env_verbs", HERE / "env_verbs.py")
env_verbs = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
SPEC.loader.exec_module(env_verbs)

def stock_log(role="client", size=256, count=3):
    rows = ["rdma_listen", "cma_event type RDMA_CM_EVENT_ESTABLISHED cma_id 0x1234 (parent)"]
    for i in range(count):
        rows.append(f"RDMA addr {0x1000 + i:x} rkey {0x2000 + i:x} len {size}")
        rows.append(f"Received rkey {0x2000 + i:x} addr {0x1000 + i:x} len {size} from peer")
        rows.append(f"RDMA addr {0x3000 + i:x} rkey {0x4000 + i:x} len {size}")
        rows.append(f"Received rkey {0x4000 + i:x} addr {0x3000 + i:x} len {size} from peer")
        rows.append("send completion")
        rows.append("recv completion")
        rows.append("send completion")
        rows.append("recv completion")
        if role == "server":
            rows.append("rdma read completion")
            rows.append("rdma write completion")
            rows.append(f"server ping data: {env_verbs.payload_line(i, size)}")
        else:
            rows.append(f"ping data: {env_verbs.payload_line(i, size)}")
    if role == "server":
        rows.append("server DISCONNECT EVENT...")
        rows.append("wait for RDMA_READ_ADV state 10")
    else:
        rows.append("DISCONNECTED")
    rows.extend(["cma_event type RDMA_CM_EVENT_DISCONNECTED cma_id 0x1234 (parent)", "rping_free_buffers called on cb 0x1234", "destroy cm_id 0x1234"])
    return "\n".join(rows) + "\n"


RUN_ID = "11111111-1111-4111-8111-111111111111"

def inventory(bind, peer, *, binary_hash=None, route=True, gid=True, mtu="1500", link=True, provider=True):
    return {
        "binary": {"sha256": binary_hash or env_verbs.EXPECTED_RPING_SHA256},
        "route": {"stdout": f"{peer} dev eth0 src {bind}\n" if route else "default via 10.0.2.2\n"},
        "rdma_link": {"stdout": "link rxe0/1 state ACTIVE physical_state LINK_UP netdev eth0\n" if link else "link rxe0/1 state DOWN\n"},
        "sysfs": {"gid": "::ffff:" + ":".join(env_verbs.ipv4_mapped_gid(bind).split(":")[-2:]) if gid else "::", "eth0_mtu": mtu},
        "installed_provider": {
            "libibverbs_so_sha256": "a" * 64 if provider else None,
            "rxe_provider_so_sha256": "b" * 64 if provider else None,
        },
    }

def endpoint(role, *, raw=None, binary_hash=None, route=True, rc=0, size=256, count=3, host=None, **inv):
    bind = "192.168.109.12" if role == "client" else "192.168.109.13"
    peer = "192.168.109.13" if role == "client" else "192.168.109.12"
    raw_log = raw if raw is not None else stock_log(role, size, count)
    return {
        "run_id": RUN_ID,
        "role": role,
        "bind": bind,
        "peer": peer,
        "port": 19669,
        "size": size,
        "count": count,
        "returncode": rc,
        "timed_out": False,
        "timeout_seconds": 3,
        "raw_log": raw_log,
        "raw_log_sha256": env_verbs.sha256_bytes(raw_log.encode("utf-8")),
        "host": host or {"machine_id": "client-machine" if role == "client" else "server-machine"},
        "process": {"pid": 123, "start_ticks": 7, "exe_sha256": env_verbs.EXPECTED_RPING_SHA256},
        "resources_before": {"rdma_resource": {"stdout": ""}},
        "resources_after": {"rdma_resource": {"stdout": ""}},
        "argv": env_verbs.rping_argv(role, bind, peer, 19669, size, count),
        "inventory": inventory(bind, peer, binary_hash=binary_hash, route=route, **inv),
    }


class EnvVerbsTests(unittest.TestCase):
    def test_missing_run_identity_is_not_generated_during_evaluation(self):
        for value in (None, ""):
            with self.subTest(run_id=value):
                client, server = endpoint("client"), endpoint("server")
                client["run_id"] = server["run_id"] = value
                self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])

    def test_actual_from_route_and_client_cleanup_without_async_event(self):
        client, server = endpoint("client"), endpoint("server")
        for ep in (client, server):
            ep["inventory"]["route"]["stdout"] = f"{ep['peer']} from {ep['bind']} dev eth0 uid 501\n    cache\n"
        client["raw_log"] = client["raw_log"].replace("cma_event type RDMA_CM_EVENT_DISCONNECTED cma_id 0x1234 (parent)\n", "").replace("DISCONNECTED\n", "")
        client["raw_log_sha256"] = env_verbs.sha256_bytes(client["raw_log"].encode())
        self.assertEqual("PASS", env_verbs.evaluate_pair(client, server)["status"])

    def test_zero_count_and_invalid_identity_cannot_pass(self):
        client, server = endpoint("client", count=0), endpoint("server", count=0)
        self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])
        for field, value in (("run_id", "bad"), ("port", 80), ("timed_out", None)):
            with self.subTest(field=field):
                client, server = endpoint("client"), endpoint("server")
                client[field] = server[field] = value
                self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])

    def test_oversized_descriptor_and_malformed_nested_evidence_are_failures(self):
        for replacement in ("10000000000000000", "ffffffffffffffffffffffff"):
            client, server = endpoint("client"), endpoint("server")
            for ep in (client, server):
                ep["raw_log"] = ep["raw_log"].replace("addr 1000", "addr " + replacement)
                ep["raw_log_sha256"] = env_verbs.sha256_bytes(ep["raw_log"].encode())
            self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])
        for field in ("inventory", "process", "host"):
            client, server = endpoint("client"), endpoint("server")
            client[field] = None
            self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])

    def test_comment_lifecycle_and_wrong_route_are_not_proof(self):
        client, server = endpoint("client"), endpoint("server")
        for ep in (client, server):
            ep["raw_log"] = ep["raw_log"].replace("cma_event type ", "comment cma_event type ")
            ep["raw_log"] = ep["raw_log"].replace("DISCONNECTED", "comment DISCONNECTED")
            ep["raw_log"] = ep["raw_log"].replace("server DISCONNECT EVENT...", "comment DISCONNECT EVENT")
            ep["raw_log_sha256"] = env_verbs.sha256_bytes(ep["raw_log"].encode())
        self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])
        for route in ("192.168.109.99 dev eth0 src 192.168.109.12", "192.168.109.13 dev lo src 192.168.109.12"):
            client, server = endpoint("client"), endpoint("server")
            client["inventory"]["route"]["stdout"] = route
            self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])

    def test_expected_payload_is_full_sized_and_deterministic(self):
        payload = env_verbs.expected_payload(2, 64)
        self.assertEqual(64, len(payload))
        self.assertTrue(payload.startswith(b"rdma-ping-2: "))
        self.assertEqual(0, payload[-1])
        self.assertEqual(env_verbs.sha256_bytes(payload), env_verbs.sha256_bytes(env_verbs.expected_payload(2, 64)))

    def test_synthetic_stock_pair_passes_semantic_evaluation(self):
        result = env_verbs.evaluate_pair(endpoint("client"), endpoint("server"))
        self.assertEqual("PASS", result["status"])
        self.assertEqual(6, len(result["descriptor_sha256"]))
        self.assertIn("stock rping server", result["limitations"][0])

    def test_exit_zero_without_payload_or_completion_evidence_fails(self):
        cases = {
            "missing-payload": stock_log("client").replace("ping data:", "not payload:"),
            "missing-completion": stock_log("client").replace("send completion", "sent"),
            "server-missing-write": stock_log("server").replace("rdma write completion", "write done"),
            "error-line": stock_log("client") + "cq completion failed status 12\n",
        }
        for name, text in cases.items():
            with self.subTest(name=name):
                client = endpoint("client", raw=text) if name != "server-missing-write" else endpoint("client")
                server = endpoint("server", raw=text) if name == "server-missing-write" else endpoint("server")
                self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])

    def test_descriptor_and_identity_mismatches_fail(self):
        cases = [
            (endpoint("client", raw=stock_log("client").replace("rkey 2000", "rkey 9999", 1)), endpoint("server")),
            (endpoint("client", binary_hash="0" * 64), endpoint("server")),
            (endpoint("client", route=False), endpoint("server")),
            (endpoint("client", rc=7), endpoint("server")),
            (endpoint("client", gid=False), endpoint("server")),
            (endpoint("client", provider=False), endpoint("server")),
        ]
        for client, server in cases:
            with self.subTest(problems=env_verbs.evaluate_pair(client, server)["problems"]):
                self.assertEqual("FAIL", env_verbs.evaluate_pair(client, server)["status"])

    def test_server_tail_wait_is_allowed_after_complete_disconnect(self):
        server = endpoint("server", raw=stock_log("server") + "wait for RDMA_READ_ADV state 10\n")
        self.assertEqual("PASS", env_verbs.evaluate_pair(endpoint("client"), server)["status"])

    def test_output_paths_refuse_overwrite(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            report, raw = env_verbs.output_paths(root, "rid", "client")
            report.write_text("{}")
            with self.assertRaises(env_verbs.VerbsError):
                env_verbs.output_paths(root, "rid", "client")
            self.assertFalse(raw.exists())

    def test_evaluate_pair_cli_writes_refuses_overwrite(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td)
            client, server, out = root / "client.json", root / "server.json", root / "eval.json"
            client.write_text(json.dumps(endpoint("client")))
            server.write_text(json.dumps(endpoint("server")))
            out.write_text("{}")
            with self.assertRaises(env_verbs.VerbsError):
                env_verbs.main(["evaluate-pair", "--client", str(client), "--server", str(server), "--output", str(out)])

    def test_run_endpoint_uses_fake_subprocess_and_writes_raw_report(self):
        chunks = [b"rdma_listen\n", stock_log("client").encode("utf-8")]

        class FakeStdout:
            def readline(self):
                return chunks.pop(0) if chunks else b""

        class FakePopen:
            def __init__(self, argv, stdout, stderr, start_new_session):
                self.pid = 12345
                self.stdout = FakeStdout()
                self.argv = argv

            def wait(self, timeout=None):
                return 0

        with tempfile.TemporaryDirectory() as td, \
                mock.patch.object(env_verbs.subprocess, "Popen", FakePopen), \
                mock.patch.object(env_verbs, "wait_rping_identity", return_value={"pid": 12345, "start_ticks": 7, "exe_sha256": env_verbs.EXPECTED_RPING_SHA256}), \
                mock.patch.object(env_verbs, "rdma_inventory", return_value={"binary": {"sha256": env_verbs.EXPECTED_RPING_SHA256}}), \
                mock.patch.object(env_verbs, "resources_snapshot", return_value={"rdma_resource": {"stdout": ""}}), \
                mock.patch.object(env_verbs, "run_cmd", return_value={"stdout": ""}), \
                mock.patch.object(env_verbs, "sha256_file", return_value=env_verbs.EXPECTED_RPING_SHA256):
            rc = env_verbs.run_endpoint(Namespace(
                role="client",
                bind="192.168.109.12",
                peer="192.168.109.13",
                port=19669,
                size=256,
                count=3,
                run_id=RUN_ID,
                output=Path(td),
                timeout=3,
            ))
            self.assertEqual(0, rc)
            report = json.loads((Path(td) / f"{RUN_ID}-client.json").read_text())
            self.assertEqual("client", report["role"])
            self.assertEqual(env_verbs.sha256_bytes((Path(td) / f"{RUN_ID}-client.raw.log").read_bytes()), report["raw_log_sha256"])

    def test_timeout_is_reported_without_claiming_success(self):
        with tempfile.TemporaryDirectory() as td:
            with self.assertRaises(env_verbs.VerbsError):
                env_verbs.run_endpoint(Namespace(
                    role="client",
                    bind="192.168.109.12",
                    peer="192.168.109.13",
                    port=19669,
                    size=256,
                    count=3,
                    run_id=RUN_ID,
                    output=Path(td),
                    timeout=31,
                ))


if __name__ == "__main__":
    unittest.main()
