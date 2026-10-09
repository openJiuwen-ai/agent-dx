#!/usr/bin/env python3
"""Network preparation predicates, seeded by retained Linux observations."""
import json
import shutil
import tempfile
import unittest
from pathlib import Path

import environment
import test_environment


FIXTURE = Path(__file__).resolve().parent / "fixtures/network-preparation"


class NetworkEnvironmentTests(unittest.TestCase):
    def make_bundle(self, root):
        bundle = test_environment.EnvironmentEvaluatorTests().make_bundle(root)
        net = root / "network"
        shutil.copytree(FIXTURE / "linux", net)
        shutil.copyfile(FIXTURE / "semantic-final/env_network.py", net / "probe.py")
        shutil.copyfile(FIXTURE / "attempt-2/commands.jsonl", net / "commands.jsonl")
        shutil.copyfile(FIXTURE / "afs-v67-fault-r2.sh", net / "fault.sh")
        bundle["network"] = {"prefix": "network", "probe_source": "network/probe.py",
                             "commands": "network/commands.jsonl", "fault_source": "network/fault.sh"}
        for path in net.rglob("*"):
            if path.is_file():
                bundle["artifact_references"][path.relative_to(root).as_posix()] = test_environment.sha(path)
        return bundle

    def outcome(self, root, bundle):
        report = environment.evaluate_environment(test_environment.lock(bundle["contract"]["sha256"]), bundle, root)
        self.assertNotEqual("PASS", report["status"], "network cannot qualify complete ENV")
        return next(item for item in report["checks"] if item["name"] == "network-tls-fault-recovery")

    def edit_json(self, root, bundle, rel, edit):
        path = root / rel
        value = json.loads(path.read_text())
        edit(value)
        test_environment.write_json(path, value)
        bundle["artifact_references"][rel] = test_environment.sha(path)

    def test_retained_valid_network_observations_pass_only_network_predicate(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            self.assertEqual("PASS", self.outcome(root, bundle)["status"])

    def test_generic_pass_receipt_is_not_network_evidence(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            bundle["network"] = {"status": "PASS", "summary": {"pass": 45}}
            self.assertNotEqual("PASS", self.outcome(root, bundle)["status"])

    def test_missing_artifact_is_blocked(self):
        for rel in ("network/a/logs/pair-a-b.json", "network/a/ready.json",
                    "network/commands.jsonl", "network/probe.py", "network/fault.sh"):
            with self.subTest(rel=rel), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                del bundle["artifact_references"][rel]
                self.assertEqual("BLOCKED", self.outcome(root, bundle)["status"])

    def test_tampered_artifact_fails(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            (root / "network/a/logs/pair-a-b.json").write_text("{}\n")
            self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_rehashed_bad_semantics_cannot_pass(self):
        cases = (
            ("wrong-local", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tcp"].update(local=["127.0.0.1", 1234])),
            ("wrong-peer", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["udp"].update(sender=["192.168.109.14", 19566])),
            ("wrong-bytes", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tcp"].update(bytes=0)),
            ("boolean-bytes", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tcp"].update(bytes=True)),
            ("nan-time", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tcp"].update(elapsed_seconds=float("nan"))),
            ("empty-nonce", "network/a/logs/pair-a-b.json", lambda v: v.update(token_sha256="")),
            ("nonhex-nonce", "network/a/logs/pair-a-b.json", lambda v: v.update(token_sha256="z" * 64)),
            ("tls-disabled", "network/b/ready.json", lambda v: v["tls"].update(mtls=False)),
            ("weak-tls", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tls"].update(tls_version="TLSv1.1")),
            ("fake-negative", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tls_missing_client_cert"]["observed"].update(reason="timeout")),
            ("wrong-negative-name", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tls_missing_client_cert"].update(negative="untrusted_ca")),
            ("wrong-name-code", "network/a/logs/pair-a-b.json", lambda v: v["checks"]["tls_wrong_hostname"]["observed"].update(verify_code="19")),
            ("wrong-script", "network/a/ready.json", lambda v: v.update(script_sha256="0" * 64)),
            ("boolean-pid", "network/a/ready.json", lambda v: v.update(pid=True)),
            ("boot-changed", "network/a/logs/preserved-after.json", lambda v: v.update(boot_id="different")),
            ("stale-live", "network/a/logs/server-live-after.json", lambda v: v.update(start_ticks=1)),
            ("wrong-fault-pair", "network/a/logs/fault-injected.json", lambda v: v.update(target="192.168.109.14")),
            ("unbounded-fault", "network/a/logs/fault-injected.json", lambda v: v["checks"]["tcp"].update(elapsed_seconds=60)),
            ("fake-fault-failure", "network/a/logs/fault-injected.json", lambda v: v["checks"]["tcp"].update(reason="mismatch")),
            ("missing-fault-nonce", "network/a/logs/fault-injected.json", lambda v: v.pop("token_sha256")),
            ("unbounded-fault-timeout", "network/a/logs/fault-injected.json", lambda v: v.update(timeout_seconds=60)),
            ("listener-not-stopped", "network/a/logs/server-stopped.json", lambda v: v.update(status="LIVE")),
        )
        for name, rel, edit in cases:
            with self.subTest(name=name), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                self.edit_json(root, bundle, rel, edit)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_missing_live_schema_is_not_an_exception_or_pass(self):
        for value in ([], None, "PASS", {"status": "PASS"}):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                rel = "network/a/logs/pair-a-b.json"
                test_environment.write_json(root / rel, value)
                bundle["artifact_references"][rel] = test_environment.sha(root / rel)
                self.assertNotEqual("PASS", self.outcome(root, bundle)["status"])

    def test_fault_hit_and_exact_restoration_are_required(self):
        for rel, text in (("network/b/logs/iptables-hit.txt", "0 0 DROP 6 -- * * 192.168.109.12 192.168.109.13 multiport dports 19566,19567 /* afs-env-v67-only */\n"),
                          ("network/b/logs/iptables-final-restored.txt", "*filter\n:INPUT ACCEPT [0:0]\nCOMMIT\n")):
            with self.subTest(rel=rel), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                (root / rel).write_text(text)
                bundle["artifact_references"][rel] = test_environment.sha(root / rel)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_duplicate_nonce_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            nonce = json.loads((root / "network/a/logs/pair-a-c.json").read_text())["token_sha256"]
            self.edit_json(root, bundle, "network/a/logs/pair-a-b.json", lambda v: v.update(token_sha256=nonce))
            self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_empty_or_failed_command_transcript_is_rejected(self):
        for text in ("", json.dumps({"argv": [], "returncode": 0}) + "\n"):
            with self.subTest(text=text), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                path = root / "network/commands.jsonl"; path.write_text(text)
                bundle["artifact_references"]["network/commands.jsonl"] = test_environment.sha(path)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_declared_network_path_cannot_escape_bundle(self):
        for field in ("prefix", "probe_source", "commands", "fault_source"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                bundle["network"][field] = "../outside"
                self.assertNotEqual("PASS", self.outcome(root, bundle)["status"])

    def test_consistent_unknown_probe_is_rejected(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            data = b"print('PASS')\n"
            for rel in ["network/probe.py"] + [f"network/{name}/env_network.py" for name in ("ctl", "a", "b", "c")]:
                (root / rel).write_bytes(data)
                bundle["artifact_references"][rel] = test_environment.sha(root / rel)
            digest = test_environment.sha(root / "network/probe.py")
            for name in ("ctl", "a", "b", "c"):
                self.edit_json(root, bundle, f"network/{name}/ready.json", lambda v: v.update(script_sha256=digest))
            self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_watchdog_completion_cannot_replace_fault_transcript(self):
        # A completion marker cannot substitute for the directed fault transcript.
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            path = root / "network/commands.jsonl"
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows = [row for row in rows if "fault-injected.json" not in " ".join(row["argv"])]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            bundle["artifact_references"]["network/commands.jsonl"] = test_environment.sha(path)
            self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_command_identity_exit_and_tls_options_are_bound(self):
        for mutation in ("wrong-guest", "failed-call", "wrong-source", "missing-negative", "echo-only"):
            with self.subTest(mutation=mutation), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                path = root / "network/commands.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                row = next(row for row in rows if "pair-a-b.json" in " ".join(row["argv"]))
                if mutation == "wrong-guest":
                    row["argv"] = ["afs-accept-c" if v == "afs-accept-a" else v for v in row["argv"]]
                elif mutation == "failed-call":
                    row["returncode"] = 1
                elif mutation == "wrong-source":
                    row["argv"][-1] = row["argv"][-1].replace("--source-ip 192.168.109.12", "--source-ip 127.0.0.1")
                elif mutation == "missing-negative":
                    row["argv"][-1] = row["argv"][-1].replace("--missing-client-cert", "")
                else:
                    row["argv"][-1] = "echo " + row["argv"][-1]
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                bundle["artifact_references"]["network/commands.jsonl"] = test_environment.sha(path)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_fault_commands_must_execute_instead_of_echo(self):
        for step in ("install", "inspect;", "restore"):
            with self.subTest(step=step), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                path = root / "network/commands.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                row = next(row for row in rows if f"afs-v67-fault.sh {step}" in " ".join(row["argv"]))
                row["argv"][-1] = "echo " + row["argv"][-1]
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                bundle["artifact_references"]["network/commands.jsonl"] = test_environment.sha(path)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_declared_fault_recipe_must_be_supported(self):
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            path = root / "network/fault.sh"; path.write_text("#!/bin/sh\necho PASS\n")
            bundle["artifact_references"]["network/fault.sh"] = test_environment.sha(path)
            self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_malformed_command_fields_return_failure_without_exception(self):
        for field, value in (("argv", None), ("argv", [None]),
                             ("time_unix_ms", None), ("time_unix_ms", "now")):
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                path = root / "network/commands.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                rows[0][field] = value
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                bundle["artifact_references"]["network/commands.jsonl"] = test_environment.sha(path)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_wrong_client_negative_schema_and_command_are_required(self):
        for value in (None, [], "PASS", {"status": "FAIL", "negative": "wrong_hostname",
                       "observed": {"status": "FAIL", "reason": "untrusted_ca", "detail": "unknown ca"}}):
            with self.subTest(value=value), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                self.edit_json(root, bundle, "network/a/logs/wrong-client.json",
                               lambda v: v["checks"].update(tls_untrusted_client_cert=value))
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])
        with tempfile.TemporaryDirectory() as td:
            root = Path(td); bundle = self.make_bundle(root)
            path = root / "network/commands.jsonl"
            rows = [json.loads(line) for line in path.read_text().splitlines()]
            rows = [row for row in rows if "wrong-client.json" not in " ".join(row["argv"])]
            path.write_text("".join(json.dumps(row) + "\n" for row in rows))
            bundle["artifact_references"]["network/commands.jsonl"] = test_environment.sha(path)
            self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_preserved_observations_cannot_compare_missing_equal(self):
        for field in ("processes", "mountinfo", "iptables"):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                for suffix in ("before", "after"):
                    self.edit_json(root, bundle, f"network/a/logs/preserved-{suffix}.json",
                                   lambda v: v.pop(field))
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_fault_counter_addresses_and_scope_are_exact(self):
        for old, new in (("192.168.109.13", "192.168.109.130"),
                         ("192.168.109.12", "192.168.109.12/0"),
                         ("*      *", "eth0   *"),
                         ("udp dpt:19566", "udp dpt:195660")):
            with self.subTest(new=new), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                rel = "network/b/logs/iptables-hit.txt"; path = root / rel
                self.assertIn(old, path.read_text())
                path.write_text(path.read_text().replace(old, new))
                bundle["artifact_references"][rel] = test_environment.sha(path)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])

    def test_client_command_cannot_mask_exit_with_extra_shell_commands(self):
        for suffix in ("; exit 0", " || true", " --missing-client-cert"):
            with self.subTest(suffix=suffix), tempfile.TemporaryDirectory() as td:
                root = Path(td); bundle = self.make_bundle(root)
                path = root / "network/commands.jsonl"
                rows = [json.loads(line) for line in path.read_text().splitlines()]
                row = next(row for row in rows if "pair-a-b.json" in " ".join(row["argv"]))
                row["argv"][-1] += suffix
                path.write_text("".join(json.dumps(row) + "\n" for row in rows))
                bundle["artifact_references"]["network/commands.jsonl"] = test_environment.sha(path)
                self.assertEqual("FAIL", self.outcome(root, bundle)["status"])


if __name__ == "__main__":
    unittest.main()
