"""Source-only process planner regressions; all external I/O is injected."""
import copy
from functools import partial
import importlib.util
import io
import json
import logging
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import MagicMock, patch


SPEC = importlib.util.spec_from_file_location("node_rollout", Path(__file__).parents[1] / "node_rollout.py")
rollout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rollout)


def arguments(role="adxlet", nodes=("pod-b=node-b", "pod-a=node-a", "pod-c=node-c", "pod-d=node-d")):
    argv = ["plan-process", "--context", "cluster", "--namespace", "platform",
            "--container", "supervisor", "--config", "/etc/adx/deployment.yaml", "--role", role,
            "--commit", "c" * 40, "--sha256", "d" * 64, "--task-id", "release", "--epoch", "2",
            "--batch-size", "2"]
    if role == "adxlet":
        argv.extend(["--origin", "http://coordinator:9090"])
    for node in nodes:
        argv.extend(["--node", node])
    return rollout.parser().parse_args(argv)


def pod(name):
    return {"apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": name, "namespace": "platform", "uid": "uid-" + name, "resourceVersion": "1"},
            "spec": {"nodeName": "host-" + name, "containers": [{"name": "supervisor"}]},
            "status": {"containerStatuses": [{"name": "supervisor", "containerID": "containerd://" + name,
                                              "restartCount": 0, "state": {"running": {"startedAt": "now"}}}]}}


def info(name, role):
    runtime = {"boot_nonce": "boot", "pod_uid": "uid-" + name, "pid": 42,
               "configuration_sha256": "e" * 64,
               "executable_sha256": "a" * 64, "node_session": "node-session", "relay_session": "relay-session"}
    # step_protocol 2 marks an adxctl whose Submit only prepares files and whose
    # exit/start/rollback run as separate publisher-driven Steps.
    return {"ok": True, "node_info": {"schema_version": 1, "step_protocol": 2, "boot_nonce": "boot",
            "pod_uid": "uid-" + name,
            "components": [{"service_id": "service", "node_id": name.replace("pod-", "node-"),
                            "role": role, "mode": "standalone", "can_upgrade": True, "runtime": runtime}]}}


def without_steps(name, role, marker=KeyError):
    """node-info of an old adxctl (no marker) or of an invalid marker value."""
    value = info(name, role)
    if marker is KeyError:
        del value["node_info"]["step_protocol"]
    else:
        value["node_info"]["step_protocol"] = marker
    return value


def plan(args, change_pod=None, change_info=None):
    def kube(argv):
        name = argv[2]
        value = pod(name)
        if change_pod:
            change_pod(value)
        return value

    def local(name):
        value = info(name, args.role)
        if change_info:
            change_info(value)
        return value

    return rollout.plan_process(args, kube, local)


class ProcessPlanTests(unittest.TestCase):
    def test_submission_adapter_refuses_lossy_json_before_any_execution(self):
        args = arguments()
        args.archive_url, args.archive_sha256 = "https://packages.example/release.tar.gz", "e" * 64
        args.component_config_path = "/mounted/node.json"
        snapshot = plan(args)
        changes = (((), "extra", True), (("selection",), "extra", True),
                   (("selection", "expected"), "extra", True), (("process",), "extra", True),
                   (("process", "identity"), "extra", True), (("remote_archive",), "extra", True),
                   (("component_config",), "sha256", "f" * 64), ((), "sha256", None),
                   ((), "remote_archive", None), ((), "component_config", None), ((), "process", None),
                   ((), "commit", 42), (("remote_archive",), "url", 42),
                   (("component_config",), "mount_path", "bad\x00path"))
        for path, key, value in changes:
            with self.subTest(path=path, key=key, value=value):
                changed = copy.deepcopy(snapshot)
                submission = changed["nodes"][0]["submission"]
                target = submission
                for part in path:
                    target = target[part]
                target[key] = value
                with self.assertRaises(rollout.Refusal):
                    rollout.submit_arguments(submission)
                with self.assertRaises(rollout.Refusal):
                    rollout.validate_process_plan(changed)

    def test_process_plan_rejects_different_node_archive_sources(self):
        args = arguments("relay")
        args.archive_url, args.archive_sha256 = "https://packages.example/release.tar.gz", "e" * 64
        snapshot = plan(args)
        snapshot["nodes"][-1]["submission"]["remote_archive"]["url"] += "?different"
        with self.assertRaisesRegex(rollout.Refusal, "archive"):
            rollout.validate_process_plan(snapshot)

    def test_process_plan_keeps_global_config_descriptor_and_node_runtime_config(self):
        args = arguments()
        args.component_config_path = "/mounted/node.json"
        snapshot = plan(args)
        snapshot["nodes"][-1]["submission"]["selection"]["expected"]["configuration_sha256"] = "b" * 64
        rollout.validate_process_plan(snapshot)
        snapshot["nodes"][-1]["submission"]["component_config"]["mount_path"] = "/mounted/different.json"
        with self.assertRaisesRegex(rollout.Refusal, "configuration"):
            rollout.validate_process_plan(snapshot)

    def test_node_info_schema_and_sessions_are_checked_by_consumer(self):
        for marker in (None, True, 2):
            with self.subTest(schema_version=marker):
                response = info("pod-a", "adxlet")
                response["node_info"]["schema_version"] = marker
                with self.assertRaisesRegex(rollout.Refusal, "schema"):
                    rollout.process_component(response, "adxlet", "node-a", "uid-pod-a")
        for key in ("node_session", "relay_session"):
            with self.subTest(session=key):
                response = info("pod-a", "adxlet")
                response["node_info"]["components"][0]["runtime"][key] = ""
                with self.assertRaisesRegex(rollout.Refusal, "session"):
                    rollout.process_component(response, "adxlet", "node-a", "uid-pod-a")
        response = info("pod-a", "adxlet")
        response["node_info"]["components"][0]["mode"] = "embedded"
        response["node_info"]["components"][0]["runtime"]["relay_session"] = ""
        rollout.process_component(response, "adxlet", "node-a", "uid-pod-a", modes=("embedded",))


    def test_remote_omits_expected_binary_digest_but_local_requires_it(self):
        args = arguments("relay")
        args.sha256 = None
        with self.assertRaises(rollout.Refusal):
            plan(args)
        args.archive_url, args.archive_sha256 = "https://packages.example/release.tar.gz", "e" * 64
        snapshot = plan(args)
        rollout.validate_process_plan(snapshot)
        node = snapshot["nodes"][0]
        self.assertNotIn("sha256", node["submission"])
        self.assertNotIn("--sha256", rollout.submit_arguments(node["submission"]))
        original = copy.deepcopy(node["submission"])
        runtime = dict(node["submission"]["selection"]["expected"], executable_sha256="d" * 64,
                       relay_session="candidate")
        task = {"phase": "completed", "execution": {"phase": "completed", "candidate_session": "candidate"}}
        self.assertIsNone(rollout.finish_process(snapshot, node, task, runtime))
        task["prepared"] = {"binary_sha256": "d" * 64, "configuration_sha256": "e" * 64}
        self.assertEqual(rollout.finish_process(snapshot, node, task, runtime), ("completed", None))
        task["prepared"]["configuration_sha256"] = "f" * 64
        self.assertIsNone(rollout.finish_process(snapshot, node, task, runtime))
        task["prepared"]["configuration_sha256"] = "e" * 64
        task["prepared"]["binary_sha256"] = "f" * 64
        self.assertIsNone(rollout.finish_process(snapshot, node, task, runtime))
        self.assertEqual(node["submission"], original)

    def test_remote_archive_frozen_only_in_submission_and_exact_cli(self):
        args = arguments("relay")
        args.archive_url = "https://packages.example/release.tar.gz"
        args.archive_sha256 = "e" * 64
        args.component_config_path = "/mounted/relay.json"
        snapshot = plan(args)
        rollout.validate_process_plan(snapshot)
        self.assertNotIn("remote_archive", snapshot)
        for node in snapshot["nodes"]:
            submission = node["submission"]
            self.assertEqual(submission["remote_archive"], {
                "url": args.archive_url, "archive_sha256": args.archive_sha256})
            self.assertEqual(submission["component_config"], {"mount_path": args.component_config_path})
            argv = rollout.submit_arguments(submission)
            self.assertEqual(argv[argv.index("--component-config-path") + 1], args.component_config_path)
            self.assertNotIn("--component-config-sha256", argv)
            self.assertEqual(argv[argv.index("--archive-url") + 1], args.archive_url)
            self.assertEqual(argv[argv.index("--archive-sha256") + 1], args.archive_sha256)
        changed = copy.deepcopy(snapshot)
        changed["nodes"][0]["submission"]["remote_archive"]["url"] += "?changed"
        self.assertNotEqual(rollout.canonical(snapshot), rollout.canonical(changed))

    def test_component_config_path_requires_no_digest_argument(self):
        argv = ["plan-process", "--context", "cluster", "--namespace", "platform", "--container", "supervisor",
                "--config", "/etc/adx/deployment.yaml", "--role", "relay", "--commit", "c" * 40,
                "--sha256", "d" * 64, "--task-id", "release", "--epoch", "2", "--node", "pod-a=node-a",
                "--component-config-path", "/mounted/relay.json"]
        snapshot = plan(rollout.parser().parse_args(argv))
        self.assertEqual(snapshot["nodes"][0]["submission"]["component_config"],
                         {"mount_path": "/mounted/relay.json"})
        with patch.object(rollout.argparse.ArgumentParser, "error", side_effect=ValueError) as error:
            with self.assertRaises(ValueError):
                rollout.parser().parse_args([*argv, "--component-config-sha256", "e" * 64])
        error.assert_called_once()

    def test_remote_unpaired_before_reads(self):
        for url, digest in ((None, "a" * 64), ("https://host/a", None)):
            with self.subTest(url=url, digest=digest):
                args = arguments()
                args.archive_url, args.archive_sha256 = url, digest
                kube, local = MagicMock(), MagicMock()
                with self.assertRaises(rollout.Refusal):
                    rollout.plan_process(args, kube, local)
                kube.assert_not_called()
                local.assert_not_called()

    def test_remote_standard_hosts_and_archive_paths_remain_accepted(self):
        for url in ("http://host/a", "https://user@host/a", "https://packages.example/archive.tar.gz",
                    "https://127.0.0.1:8443/archive.tar.gz",
                    "https://[2001:db8::1]:8443/sdk/aarch64/long%20wheel.tar.gz?version=1",
                    "https://%65xample.com/archive.tar.gz"):
            with self.subTest(url=url):
                args = arguments("relay", nodes=("pod-a=node-a",))
                args.archive_url, args.archive_sha256 = url, "e" * 64
                snapshot = plan(args)
                rollout.validate_process_plan(snapshot)
                self.assertEqual(snapshot["nodes"][0]["submission"]["remote_archive"]["url"], url)

    def test_invalid_config_refused_before_external_reads(self):
        for config in ("/etc/adx/deployment.json", "/etc/adx/deployment", "relative.yaml"):
            with self.subTest(config=config):
                args = arguments()
                args.config = config
                kube = MagicMock()
                local = MagicMock()
                with self.assertRaises(rollout.Refusal):
                    rollout.plan_process(args, kube, local)
                kube.assert_not_called()
                local.assert_not_called()

    def test_unicode_rollout_and_node_ids_use_utf8_byte_limits(self):
        oversized = "\u4e2d" * 43
        for field in ("task_id", "node"):
            with self.subTest(field=field):
                args = arguments()
                if field == "task_id":
                    args.task_id = oversized
                else:
                    args.node = ["pod-a=" + oversized]
                kube = MagicMock()
                local = MagicMock()
                with self.assertRaises(rollout.Refusal):
                    rollout.plan_process(args, kube, local)
                kube.assert_not_called()
                local.assert_not_called()

    def test_exact_submissions_for_both_roles(self):
        for role in ("adxlet", "relay"):
            with self.subTest(role=role):
                result = plan(arguments(role))
                node = result["nodes"][0]
                runtime = info("pod-a", role)["node_info"]["components"][0]["runtime"]
                expected = {"selection": {"mode": "publisher", "expected": runtime},
                            "task_id": node["submission"]["task_id"], "role": role, "service_id": "service",
                            "commit": "c" * 40, "sha256": "d" * 64}
                if role == "adxlet":
                    expected["process"] = {"node_id": "node-a", "identity": {
                        "rollout_id": "release", "epoch": 2, "old_node_session": "node-session",
                        "old_relay_session": "relay-session", "original_sha256": "a" * 64}}
                    self.assertEqual(result["origin"], "http://coordinator:9090")
                else:
                    self.assertNotIn("origin", result)
                self.assertEqual(node["submission"], expected)
                self.assertEqual(set(node), {"node_id", "kube", "submission"})

    def test_sorting_canary_and_stable_distinct_local_ids(self):
        first = plan(arguments())
        second_args = arguments(nodes=("pod-d=node-d", "pod-c=node-c", "pod-a=node-a", "pod-b=node-b"))
        second_args.batch_size = 3
        second = plan(second_args)
        self.assertEqual([n["node_id"] for n in first["nodes"]], ["node-a", "node-b", "node-c", "node-d"])
        self.assertEqual(first["batches"], [["node-a"], ["node-b", "node-c"], ["node-d"]])
        self.assertEqual(second["batches"][0], ["node-a"])
        ids = [n["submission"]["task_id"] for n in first["nodes"]]
        self.assertEqual(ids, [n["submission"]["task_id"] for n in second["nodes"]])
        self.assertEqual(len(set(ids)), 4)
        self.assertTrue(all(len(value) <= 64 for value in ids))

    def test_pod_and_container_changes_refused(self):
        changes = [
            ("metadata", "uid", "replacement"), ("metadata", "name", "other"),
            ("metadata", "namespace", "other"), ("metadata", "deletionTimestamp", "now"),
        ]
        for section, key, value in changes:
            with self.subTest(section=section, key=key):
                with self.assertRaises(rollout.Refusal):
                    plan(arguments(), change_pod=lambda obj, s=section, k=key, v=value: obj[s].update({k: v}))
        for key, value in [("state", {"terminated": {}}), ("name", "other")]:
            with self.subTest(key=key):
                with self.assertRaises(rollout.Refusal):

                    def change(obj, k=key, v=value):
                        obj["status"]["containerStatuses"][0][k] = v

                    plan(arguments(), change_pod=change)

    def test_runtime_and_component_changes_refused(self):
        for key, value in [("boot_nonce", "new"), ("pod_uid", "forged")]:
            with self.subTest(key=key):

                def change(obj, k=key, v=value):
                    obj["node_info"]["components"][0]["runtime"][k] = v
                with self.assertRaises(rollout.Refusal):
                    plan(arguments(), change_info=change)
        for key, value in [("node_id", "wrong"), ("mode", "embedded"),
                           ("can_upgrade", False), ("can_upgrade", 1), ("runtime", None)]:
            with self.subTest(key=key, value=value):
                with self.assertRaises(rollout.Refusal):

                    def change_component(obj, k=key, v=value):
                        obj["node_info"]["components"][0][k] = v

                    plan(arguments(), change_info=change_component)

    def test_both_node_info_and_runtime_cannot_forge_kube_uid(self):
        def forged(obj):
            obj["node_info"]["pod_uid"] = "forged"
            obj["node_info"]["components"][0]["runtime"]["pod_uid"] = "forged"
        with self.assertRaises(rollout.Refusal):
            plan(arguments(), change_info=forged)

    def test_initial_boot_is_frozen_and_ambiguous_component_refused(self):
        def reboot(obj):
            obj["node_info"]["boot_nonce"] = "reboot"
            obj["node_info"]["components"][0]["runtime"]["boot_nonce"] = "reboot"

        def duplicate(obj):
            components = obj["node_info"]["components"]
            components.append(copy.deepcopy(components[0]))

        snapshot = plan(arguments(), change_info=reboot)
        self.assertEqual(snapshot["nodes"][0]["submission"]["selection"]["expected"]["boot_nonce"], "reboot")
        with self.assertRaises(rollout.Refusal):
            plan(arguments(), change_info=duplicate)

    def test_plan_records_one_observation_and_runtime_fence(self):
        args = arguments(nodes=("pod-a=node-a",))
        kube = MagicMock(return_value=pod("pod-a"))
        local = MagicMock(return_value=info("pod-a", args.role))
        result = rollout.plan_process(args, kube, local)
        kube.assert_called_once()
        local.assert_called_once()
        self.assertEqual(result["nodes"][0]["kube"]["resource_version"], "1")
        self.assertNotIn("rechecked_resource_version", result["nodes"][0]["kube"])
        self.assertEqual(result["nodes"][0]["submission"]["selection"]["expected"],
                         info("pod-a", args.role)["node_info"]["components"][0]["runtime"])

    def test_cli_uses_absolute_kubectl_and_literal_exec_argv(self):
        args = arguments(nodes=("pod-a=node-a",))
        args.config = "/etc/adx/config;not-shell.yaml"
        output = info("pod-a", "adxlet")
        process = MagicMock()
        process.stdout = io.BytesIO(json.dumps(output).encode())
        process.wait.return_value = 0
        process.poll.return_value = 0
        process.__enter__.return_value = process
        with patch.object(rollout.shutil, "which", return_value="kubectl"):
            with patch.object(rollout.subprocess, "Popen", return_value=process) as popen:
                with patch.object(rollout, "plan_process") as planner:
                    def observe_node(selected, kube, local):
                        return local("pod-a")

                    planner.side_effect = observe_node
                    with patch("sys.stdout", new_callable=io.StringIO):
                        argv = ["plan-process", "--context", args.context, "--namespace", args.namespace,
                                "--container", args.container, "--config", args.config, "--role", args.role,
                                "--commit", args.commit, "--sha256", args.sha256, "--task-id", args.task_id,
                                "--epoch", "2", "--node", "pod-a=node-a", "--origin", args.origin]
                        self.assertEqual(rollout.main(argv), 0)
                command = popen.call_args.args[0]
                self.assertTrue(Path(command[0]).is_absolute())
                self.assertEqual(command[1:], ["--request-timeout=10s", "--context", "cluster", "--namespace",
                                 "platform", "exec", "pod-a", "--container", "supervisor", "--", "adxctl",
                                 "--config", args.config, "rollout", "node-info"])
                self.assertNotIn("shell", popen.call_args.kwargs)

    def test_plan_uses_explicit_adxctl_path(self):
        args = arguments(nodes=("pod-a=node-a",))
        argv = ["plan-process", "--context", args.context, "--namespace", args.namespace,
                "--container", args.container, "--config", args.config, "--role", args.role,
                "--commit", args.commit, "--sha256", args.sha256, "--task-id", args.task_id,
                "--epoch", "2", "--node", "pod-a=node-a", "--origin", args.origin]
        with (patch.dict(rollout.os.environ, {"ADX_ROLLOUT_ADXCTL": "/opt/adx/bin/adxctl"}),
              patch.object(rollout, "kubectl_json", return_value={}) as kube,
              patch.object(rollout, "plan_process", side_effect=lambda args, get, local: local("pod-a")),
              patch("sys.stdout", new_callable=io.StringIO)):
            self.assertEqual(rollout.main(argv), 0)
        self.assertEqual(kube.call_args.args[0][-5:],
                         ["/opt/adx/bin/adxctl", "--config", args.config, "rollout", "node-info"])


class ProcessUpgradeHttpTests(unittest.TestCase):
    def owner(self, epoch=7):
        return dict(node_id="node-a", rollout_id="release", local_task_id="task", epoch=epoch)

    def test_flat_status_preserves_exact_u64_epoch_and_session_boundaries(self):
        for epoch in (1, 2**53 + 1, 2**63 + 5, 2**64 - 1):
            owner = self.owner(epoch)
            for status in ("absent", "active", "released"):
                with self.subTest(epoch=epoch, status=status):
                    result = dict(status=status, owner=owner, server_metadata="ignored")
                    if status == "active":
                        result.update(origin_session="s0", current_session="一" * 42 + "ab", yielded=True)
                    elif status == "released":
                        result["node_session"] = "x" * 128
                    post = MagicMock(return_value=copy.deepcopy(result))
                    self.assertEqual(rollout.process_upgrade_status(post, owner), result)
                    post.assert_called_once_with("/v1/adxlet-process-upgrades/query", dict(owner=owner))

    def test_query_rejects_invalid_payload_owner_and_session_without_absence_fallback(self):
        owner = self.owner()
        active = dict(status="active", owner=owner, origin_session="s0", current_session="s1", yielded=True)
        invalid = [None, [], "<html>old coordinator</html>", {}, dict(status="absent", owner={}),
                   dict(status="missing", owner=owner),
                   dict(active, owner=dict(owner, epoch=7.0)), dict(active, yielded=1)]
        for session in ("", 5, None):
            for field in ("origin_session", "current_session"):
                invalid.append(dict(active, **{field: session}))
            invalid.append(dict(status="released", owner=owner, node_session=session))
        for response in invalid:
            with self.subTest(response=response), self.assertRaises((rollout.Refusal, UnicodeError)):
                rollout.process_upgrade_status(MagicMock(return_value=response), owner)

    def test_query_transport_errors_remain_unknown(self):
        for error in (rollout.Refusal("HTTP 404"), rollout.Refusal("HTTP 409"), rollout.Refusal("HTTP 503"),
                      OSError("request timed out"), rollout.http.client.HTTPException("connection lost")):
            with self.subTest(error=error), self.assertRaises(type(error)):
                rollout.process_upgrade_status(MagicMock(side_effect=error), self.owner())

    def test_release_receipt_requires_same_owner_and_frozen_session(self):
        owner = self.owner()
        self.assertEqual(rollout.process_release_receipt(MagicMock(return_value=dict(
            status="released", owner=owner, node_session="s1")), owner, "s1"), dict(owner=owner, node_session="s1"))
        for response in (dict(status="released", owner=dict(owner, epoch=8), node_session="s1"),
                         dict(status="released", owner=owner, node_session="s2"),
                         dict(status="active", owner=owner, origin_session="s0", current_session="s2", yielded=True)):
            with self.subTest(response=response), self.assertRaises(rollout.Refusal):
                rollout.process_release_receipt(MagicMock(return_value=response), owner, "s1")
        self.assertIsNone(rollout.process_release_receipt(MagicMock(return_value=dict(
            status="absent", owner=owner)), owner, "s1"))


class AdxletPublisherFixture:
    """Existing node-info/Task shapes with injected local and HTTP effects."""

    def __init__(self, case, snapshot, journal):
        self.case, self.snapshot, self.journal = case, snapshot, journal
        self.node = snapshot["nodes"][0]
        self.owner = rollout.maintenance_owner(snapshot, self.node)
        self.task, self.marker, self.receipt = None, None, None
        self.view = info("pod-a", "adxlet")
        self.events, self.lost_acks = [], set()
        self.query_unknown = False
        self.failed_preparation = False
        self.target_fails, self.original_fails = False, False
        self.unknown_steps = {}
        self.started_attempts, self.stopped_attempts = [], []
        self.stop_deadline = None
        self.cli = MagicMock(side_effect=self.command)
        self.post = MagicMock(side_effect=self.http)

    def http(self, path, body):
        if path == "/v1/adxlet-process-upgrades/query":
            self.case.assertEqual(body, dict(owner=self.owner))
            if self.receipt is not None:
                response = dict(status="released", **copy.deepcopy(self.receipt))
            elif self.marker is not None:
                fields = ("owner", "origin_session", "current_session", "yielded")
                response = dict(status="active", **{key: copy.deepcopy(self.marker[key]) for key in fields})
            else:
                response = dict(status="absent", owner=copy.deepcopy(self.owner))
            self.events.append("http.query." + response["status"])
            if response["status"] == "released":
                self.acknowledge("query_release")
            return response
        self.case.assertEqual(path, "/v1/adxlet-process-upgrades")
        self.case.assertIn(body.get("action"), ("prepare", "release"))
        action = body["action"]
        execution = self.task.get("adxlet_execution")
        session = execution["node_session"] if action == "release" and execution is not None else "node-session"
        self.case.assertEqual(body, dict(action=action, owner=self.owner, node_session=session))
        if action == "prepare":
            self.effect("maintenance", "prepare_intent")
            self.case.assertIsNotNone(self.task["prepared"])
            self.marker = dict(owner=self.owner, origin_session="node-session",
                               current_session="node-session", yielded=False, replacement=None)
            self.acknowledge("maintenance")
            return copy.deepcopy(self.marker)
        self.effect("release", "release_intent")
        self.receipt = dict(owner=self.owner, node_session=session)
        self.marker = None
        self.acknowledge("release")
        return copy.deepcopy(self.receipt)

    def effect(self, name, stage):
        self.events.append(name)
        state = rollout.read_json_file(self.journal)["nodes"]["node-a"]
        self.case.assertEqual(state["stage"], stage)

    def acknowledge(self, name):
        if name in self.lost_acks:
            self.lost_acks.remove(name)
            raise rollout.Refusal("lost " + name + " ACK")

    def local(self, name):
        self.events.append("runtime")
        return copy.deepcopy(self.view)

    def command(self, node, argv):
        self.events.append(argv[0])
        if argv[0] == "submit":
            self.effect("submit.effect", "submit_intent")
            self.case.assertEqual(argv, rollout.submit_arguments(self.node["submission"]))
            self.task = dict(submission=self.node["submission"],
                             phase="failed" if self.failed_preparation else "running",
                             prepared=dict(binary_sha256="d" * 64, configuration_sha256="e" * 64),
                             execution=None, adxlet_execution=None)
            self.acknowledge("submit")
        elif argv[0] == "query":
            self.case.assertEqual(argv, ["query", "--task-id", self.node["submission"]["task_id"]])
            if self.query_unknown:
                self.query_unknown = False
                raise rollout.Refusal("query unknown")
        else:
            step = argv[4]
            state = rollout.read_json_file(self.journal)["nodes"]["node-a"]
            self.events.append(step)
            self.case.assertEqual(state["stage"], "rollback_intent" if step == "rollback" else step + "_intent")
            identity = ["", ""] if step == "exit" else ["attempt", "candidate"]
            self.case.assertEqual(argv, ["step", "--task-id", self.node["submission"]["task_id"],
                                        "--step", step, "--attempt", identity[0], "--node-session", identity[1]])
            if step == "exit":
                self.task["adxlet_execution"] = dict(stage="exited", old_pid=42, attempt="attempt",
                                                     node_session="candidate", deadline_unix_ms=4102444800000,
                                                     leg=dict(kind="target"), error=None)
                self.view["node_info"]["components"][0]["runtime"] = None
                self.marker["yielded"] = True
            elif step == "rollback":
                execution = self.task["adxlet_execution"]
                if execution["leg"] == dict(kind="target"):
                    if self.stop_deadline is None:
                        self.stop_deadline = 4102444500000
                    execution["deadline_unix_ms"] = self.stop_deadline
                    if step not in self.unknown_steps:
                        self.stopped_attempts.append("attempt")
                        execution.update(stage="exited", attempt="original-attempt",
                            node_session="original-session", error=None,
                            leg={"kind": "rollback", "from": dict(attempt="attempt", node_session="candidate")})
                        execution.pop("candidate_pid", None)
                        self.view["node_info"]["components"][0]["runtime"] = None
                    self.task["phase"] = "unknown"
                else:
                    self.case.assertEqual(execution["leg"]["from"], dict(attempt="attempt", node_session="candidate"))
                if step not in self.unknown_steps:
                    self.start_attempt(original=True)
            elif step == "start":
                self.case.assertEqual(self.task["adxlet_execution"]["leg"], dict(kind="target"))
                self.start_attempt(original=False)
            else:
                self.case.fail("unexpected destructive Step: " + step)
            if step in self.unknown_steps:
                self.task["phase"] = "unknown"
                self.task["adxlet_execution"].update(self.unknown_steps.pop(step))
                if step == "exit":
                    self.task["adxlet_execution"].update(attempt="", node_session="node-session")
                    self.view = info("pod-a", "adxlet")
                    self.marker["yielded"] = False
            self.acknowledge(step)
        return dict(ok=True, task=copy.deepcopy(self.task))

    def start_attempt(self, original):
        action = "original_start" if original else "start"
        if original:
            # Internal local work: this is not a publisher Start(B) request.
            self.events.append(action)
        self.case.assertTrue(self.marker["yielded"])
        if not original:
            self.case.assertIn(self.marker["current_session"],
                               ("node-session",) if self.task["adxlet_execution"]["stage"] == "exited" else
                               ("node-session", "candidate"))
        execution = self.task["adxlet_execution"]
        if original and execution["deadline_unix_ms"] <= 1:
            return  # expiry only observes, and cannot manufacture a recovered B
        failed = self.original_fails if original else self.target_fails
        self.task["phase"] = "unknown" if failed else ("failed" if original else "completed")
        execution.update(stage="starting" if failed else "selected",
                         error="candidate readiness unknown" if failed else None,
                         candidate_pid=None if failed else (44 if original else 43))
        if not failed:
            runtime = info("pod-a", "adxlet")["node_info"]["components"][0]["runtime"]
            runtime.update(pid=44 if original else 43, node_session=execution["node_session"],
                           executable_sha256="a" * 64 if original else "d" * 64)
            self.view["node_info"]["components"][0]["runtime"] = runtime
            if self.marker["current_session"] != execution["node_session"] and self.marker["current_session"] in (
                    "node-session", "candidate", "original-session"):
                self.marker["current_session"] = execution["node_session"]
                self.events.append("register")
            if execution["attempt"] not in self.started_attempts:
                self.started_attempts.append(execution["attempt"])
        if action in self.unknown_steps:
            self.task["phase"] = "unknown"
            execution.update(self.unknown_steps.pop(action))
            if execution["stage"] != "committing":
                self.view["node_info"]["components"][0]["runtime"] = None
                execution["candidate_pid"] = None
        if original:
            self.acknowledge(action)


class ProcessApplyTests(unittest.TestCase):
    INVALID_STEP_MARKERS = (KeyError, None, True, False, 0, 1, "2", 2.0)

    def test_adxlet_plan_requires_step_protocol_two(self):
        for marker in self.INVALID_STEP_MARKERS:
            with self.subTest(marker=marker):
                args = arguments(nodes=("pod-a=node-a",))
                with self.assertRaises(rollout.Refusal):
                    rollout.plan_process(args, lambda argv: pod(argv[2]),
                                         lambda name, view=marker: without_steps(name, "adxlet", view))
        self.assertEqual(plan(arguments(nodes=("pod-a=node-a",)))["role"], "adxlet")

    def test_old_adxctl_marker_refused_before_submit(self):
        # An old adxctl's Submit runs the whole upgrade on its own, so a marker
        # lost between planning and apply is refused before any Submit or maintenance.
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for marker in self.INVALID_STEP_MARKERS:
            with self.subTest(marker=marker):
                with tempfile.TemporaryDirectory() as directory:
                    cli, post = MagicMock(), MagicMock()
                    result = self.run_batch(snapshot, Path(directory) / "journal", cli, post=post,
                                            local=lambda name, view=marker: without_steps(name, "adxlet", view))
                    self.assertEqual(result["status"], "pending")
                    cli.assert_not_called()
                    post.assert_not_called()

    def test_relay_does_not_require_step_protocol(self):
        snapshot = rollout.plan_process(arguments("relay", nodes=("pod-a=node-a",)), lambda argv: pod(argv[2]),
                                        lambda name: without_steps(name, "relay"))
        submission = snapshot["nodes"][0]["submission"]
        with tempfile.TemporaryDirectory() as directory:
            cli = MagicMock(side_effect=rollout.Refusal("lost ACK"))
            self.assertEqual(self.run_batch(snapshot, Path(directory) / "journal", cli,
                                            local=lambda name: without_steps(name, "relay"))["status"], "pending")
            self.assertEqual(cli.call_args.args[1], rollout.submit_arguments(submission))

    def test_require_process_steps_accepts_only_int_two(self):
        rollout.require_process_steps(info("pod-a", "adxlet"))
        for marker in self.INVALID_STEP_MARKERS:
            with self.subTest(marker=marker), self.assertRaises(rollout.Refusal):
                rollout.require_process_steps(without_steps("pod-a", "adxlet", marker))
        with self.assertRaises(rollout.Refusal):
            rollout.require_process_steps({"ok": True, "node_info": []})

    def test_remote_query_unknown_replays_exact_descriptor_and_rejects_changed_reply(self):
        args = arguments("relay", nodes=("pod-a=node-a",))
        args.archive_url, args.archive_sha256 = "https://packages.example/archive.tar.gz", "e" * 64
        snapshot = plan(args)
        submission = snapshot["nodes"][0]["submission"]
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal.json"
            self.run_batch(snapshot, journal, MagicMock(side_effect=rollout.Refusal("lost")))
            cli = MagicMock(side_effect=[rollout.Refusal("query unknown"), rollout.Refusal("submit unknown")])
            self.assertEqual(self.run_batch(snapshot, journal, cli)["status"], "pending")
            self.assertEqual(cli.call_args_list[1].args[1], rollout.submit_arguments(submission))
            changed = copy.deepcopy(submission)
            changed["remote_archive"]["url"] += "?changed"
            cli = MagicMock(return_value={"ok": True, "task": {
                "submission": changed, "phase": "completed", "execution": None}})
            self.assertEqual(self.run_batch(snapshot, journal, cli)["status"], "pending")

    def run_batch(self, snapshot, journal, cli, post=None, kube=None, local=None, batch=0):
        return rollout.apply_process(snapshot, journal, batch,
                                     kube or (lambda argv: pod(argv[2])),
                                     local or (lambda name: info(name, snapshot["role"])),
                                     cli, post or MagicMock(), sleep=lambda seconds: None)

    def run_adxlet(self, fixture):
        return self.run_batch(fixture.snapshot, fixture.journal, fixture.cli, post=fixture.post,
                              local=fixture.local)

    def test_adxlet_http_only_upgrade_registers_on_start_and_recovers_release_ack(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.add("release")
            result = rollout.apply_process(snapshot, fixture.journal, 0, lambda argv: pod(argv[2]),
                                           fixture.local, fixture.cli, fixture.post, sleep=lambda _: None)
            self.assertEqual(result["status"], "completed")
            effects = [event for event in fixture.events
                       if event in ("submit.effect", "maintenance", "exit", "start", "register", "release")]
            self.assertEqual(effects, ["submit.effect", "maintenance", "exit", "start", "register", "release"])
            for status in ("absent", "active", "released"):
                self.assertIn("http.query." + status, fixture.events)
            self.assertLess(fixture.events.index("http.query.absent"), fixture.events.index("submit.effect"))
            self.assertEqual([call.args[1]["action"] for call in fixture.post.call_args_list
                              if call.args[0] == "/v1/adxlet-process-upgrades"], ["prepare", "release"])
            state = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]
            self.assertEqual((state["attempt"], state["node_session"], state["release_session"]),
                             ("attempt", "candidate", "candidate"))

    def test_adxlet_http_preflight_rejects_404_and_invalid_owner_or_session_before_submit(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        owner = rollout.maintenance_owner(snapshot, snapshot["nodes"][0])
        invalid = (rollout.Refusal("HTTP 404"), rollout.Refusal("HTTP 409"),
                   rollout.Refusal("HTTP 503"), OSError("request timed out"),
                   {}, dict(status="unknown", owner=owner), dict(status="absent", owner={}),
                   dict(status="active", owner=owner, origin_session="foreign",
                        current_session="node-session", yielded=False),
                   dict(status="active", owner=owner, origin_session="node-session",
                        current_session=None, yielded=False))
        for response in invalid:
            with self.subTest(response=response), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.post.side_effect = [response]
                result = rollout.apply_process(snapshot, fixture.journal, 0, lambda argv: pod(argv[2]),
                                               fixture.local, fixture.cli, fixture.post, sleep=lambda _: None)
                self.assertEqual(result["status"], "pending")
                fixture.post.assert_called_once_with("/v1/adxlet-process-upgrades/query", dict(owner=owner))
                fixture.cli.assert_not_called()

    def test_adxlet_prepares_files_then_maintenance_exit_start_register_release(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            effects = [event for event in fixture.events
                       if event in ("submit.effect", "maintenance", "exit", "start", "register", "release")]
            self.assertEqual(effects, ["submit.effect", "maintenance", "exit", "start", "register", "release"])
            before_exit = fixture.events[fixture.events.index("maintenance") + 1:fixture.events.index("exit")]
            self.assertEqual(before_exit.count("http.query.active"), 1)
            self.assertEqual(before_exit[-2:], ["http.query.active", "step"])
            self.assertLess(fixture.events.index("http.query.absent"), fixture.events.index("submit.effect"))
            self.assertNotIn("http.query.released", fixture.events)
            state = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]
            self.assertEqual((state["attempt"], state["node_session"], state["release_session"]),
                             ("attempt", "candidate", "candidate"))

    def test_adxlet_failed_preparation_does_not_maintain_or_stop_original(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.failed_preparation = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "failed")
            self.assertTrue(all(call.args[0].endswith("/query") for call in fixture.post.call_args_list))
            self.assertNotIn("step", fixture.events)
            self.assertEqual(fixture.view, info("pod-a", "adxlet"))

    def test_adxlet_http_preflight_failure_prevents_submit_and_maintenance(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.post.side_effect = rollout.Refusal("HTTP request denied")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.cli.assert_not_called()
            fixture.post.assert_called_once_with("/v1/adxlet-process-upgrades/query", dict(owner=fixture.owner))

    def test_adxlet_lost_step_acks_resume_without_repeating_effect_or_resetting_deadline(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for effect, stage in (("maintenance", "prepare_intent"), ("exit", "exit_intent"), ("start", "start_intent")):
            with self.subTest(effect=effect), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.lost_acks.add(effect)
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                state = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]
                self.assertEqual(state["stage"], stage)
                first = state["first_submit_unix_ms"]
                resumed_at = len(fixture.events)
                self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
                self.assertEqual(fixture.events.count(effect), 1)
                if effect == "maintenance":
                    before_exit = fixture.events[resumed_at:fixture.events.index("exit")]
                    self.assertEqual(before_exit.count("http.query.active"), 2)
                    self.assertEqual(before_exit[-2:], ["http.query.active", "step"])
                self.assertEqual(fixture.task["adxlet_execution"]["deadline_unix_ms"], 4102444800000)
                self.assertEqual(
                    rollout.read_json_file(fixture.journal)["nodes"]["node-a"]["first_submit_unix_ms"], first)

    def test_adxlet_unknown_submit_query_replays_only_exact_original_submission(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.add("submit")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            first = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]["first_submit_unix_ms"]
            fixture.query_unknown = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            self.assertEqual(fixture.events.count("submit.effect"), 2)
            self.assertEqual(rollout.read_json_file(fixture.journal)["nodes"]["node-a"]["first_submit_unix_ms"], first)

    def test_adxlet_release_lost_ack_recovers_receipt_after_later_restart(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.update(("release", "query_release"))
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertLess(fixture.events.index("release"), fixture.events.index("http.query.released"))
            fixture.view["node_info"]["components"][0]["runtime"]["pid"] = 99
            fixture.cli.reset_mock()
            fixture.post.reset_mock()
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            fixture.cli.assert_called_once_with(fixture.node, ["query", "--task-id", fixture.owner["local_task_id"]])
            fixture.post.assert_called_once_with("/v1/adxlet-process-upgrades/query", dict(owner=fixture.owner))

    def test_adxlet_release_receipt_does_not_override_inconsistent_local_task(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.update(("release", "query_release"))
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertIsNotNone(fixture.receipt)
            fixture.task["adxlet_execution"]["attempt"] = "different-attempt"
            fixture.events.clear()
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertEqual(fixture.events, ["http.query.released", "runtime", "query"])

    def test_adxlet_absent_release_receipt_rechecks_proof_and_replays_release(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")

            def unavailable_release(path, body):
                if body.get("action") == "release":
                    fixture.effect("release.unavailable", "release_intent")
                    raise rollout.Refusal("release not received")
                return fixture.http(path, body)

            fixture.post.side_effect = unavailable_release
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertIsNone(fixture.receipt)
            fixture.post.side_effect = fixture.http
            fixture.events.clear()
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            self.assertLess(fixture.events.index("http.query.active"), fixture.events.index("query"))
            self.assertLess(fixture.events.index("query"), fixture.events.index("release"))

    def test_adxlet_unknown_submit_query_cannot_replay_under_changed_runtime(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.add("submit")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.query_unknown = True
            fixture.view["node_info"]["components"][0]["runtime"]["pid"] = 99
            fixture.cli.reset_mock()
            fixture.post.reset_mock()
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertEqual(fixture.cli.call_count, 1)
            self.assertEqual(fixture.cli.call_args.args[1][0], "query")
            fixture.post.assert_not_called()

    def test_rollback_selected_b_is_proven_without_external_start(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            command = fixture.cli.side_effect

            def rollback_completes_original(node, argv):
                result = command(node, argv)
                if argv[0] == "step" and argv[4] == "rollback":
                    self.assertEqual(fixture.task["adxlet_execution"]["stage"], "selected")
                return result

            fixture.cli.side_effect = rollback_completes_original
            self.assertEqual(self.run_adxlet(fixture)["status"], "failed")
            state = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]
            self.assertEqual((state["attempt"], state["node_session"], state["release_session"]),
                             ("attempt", "candidate", "original-session"))
            self.assertNotIn("rollback_from", state)
            self.assertEqual(fixture.stopped_attempts, ["attempt"])
            self.assertEqual(fixture.started_attempts, ["original-attempt"])
            self.assertEqual(fixture.events.count("original_start"), 1)
            self.assertFalse(any(call.args[1][0] == "step" and call.args[1][4] == "start"
                                 and call.args[1][6] == "original-attempt"
                                 for call in fixture.cli.call_args_list))
            self.assertEqual(fixture.receipt["node_session"], "original-session")

    def test_rollback_v2_adxlet_schema_one_journal_refused_before_remote_io(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal"
            rollout.write_journal(journal, dict(schema_version=1, plan_hash=rollout.template_hash(snapshot),
                                                nodes={}))
            before = journal.read_bytes()
            cli, post, kube, local = MagicMock(), MagicMock(), MagicMock(), MagicMock()
            with self.assertRaisesRegex(rollout.Refusal, "journal schema"):
                self.run_batch(snapshot, journal, cli, post=post, kube=kube, local=local)
            for effect in (cli, post, kube, local):
                effect.assert_not_called()
            self.assertEqual(journal.read_bytes(), before)

    def test_rollback_v2_relay_schema_one_journal_still_supported(self):
        snapshot = plan(arguments("relay", nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal"
            rollout.write_journal(journal, dict(schema_version=1, plan_hash=rollout.template_hash(snapshot),
                                                nodes={}))
            cli = MagicMock(side_effect=rollout.Refusal("lost ACK"))
            post = MagicMock()
            self.assertEqual(self.run_batch(snapshot, journal, cli, post=post)["status"], "pending")
            cli.assert_called_once()
            self.assertEqual(cli.call_args.args[1], rollout.submit_arguments(snapshot["nodes"][0]["submission"]))
            post.assert_not_called()
            self.assertEqual(rollout.read_json_file(journal)["schema_version"], 1)

    def test_adxlet_target_failure_restores_original_as_new_b_and_releases_failed(self):
        snapshot = plan(arguments())
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertEqual(self.run_adxlet(fixture)["status"], "failed")
            execution = fixture.task["adxlet_execution"]
            self.assertEqual(execution["leg"],
                             {"kind": "rollback", "from": dict(attempt="attempt", node_session="candidate")})
            self.assertEqual((execution["attempt"], execution["node_session"], execution["stage"]),
                             ("original-attempt", "original-session", "selected"))
            runtime = fixture.view["node_info"]["components"][0]["runtime"]
            self.assertEqual((runtime["pid"], runtime["executable_sha256"], runtime["configuration_sha256"],
                              runtime["relay_session"]), (44, "a" * 64, "e" * 64, "relay-session"))
            self.assertEqual(fixture.stopped_attempts, ["attempt"])
            self.assertEqual(fixture.receipt["node_session"], "original-session")
            fixture.cli.reset_mock()
            with self.assertRaises(rollout.Refusal):
                self.run_batch(snapshot, fixture.journal, fixture.cli, post=fixture.post,
                               local=fixture.local, batch=1)
            fixture.cli.assert_not_called()

    def test_adxlet_rollback_lost_acks_keep_a_b_and_original_deadline(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for action, stage in (("rollback", "rollback_intent"),
                              ("original_start", "rollback_intent")):
            with self.subTest(action=action), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.target_fails = True
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                fixture.lost_acks.add(action)
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                state = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]
                self.assertEqual(state["stage"], stage)
                deadline = fixture.task["adxlet_execution"]["deadline_unix_ms"]
                self.assertEqual(self.run_adxlet(fixture)["status"], "failed")
                self.assertEqual(fixture.task["adxlet_execution"]["deadline_unix_ms"], deadline)
                self.assertEqual(fixture.stopped_attempts, ["attempt"])
                self.assertEqual(fixture.started_attempts.count("original-attempt"), 1)
                self.assertEqual(fixture.events.count(action), 1)

    def test_adxlet_nonzero_start_with_queried_starting_error_can_recover_original(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = True
            command = fixture.cli.side_effect

            def local_start_error(node, argv):
                result = command(node, argv)
                if (argv[0] == "step" and argv[4] == "start"
                        and fixture.task["adxlet_execution"]["leg"] == dict(kind="target")):
                    raise rollout.Refusal("local Start returned nonzero")
                return result

            fixture.cli.side_effect = local_start_error
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertEqual(self.run_adxlet(fixture)["status"], "failed")
            self.assertEqual(fixture.stopped_attempts, ["attempt"])
            self.assertEqual(fixture.task["adxlet_execution"]["stage"], "selected")

    def test_adxlet_local_unknown_stages_resume_same_step_identity(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        cases = (("exit", "exiting", None), ("start", "starting", None),
                 ("start", "starting", "spawn outcome unknown"), ("start", "committing", "commit reply unknown"),
                 ("rollback", "stopping_for_rollback", "candidate reap unknown"),
                 ("original_start", "starting", None), ("original_start", "starting", "readiness unknown"),
                 ("original_start", "committing", "commit reply unknown"))
        for action, local_stage, error in cases:
            with (self.subTest(action=action, stage=local_stage, error=error),
                  tempfile.TemporaryDirectory() as directory):
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.target_fails = action in ("rollback", "original_start")
                fixture.unknown_steps[action] = dict(stage=local_stage, error=error)
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                if local_stage == "exiting":
                    # Before reap, no successor identity exists. The fixture checks
                    # that every Exit replay still sends the original empty input.
                    self.assertEqual(fixture.task["adxlet_execution"]["attempt"], "")
                    self.assertEqual(fixture.task["adxlet_execution"]["node_session"], "node-session")
                if fixture.target_fails:
                    self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                deadline = fixture.task["adxlet_execution"]["deadline_unix_ms"]
                self.assertEqual(self.run_adxlet(fixture)["status"], "failed" if fixture.target_fails else "completed")
                self.assertEqual(fixture.task["adxlet_execution"]["deadline_unix_ms"], deadline)
                self.assertEqual(fixture.events.count(action), 2)
                self.assertEqual(fixture.events.count("maintenance"), 1)
                self.assertEqual(fixture.stopped_attempts, ["attempt"] if fixture.target_fails else [])

    def test_adxlet_starting_without_error_cannot_turn_cli_failure_into_rollback_proof(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.unknown_steps["start"] = dict(stage="starting", error=None)
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            command = fixture.cli.side_effect

            def unavailable_start(node, argv):
                if argv[0] == "step" and argv[4] == "start":
                    raise rollout.Refusal("Start transport unavailable")
                return command(node, argv)

            fixture.cli.side_effect = unavailable_start
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertNotIn("rollback", fixture.events)
            self.assertIsNotNone(fixture.marker)
            self.assertIsNone(fixture.receipt)

    def test_adxlet_invalid_execution_cannot_authorize_steps_or_release(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        cases = [(False, invalid) for invalid in (
            dict(stage="unrecognized"), dict(leg={"kind": "rollback"}), dict(leg=None),
            dict(stage="committing", candidate_pid=None), dict(candidate_pid=None),
            dict(candidate_pid=True), dict(stage="exited"))]
        cases += [(True, dict(stage="unrecognized")), (True, dict(stage="committing", candidate_pid=None))]
        for rollback, invalid in cases:
            with self.subTest(rollback=rollback, invalid=invalid), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.target_fails = fixture.original_fails = rollback
                if not rollback:
                    fixture.lost_acks.add("start")
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                if rollback:
                    self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                fixture.task["adxlet_execution"].update(invalid)
                if "stage" in invalid:
                    fixture.task["phase"] = "unknown"
                fixture.cli.reset_mock()
                fixture.post.reset_mock()
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                self.assertTrue(all(call.args[1][0] == "query" for call in fixture.cli.call_args_list))
                self.assertTrue(all(call.args[1].get("action") not in ("prepare", "release")
                                    for call in fixture.post.call_args_list))
                self.assertIsNotNone(fixture.marker)
                self.assertIsNone(fixture.receipt)

    def test_adxlet_expired_target_rollback_starts_original_from_origin_or_a(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for current in ("node-session", "candidate"):
            with self.subTest(current=current), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.unknown_steps["start"] = dict(stage="starting", error="spawn outcome unknown")
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                fixture.task["adxlet_execution"]["deadline_unix_ms"] = 1
                fixture.marker["current_session"] = current
                fixture.events.clear()
                self.assertEqual(self.run_adxlet(fixture)["status"], "failed")
                self.assertNotIn("start", fixture.events)
                self.assertIn("original_start", fixture.events)
                self.assertEqual(fixture.stopped_attempts, ["attempt"])
                self.assertEqual(fixture.marker, None)
                self.assertEqual(fixture.receipt["node_session"], "original-session")

    def test_adxlet_target_committing_only_replays_receipt_even_after_expiry(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.unknown_steps["start"] = dict(stage="committing", error="commit reply unknown")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.task["adxlet_execution"]["deadline_unix_ms"] = 1
            command = fixture.cli.side_effect

            def pending_receipt(node, argv):
                if argv[0] == "step":
                    self.assertEqual(argv[4:], ["start", "--attempt", "attempt", "--node-session", "candidate"])
                    fixture.effect("target.receipt", "start_intent")
                    return dict(ok=True, task=copy.deepcopy(fixture.task))
                return command(node, argv)

            fixture.cli.side_effect = pending_receipt
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertNotIn("rollback", fixture.events)
            self.assertEqual(fixture.started_attempts, ["attempt"])
            fixture.task["phase"] = "completed"
            fixture.task["adxlet_execution"].update(stage="selected", error=None)
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            self.assertNotIn("rollback", fixture.events)

    def test_adxlet_rollback_committed_receipt_finishes_a_without_minting_b(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            command = fixture.cli.side_effect

            def committed_receipt(node, argv):
                if argv[0] == "step" and argv[4] == "rollback":
                    fixture.effect("rollback.receipt", "rollback_intent")
                    fixture.task["phase"] = "completed"
                    fixture.task["adxlet_execution"].update(stage="selected", candidate_pid=43, error=None,
                                                           deadline_unix_ms=4102444500000)
                    runtime = info("pod-a", "adxlet")["node_info"]["components"][0]["runtime"]
                    runtime.update(pid=43, node_session="candidate", executable_sha256="d" * 64)
                    fixture.view["node_info"]["components"][0]["runtime"] = runtime
                    fixture.marker["current_session"] = "candidate"  # A registered before its commit ACK was lost.
                    return dict(ok=True, task=copy.deepcopy(fixture.task))
                return command(node, argv)

            fixture.cli.side_effect = committed_receipt
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            self.assertEqual(fixture.stopped_attempts, [])
            self.assertNotIn("original_start", fixture.events)
            self.assertEqual(fixture.task["adxlet_execution"]["leg"], dict(kind="target"))

    def test_adxlet_original_failure_and_expiry_keep_maintenance_without_recursive_rollback(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for expired in (False, True):
            with self.subTest(expired=expired), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.target_fails = fixture.original_fails = True
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                if expired:
                    fixture.task["adxlet_execution"]["deadline_unix_ms"] = 1
                deadline = fixture.task["adxlet_execution"]["deadline_unix_ms"]
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                self.assertEqual(fixture.task["adxlet_execution"]["deadline_unix_ms"], deadline)
                self.assertEqual(fixture.stopped_attempts, ["attempt"])
                self.assertEqual(fixture.events.count("rollback"), 2)
                self.assertTrue(all(call.args[1][6:10:2] == ["attempt", "candidate"]
                                    for call in fixture.cli.call_args_list
                                    if call.args[1][0] == "step" and call.args[1][4] == "rollback"))
                self.assertIsNone(fixture.receipt)
                self.assertIsNotNone(fixture.marker)

    def test_adxlet_saved_b_resumes_rollback_a_while_coordinator_unavailable(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = fixture.original_fails = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            saved = copy.deepcopy(fixture.task["adxlet_execution"])
            fixture.original_fails = False
            fixture.post.side_effect = rollout.Refusal("Coordinator unavailable")
            fixture.cli.reset_mock()
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            steps = [call.args[1] for call in fixture.cli.call_args_list if call.args[1][0] == "step"]
            self.assertEqual(steps, [["step", "--task-id", fixture.node["submission"]["task_id"],
                                     "--step", "rollback", "--attempt", "attempt", "--node-session", "candidate"]])
            execution = fixture.task["adxlet_execution"]
            self.assertEqual(execution["stage"], "selected")
            for field in ("attempt", "node_session", "leg", "deadline_unix_ms"):
                self.assertEqual(execution[field], saved[field])
            self.assertIsNotNone(fixture.marker)
            self.assertIsNone(fixture.receipt)

    def test_adxlet_pre_exit_cancel_releases_only_exact_retained_unyielded_original(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for change in (None, "pid", "yielded", "execution"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.lost_acks.add("maintenance")
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                fixture.task["phase"] = "failed"
                if change == "pid":
                    fixture.view["node_info"]["components"][0]["runtime"]["pid"] = 99
                elif change == "yielded":
                    fixture.marker["yielded"] = True
                elif change == "execution":
                    fixture.task["adxlet_execution"] = dict(stage="exiting", leg=dict(kind="target"))
                self.assertEqual(self.run_adxlet(fixture)["status"], "failed" if change is None else "pending")
                self.assertNotIn("exit", fixture.events)
                self.assertEqual(fixture.receipt is not None, change is None)

    def test_adxlet_rollback_b_must_name_the_frozen_a(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.lost_acks.add("rollback")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.task["adxlet_execution"]["leg"]["from"]["node_session"] = "foreign"
            fixture.events.clear()
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertNotIn("original_start", fixture.events)

    def test_adxlet_original_release_replay_requires_b_source_proof_without_receipt(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")

            def unavailable_release(path, body):
                if body.get("action") == "release":
                    raise rollout.Refusal("release not received")
                return fixture.http(path, body)

            fixture.post.side_effect = unavailable_release
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertEqual(rollout.read_json_file(fixture.journal)["nodes"]["node-a"]["stage"], "release_intent")
            fixture.task["adxlet_execution"]["leg"]["from"]["node_session"] = "foreign"
            fixture.post.reset_mock()
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.post.assert_called_once_with("/v1/adxlet-process-upgrades/query", dict(owner=fixture.owner))

    def test_adxlet_original_replay_keeps_maintenance_with_unrelated_coordinator_session(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.target_fails = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.lost_acks.add("rollback")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.task["phase"] = "unknown"
            fixture.task["adxlet_execution"].update(stage="starting", error="readiness unknown")
            fixture.marker["current_session"] = "foreign-session"
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            state = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]
            self.assertEqual((state["attempt"], state["node_session"]), ("attempt", "candidate"))
            self.assertEqual(state["stage"], "rollback_intent")
            self.assertNotIn("rollback_from", state)
            self.assertEqual(fixture.events.count("rollback"), 2)
            self.assertEqual(fixture.events.count("original_start"), 2)
            self.assertIsNone(fixture.receipt)

    def test_adxlet_second_publisher_cannot_enter_same_journal_lock(self):
        import fcntl

        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            with open(str(fixture.journal) + ".lock", "a+b") as held:
                fcntl.flock(held.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                with self.assertRaises(rollout.Refusal):
                    self.run_adxlet(fixture)
            fixture.cli.assert_not_called()
            fixture.post.assert_not_called()

    def test_adxlet_prepared_terminal_task_cannot_acquire_maintenance(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for phase in ("failed", "completed", "unknown"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.lost_acks.add("submit")
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                journal = rollout.read_json_file(fixture.journal)
                journal["nodes"]["node-a"]["stage"] = "prepared"
                rollout.write_journal(fixture.journal, journal)
                fixture.task["phase"] = phase
                fixture.post.reset_mock()
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                fixture.post.assert_not_called()
                self.assertNotIn("step", fixture.events)

    def test_adxlet_prepared_files_waiting_for_maintenance_do_not_use_submit_as_execution_clock(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.add("submit")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            journal = rollout.read_json_file(fixture.journal)
            journal["nodes"]["node-a"].update(stage="prepare_intent", first_submit_unix_ms=1)
            rollout.write_journal(fixture.journal, journal)
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            self.assertEqual(fixture.events.count("submit.effect"), 1)
            self.assertEqual(rollout.read_json_file(fixture.journal)["nodes"]["node-a"]["first_submit_unix_ms"], 1)

    def test_adxlet_exited_task_also_requires_coordinator_yield(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            command = fixture.cli.side_effect

            def hold_yield(node, argv):
                result = command(node, argv)
                if argv[0] == "step" and argv[4] == "exit":
                    fixture.marker["yielded"] = False
                return result

            fixture.cli.side_effect = hold_yield
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertNotIn("start", fixture.events)
            fixture.marker["yielded"] = True
            self.assertEqual(self.run_adxlet(fixture)["status"], "completed")
            self.assertEqual(fixture.events.count("exit"), 1)


    def test_adxlet_start_refuses_unyielded_or_unrelated_current_session(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for change in ("yielded", "current_session"):
            with self.subTest(change=change), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                fixture.lost_acks.add("exit")
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                fixture.marker[change] = False if change == "yielded" else "foreign-session"
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                self.assertNotIn("start", fixture.events)
                self.assertIsNone(fixture.receipt)

    def test_adxlet_resume_rejects_missing_frozen_attempt_before_side_effects(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.add("start")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            journal = rollout.read_json_file(fixture.journal)
            del journal["nodes"]["node-a"]["attempt"]
            rollout.write_journal(fixture.journal, journal)
            fixture.cli.reset_mock()
            fixture.post.reset_mock()
            with self.assertRaises(rollout.Refusal):
                self.run_adxlet(fixture)
            fixture.cli.assert_not_called()
            fixture.post.assert_not_called()

    def test_adxlet_cli_needs_no_handoff_or_grpc_dependencies(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with patch.dict("sys.modules", {"grpc": None, "node_rollout_rpc": None}), \
                patch.object(rollout.sys, "platform", "linux"), \
                patch.object(rollout, "read_json_file", return_value=snapshot), \
                patch.object(rollout, "apply_process", return_value=dict(status="completed")) as apply, \
                patch("sys.stdout", new_callable=io.StringIO) as output, \
                patch("sys.stderr", new_callable=io.StringIO) as errors:
            result = rollout.main(["apply-process", "--plan", "plan", "--journal", "journal", "--batch-index", "0"])
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(output.getvalue())["status"], "completed")
        self.assertEqual(output.getvalue(), rollout.canonical(dict(status="completed")) + "\n")
        self.assertEqual(errors.getvalue(), "")
        self.assertNotIn("handoff", apply.call_args.kwargs)
        selected_parser = rollout.parser()
        with patch.object(rollout.argparse.ArgumentParser, "error", side_effect=ValueError) as error, \
                self.assertRaises(ValueError):
            selected_parser.parse_args(["apply-process", "--plan", "plan", "--journal", "journal",
                                        "--batch-index", "0", "--handoff-config", "/removed"])

        error.assert_called_once()

    def test_invalid_cli_arguments_exit_before_opening_plan(self):
        for command in ("apply-process", "apply-pod"):
            base = [command, "--plan", "/missing-plan", "--journal", "/missing-journal"]
            choices = ([], ["--prune"], ["--promote-ready", "--batch-index", "0"],
                       ["--batch-index", "0", "--handoff-config", "/removed"])
            for flags in choices:
                with self.subTest(command=command, flags=flags):
                    result = subprocess.run([sys.executable, str(SPEC.origin), *base, *flags],
                                            capture_output=True, text=True, check=False, timeout=10)
                    self.assertEqual(result.returncode, 2)
                    self.assertIn("error:", result.stderr)
                    self.assertNotIn("PlanRefused", result.stderr)
                    self.assertEqual(result.stdout, "")

    def test_cli_output_ignores_global_logging_disable(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        previous = logging.root.manager.disable
        try:
            logging.disable(logging.CRITICAL)
            for refused in (False, True):
                with self.subTest(refused=refused), \
                        patch.object(rollout.sys, "platform", "linux"), \
                        patch.object(rollout, "read_json_file", return_value=snapshot,
                                     side_effect=rollout.Refusal("plan unavailable") if refused else None), \
                        patch.object(rollout, "apply_process", return_value=dict(status="completed")), \
                        patch("sys.stdout", new_callable=io.StringIO) as output, \
                        patch("sys.stderr", new_callable=io.StringIO) as errors:
                    result = rollout.main(["apply-process", "--plan", "plan", "--journal", "journal",
                                           "--batch-index", "0"])
                    self.assertEqual(result, 1 if refused else 0)
                    success = rollout.canonical(dict(status="completed")) + "\n"
                    failure = json.dumps({"error": {"code": "PlanRefused", "message": "plan unavailable"}}) + "\n"
                    self.assertEqual(output.getvalue(), "" if refused else success)
                    self.assertEqual(errors.getvalue(), failure if refused else "")
        finally:
            logging.disable(previous)

    def test_cli_output_write_error_returns_refusal_json(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        output = MagicMock(spec=("write", "flush"))
        output.write.side_effect = OSError("output unavailable")
        with patch.object(rollout.sys, "platform", "linux"), \
                patch.object(rollout, "read_json_file", return_value=snapshot), \
                patch.object(rollout, "apply_process", return_value=dict(status="completed")), \
                patch("sys.stdout", output), \
                patch("sys.stderr", new_callable=io.StringIO) as errors:
            result = rollout.main(["apply-process", "--plan", "plan", "--journal", "journal", "--batch-index", "0"])
        self.assertEqual(result, 1)
        output.write.assert_called()
        self.assertEqual(errors.getvalue(), json.dumps({
            "error": {"code": "PlanRefused", "message": "output unavailable"}}) + "\n")

    def test_apply_cli_rejects_invalid_plan_before_io(self):
        for command in ("apply-process", "apply-pod", "stop-pod", "abort-pod"):
            with self.subTest(command=command), \
                    patch.object(rollout.sys, "platform", "linux"), \
                    patch.object(rollout, "read_json_file", return_value={"schema_version": 2}), \
                    patch.object(rollout.os, "open") as journal_open, \
                    patch.object(rollout, "kubectl_get") as kube_get, \
                    patch.object(rollout, "kubectl_json") as kube_write, \
                    patch.object(rollout, "http_json") as http, \
                    patch("sys.stdout", new_callable=io.StringIO) as output, \
                    patch("sys.stderr", new_callable=io.StringIO) as errors:
                argv = [command, "--plan", "plan", "--journal", "journal"]
                if command in ("apply-process", "apply-pod"):
                    argv.extend(["--batch-index", "0"])
                self.assertEqual(rollout.main(argv), 1)
                self.assertEqual(json.loads(errors.getvalue())["error"]["code"], "PlanRefused")
                self.assertEqual(output.getvalue(), "")
                self.assertEqual(errors.getvalue().count("\n"), 1)
                journal_open.assert_not_called()
                kube_get.assert_not_called()
                kube_write.assert_not_called()
                http.assert_not_called()

    def test_submit_lost_ack_resumes_same_task_then_unknown_is_pending(self):
        snapshot = plan(arguments("relay", nodes=("pod-a=node-a",)))
        submission = snapshot["nodes"][0]["submission"]
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal.json"
            cli = MagicMock(side_effect=rollout.Refusal("lost ACK"))
            self.assertEqual(self.run_batch(snapshot, journal, cli)["status"], "pending")
            first = rollout.read_json_file(journal)["nodes"]["node-a"]["first_submit_unix_ms"]
            response = {"ok": True, "task": {"submission": submission, "phase": "unknown", "execution": None}}
            cli = MagicMock(return_value=response)
            self.assertEqual(self.run_batch(snapshot, journal, cli)["status"], "pending")
            self.assertEqual(cli.call_args.args[1], ["query", "--task-id", submission["task_id"]])
            self.assertEqual(rollout.read_json_file(journal)["nodes"]["node-a"]["first_submit_unix_ms"], first)

    def test_query_error_replays_exact_submission_only_under_original_runtime(self):
        snapshot = plan(arguments("relay", nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal.json"
            self.run_batch(snapshot, journal, MagicMock(side_effect=rollout.Refusal("lost")))
            first = rollout.read_json_file(journal)["nodes"]["node-a"]["first_submit_unix_ms"]
            cli = MagicMock(side_effect=[rollout.Refusal("kubectl failed or timed out"),
                                        rollout.Refusal("lost again")])
            self.assertEqual(self.run_batch(snapshot, journal, cli)["status"], "pending")
            command = cli.call_args.args[1]
            self.assertEqual(command[:3], ["submit", "--task-id", snapshot["nodes"][0]["submission"]["task_id"]])
            self.assertIn(rollout.canonical(snapshot["nodes"][0]["submission"]["selection"]["expected"]), command)
            self.assertEqual(command, rollout.submit_arguments(snapshot["nodes"][0]["submission"]))
            self.assertEqual(rollout.read_json_file(journal)["nodes"]["node-a"]["first_submit_unix_ms"], first)
            for runtime in (None, {**info("pod-a", "relay")["node_info"]["components"][0]["runtime"], "pid": 99}):
                fresh = info("pod-a", "relay")
                fresh["node_info"]["components"][0]["runtime"] = runtime
                cli = MagicMock(side_effect=rollout.Refusal("query failed"))
                self.assertEqual(self.run_batch(snapshot, journal, cli,
                                                local=lambda name, view=fresh: view)["status"], "pending")
                self.assertEqual(cli.call_count, 1)
                self.assertEqual(cli.call_args.args[1][0], "query")

    def test_identity_change_prevents_replay_or_query(self):
        snapshot = plan(arguments("relay", nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal.json"
            self.run_batch(snapshot, journal, MagicMock(side_effect=rollout.Refusal("lost")))
            changed = pod("pod-a")
            changed["metadata"]["uid"] = "replacement"
            cli = MagicMock()
            self.assertEqual(self.run_batch(snapshot, journal, cli, kube=lambda argv: changed)["status"], "pending")
            cli.assert_not_called()

    def test_release_lost_ack_and_original_recovery_remain_failed(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal.json"
            source = dict(attempt="target-attempt", node_session="target-session")
            state = dict(stage="release_intent", outcome="failed", release_session="recovered",
                         **source,
                         first_submit_unix_ms=1)
            rollout.write_journal(journal, dict(schema_version=2, plan_hash=rollout.template_hash(snapshot),
                                                nodes={"node-a": state}))
            node = snapshot["nodes"][0]
            owner = rollout.maintenance_owner(snapshot, node)
            execution = dict(stage="selected", candidate_pid=44, attempt="original-attempt",
                             node_session="recovered", deadline_unix_ms=4102444500000,
                             leg={"kind": "rollback", "from": source})
            task = dict(submission=node["submission"], phase="failed", adxlet_execution=execution)
            cli = MagicMock(return_value=dict(ok=True, task=task))
            post = MagicMock(return_value=dict(status="released", owner=owner, node_session="recovered"))
            result = self.run_batch(snapshot, journal, cli, post=post)
            self.assertEqual(result["status"], "failed")
            cli.assert_called_once_with(node, ["query", "--task-id", owner["local_task_id"]])
            post.assert_called_once_with("/v1/adxlet-process-upgrades/query", dict(owner=owner))

    def test_relay_completed_never_calls_coordinator(self):
        snapshot = plan(arguments("relay", nodes=("pod-a=node-a",)))
        submission = snapshot["nodes"][0]["submission"]
        response = {"ok": True, "task": {"submission": submission, "phase": "completed",
                     "prepared": {"binary_sha256": "d" * 64, "configuration_sha256": "e" * 64},
                    "execution": {"phase": "completed", "candidate_session": "new-relay"}}}
        count = []

        def local(name):
            value = info(name, "relay")
            if len(count) >= 2:
                value["node_info"]["components"][0]["runtime"].update(
                    executable_sha256="d" * 64, relay_session="new-relay")
            count.append(name)
            return value

        with tempfile.TemporaryDirectory() as directory:
            post = MagicMock()
            result = self.run_batch(snapshot, Path(directory) / "journal", MagicMock(return_value=response),
                                    post=post, local=local)
            self.assertEqual(result["status"], "completed")
            post.assert_not_called()

    def test_relay_stage_is_not_the_execution_phase(self):
        snapshot = plan(arguments("relay", nodes=("pod-a=node-a",)))
        node = snapshot["nodes"][0]
        runtime = dict(node["submission"]["selection"]["expected"],
                       executable_sha256=snapshot["sha256"], relay_session="candidate")
        task = {"phase": "completed",
                "prepared": {"binary_sha256": snapshot["sha256"], "configuration_sha256": "e" * 64},
                "execution": {"stage": "completed", "candidate_session": "candidate"}}
        self.assertIsNone(rollout.finish_process(snapshot, node, task, runtime))

    def test_release_replay_requires_current_terminal_candidate(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        node = snapshot["nodes"][0]
        owner = rollout.maintenance_owner(snapshot, node)
        body = {"owner": owner, "node_session": "recovered"}
        task = {"submission": node["submission"], "phase": "failed", "execution": None,
                "adxlet_execution": {"stage": "selected", "candidate_pid": 43,
                                     "leg": {"kind": "rollback",
                                             "from": {"attempt": "target", "node_session": "target"}},
                                     "attempt": "attempt", "node_session": "recovered",
                                     "deadline_unix_ms": 4102444500000}}
        fresh = info("pod-a", "adxlet")
        fresh["node_info"]["components"][0]["runtime"].update(pid=43, node_session="recovered")
        for receipt in (None, rollout.Refusal("no receipt")):
            for change in ("none", "pid", "session", "relay"):
                with self.subTest(receipt=receipt, change=change):
                    state = dict(stage="release_intent", outcome="failed", release_session=body["node_session"],
                                 first_submit_unix_ms=1, attempt="target", node_session="target")
                    observed = copy.deepcopy(fresh)
                    runtime = observed["node_info"]["components"][0]["runtime"]
                    if change == "none":
                        observed["node_info"]["components"][0]["runtime"] = None
                    else:
                        key = {"pid": "pid", "session": "node_session", "relay": "relay_session"}[change]
                        runtime[key] = 99 if change == "pid" else "changed"
                    post = MagicMock(side_effect=[dict(status="absent", owner=owner) if receipt is None else receipt])
                    with self.assertRaises(rollout.Refusal):
                        rollout.advance_process(snapshot, node, state, MagicMock(), lambda argv: pod("pod-a"),
                                                lambda name, view=observed: view,
                                                MagicMock(return_value={"ok": True, "task": task}), post,
                                                lambda seconds: None)
                    post.assert_called_once_with("/v1/adxlet-process-upgrades/query", dict(owner=owner))
                    self.assertEqual(state["release_session"], body["node_session"])

    def test_adxlet_foreign_maintenance_marker_never_exits_original(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for field, value in (("owner", {}), ("origin_session", "foreign"), ("current_session", "foreign"),
                             ("yielded", True), ("released", True)):
            with self.subTest(field=field), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                original_post = fixture.post.side_effect

                def post(path, body, original_post=original_post, field=field, *, fixture, value):
                    receipt = original_post(path, body)
                    if body.get("action") == "prepare":
                        if field == "released":
                            fixture.receipt = dict(owner=fixture.owner, node_session="node-session")
                            fixture.marker = None
                        else:
                            fixture.marker[field] = value
                    return receipt
                post = partial(post, fixture=fixture, value=value)

                fixture.post.side_effect = post
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                self.assertNotIn("exit", fixture.events)
                self.assertNotIn("replace", fixture.events)

    def test_adxlet_prepare_ack_rejects_wrong_current_session_or_yield_state(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        for field, value in (("current_session", KeyError), ("yielded", KeyError),
                             ("current_session", "foreign"), ("yielded", True), ("yielded", 0), ("yielded", None)):
            with self.subTest(field=field, value=value), tempfile.TemporaryDirectory() as directory:
                fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
                original_post = fixture.post.side_effect

                def post(path, body, original_post=original_post, field=field, *, value):
                    receipt = original_post(path, body)
                    if body.get("action") == "prepare":
                        if value is KeyError:
                            del receipt[field]
                        else:
                            receipt[field] = value
                    return receipt
                post = partial(post, value=value)

                fixture.post.side_effect = post
                self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
                self.assertNotIn("exit", fixture.events)
                self.assertNotIn("start", fixture.events)
                self.assertNotIn("release", fixture.events)
                state = rollout.read_json_file(fixture.journal)["nodes"]["node-a"]
                self.assertEqual(state["stage"], "prepare_intent")

    def test_adxlet_uncertain_running_candidate_is_not_started_or_stopped_again(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        with tempfile.TemporaryDirectory() as directory:
            fixture = AdxletPublisherFixture(self, snapshot, Path(directory) / "journal")
            fixture.lost_acks.add("start")
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            fixture.task["phase"] = "running"
            fixture.task["adxlet_execution"]["stage"] = "exited"
            fixture.task["adxlet_execution"].pop("candidate_pid")
            fixture.cli.reset_mock()
            self.assertEqual(self.run_adxlet(fixture)["status"], "pending")
            self.assertTrue(all(call.args[1][0] == "query" for call in fixture.cli.call_args_list))
            self.assertEqual(fixture.events.count("start"), 1)
            self.assertIsNone(fixture.receipt)

    def test_adxlet_terminal_requires_unchanged_independent_relay(self):
        snapshot = plan(arguments(nodes=("pod-a=node-a",)))
        node = snapshot["nodes"][0]
        rollback_leg = {"kind": "rollback", "from": dict(attempt="target", node_session="target")}
        for phase, leg, digest in (("completed", dict(kind="target"), snapshot["sha256"]),
                                   ("failed", rollback_leg, "a" * 64)):
            runtime = dict(node["submission"]["selection"]["expected"], pid=43,
                           node_session="candidate", relay_session="changed", executable_sha256=digest)
            task = {"phase": phase, "prepared": {"binary_sha256": snapshot["sha256"], "configuration_sha256": "e" * 64},
                    "adxlet_execution": {"stage": "selected", "leg": leg, "candidate_pid": 43,
                    "attempt": "attempt", "node_session": "candidate"}}
            self.assertIsNone(rollout.finish_process(snapshot, node, task, runtime))

    def test_failed_exit_zero_does_not_advance_and_plan_hash_cannot_change(self):
        snapshot = plan(arguments("relay"))
        submission = snapshot["nodes"][0]["submission"]
        response = {"ok": True, "task": {"submission": submission, "phase": "failed", "execution": None}}
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "journal"
            self.assertEqual(self.run_batch(snapshot, journal, MagicMock(return_value=response))["status"], "failed")
            with self.assertRaises(rollout.Refusal):
                self.run_batch(snapshot, journal, MagicMock(), batch=1)
            changed = copy.deepcopy(snapshot)
            changed["context"] = "other"
            with self.assertRaises(rollout.Refusal):
                self.run_batch(changed, journal, MagicMock())


if __name__ == "__main__":
    unittest.main()
