"""Source-only regression tests; all Kubernetes and HTTP I/O is injected."""
import copy
from functools import partial
import importlib.util
import io
from pathlib import Path
import unittest
import tempfile
from unittest.mock import MagicMock, patch

SPEC = importlib.util.spec_from_file_location(
    "node_rollout", Path(__file__).resolve().parents[1] / "node_rollout.py"
)
rollout = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(rollout)


def patched_resource(case, target, changes, check_write=None):
    staged = copy.deepcopy(target)
    for change in changes:
        parts = [part.replace("~1", "/").replace("~0", "~") for part in change["path"].strip("/").split("/")]
        parent = staged
        for part in parts[:-1]:
            parent = parent[int(part)] if isinstance(parent, list) else parent[part]
        key = int(parts[-1]) if isinstance(parent, list) else parts[-1]
        if change["op"] == "test":
            case.assertEqual(parent[key], change["value"])
        else:
            if check_write is not None:
                check_write(change, parent, key)
            if change["op"] == "remove":
                del parent[key]
            else:
                case.assertIn(change["op"], ("add", "replace"))
                parent[key] = change["value"]
    return staged


class PodFixture(unittest.TestCase):
    def setUp(self):
        self.ds = {
            "apiVersion": "apps/v1", "kind": "DaemonSet",
            "metadata": {"name": "workers", "namespace": "ns", "uid": "ds-uid",
                         "resourceVersion": "10"},
            "spec": {"updateStrategy": {"type": "OnDelete"}, "template": {
                "spec": {"containers": [{"name": "adxlet", "image": "old:v1"}]}}},
        }
        self.pods = {"apiVersion": "v1", "kind": "PodList", "metadata": {"resourceVersion": "20"}, "items": [{
            "apiVersion": "v1", "kind": "Pod",
            "metadata": {"name": "workers-a", "namespace": "ns", "uid": "pod-uid",
                         "resourceVersion": "20", "ownerReferences": [{
                             "apiVersion": "apps/v1", "kind": "DaemonSet",
                             "name": "workers", "uid": "ds-uid", "controller": True}]},
            "spec": {"nodeName": "kube-node", "containers": [
                {"name": "adxlet", "image": "old:v1"}]},
        }]}
        for obj in (self.ds["spec"]["template"], self.pods["items"][0]):
            obj["spec"]["containers"][0]["env"] = [{
                "name": "ADX_POD_UID", "valueFrom": {"fieldRef": {
                    "apiVersion": "v1", "fieldPath": "metadata.uid"}}}]
        self.pods["items"][0]["status"] = {"containerStatuses": [{
            "name": "adxlet", "containerID": "container-original", "restartCount": 0,
            "imageID": "opaque-runtime-content-id", "state": {"running": {"startedAt": "now"}}}]}
        self.cap = dict(version=2, node_protocol_version=3, **{key: True for key in rollout.CAPABILITIES})
        self.inspect = {"node_id": "adx-node", "node_session_id": "session-1",
                        "observed_pod_uid": "pod-uid",
                        "protocol_version": 3, "unknown": False,
                        "runtime_rootfs_sha256": "a" * 64,
                        "runtime_rootfs_path": "/rootfs.erofs", "strict_empty": False}
        self.args = rollout.parser().parse_args([
            "plan-pod", "--context", "test-cluster", "--origin", "http://coordinator:9090", "--namespace", "ns",
            "--daemonset", "workers", "--container", "adxlet", "--config", "/etc/adx/deployment.yaml", "--target-image",
            "registry:5000/worker:release-20261124-1", "--task-id", "release-1",
            "--epoch", "1", "--node", "workers-a=adx-node"])
        self.calls = []

    def kube(self, args):
        self.calls.append(args)
        if "daemonset" in args or "statefulset" in args:
            return copy.deepcopy(self.ds)
        self.assertEqual(args, ["get", "--raw", "/api/v1/namespaces/ns/pods"])
        return copy.deepcopy(self.pods)

    def get(self, path):
        self.calls.append(path)
        return copy.deepcopy(self.cap if path.endswith("capabilities") else self.inspect)

    def plan(self):
        return rollout.plan_pod(self.args, self.kube, self.get, self.local)

    def local(self, name):
        pod = next(p for p in self.pods["items"] if p["metadata"]["name"] == name)
        uid = pod["metadata"]["uid"]
        bindings = getattr(self, "snapshot", {}).get("pods", [])
        binding = next((item for item in bindings if item["name"] == name), None) or next(
            (item for item in bindings if item["kubernetes_node_name"] == pod["spec"]["nodeName"]), None)
        mapping = dict(entry.split("=", 1) for entry in self.args.node)
        node = binding["node_id"] if binding else mapping.get(name, "adx-node")
        replacement = uid != (binding["uid"] if binding else "pod-uid")
        session = "new-session" if replacement else (
            binding["inspection"]["node_session_id"] if binding else "session-1")
        runtime = dict(boot_nonce="new-boot" if replacement else "original-boot", pod_uid=uid,
                       pid=123, executable_sha256="c" * 64, configuration_sha256="d" * 64,
                       node_session=session, relay_session="relay-session")
        return dict(ok=True, node_info=dict(schema_version=1, boot_nonce=runtime["boot_nonce"], pod_uid=uid,
                    components=[dict(role="adxlet", mode="embedded", node_id=node, service_id="node",
                                     runtime=runtime, sources={} if replacement else {"adxlet": {
                                         "version": "a" * 40, "archive_sha256": None, "raw_config_sha256": None}})]))

    def sts(self):
        self.ds["kind"] = "StatefulSet"
        self.pods["items"][0]["metadata"].update(name="workers-0")
        self.pods["items"][0]["metadata"]["ownerReferences"][0].update(kind="StatefulSet")
        self.args = rollout.parser().parse_args([
            "plan-pod", "--context", "test-cluster", "--origin", "http://coordinator:9090", "--namespace", "ns",
            "--statefulset", "workers", "--container", "adxlet", "--config", "/etc/adx/deployment.yaml",
            "--target-image", "registry:5000/worker:release-20261124-1", "--task-id", "release-1",
            "--epoch", "1", "--node", "workers-0=adx-node"])


class PlanTests(PodFixture):
    def test_pod_task_identity_is_addressable_before_planning_or_consumption(self):
        snapshot = self.plan()
        for task_id in ("release/1", "a%20b", "a?b", "a#b", "a b", "\u4e2d", ".", "..", "a" * 129):
            with self.subTest(task_id=task_id):
                changed = copy.deepcopy(snapshot)
                changed["begin"]["task_id"] = task_id
                with self.assertRaisesRegex(rollout.Refusal, "task ID"):
                    rollout.validate_pod_plan(changed)
                self.args.task_id = task_id
                with self.assertRaisesRegex(rollout.Refusal, "task ID"):
                    self.plan()
        for epoch in (0, -1, True, 2**64):
            with self.subTest(epoch=epoch):
                changed = copy.deepcopy(snapshot)
                changed["begin"]["epoch"] = epoch
                with self.assertRaisesRegex(rollout.Refusal, "epoch"):
                    rollout.validate_pod_plan(changed)
        for task_id in ("release-1._~", "-release", "a" * 128):
            snapshot["begin"]["task_id"] = task_id
            rollout.validate_pod_plan(snapshot)
            self.args.task_id = task_id
            self.assertEqual(self.plan()["begin"]["task_id"], task_id)

    def test_pod_consumption_requires_local_workload_evidence(self):
        snapshot = self.plan()
        changes = (("workload", "original_template_hash", "bad"),
                   ("workload", "target_template_hash", None), ("workload", "original_image", ""),
                   ("pod", "kubernetes_node_name", ""), ("pod", "original_image", None))
        for section, key, value in changes:
            with self.subTest(section=section, key=key):
                changed = copy.deepcopy(snapshot)
                target = changed["workload"] if section == "workload" else changed["pods"][0]
                target[key] = value
                with self.assertRaises(rollout.Refusal):
                    rollout.validate_pod_plan(changed)
        self.sts()
        snapshot = self.plan()
        for name in ("workers-00", "workers-debug"):
            with self.subTest(name=name):
                snapshot["pods"][0]["name"] = name
                with self.assertRaisesRegex(rollout.Refusal, "canonical ordinal"):
                    rollout.validate_pod_plan(snapshot)

    def test_process_apply_needs_no_handoff_options(self):
        base = ["apply-process", "--plan", "plan.json", "--journal", "journal.json", "--batch-index", "0"]
        parsed = rollout.parser().parse_args(base)
        for name in ("handoff_config", "handoff_stubs", "handoff_timeout"):
            self.assertFalse(hasattr(parsed, name))

    def test_apply_actions_are_mutually_exclusive(self):
        rejected = []
        for command in ("apply-process", "apply-pod"):
            base = [command, "--plan", "plan.json", "--journal", "journal.json"]
            self.assertEqual(rollout.parser().parse_args([*base, "--batch-index", "0"]).batch_index, 0)
            rejected.extend([base, [*base, "--promote-ready"], [*base, "--batch-index", "0", "--restore-input"]])
        for command in ("plan-process", "plan-pod"):
            base = [command, "--context", "cluster", "--namespace", "ns", "--container", "adxlet",
                    "--config", "/config.yaml", "--task-id", "release", "--epoch", "1", "--node", "pod=node"]
            if command == "plan-pod":
                base.extend(["--origin", "http://coordinator", "--daemonset", "workers", "--target-image", "node:v1"])
            else:
                base.extend(["--role", "adxlet", "--commit", "c" * 40])
            rejected.append([*base, "--declaration-candidate", "candidate.json"])
        for argv in rejected:
            with self.subTest(argv=argv), \
                    patch.object(rollout.argparse.ArgumentParser, "error", side_effect=ValueError) as error, \
                    self.assertRaises(ValueError):
                rollout.parser().parse_args(argv)
            error.assert_called_once()

    def test_begin_payload_and_snapshot(self):
        result = self.plan()
        self.assertEqual(result["begin"], {
            "task_id": "release-1", "epoch": 1, "namespace": "ns",
            "daemonset_uid": "ds-uid", "target_image": self.args.target_image,
            "nodes": [{"node_id": "adx-node", "pod_uid": "pod-uid",
                       "node_session_id": "session-1"}]})
        self.assertEqual(result["pods"][0]["resource_version"], "20")
        self.assertEqual(result["pods"][0]["original_image"], "old:v1")
        self.assertEqual(result["workload"]["resource_version"], "10")
        self.assertEqual(result["context"], "test-cluster")
        self.assertFalse(result["pods"][0]["inspection"]["strict_empty"])
        self.assertEqual(result["batches"], [["adx-node"]])
        self.assertEqual(len(result["workload"]["original_template_hash"]), 64)
        self.assertNotEqual(result["workload"]["original_template_hash"],
                            result["workload"]["target_template_hash"])

    def test_target_image_syntax_is_validated_by_coordinator(self):
        for target in ("worker", "worker:latest", "worker:-1"):
            with self.subTest(target=target):
                self.args.target_image = target
                plan = self.plan()
                rollout.validate_pod_plan(plan)
                self.assertEqual(plan["begin"]["target_image"], target)

    def test_changed_pod_target_tag_rejected_by_plan_validation(self):
        plan = self.plan()
        plan["pods"][0]["target_image"] = "registry:5000/worker:release-20261124-2"
        with self.assertRaisesRegex(rollout.Refusal, "Pod plan binding differs"):
            rollout.validate_pod_plan(plan)

    def test_wrong_owner_is_not_a_source(self):
        self.pods["items"][0]["metadata"]["ownerReferences"][0]["uid"] = "other"
        with self.assertRaises(rollout.Refusal):
            self.plan()

    def test_same_uid_malformed_owner_is_rejected(self):
        base = {"apiVersion": "apps/v1", "kind": "DaemonSet", "name": "workers",
                "uid": "ds-uid", "controller": True}
        for label, fault in (("kind", {"kind": "Deployment"}), ("name", {"name": "other"}),
                             ("apiVersion", {"apiVersion": "v1"})):
            with self.subTest(label=label):
                self.pods["items"][0]["metadata"]["ownerReferences"] = [dict(base, **fault)]
                with self.assertRaises(rollout.Refusal):
                    self.plan()
        with self.subTest(label="duplicate"):
            self.pods["items"][0]["metadata"]["ownerReferences"] = [dict(base), dict(base)]
            with self.assertRaises(rollout.Refusal):
                self.plan()

    def test_statefulset_daemonset_mutually_exclusive(self):
        base = ["plan-pod", "--context", "c", "--origin", "http://o", "--namespace", "ns",
                "--container", "adxlet", "--config", "/x.yaml", "--target-image", "r@sha256:" + "b" * 64,
                "--task-id", "t", "--epoch", "1"]
        ds = rollout.parser().parse_args([*base, "--daemonset", "workers"])
        self.assertEqual((ds.daemonset, ds.statefulset), ("workers", None))
        sts = rollout.parser().parse_args([*base, "--statefulset", "workers"])
        self.assertEqual((sts.statefulset, sts.daemonset), ("workers", None))
        for choices in ([], ["--daemonset", "a", "--statefulset", "b"]):
            selected_parser = rollout.parser()
            with patch.object(rollout.argparse.ArgumentParser, "error", side_effect=ValueError) as error, \
                    self.assertRaises(ValueError):
                selected_parser.parse_args([*base, *choices])

            error.assert_called_once()

    def test_statefulset_plan_does_not_select_update_strategy(self):
        self.sts()
        result = self.plan()
        self.assertEqual(result["workload"]["kind"], "StatefulSet")
        self.assertEqual(result["pods"][0]["name"], "workers-0")
        self.ds["spec"]["updateStrategy"]["type"] = "RollingUpdate"
        self.assertEqual(self.plan(), result)
        del self.ds["spec"]["updateStrategy"]
        self.assertEqual(self.plan(), result)

    def test_statefulset_requires_stable_pod_names(self):
        self.sts()
        self.pods["items"][0]["metadata"]["name"] = "not-stable"
        self.args.node = ["not-stable=adx-node"]
        with self.assertRaises(rollout.Refusal):
            self.plan()

    def test_statefulset_non_canonical_ordinal_names_rejected(self):
        for name in ("workers-debug", "workers-00"):
            with self.subTest(name=name):
                self.sts()
                self.pods["items"][0]["metadata"]["name"] = name
                self.args.node = [name + "=adx-node"]
                with self.assertRaises(rollout.Refusal):
                    self.plan()

    def test_statefulset_nonzero_ordinal_accepted(self):
        self.sts()
        self.pods["items"][0]["metadata"]["name"] = "workers-7"
        self.args.node = ["workers-7=adx-node"]
        result = self.plan()
        self.assertEqual(result["pods"][0]["name"], "workers-7")

    def test_replacement_opaque_content_identity(self):
        pod = self.pods["items"][0]
        pod["metadata"].update(name="workers-new", uid="new-uid")
        response = self.local("workers-new")
        actual = rollout.pod_actual(pod, "ns", "adxlet", "adx-node", response, "new-session")
        self.assertEqual(actual["image_id"], "opaque-runtime-content-id")

    def test_context_required_before_io(self):
        self.args.context = None
        before = len(self.calls)
        with self.assertRaises(rollout.Refusal):
            self.plan()
        self.assertEqual(len(self.calls), before)

    def test_downward_pod_uid_required_in_template_and_source(self):
        for obj in (self.ds["spec"]["template"], self.pods["items"][0]):
            container = obj["spec"]["containers"][0]
            original = copy.deepcopy(container["env"])
            for entries in ([], [{"name": "ADX_POD_UID", "value": "pod-uid"}],
                            original * 2,
                            [{"name": "ADX_POD_UID", "valueFrom": {"fieldRef": {
                                "fieldPath": "metadata.name"}}}],
                            [{"name": "ADX_POD_UID", "valueFrom": {"fieldRef": {
                                "apiVersion": "v2", "fieldPath": "metadata.uid"}}}]):
                with self.subTest(entries=entries):
                    container["env"] = entries
                    with self.assertRaises(rollout.Refusal):
                        self.plan()
            container["env"] = original

    def test_downward_default_api_version(self):
        for obj in (self.ds["spec"]["template"], self.pods["items"][0]):
            del obj["spec"]["containers"][0]["env"][0]["valueFrom"]["fieldRef"]["apiVersion"]
        self.plan()

    def test_daemonset_plan_does_not_select_update_strategy(self):
        expected = self.plan()
        self.ds["spec"]["updateStrategy"]["type"] = "RollingUpdate"
        self.assertEqual(self.plan(), expected)
        del self.ds["spec"]["updateStrategy"]
        self.assertEqual(self.plan(), expected)

    def test_mapping_must_cover_exactly_and_be_unique(self):
        for mappings in ([], ["workers-a=adx-node"] * 2,
                         ["other=adx-node"], ["workers-a="],
                         ["workers-a=adx-node", "extra=adx-node"]):
            with self.subTest(mappings=mappings):
                self.args.node = mappings
                with self.assertRaises(rollout.Refusal):
                    self.plan()

    def test_unknown_and_missing_evidence_fail_closed(self):
        for field, value in (("unknown", True), ("unknown", None),
                             ("protocol_version", 2), ("node_session_id", ""),
                             ("node_id", "other"), ("runtime_rootfs_sha256", "")):
            with self.subTest(field=field):
                original = self.inspect.copy()
                self.inspect[field] = value
                with self.assertRaises(rollout.Refusal):
                    self.plan()
                self.inspect = original

    def test_capability_required(self):
        self.cap["session_isolation"] = False
        with self.assertRaises(rollout.Refusal):
            self.plan()

    def test_plan_records_one_inventory_and_coordinator_observation(self):
        result = self.plan()
        self.assertEqual(sum(isinstance(call, list) for call in self.calls), 2)
        self.assertEqual(sum(isinstance(call, str) and call.endswith("inspect") for call in self.calls), 1)
        self.assertEqual(result["pods"][0]["inspection"]["node_session_id"], "session-1")

    def test_untrusted_origin_and_digest_rejected_before_io(self):
        self.args.origin = "http://user:secret@coordinator"
        before = len(self.calls)
        with self.assertRaises(rollout.Refusal):
            self.plan()
        self.assertEqual(len(self.calls), before)

    def test_actual_pod_uid_required_not_publisher_echo(self):
        self.inspect["pod_uid"] = "pod-uid"
        for uid in (None, "", "another-pod"):
            self.inspect["observed_pod_uid"] = uid
            with self.assertRaises(rollout.Refusal):
                self.plan()

    def test_raw_inventory_rejects_incomplete_or_untyped_evidence(self):
        for fault in ("List", "continue", "list-rv", "kind", "version", "namespace", "name", "uid", "rv",
                      "duplicate"):
            with self.subTest(fault=fault):
                listing = copy.deepcopy(self.pods)
                item = listing["items"][0]
                if fault == "List":
                    listing["kind"] = "List"
                elif fault == "continue":
                    listing["metadata"]["continue"] = "next-page"
                elif fault == "list-rv":
                    listing["metadata"]["resourceVersion"] = ""
                elif fault == "kind":
                    item["kind"] = "ConfigMap"
                elif fault == "version":
                    item["apiVersion"] = "v2"
                elif fault == "namespace":
                    item["metadata"]["namespace"] = "other"
                elif fault == "duplicate":
                    listing["items"].append(copy.deepcopy(item))
                else:
                    key = {"name": "name", "uid": "uid", "rv": "resourceVersion"}[fault]
                    item["metadata"][key] = ""
                with self.assertRaises(rollout.Refusal):
                    rollout.pod_inventory("ns", lambda _, current=listing: current)

    def test_raw_inventory_keeps_terminating_items_for_state_reconciliation(self):
        self.pods["items"][0]["metadata"]["deletionTimestamp"] = "2026-11-11T00:00:00Z"
        items = rollout.pod_inventory("ns", self.kube)
        self.assertEqual(items[0]["metadata"]["uid"], "pod-uid")
        with self.assertRaises(rollout.Refusal):
            self.plan()

    def test_raw_inventory_transport_has_no_output_formatter(self):
        process = MagicMock()
        process.stdout.read.return_value = (
            b'{"apiVersion":"v1","kind":"PodList","metadata":{"resourceVersion":"20"},"items":[]}'
        )
        process.wait.return_value = 0
        process.poll.return_value = 0
        manager = MagicMock()
        manager.__enter__.return_value = process
        arguments = ["--context", "test-cluster", "get", "--raw", "/api/v1/namespaces/ns/pods"]
        with patch.object(rollout.shutil, "which", return_value="kubectl"), \
                patch.object(rollout.subprocess, "Popen", return_value=manager) as popen, \
                patch.object(rollout.threading, "Timer"):
            self.assertEqual(rollout.kubectl_get(arguments)["kind"], "PodList")
        self.assertEqual(popen.call_args.args[0][1:], ["--request-timeout=10s", *arguments])
        self.assertNotIn("-o", popen.call_args.args[0])

    def test_path_tokens_and_one_pod_per_host(self):
        self.args.task_id = "task/encoded%2F"
        with self.assertRaises(rollout.Refusal):
            self.plan()
        self.args.task_id = "task"
        extra = copy.deepcopy(self.pods["items"][0])
        extra["metadata"].update(name="workers-b", uid="pod-b")
        self.pods["items"].append(extra)
        self.args.node.append("workers-b=adx-b")
        with self.assertRaises(rollout.Refusal):
            self.plan()

    def test_statefulset_replacement_helper_allows_new_host(self):
        self.sts()
        snapshot = self.plan()
        moved = copy.deepcopy(self.pods["items"][0])
        moved["metadata"].update(uid="new-uid")
        moved["spec"]["nodeName"] = "kube-node-2"
        moved["spec"]["containers"][0]["image"] = self.args.target_image
        _, evidence, host = rollout.pod_replacement(snapshot, snapshot["pods"][0], [moved])
        self.assertEqual((evidence["uid"], host), ("new-uid", "kube-node-2"))

    def test_workload_kind_must_be_explicit(self):
        for kind in ("DaemonSet", "StatefulSet"):
            self.assertEqual(rollout.workload_kind({"kind": kind}), kind)
        for workload in ({}, {"kind": "Deployment"}):
            with self.subTest(workload=workload), self.assertRaises(rollout.Refusal):
                rollout.workload_kind(workload)

    def test_replacement_host_must_be_recorded(self):
        self.assertEqual(rollout.replacement_host({"replacement_host": "host-b"}), "host-b")
        for state in ({}, {"replacement_host": ""}, {"replacement_host": None}):
            with self.subTest(state=state), self.assertRaises(rollout.Refusal):
                rollout.replacement_host(state)


class ApplyTests(PodFixture):
    def setUp(self):
        super().setUp()
        self.snapshot = self.plan()
        self.task = None
        self.writes = []
        self.deleted = False
        self.lost = set()
        self.instance = None
        self.action_losses = set()
        self.source_permit = "original-permit"

    def get_live(self, path):
        if "/tasks/" in path:
            if self.task is None:
                raise rollout.Refusal("task unavailable")
            return copy.deepcopy(self.task)
        if path.startswith("/instance-manager/"):
            items = [] if self.instance is None else [self.instance]
            verified = self.task["nodes"]["adx-node"]["phase"] in ("verified", "RestoredOriginal")
            return dict(count=len(items), instanceInfos=copy.deepcopy(items), epoch=1, revision=1,
                        node=dict(node_id="adx-node", scheduling_paused=not verified, accepting_allocations=verified))
        result = copy.deepcopy(self.inspect)
        if self.task is not None:
            node = self.task["nodes"]["adx-node"]
            result.update(task_id="release-1", epoch=1, revision=node["control_revision"],
                          phase="Quiescing" if node["phase"] == "quiescing" else node["phase"],
                          permit_id=node["permit_id"], strict_empty=node["phase"] == "quiescing",
                          controllers=0, holds=0, backends=0, authority_tasks=0)
            if node["phase"] == "RestoredOriginal" or self.task["phase"] in ("Aborted", "Completed"):
                result["phase"] = "released"
        if self.deleted:
            result.update(observed_pod_uid="new-uid", node_session_id="new-session")
            if self.task["nodes"]["adx-node"]["pod_uid"] == "pod-uid":
                result.update(phase="isolated", strict_empty=False)
        return result

    def post(self, path, body):
        self.writes.append((path, copy.deepcopy(body)))
        if path.endswith("/tasks"):
            if self.task is None:
                self.task = dict(body, kind="pod", protocol_version=3, phase="Active", revision=1,
                                 operations={}, nodes={
                    "adx-node": dict(body["nodes"][0], original_pod_uid="pod-uid",
                                     original_session_id="session-1", phase="pending",
                                     original_paused=False,
                                     control_revision=0, permit_id=None,
                                     delete_resource_version=None)})
            result = self.task
        else:
            action = path.rsplit("/", 1)[1]
            self.assertEqual(body["task_id"], "release-1")
            self.assertEqual(body["epoch"], 1)
            self.assertTrue(body["operation_id"])
            node = self.task["nodes"]["adx-node"]
            if action == "prepare-node":
                node["phase"] = "evacuating"
            elif action == "migrate":
                self.task["operations"][body["operation_id"]] = dict(
                    phase="Completed", environment_id=body["environment_id"],
                    source_node_id="adx-node", source_session_id="session-1",
                    target_node_id="target-node", target_session_id="target-session", was_running=True,
                    source_generation=body["expected_source_generation"],
                    source_revision=body["expected_source_revision"])
                self.instance = None
                result = {"outcome": "completed", "completed": True}
            elif action == "quiesce":
                if not body.get("delete_intent"):
                    node.update(pod_uid=body["pod_uid"], node_session_id=body["node_session_id"],
                                phase="quiescing", control_revision=node["control_revision"] + 1,
                                permit_id="new-permit" if self.deleted else self.source_permit)
                    node["delete_resource_version"] = None
                else:
                    self.assertEqual(body["permit_id"], node["permit_id"])
                    self.assertIn(node["delete_resource_version"], (None, body["pod_resource_version"]))
                    node["delete_resource_version"] = body["pod_resource_version"]
                result = dict(task_id="release-1", epoch=1, node_id="adx-node",
                            pod_uid=node["pod_uid"], observed_pod_uid=node["pod_uid"],
                            node_session_id=node["node_session_id"], revision=node["control_revision"],
                            permit_id=node["permit_id"], strict_empty=True, unknown=False,
                            controllers=0, holds=0, backends=0, authority_tasks=0)
            elif action == "verify-node":
                if node["phase"] != "verified":
                    self.assertEqual(body["permit_id"], "new-permit")
                node.update(phase="verified", control_revision=node["control_revision"] + 1, permit_id=None)
            elif action == "complete":
                self.task["phase"] = "Completed"
            elif action == "abort":
                if body["stage"] == "freeze":
                    self.task["phase"] = "Aborting"
                elif body["stage"] == "restore-original":
                    node.update(phase="RestoredOriginal", control_revision=node["control_revision"] + 1,
                                permit_id=None)
                    result = dict(restored_original=True, node_id=body["node_id"],
                                  pod_uid=body["pod_uid"], node_session_id=body["node_session_id"])
                elif body["stage"] == "finalize":
                    self.assertTrue(body["template_restored"])
                    self.assertTrue(all(n["phase"] in ("pending", "RestoredOriginal", "verified")
                                        and n["delete_resource_version"] is None for n in self.task["nodes"].values()))
                    self.assertTrue(all(entry["phase"] in ("Completed", "Cancelled")
                                        for entry in self.task["operations"].values()))
                    self.task["phase"] = "Aborted"
                else:
                    self.fail("unknown abort stage")
            elif action == "revoke-delete":
                self.assertIn(node["delete_resource_version"], (None, body["expected_delete_resource_version"]))
                self.assertNotEqual(body["pod_resource_version"], body["expected_delete_resource_version"])
                if node["delete_resource_version"] is not None:
                    node.update(delete_resource_version=None, permit_id=None)
                    self.source_permit = "renewed-permit"
            else:
                self.fail("unknown action: " + action)
            if action not in ("quiesce", "migrate") and not (
                    action == "abort" and body["stage"] == "restore-original"):
                result = self.task
            loss = body.get("stage", action)
            if loss in self.action_losses:
                self.action_losses.remove(loss)
                raise TimeoutError("lost action success reply")
        if path in self.lost:
            self.lost.remove(path)
            raise TimeoutError("lost applied reply")
        return copy.deepcopy(result)

    def mutate(self, args, body):
        self.writes.append((args, copy.deepcopy(body)))
        if "delete" in args:
            self.assertEqual(args, ["delete", "--raw", "/api/v1/namespaces/ns/pods/workers-a", "-f", "-"])
            self.assertEqual(body["apiVersion"], "v1")
            self.assertEqual(body["kind"], "DeleteOptions")
            self.assertEqual(body["preconditions"]["uid"], "pod-uid")
            if self.task:
                self.assertEqual(body["preconditions"]["resourceVersion"],
                                 self.task["nodes"]["adx-node"]["delete_resource_version"])
            metadata = self.pods["items"][0]["metadata"]
            if any(metadata[k] != v for k, v in body["preconditions"].items()):
                raise rollout.Refusal("DELETE precondition conflict")
            if getattr(self, "retain_original", False):
                if getattr(self, "change_rv", False):
                    self.pods["items"][0]["metadata"]["resourceVersion"] = "21"
                raise TimeoutError("DELETE outcome unknown; original UID remains")
            self.deleted = True
            pod = self.pods["items"][0]
            pod["metadata"].update(uid="new-uid", resourceVersion="30",
                                   name=getattr(self, "replacement_name", "workers-new"))
            pod["spec"]["nodeName"] = getattr(self, "replacement_host", pod["spec"]["nodeName"])
            pod["spec"]["containers"][0]["image"] = self.args.target_image
            pod["status"]["containerStatuses"][0]["containerID"] = "container-replacement"
        else:
            self.assertEqual(args[0], "patch")
            self.assertIn(args[1], ("daemonset", "statefulset", "pod"))
            self.assertEqual(args[3:6], ["--namespace", "ns", "--type=json"])
            self.assertEqual(args[-2:], ["-o", "json"])
            self.assertIsNone(body)
            changes = rollout.json.loads(args[args.index("--patch") + 1])
            target = self.ds if args[1] != "pod" else self.pods["items"][0]
            self.assertEqual(args[2], target["metadata"]["name"])
            self.assertEqual(changes[0], dict(op="test", path="/metadata/uid", value=target["metadata"]["uid"]))
            self.assertEqual(changes[1]["path"], "/metadata/resourceVersion")

            def check_write(change, parent, key):
                self.assertIn(change["op"], ("add", "replace"))
                self.assertTrue(change["path"] == "/spec/template/spec/containers/0/image"
                                or change["path"] == "/metadata/annotations"
                                or change["path"].startswith("/metadata/annotations/adx.dev~1rollout-"))
                if change["op"] == "replace":
                    self.assertIn(key, parent)

            staged = patched_resource(self, target, changes, check_write)
            staged["metadata"]["resourceVersion"] = str(int(staged["metadata"]["resourceVersion"]) + 1)
            if args[1] != "pod":
                self.ds = staged
            else:
                self.pods["items"][0] = staged
        action = args[0]
        if action in self.lost:
            self.lost.remove(action)
            raise TimeoutError("lost Kubernetes write ACK")

    def drive(self, path):
        for _ in range(60):
            with patch.object(rollout.sys, "platform", "linux"):
                result = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                           self.post, self.mutate), sleep=lambda _: None, local=self.local)
            if result["status"] == "completed":
                return rollout.read_json_file(path)
        return self.fail("normal path did not finish")

    def test_single_empty_worker_completes_without_migration(self):
        self.assertEqual(len(self.snapshot["pods"]), 1)
        with tempfile.TemporaryDirectory() as directory:
            journal = self.drive(Path(directory) / "rollout.json")
        self.assertEqual(journal["stage"], "completed")
        self.assertEqual(self.task["phase"], "Completed")
        self.assertTrue(self.deleted)
        self.assertFalse(any(isinstance(p, str) and p.endswith("/migrate") for p, _ in self.writes))

    def test_version_tag_plan_validate_apply(self):
        rollout.validate_pod_plan(self.snapshot)
        with tempfile.TemporaryDirectory() as directory:
            journal = self.drive(Path(directory) / "rollout.json")
        self.assertEqual(journal["stage"], "completed")
        target = "registry:5000/worker:release-20261124-1"
        self.assertEqual(self.task["target_image"], target)
        self.assertEqual(rollout.image(self.ds["spec"]["template"], "adxlet"), target)
        self.assertEqual(rollout.image(self.pods["items"][0], "adxlet"), target)

    def test_changed_target_tag_cannot_reuse_journal(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            self.drive(path)
            changed = copy.deepcopy(self.snapshot)
            target = "registry:5000/worker:release-20261124-2"
            changed["begin"]["target_image"] = target
            changed["pods"][0]["target_image"] = target
            before = len(self.writes)
            with self.assertRaisesRegex(rollout.Refusal, "Pod journal plan hash differs"):
                rollout.apply_pod(changed, path, 0, rollout.PodClients(
                    self.kube, self.get_live, self.post, self.mutate), local=self.local)
            self.assertEqual(len(self.writes), before)

    def test_changed_task_target_tag_refused(self):
        with tempfile.TemporaryDirectory() as directory:
            self.drive(Path(directory) / "journal.json")
        self.task["target_image"] = "registry:5000/worker:release-20261124-2"
        with self.assertRaisesRegex(rollout.Refusal, "task owner differs"):
            rollout.pod_task(self.snapshot, self.get_live)

    def test_single_populated_worker_without_target_keeps_original_pod(self):
        self.instance = dict(instanceID="env", node_id="adx-node", generation=0, revision=0,
                             status="running", resources_held=True, invalidated=False,
                             restart_pending=False, resume_pending=False, recovery_pending=False)
        original_post = self.post

        def no_target(endpoint, body):
            if endpoint.endswith("/migrate"):
                self.writes.append((endpoint, copy.deepcopy(body)))
                raise rollout.Refusal("HTTP 429 ResourceExhausted: capacity unavailable")
            return original_post(endpoint, body)

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.json"
            for _ in range(2):
                with patch.object(rollout.sys, "platform", "linux"):
                    result = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                               no_target, self.mutate), sleep=lambda _: None, local=self.local)
                self.assertEqual(result["status"], "pending")
                self.assertIn("capacity unavailable", result["detail"])
                self.assertEqual(rollout.read_json_file(path)["nodes"]["adx-node"]["stage"], "evacuate")
        migrations = [b for p, b in self.writes if isinstance(p, str) and p.endswith("/migrate")]
        self.assertEqual(len(migrations), 2)
        self.assertEqual(migrations[0], migrations[1])
        self.assertFalse(self.deleted)
        self.assertEqual(self.pods["items"][0]["metadata"]["uid"], "pod-uid")
        self.assertEqual(rollout.image(self.pods["items"][0], self.args.container), "old:v1")
        self.assertEqual(self.task["operations"], {})
        self.assertEqual(self.task["nodes"]["adx-node"]["phase"], "evacuating")

    def test_normal_path_lost_prepare_delete_and_zero_revision_migration(self):
        self.instance = dict(instanceID="env", node_id="adx-node", generation=0, revision=0,
                             status="running", resources_held=True, invalidated=False,
                             restart_pending=False, resume_pending=False, recovery_pending=False)
        self.lost.add("/v1/node-upgrades/tasks/release-1/prepare-node")
        with tempfile.TemporaryDirectory() as directory:
            journal = self.drive(Path(directory) / "rollout.json")
            self.assertEqual(journal["stage"], "completed")
            prepares = [b for p, b in self.writes if isinstance(p, str) and p.endswith("prepare-node")]
            self.assertEqual(len(prepares), 2)
            self.assertEqual(prepares[0], prepares[1])
            migration = [b for p, b in self.writes if isinstance(p, str) and p.endswith("migrate")][0]
            self.assertEqual(migration["expected_source_generation"], 0)
            self.assertEqual(migration["expected_source_revision"], 0)
            self.assertEqual(self.task["phase"], "Completed")
            saved = self.drive(Path(directory) / "rollout.json")
            self.assertEqual(saved, journal)

    def test_paused_env_can_move_while_running_migration_waits_for_idle(self):
        def instance(env, state):
            return dict(instanceID=env, node_id="adx-node", generation=1, revision=2,
                        status=state, resources_held=state == "running", invalidated=False,
                        restart_pending=False, resume_pending=False, recovery_pending=False)

        running = "busy-env"
        paused = "paused-env"
        instances = {running: instance(running, "running")}
        allow_running = False
        paused_attempts = 0

        def get_live(path):
            value = self.get_live(path)
            if path.startswith("/instance-manager/"):
                value.update(count=len(instances), instanceInfos=copy.deepcopy(list(instances.values())))
            return value

        def post(endpoint, body):
            nonlocal paused_attempts
            result = self.post(endpoint, body)
            if endpoint.endswith("/migrate"):
                env = body["environment_id"]
                pending = env == running and not allow_running
                if env == paused:
                    paused_attempts += 1
                    pending = paused_attempts == 1
                if pending:
                    self.task["operations"][body["operation_id"]]["phase"] = "Prepared"
                    raise rollout.Refusal("migration response unavailable")
                instances.pop(env)
            return result

        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.json"

            def apply():
                with patch.object(rollout.sys, "platform", "linux"):
                    return rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, get_live, post,
                                             self.mutate), sleep=lambda _: None, local=self.local)

            self.assertEqual(apply()["status"], "pending")
            instances[paused] = instance(paused, "paused")
            self.assertEqual(apply()["status"], "pending")
            self.assertEqual(apply()["status"], "pending")
            self.assertNotIn(paused, instances)
            self.assertFalse(self.deleted)
            allow_running = True
            self.assertEqual(apply()["status"], "completed")

        migrations = [b for p, b in self.writes if isinstance(p, str) and p.endswith("/migrate")]
        self.assertEqual([b["environment_id"] for b in migrations],
                         [running, paused, paused, running, running])
        self.assertEqual(migrations[0], migrations[3])
        self.assertEqual(migrations[0], migrations[4])
        self.assertEqual(migrations[1], migrations[2])

    def test_apply_preserves_deployment_owned_update_strategy(self):
        strategy = {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 1}}
        self.ds["spec"]["updateStrategy"] = copy.deepcopy(strategy)
        with tempfile.TemporaryDirectory() as directory:
            self.drive(Path(directory) / "rollout.json")
        self.assertEqual(self.ds["spec"]["updateStrategy"], strategy)

    def test_abort_preserves_deployment_owned_update_strategy(self):
        strategy = {"type": "RollingUpdate", "rollingUpdate": {"maxUnavailable": 1}}
        self.ds["spec"]["updateStrategy"] = copy.deepcopy(strategy)
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "rollout.json"
            self.cleanup_seed(path, replacement=True)
            outcome = self.cleanup_drive(path)
        self.assertEqual(outcome["task_phase"], "Aborted")
        self.assertEqual(self.ds["spec"]["updateStrategy"], strategy)

    def test_unpermitted_replacement_stops_before_prepare_or_delete(self):
        self.pods["items"][0]["metadata"]["uid"] = "unexpected"
        with tempfile.TemporaryDirectory() as directory:
            with patch.object(rollout.sys, "platform", "linux"):
                result = rollout.apply_pod(self.snapshot, Path(directory) / "rollout.json", 0,
                                           rollout.PodClients(self.kube, self.get_live, self.post, self.mutate),
                                           sleep=lambda _: None, local=self.local)
            self.assertEqual(result["status"], "pending")
        self.assertFalse(any(isinstance(p, list) and "delete" in p for p, _ in self.writes))

    def test_private_journal_does_not_store_template_environment_values(self):
        self.ds["spec"]["template"]["spec"]["containers"][0]["env"] = [
            *self.ds["spec"]["template"]["spec"]["containers"][0]["env"],
            {"name": "SECRET", "value": "must-not-persist"}]
        snapshot = self.plan()
        self.assertNotIn("must-not-persist", str(snapshot))
        self.assertNotIn("original_template", snapshot)

    def test_each_ambiguous_write_resumes_same_intent(self):
        for action in ("prepare-node", "quiesce", "verify-node", "complete", "patch", "delete"):
            with self.subTest(action=action):
                self.setUp()
                key = action if action in ("patch", "delete") else f'/v1/node-upgrades/tasks/release-1/{action}'
                self.lost.add(key)
                with tempfile.TemporaryDirectory() as directory:
                    self.drive(Path(directory) / "journal.json")
                self.assertFalse(self.lost)
                if action == "verify-node":
                    replies = [b for p, b in self.writes if isinstance(p, str) and p.endswith("/verify-node")]
                    self.assertEqual(len(replies), 2)
                    self.assertEqual(replies[0], replies[1])

    def test_delete_intent_ack_loss_does_not_issue_new_quiesce_permit(self):
        original = self.post

        def post(path, body):
            if body.get("delete_intent") and not getattr(self, "lost_delete_intent", False):
                self.lost_delete_intent = True
                self.lost.add(path)
            return original(path, body)

        with patch.object(self, "post", post), tempfile.TemporaryDirectory() as directory:
            self.drive(Path(directory) / "journal.json")
        quiesces = [b for p, b in self.writes if isinstance(p, str) and p.endswith("/quiesce")]
        self.assertEqual(sum(not b.get("delete_intent", False) for b in quiesces), 2)

    def test_masked_isolated_empty_still_requires_new_quiesce_permit(self):
        with tempfile.TemporaryDirectory() as directory:
            self.drive(Path(directory) / "journal.json")
        actions = [(p.rsplit("/", 1)[-1], b) for p, b in self.writes if isinstance(p, str)]
        replacement = [b for a, b in actions if a == "quiesce" and b.get("pod_uid") == "new-uid"]
        self.assertEqual(len(replacement), 1)
        verified = [b for a, b in actions if a == "verify-node"]
        self.assertEqual(verified[0]["permit_id"], "new-permit")

    def test_original_paused_verify_requires_complete_http_reply(self):
        original_post, original_get = self.post, self.get_live
        fault = {"body": None, "observation": None, "replayed": False}

        def post(path, body):
            if path.endswith("/tasks"):
                result = original_post(path, body)
                self.task["nodes"]["adx-node"]["original_paused"] = True
                return result
            if path.endswith("/verify-node"):
                node = self.task["nodes"]["adx-node"]
                if fault["body"] is None:
                    fault["body"] = copy.deepcopy(body)
                    node.update(phase="verified", control_revision=node["control_revision"] + 1, permit_id=None)
                    fault["observation"] = original_get("/v1/node-upgrades/nodes/adx-node/inspect")
                    raise TimeoutError("Node applied verified but RPC ACK lost before scheduling cleanup")
                self.assertEqual(body, fault["body"])
                fault["replayed"] = True
                fault["observation"] = None
                node["control_revision"] += 1
            return original_post(path, body)

        def get(path):
            if path.endswith("/inspect") and fault["observation"] is not None:
                return copy.deepcopy(fault["observation"])
            result = original_get(path)
            if path.startswith("/instance-manager/"):
                result["node"].update(scheduling_paused=True, accepting_allocations=False)
            return result

        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"), \
                patch.object(self, "post", post), patch.object(self, "get_live", get):
            path = Path(directory) / "journal.json"
            outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, get, post,
                                        self.mutate), sleep=lambda _: None, local=self.local)
            self.assertEqual(outcome["status"], "pending")
            journal = rollout.read_json_file(path)
            self.assertEqual(journal["nodes"]["adx-node"]["stage"], "verify")
            self.assertEqual(self.task["nodes"]["adx-node"]["phase"], "verified")
            self.assertFalse(fault["replayed"])
            journal = self.drive(path)
        self.assertTrue(fault["replayed"])
        self.assertEqual(journal["stage"], "completed")

    def test_partial_quiesce_control_and_permit_persistence_recover_same_body(self):
        for applied in (False, True):
            for replacement in (False, True):
                with self.subTest(node_applied=applied, replacement=replacement):
                    self.setUp()
                    original_post, original_get = self.post, self.get_live
                    fault = {"body": None, "observation": None, "replayed": False}

                    def post(path, body, node_applied=applied, on_replacement=replacement,
                             send=original_post, read=original_get, *, progress):
                        matches = (path.endswith("/quiesce") and not body.get("delete_intent")
                                   and (body["pod_uid"] == "new-uid") == on_replacement)
                        if matches and progress["body"] is None:
                            previous = read("/v1/node-upgrades/nodes/adx-node/inspect")
                            node = self.task["nodes"]["adx-node"]
                            node.update(pod_uid=body["pod_uid"], node_session_id=body["node_session_id"],
                                        phase="quiescing", control_revision=node["control_revision"] + 1,
                                        permit_id=None, delete_resource_version=None)
                            progress["body"] = copy.deepcopy(body)
                            progress["observation"] = None if node_applied else previous
                            raise TimeoutError("control saved before permit persistence")
                        if matches and not progress["replayed"]:
                            self.assertEqual(body, progress["body"])
                            progress["replayed"] = True
                            progress["observation"] = None
                        return send(path, body)
                    post = partial(post, progress=fault)

                    def get(path, read=original_get, *, progress):
                        if path.endswith("/inspect") and progress["observation"] is not None:
                            return copy.deepcopy(progress["observation"])
                        return read(path)
                    get = partial(get, progress=fault)

                    with patch.object(self, "post", post), patch.object(self, "get_live", get), \
                            tempfile.TemporaryDirectory() as directory:
                        journal = self.drive(Path(directory) / "journal.json")
                    self.assertTrue(fault["replayed"])
                    self.assertEqual(journal["stage"], "completed")
                    verified = [b for p, b in self.writes if isinstance(p, str) and p.endswith("/verify-node")]
                    self.assertEqual(verified[0]["permit_id"], "new-permit")

    def test_status_only_rv_change_refreshes_patch_cas_intent(self):
        original = self.mutate
        attempted = []

        def mutate(args, body):
            if args[0] == "patch":
                changes = rollout.json.loads(args[args.index("--patch") + 1])
                attempted.append(changes)
                if len(attempted) == 1:
                    self.ds["metadata"]["resourceVersion"] = "11"
                    raise TimeoutError("PATCH not applied; status changed")
            return original(args, body)

        with patch.object(self, "mutate", mutate), tempfile.TemporaryDirectory() as directory:
            journal = self.drive(Path(directory) / "journal.json")
        self.assertEqual([p[1]["value"] for p in attempted], ["10", "11"])
        self.assertEqual(attempted[0][:1] + attempted[0][2:], attempted[1][:1] + attempted[1][2:])
        self.assertEqual(journal["patch"][1]["value"], "11")

    def test_unknown_patch_success_converges_without_repatch(self):
        self.lost.add("patch")
        with tempfile.TemporaryDirectory() as directory:
            self.drive(Path(directory) / "journal.json")
        patches = [a for a, _ in self.writes if isinstance(a, list) and a[0] == "patch"]
        self.assertEqual(len(patches), 1)

    def test_terminating_uid_waits_before_rv_mismatch(self):
        original = self.mutate

        def mutate(args, body):
            if args[0] == "delete":
                metadata = self.pods["items"][0]["metadata"]
                metadata.update(resourceVersion="21", deletionTimestamp="2026-11-11T00:00:00Z")
                raise TimeoutError("DELETE accepted; original terminating")
            return original(args, body)

        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                              self.post, mutate), sleep=lambda _: None, local=self.local)
            outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                        self.post, mutate), sleep=lambda _: None, local=self.local)
            self.assertEqual(outcome["status"], "pending")
            self.assertIn("terminating", outcome["detail"])
            self.assertNotIn("revoke", outcome["detail"])
            self.assertEqual(rollout.read_json_file(path)["nodes"]["adx-node"]["delete_rv"], "20")

    def test_original_uid_and_unchanged_rv_retry_identical_delete(self):
        self.retain_original = True
        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            for _ in range(2):
                outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                            self.post, self.mutate), sleep=lambda _: None, local=self.local)
                self.assertEqual(outcome["status"], "pending")
            self.assertEqual(rollout.read_json_file(path)["nodes"]["adx-node"]["delete_rv"], "20")
        deletes = [body["preconditions"] for args, body in self.writes
                   if isinstance(args, list) and args[0] == "delete"]
        self.assertEqual(deletes, [{"uid": "pod-uid", "resourceVersion": "20"}] * 2)
        self.assertFalse(self.deleted)

    def test_changed_pod_rv_reauthorizes_after_lost_delete_or_confirmation_reply(self):
        for lost_at in ("delete", "confirm-before-save", "confirm-after-save"):
            with self.subTest(lost_at=lost_at), tempfile.TemporaryDirectory() as directory:
                self.setUp()
                post = self.post
                first_confirmation = True

                def lose_confirmation(endpoint, body, send=post, persisted=lost_at == "confirm-after-save"):
                    nonlocal first_confirmation
                    if first_confirmation and body.get("delete_intent"):
                        first_confirmation = False
                        self.pods["items"][0]["metadata"]["resourceVersion"] = "21"
                        if persisted:
                            send(endpoint, body)
                        raise TimeoutError("lost delete confirmation reply")
                    return send(endpoint, body)

                if lost_at == "delete":
                    self.retain_original = self.change_rv = True
                else:
                    self.post = lose_confirmation
                try:
                    path = Path(directory) / "journal.json"
                    with patch.object(rollout.sys, "platform", "linux"):
                        pending = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                                    self.post, self.mutate), sleep=lambda _: None, local=self.local)
                    self.assertEqual(pending["status"], "pending")
                    self.retain_original = self.change_rv = False
                    self.action_losses.add("revoke-delete")
                    self.drive(path)
                finally:
                    self.post = post
                revokes = [b for p, b in self.writes if isinstance(p, str) and p.endswith("/revoke-delete")]
                self.assertTrue(revokes)
                self.assertTrue(all(body == revokes[0] for body in revokes))
                self.assertEqual(revokes[0]["expected_delete_resource_version"], "20")
                self.assertEqual(revokes[0]["pod_resource_version"], "21")
                confirmations = [(b["pod_resource_version"], b["permit_id"]) for p, b in self.writes
                                 if isinstance(p, str) and p.endswith("/quiesce") and b.get("delete_intent")]
                self.assertEqual(confirmations, [("20", "original-permit"), ("21", "renewed-permit")])
                deletes = [b["preconditions"] for p, b in self.writes if isinstance(p, list) and p[0] == "delete"]
                expected_rvs = ["20", "21"] if lost_at == "delete" else ["21"]
                self.assertEqual(deletes, [dict(uid="pod-uid", resourceVersion=rv) for rv in expected_rvs])

    def test_fake_jsonpatch_tests_are_enforced(self):
        changes = [{"op": "test", "path": "/metadata/uid", "value": "wrong"}] * 4
        changes.append({"op": "replace", "path": "/spec/template/spec/containers/0/image", "value": "new"})
        with self.assertRaises(AssertionError):
            self.mutate(["patch", "daemonset", "workers", "--namespace", "ns", "--type=json", "--patch",
                         rollout.canonical(changes), "-o", "json"], None)

    def test_stop_freezes_and_blocks_normal_resume(self):
        self.post("/v1/node-upgrades/tasks", self.snapshot["begin"])
        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            rollout.write_journal(path, dict(schema_version=1, plan_hash=rollout.template_hash(self.snapshot),
                                            stage="template", nodes={}))
            outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                        self.post, self.mutate), sleep=lambda _: None, mode="stop", local=self.local)
            self.assertEqual(outcome["status"], "stopped")
            with self.assertRaises(rollout.Refusal):
                rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                  self.post, self.mutate), local=self.local)

    def test_abort_untouched_original_finalizes_without_migration(self):
        self.post("/v1/node-upgrades/tasks", self.snapshot["begin"])
        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            rollout.write_journal(path, dict(schema_version=1, plan_hash=rollout.template_hash(self.snapshot),
                                            stage="template", nodes={}))
            outcome = rollout.apply_pod(self.snapshot, path, 0,
                                        rollout.PodClients(self.kube, self.get_live, self.post, self.mutate),
                                        sleep=lambda _: None, mode="abort", local=self.local)
        self.assertEqual(outcome["task_phase"], "Aborted")
        self.assertEqual(outcome["nodes"]["adx-node"]["outcome"], "original")

    def cleanup_seed(self, path, stage="nodes", delete=False, replacement=False):
        self.post("/v1/node-upgrades/tasks", self.snapshot["begin"])
        normal = {}
        if delete or replacement:
            node = self.task["nodes"]["adx-node"]
            node.update(phase="quiescing", control_revision=2, permit_id="original-permit",
                        delete_resource_version="20")
            normal = {"adx-node": dict(stage="delete", permit_id="original-permit", control_revision=2,
                                      delete_rv="20")}
        journal = dict(schema_version=1, plan_hash=rollout.template_hash(self.snapshot), stage=stage, nodes=normal)
        if replacement:
            journal["patch"] = [dict(op="test", path="/metadata/uid", value="ds-uid"),
                                dict(op="test", path="/metadata/resourceVersion", value="10")]
            self.ds["metadata"]["resourceVersion"] = "11"
            self.ds["spec"]["template"]["spec"]["containers"][0]["image"] = self.args.target_image
            self.mutate(["delete", "--raw", "/api/v1/namespaces/ns/pods/workers-a", "-f", "-"],
                        dict(apiVersion="v1", kind="DeleteOptions",
                             preconditions=dict(uid="pod-uid", resourceVersion="20")))
        rollout.write_journal(path, journal)
        return journal

    def cleanup_drive(self, path, mode="abort"):
        outcome = None
        for _ in range(12):
            with patch.object(rollout.sys, "platform", "linux"):
                outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                            self.post, self.mutate), sleep=lambda _: None, mode=mode, local=self.local)
            if outcome["status"] != "pending":
                return outcome
        return self.fail("cleanup did not converge: " + str(outcome))

    def test_local_stop_and_abort_never_begin(self):
        for mode, expected in (("stop", "stopped"), ("abort", "aborted")):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                self.setUp()
                outcome = self.cleanup_drive(Path(directory) / "journal.json", mode)
                self.assertEqual(outcome["status"], expected)
                self.assertFalse(outcome["remote_task"])
                self.assertEqual(self.writes, [])

    def test_begin_unknown_closes_same_begin_then_freezes(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            self.cleanup_seed(path, stage="begin_intent")
            self.lost.add("/v1/node-upgrades/tasks")
            self.assertEqual(self.cleanup_drive(path, "stop")["status"], "stopped")
            begins = [b for p, b in self.writes if p == "/v1/node-upgrades/tasks"]
            self.assertEqual(len(begins), 3)
            self.assertTrue(all(b == self.snapshot["begin"] for b in begins))

    def test_confirmed_initial_begin_rejection_allows_local_cleanup_without_replay(self):
        for stop in (False, True):
            with self.subTest(stop=stop):
                journal = dict(stage="new", nodes={})
                connection = HttpJsonTests.response(400, rollout.canonical({"error": {
                    "code": "InvalidArgument", "message": "invalid target image",
                }}).encode("utf-8"))

                def post(path, body):
                    return rollout.http_json("http://coordinator:9090", path, body)

                with patch.object(rollout.http.client, "HTTPConnection", return_value=connection):
                    with self.assertRaises(rollout.Refusal):
                        rollout.advance_pod(self.snapshot, journal, lambda: None, 0,
                                            rollout.PodClients(self.kube, self.get_live, post, self.mutate),
                                            local=self.local)
                post_again = MagicMock(side_effect=AssertionError("replayed rejected Begin"))
                unknown = MagicMock(side_effect=OSError("query unavailable"))
                with self.assertRaises(OSError):
                    rollout.advance_pod_cleanup(self.snapshot, journal, lambda: None, self.kube,
                                                unknown, post_again, self.mutate, stop=stop)
                self.post("/v1/node-upgrades/tasks", self.snapshot["begin"])
                existing = copy.deepcopy(journal)
                self.assertIsNone(rollout.advance_pod_cleanup(self.snapshot, existing, lambda: None, self.kube,
                                                              self.get_live, post_again, self.mutate, stop=stop))
                self.assertEqual(existing["stage"], "template")
                self.assertNotIn("local_terminal", existing["cleanup"])
                self.task = None
                missing = HttpJsonTests.response(404, rollout.canonical({"error": {
                    "code": "NotFound", "message": "task unavailable",
                }}).encode("utf-8"))

                def get(path):
                    return rollout.http_json("http://coordinator:9090", path)

                with patch.object(rollout.http.client, "HTTPConnection", return_value=missing):
                    outcome = rollout.advance_pod_cleanup(self.snapshot, journal, lambda: None, self.kube,
                                                          get, post_again, self.mutate, stop=stop)
                self.assertEqual(outcome, dict(status="stopped" if stop else "aborted",
                                               remote_task=False, effects=False))
                post_again.assert_not_called()

    def test_ambiguous_begin_replay_rejection_keeps_remote_outcome_unknown(self):
        self.post("/v1/node-upgrades/tasks", self.snapshot["begin"])
        journal = dict(stage="begin_intent", nodes={})
        connection = HttpJsonTests.response(400, rollout.canonical({"error": {
            "code": "InvalidArgument", "message": "replay rejected",
        }}).encode("utf-8"))

        def post(path, body):
            return rollout.http_json("http://coordinator:9090", path, body)

        with patch.object(rollout.http.client, "HTTPConnection", return_value=connection):
            with self.assertRaises(rollout.Refusal):
                rollout.advance_pod_cleanup(self.snapshot, journal, lambda: None, self.kube,
                                            self.get_live, post, self.mutate)
        self.assertNotIn("begin_rejected", journal)
        self.assertNotIn("local_terminal", journal["cleanup"])
        self.assertEqual(self.task["phase"], "Active")

    def test_cleanup_lost_action_success_replays_same_body(self):
        for loss in ("freeze", "revoke-delete", "restore-original", "verify-node", "finalize"):
            with self.subTest(loss=loss), tempfile.TemporaryDirectory() as directory:
                self.setUp()
                path = Path(directory) / "journal.json"
                self.cleanup_seed(path, delete=True, replacement=loss == "verify-node")
                self.action_losses.add(loss)
                if loss == "revoke-delete":
                    with patch.object(rollout.sys, "platform", "linux"):
                        pending = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                                    self.post, self.mutate), sleep=lambda _: None,
                                                    mode="abort", local=self.local)
                    self.assertEqual(pending["status"], "pending")
                    self.assertIsNone(self.task["nodes"]["adx-node"]["delete_resource_version"])
                    self.pods["items"][0]["metadata"]["resourceVersion"] = "30"
                outcome = self.cleanup_drive(path)
                self.assertEqual(outcome["task_phase"], "Aborted")
                attempts = []
                for request_path, body in self.writes:
                    if isinstance(request_path, str) and body.get("stage", request_path.rsplit("/", 1)[-1]) == loss:
                        attempts.append(body)
                if loss != "finalize":
                    self.assertGreaterEqual(len(attempts), 2)
                    self.assertEqual(attempts[0], attempts[1])
                else:
                    self.assertEqual(len(attempts), 1)
                self.assertFalse(self.action_losses)
                if loss == "revoke-delete":
                    self.assertFalse(any(isinstance(p, str) and p.endswith("/quiesce") for p, _ in self.writes))

    def test_typed_raw_items_without_typemeta_plan_and_apply(self):
        for item in self.pods["items"]:
            del item["apiVersion"]
            del item["kind"]
        self.snapshot = self.plan()
        with tempfile.TemporaryDirectory() as directory:
            journal = self.drive(Path(directory) / "journal.json")
        self.assertEqual(journal["stage"], "completed")

    def test_freeze_task_phase_alone_cannot_report_stopped(self):
        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            self.cleanup_seed(path)
            self.action_losses.add("freeze")
            outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                        self.post, self.mutate), sleep=lambda _: None, mode="stop", local=self.local)
            self.assertEqual(outcome["status"], "pending")
            self.assertEqual(self.task["phase"], "Aborting")
            self.assertFalse(rollout.read_json_file(path)["cleanup"].get("frozen", False))
            self.assertEqual(self.cleanup_drive(path, "stop")["status"], "stopped")

    def test_delete_unknown_fence_and_lost_patch_ack_revoke(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            self.cleanup_seed(path, delete=True)
            self.pods["items"][0]["metadata"]["resourceVersion"] = "25"
            self.lost.add("patch")
            outcome = self.cleanup_drive(path)
            self.assertEqual(outcome["task_phase"], "Aborted")
            revokes = [b for p, b in self.writes if isinstance(p, str) and p.endswith("/revoke-delete")]
            self.assertEqual(revokes[0]["expected_delete_resource_version"], "20")
            self.assertEqual(revokes[0]["pod_resource_version"], "26")
            self.assertEqual(rollout.read_json_file(path)["nodes"]["adx-node"]["delete_rv"], "20")

    def test_terminating_fence_ack_never_restores(self):
        original = self.mutate

        def mutate(args, body):
            original(args, body)
            if args[:2] == ["patch", "pod"]:
                self.pods["items"][0]["metadata"]["deletionTimestamp"] = "2026-11-11T00:00:00Z"
            return None

        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            self.cleanup_seed(path, delete=True)
            outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                        self.post, mutate), sleep=lambda _: None, mode="abort", local=self.local)
            self.assertEqual(outcome["status"], "pending")
            self.assertIn("terminating", outcome["detail"])
            self.assertFalse(any(isinstance(p, str) and p.endswith("/revoke-delete") for p, _ in self.writes))

    def test_delete_after_fence_fails_uid_rv_cas(self):
        self.pods["items"][0]["metadata"]["resourceVersion"] = "21"
        with self.assertRaises(rollout.Refusal):
            self.mutate(["delete", "--raw", "/api/v1/namespaces/ns/pods/workers-a", "-f", "-"],
                        dict(apiVersion="v1", kind="DeleteOptions",
                             preconditions=dict(uid="pod-uid", resourceVersion="20")))

    def test_mixed_retained_target_and_original_same_batch(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            journal = self.cleanup_seed(path, replacement=True)
            original_pod = copy.deepcopy(self.snapshot["pods"][0])
            original_pod.update(name="workers-0", uid="uid-0", node_id="original-node", kubernetes_node_name="host-0")
            original_pod["inspection"].update(node_id="original-node", observed_pod_uid="uid-0",
                                               node_session_id="session-0")
            original_pod["actual"]["node_session"] = "session-0"
            self.snapshot["pods"].insert(0, original_pod)
            binding = dict(node_id="original-node", pod_uid="uid-0", node_session_id="session-0")
            self.snapshot["begin"]["nodes"].insert(0, binding)
            self.snapshot["batches"] = [["original-node"], ["adx-node"]]
            # Exercise the reused driver with an earlier restored node in its selected batch.
            self.snapshot["batches"] = [["original-node", "adx-node"]]
            self.task["nodes"]["original-node"] = dict(binding, original_pod_uid="uid-0",
                original_session_id="session-0", phase="RestoredOriginal", control_revision=1,
                permit_id=None, delete_resource_version=None)
            self.task["phase"] = "Aborting"
            current = copy.deepcopy(self.pods["items"][0])
            current["metadata"].update(name="workers-0", uid="uid-0")
            current["spec"].update(nodeName="host-0")
            current["spec"]["containers"][0]["image"] = "old:v1"
            self.pods["items"].append(current)
            journal.update(plan_hash=rollout.template_hash(self.snapshot), cleanup=dict(frozen=True, nodes={
                "original-node": dict(outcome="restored", uid="uid-0", session="session-0", image="old:v1")}))
            rollout.write_journal(path, journal)
            get = self.get_live

            def read(url):
                if "/nodes/original-node/" in url:
                    return dict(protocol_version=3, node_id="original-node", observed_pod_uid="uid-0",
                                node_session_id="session-0", unknown=False, task_id="release-1", epoch=1,
                                phase="released", revision=1)
                if "node_id=original-node" in url:
                    return dict(node=dict(node_id="original-node", scheduling_paused=True))
                return get(url)

            # Direct advance accepts the same-batch scenario without relaxing planner canary validation.
            journal = rollout.read_json_file(path)
            with patch.object(self, "get_live", read):
                for _ in range(30):
                    outcome = rollout.advance_pod_cleanup(self.snapshot, journal, lambda: None, self.kube,
                                                          read, self.post, self.mutate, local=self.local)
                    if outcome is not None:
                        break
            self.assertEqual(outcome["task_phase"], "Aborted")
            self.assertTrue(outcome["mixed_images"])
            self.assertEqual(outcome["current_images"]["adx-node"], self.args.target_image)
            self.assertFalse(any(isinstance(p, str) and p.endswith("/migrate") for p, _ in self.writes))

    def test_unsettled_foreign_template_and_extra_owned_refuse_finalize(self):
        for fault in ("operation", "template", "extra", "same-name-new-uid"):
            with self.subTest(fault=fault), tempfile.TemporaryDirectory() as directory, \
                    patch.object(rollout.sys, "platform", "linux"):
                self.setUp()
                path = Path(directory) / "journal.json"
                self.cleanup_seed(path)
                if fault == "operation":
                    self.task["operations"]["unknown"] = dict(phase="Assigned")
                elif fault == "template":
                    self.ds["spec"]["template"]["spec"]["containers"][0]["image"] = "foreign:v1"
                elif fault == "extra":
                    extra = copy.deepcopy(self.pods["items"][0])
                    extra["metadata"].update(name="extra", uid="extra")
                    self.pods["items"].append(extra)
                else:
                    self.pods["items"][0]["metadata"]["uid"] = "imposter"
                outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                            self.post, self.mutate), sleep=lambda _: None,
                                            mode="abort", local=self.local)
                self.assertEqual(outcome["status"], "pending")
                self.assertFalse(any(b.get("stage") == "finalize" for p, b in self.writes if isinstance(p, str)))

    def test_late_target_patch_is_fenced_before_finalize(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            journal = self.cleanup_seed(path, stage="patch_intent")
            journal["patch"] = [dict(op="test", path="/metadata/uid", value="ds-uid"),
                                dict(op="test", path="/metadata/resourceVersion", value="10")]
            rollout.write_journal(path, journal)
            outcome = self.cleanup_drive(path)
            self.assertEqual(outcome["task_phase"], "Aborted")
            self.assertNotEqual(self.ds["metadata"]["resourceVersion"], "10")
            self.assertIn("adx.dev/rollout-template-fence", self.ds["metadata"]["annotations"])

    def test_terminal_reentry_reads_current_inventory_not_cached_images(self):
        for phase in ("Completed", "Aborted"):
            with self.subTest(phase=phase), tempfile.TemporaryDirectory() as directory:
                self.setUp()
                path = Path(directory) / "journal.json"
                self.cleanup_seed(path)
                self.task["phase"] = phase
                before = len(self.writes)
                self.assertEqual(self.cleanup_drive(path)["task_phase"], phase)
                self.assertEqual(len(self.writes), before)
                self.pods["items"][0]["metadata"]["uid"] = "unknown-now"
                with patch.object(rollout.sys, "platform", "linux"):
                    outcome = rollout.apply_pod(self.snapshot, path, 0, rollout.PodClients(self.kube, self.get_live,
                                                self.post, self.mutate), sleep=lambda _: None,
                                                mode="abort", local=self.local)
                self.assertEqual(outcome["status"], "pending")

    def test_restore_respects_updated_administrator_pause_intent(self):
        original_get = self.get_live

        def get(path):
            result = original_get(path)
            if path.startswith("/instance-manager/"):
                result["node"].update(scheduling_paused=True, accepting_allocations=False)
            return result

        with tempfile.TemporaryDirectory() as directory, patch.object(self, "get_live", get):
            path = Path(directory) / "journal.json"
            self.cleanup_seed(path, delete=True)
            self.assertFalse(self.task["nodes"]["adx-node"]["original_paused"])
            outcome = self.cleanup_drive(path)
            self.assertEqual(outcome["task_phase"], "Aborted")
            state = rollout.read_json_file(path)["cleanup"]["nodes"]["adx-node"]
            self.assertTrue(state["admission"]["scheduling_paused"])

    def test_replacement_cleanup_accepts_already_restored_template(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "journal.json"
            self.cleanup_seed(path, replacement=True)
            self.ds["spec"]["template"]["spec"]["containers"][0]["image"] = "old:v1"
            outcome = self.cleanup_drive(path)
            self.assertEqual(outcome["task_phase"], "Aborted")
            self.assertEqual(outcome["current_images"]["adx-node"], self.args.target_image)
            self.assertFalse(any(isinstance(p, str) and p.endswith("/migrate") for p, _ in self.writes))

    def test_daemonset_cross_host_replacement_is_rejected(self):
        self.replacement_host = "kube-node-2"
        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "rollout.json"
            for _ in range(40):
                result = rollout.apply_pod(self.snapshot, path, 0,
                                           rollout.PodClients(self.kube, self.get_live, self.post,
                                           self.mutate), sleep=lambda _: None, local=self.local)
            self.assertEqual(result["status"], "pending")
            self.assertTrue(self.deleted)
            self.assertNotIn("replacement_uid", rollout.read_json_file(path)["nodes"]["adx-node"])
        self.assertFalse(any(isinstance(p, str) and p.endswith("/verify-node") for p, _ in self.writes))

    def test_daemonset_replacement_helper_keeps_host_rule(self):
        moved = copy.deepcopy(self.pods["items"][0])
        moved["metadata"].update(uid="new-uid", name="workers-new")
        moved["spec"]["containers"][0]["image"] = self.args.target_image
        moved["spec"]["nodeName"] = "kube-node-2"
        with self.assertRaises(rollout.Refusal):
            rollout.pod_replacement(self.snapshot, self.snapshot["pods"][0], [moved])
        moved["spec"]["nodeName"] = "kube-node"
        _, evidence, host = rollout.pod_replacement(self.snapshot, self.snapshot["pods"][0], [moved])
        self.assertEqual((evidence["uid"], host), ("new-uid", "kube-node"))

    def test_replacement_faults_do_not_verify(self):
        for fault in ("uid", "session", "owner", "image"):
            with self.subTest(fault=fault):
                self.setUp()
                mutate = self.mutate

                def write(args, body, changing=fault, original=mutate):
                    original(args, body)
                    if args[0] == "delete":
                        if changing == "owner":
                            self.pods["items"][0]["metadata"]["ownerReferences"][0]["uid"] = "foreign"
                        if changing == "image":
                            self.pods["items"][0]["spec"]["containers"][0]["image"] = "foreign:v1"

                get = self.get_live

                def read(path, changing=fault, original=get):
                    value = original(path)
                    if self.deleted and path.endswith("/inspect"):
                        if changing == "uid":
                            value["observed_pod_uid"] = "foreign"
                        if changing == "session":
                            value["node_session_id"] = "session-1"
                    return value

                with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
                    outcome = rollout.apply_pod(self.snapshot, Path(directory) / "journal.json", 0,
                                                rollout.PodClients(self.kube, read, self.post, write),
                                                sleep=lambda _: None, local=self.local)
                self.assertEqual(outcome["status"], "pending")
                self.assertFalse(any(isinstance(p, str) and p.endswith("/verify-node") for p, _ in self.writes))

    def test_batch_admission_does_not_advance_next_batch(self):
        second = copy.deepcopy(self.snapshot["pods"][0])
        second.update(name="workers-b", uid="uid-b", node_id="node-b", kubernetes_node_name="host-b")
        second["inspection"].update(node_id="node-b", observed_pod_uid="uid-b", node_session_id="session-b")
        second["actual"]["node_session"] = "session-b"
        self.snapshot["pods"].append(second)
        self.snapshot["begin"]["nodes"].append(dict(node_id="node-b", pod_uid="uid-b", node_session_id="session-b"))
        self.snapshot["batches"].append(["node-b"])
        rollout.validate_pod_plan(self.snapshot)
        with tempfile.TemporaryDirectory() as directory, patch.object(rollout.sys, "platform", "linux"):
            path = Path(directory) / "journal.json"
            with self.assertRaises(rollout.Refusal):
                rollout.apply_pod(self.snapshot, path, 1, rollout.PodClients(self.kube, self.get_live,
                                  self.post, self.mutate), local=self.local)
            journal = dict(stage="nodes", nodes={"adx-node": dict(stage="verified", replacement_uid="new-uid",
                                                                   replacement_session="new-session")})
            self.task = dict(self.snapshot["begin"], kind="pod", protocol_version=3, phase="Active", operations={},
                             nodes={b["node_id"]: dict(b, original_pod_uid=b["pod_uid"],
                                                      original_session_id=b["node_session_id"], phase="pending")
                                    for b in self.snapshot["begin"]["nodes"]})
            self.task["nodes"]["adx-node"].update(phase="verified", pod_uid="new-uid", node_session_id="new-session")
            self.ds["spec"]["template"]["spec"]["containers"][0]["image"] = self.args.target_image
            self.assertTrue(rollout.advance_pod(self.snapshot, journal, lambda: None, 0, rollout.PodClients(self.kube,
                                                self.get_live, self.post, self.mutate), local=self.local))
            self.assertNotIn("node-b", journal["nodes"])
            self.assertEqual(self.writes, [])

    def test_raw_delete_transport_uses_bounded_stdin_and_explicit_context(self):
        process = MagicMock()
        process.stdout.read.return_value = b'{"kind":"Status"}'
        process.wait.return_value = 0
        process.poll.return_value = 0
        manager = MagicMock()
        manager.__enter__.return_value = process
        body = dict(apiVersion="v1", kind="DeleteOptions", preconditions=dict(uid="pod-uid", resourceVersion="20"))
        arguments = ["--context", "test-cluster", "delete", "--raw",
                     "/api/v1/namespaces/ns/pods/workers-a", "-f", "-"]
        with patch.object(rollout.shutil, "which", return_value="kubectl"), \
                patch.object(rollout.subprocess, "Popen", return_value=manager) as popen, \
                patch.object(rollout.threading, "Timer"):
            rollout.kubectl_json(arguments, body)
        self.assertEqual(popen.call_args.args[0][1:], ["--request-timeout=10s", *arguments])
        self.assertEqual(popen.call_args.kwargs["stdin"], rollout.subprocess.PIPE)
        process.stdin.write.assert_called_once_with(rollout.canonical(body).encode("utf-8"))
        process.stdin.close.assert_called_once()
        process.stdout.read.assert_called_once_with(rollout.LIMIT + 1)


class HttpJsonTests(unittest.TestCase):
    @staticmethod
    def response(status, body, content_type="application/json"):
        wire = (f"HTTP/1.1 {status} response\r\nContent-Type: {content_type}\r\n"
                f"Content-Length: {len(body)}\r\n\r\n").encode("ascii") + body
        socket = MagicMock()
        socket.makefile.return_value = io.BytesIO(wire)
        response = rollout.http.client.HTTPResponse(socket)
        response.begin()
        connection = MagicMock()
        connection.getresponse.return_value = response
        return connection

    def test_structured_http_error_keeps_coordinator_reason(self):
        connection = self.response(400, rollout.canonical({"error": {
            "code": "InvalidArgument", "message": "node recovery capabilities are unavailable or unverified",
        }}).encode("utf-8"))
        with patch.object(rollout.http.client, "HTTPConnection", return_value=connection), \
                self.assertRaises(rollout.Refusal) as rejected:
            rollout.http_json("http://coordinator:9090", "/v1/node-upgrades/tasks/task/verify-node", {})
        self.assertEqual(str(rejected.exception), "management request returned HTTP 400: InvalidArgument: "
                         "node recovery capabilities are unavailable or unverified")
        self.assertIsInstance(rejected.exception, rollout.ManagementError)
        self.assertEqual((rejected.exception.status, rejected.exception.code), (400, "InvalidArgument"))
        connection.request.assert_called_once()
        connection.close.assert_called_once()

    def test_unstructured_http_error_never_echoes_body(self):
        for content_type, body in (
                ("text/html", b"<html>not-for-diagnostics</html>"),
                ("application/json", b"{not-for-diagnostics"),
                ("application/json", b'{"error":"not-for-diagnostics"}'),
                ("application/json", b'{"error":{"code":7,"message":"not-for-diagnostics"}}'),
                ("application/json", b'{"error":{"code":"InvalidArgument"}}'),
                ("application/json", b'{"error":' + b"[" * 10000 + b"0" + b"]" * 10000 + b"}")):
            with self.subTest(content_type=content_type, size=len(body)):
                connection = self.response(400, body, content_type)
                with patch.object(rollout.http.client, "HTTPConnection", return_value=connection), \
                        self.assertRaises(rollout.Refusal) as rejected:
                    rollout.http_json("http://coordinator:9090", "/query")
                self.assertEqual(str(rejected.exception), "management request returned HTTP 400")
                connection.close.assert_called_once()

    def test_http_success_still_returns_json(self):
        connection = self.response(200, b'{"phase":"Active"}')
        with patch.object(rollout.http.client, "HTTPConnection", return_value=connection):
            self.assertEqual(rollout.http_json("http://coordinator:9090", "/query"), {"phase": "Active"})
        connection.close.assert_called_once()

    def test_http_error_body_read_failure_preserves_status(self):
        for status in (200, 400):
            for error_type in (OSError, rollout.http.client.HTTPException):
                with self.subTest(status=status, error_type=error_type):
                    connection = self.response(status, b"{}")
                    expected = rollout.Refusal if status == 400 else error_type
                    with patch.object(rollout.http.client, "HTTPConnection", return_value=connection), \
                            patch.object(connection.getresponse.return_value, "read1",
                                         side_effect=error_type("response read failed")), \
                            self.assertRaises(expected) as rejected:
                        rollout.http_json("http://coordinator:9090", "/query")
                    detail = "management request returned HTTP 400" if status == 400 else "response read failed"
                    self.assertEqual(str(rejected.exception), detail)
                    connection.close.assert_called_once()

    def test_http_error_body_obeys_response_size_limit(self):
        connection = self.response(400, b'{"error":{"code":"InvalidArgument","message":"' + b"x" * 100 + b'"}}')
        with patch.object(rollout.http.client, "HTTPConnection", return_value=connection), \
                patch.object(rollout, "LIMIT", 64), \
                self.assertRaisesRegex(rollout.Refusal, "management response exceeds size limit"):
            rollout.http_json("http://coordinator:9090", "/query")
        connection.close.assert_called_once()

    def test_http_error_body_obeys_original_deadline(self):
        connection = self.response(400, b'{"error":{"code":"InvalidArgument","message":"unavailable"}}')
        response = connection.getresponse.return_value
        with patch.object(rollout.http.client, "HTTPConnection", return_value=connection), \
                patch.object(rollout.time, "monotonic", side_effect=[0, rollout.TIMEOUT + 1]), \
                patch.object(response, "read1", wraps=response.read1) as read, \
                self.assertRaisesRegex(rollout.Refusal, "management request deadline exceeded"):
            rollout.http_json("http://coordinator:9090", "/query")
        read.assert_not_called()
        connection.close.assert_called_once()


if __name__ == "__main__":
    unittest.main()
