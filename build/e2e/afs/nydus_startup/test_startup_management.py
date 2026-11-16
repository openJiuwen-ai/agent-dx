"""Contracts at the boundary of the Nydus startup tool."""
from pathlib import Path
import sys
import unittest
from unittest.mock import patch
from urllib.error import HTTPError

sys.path.append(str(Path(__file__).resolve().parents[4] / "tools" / "nydus-startup"))
from startup import choose_targets, run_job, warm_payload


class StartupManagementTests(unittest.TestCase):
    def test_worker_and_durable_targets_come_from_snapshot(self):
        snapshot = {
            "snapshot_revision": 42,
            "nodes": [
                {"node_id": "a", "online": True, "has_dfs_storage": True,
                 "deployment": "test", "role": "seed", "connectors": ["nydus"]},
                {"node_id": "b", "online": True, "has_dfs_storage": True,
                 "deployment": "test", "role": "worker", "connectors": ["nydus"]},
                {"node_id": "c", "online": False, "has_dfs_storage": True,
                 "deployment": "test", "role": "worker", "connectors": ["nydus"]},
                {"node_id": "d", "online": True, "has_dfs_storage": True,
                 "deployment": "other", "role": "worker", "connectors": ["nydus"]},
            ],
        }
        self.assertEqual([node["node_id"] for node in choose_targets(snapshot, "test", "prewarm")], ["b"])
        self.assertEqual([node["node_id"] for node in choose_targets(snapshot, "test", "seed")], ["a", "b", "d"])
        with self.assertRaisesRegex(ValueError, "not eligible"):
            choose_targets(snapshot, "test", "seed", ["c"])

    def test_warm_payload_maps_plan_fields_and_rejects_another_image(self):
        image = {"source": "lab", "manifest_digest": "sha256:manifest"}
        plan = {"manifest": "sha256:manifest", "ranges": [
            {"blob": "sha256:blob", "offset": 32, "length": 64},
        ]}
        self.assertEqual(
            warm_payload(image, plan, "request"),
            {"source": "lab", "manifest_digest": "sha256:manifest", "request_id": "request",
             "ranges": [{"blob_digest": "sha256:blob", "offset": 32, "length": 64}]},
        )
        plan["manifest"] = "sha256:other"
        with self.assertRaisesRegex(ValueError, "manifest"):
            warm_payload(image, plan, "request")

    def test_job_restarts_on_accepting_node_with_same_request_id(self):
        node = {"node_id": "afs-b", "management_rest_addr": "https://b.example.test"}
        payload = {"request_id": "worker-request"}
        missing = HTTPError(node["management_rest_addr"], 404, "restarted", {}, None)
        with patch("startup.secret", return_value="token"), patch("startup.time.sleep"):
            with patch("startup.admin") as admin:
                admin.side_effect = [
                    {"job_id": "first", "state": "queued"}, missing,
                    {"job_id": "second", "state": "queued"}, {"job_id": "second", "state": "ready"},
                ]
                reply = run_job(node, "prewarm", payload, {})
        self.assertEqual(reply["node_id"], "afs-b")
        self.assertEqual(reply["job_id"], "second")
        self.assertEqual([call.args[0] for call in admin.call_args_list], [node["management_rest_addr"]] * 4)
        self.assertEqual(admin.call_args_list[0], admin.call_args_list[2])
        self.assertEqual(admin.call_args_list[3].args[1], "/v1/admin/nydus/prewarm-jobs/second")
