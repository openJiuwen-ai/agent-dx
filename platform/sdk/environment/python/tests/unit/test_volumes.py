import unittest
from adx_environment import Mount, VolumeSpec, DirectoryVolumeSource


class VolumeTests(unittest.TestCase):
    def test_volume_reference_is_exclusive_with_inline_sources(self):
        mount = Mount(target="/workspace", volume_id="workspace", volume_uid="uid", options=("rw",))
        self.assertEqual(
            mount.to_dict(),
            {"type": "bind", "target": "/workspace", "options": ["rw"], "volume_id": "workspace", "volume_uid": "uid"},
        )
        with self.assertRaises(ValueError):
            Mount(target="/workspace", volume_id="workspace", host_path="/mnt/data")
        with self.assertRaises(ValueError):
            Mount(target="/workspace", volume_id="workspace", create_if_missing=True)
        with self.assertRaises(ValueError):
            Mount(target="/workspace", volume_uid="uid", host_path="/mnt/data")

    def test_definition_defaults_to_shared_and_has_no_node_path(self):
        spec = VolumeSpec(
            id="workspace", source=DirectoryVolumeSource(storage_domain="sfs", path="workspaces/tenant/user")
        )
        self.assertEqual(
            spec.to_dict(),
            {
                "id": "workspace",
                "source": {"type": "directory", "storage_domain": "sfs", "path": "workspaces/tenant/user"},
                "access_mode": "shared",
                "create_if_missing": False,
            },
        )
        with self.assertRaises(ValueError):
            DirectoryVolumeSource(storage_domain="sfs", path="../escape")

    def test_management_methods_use_the_returned_definition_and_close_clients(self):
        from unittest.mock import Mock, patch
        from adx_environment import Environment

        spec = VolumeSpec(
            id="workspace", source=DirectoryVolumeSource(storage_domain="sfs", path="workspaces/tenant/user")
        )
        existing = spec.to_dict()
        existing["access_mode"] = "exclusive"
        response = {
            "uid": "fixed-uid",
            "spec": existing,
            "revision": 1,
            "snapshot_ids": ["snapshot"],
            "initialization": {"state": "ready", "error": ""},
        }
        client = Mock()
        client.ensure_volume.return_value = response
        client.get_volume.return_value = response
        client.list_volumes.return_value = {"items": [response], "nextPageToken": "next"}
        with patch("adx_environment.environment_api.EnvironmentClient", return_value=client):
            volume = Environment.ensure_volume(spec)
            self.assertEqual(volume.uid, "fixed-uid")
            self.assertEqual(volume.snapshot_ids, ("snapshot",))
            self.assertEqual(volume.spec.access_mode, "exclusive")
            self.assertEqual(Environment.get_volume("workspace").initialization["state"], "ready")
            page, token = Environment.list_volumes(page_size=1)
            self.assertEqual(page[0].uid, "fixed-uid")
            self.assertEqual(token, "next")
            Environment.delete_volume("workspace")
        client.ensure_volume.assert_called_once_with(spec.to_dict())
        self.assertEqual(client.close.call_count, 4)


class VolumeIdentifierTests(unittest.TestCase):
    def test_definition_and_mount_share_url_safe_ascii_ids(self):
        source = DirectoryVolumeSource(storage_domain="sfs", path="workspaces/user")
        for volume_id in ("workspace.A_0-z", "a" * 256):
            VolumeSpec(id=volume_id, source=source)
            Mount(target="/workspace", volume_id=volume_id)
        for volume_id in ("", "a/b", "a\\b", "$id", "{id}", "a b", "a%2Fb", "中文", "a?b", "a#b", ".", "..", "a" * 257):
            with self.subTest(volume_id=volume_id):
                with self.assertRaises(ValueError):
                    VolumeSpec(id=volume_id, source=source)
                with self.assertRaises(ValueError):
                    Mount(target="/workspace", volume_id=volume_id)

    def test_invalid_management_ids_fail_before_creating_a_client(self):
        from unittest.mock import patch
        from adx_environment import Environment
        with patch("adx_environment.environment_api.EnvironmentClient") as client:
            for volume_id in ("a/b", "$id", "中文", "a%2Fb", ".", ".."):
                for operation in (Environment.get_volume, Environment.delete_volume):
                    with self.assertRaises(ValueError):
                        operation(volume_id)
            client.assert_not_called()
