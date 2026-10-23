import inspect
import os
import unittest
from dataclasses import fields
from unittest.mock import patch

from adx_environment import (
    CommandInfo,
    ConnectionConfig,
    Mount,
    CreateOptions,
    NodeInfo,
    S3Config,
    EnvironmentInfo,
    resources,
)


class TypesTests(unittest.TestCase):
    def test_connection_config_is_immutable_and_redacts_token(self):
        config = ConnectionConfig(
            server_address=" frontend.example:443/ ",
            token=" secret-token ",
            gateway_address=" gateway.example:8080/ ",
        )
        self.assertTrue(type(config).__dataclass_params__.frozen)
        self.assertEqual(config.server_address, "frontend.example:443")
        self.assertEqual(config.gateway_address, "gateway.example:8080")
        self.assertEqual(config.token, "secret-token")
        self.assertNotIn("secret-token", repr(config))

    def test_connection_config_snapshots_environment(self):
        with patch.dict(
            os.environ,
            {
                "ADX_SERVER_ADDRESS": "frontend.example:443",
                "ADX_TOKEN": "secret",
                "ADX_TLS": "0",
                "ADX_GATEWAY_ADDRESS": "gateway.example:8080",
                "ADX_GATEWAY_TLS": "1",
            },
            clear=True,
        ):
            config = ConnectionConfig.from_env()

        self.assertEqual(config.server_address, "frontend.example:443")
        self.assertEqual(config.gateway_address, "gateway.example:8080")
        self.assertFalse(config.use_tls)
        self.assertTrue(config.gateway_use_tls)

    def test_types_are_frozen(self):
        node = NodeInfo(
            id="node-a",
            status=0,
            capacity={"cpu": 1000.0},
            allocatable={"cpu": 800.0},
            labels={"arch": "arm64"},
        )
        self.assertTrue(type(node).__dataclass_params__.frozen)

    def test_akernel_value_type_fields_match(self):
        self.assertEqual(
            [item.name for item in fields(EnvironmentInfo)],
            ["id", "state", "cpu", "memory", "image", "failure"],
        )
        self.assertEqual(
            [item.name for item in fields(CommandInfo)],
            [
                "pid",
                "command",
                "running",
                "id",
                "status",
                "exit_code",
                "created_at",
                "started_at",
                "finished_at",
                "exit_signal",
            ],
        )
        self.assertEqual(
            [item.name for item in fields(NodeInfo)],
            ["id", "status", "capacity", "allocatable", "labels"],
        )

    def test_s3config_to_dict_and_redaction(self):
        cfg = S3Config(
            endpoint="https://s3.example",
            bucket="img",
            object="base",
            access_key="AK",
            secret_key="SK",
        )
        payload = cfg.to_dict()
        self.assertEqual(payload["object"], "base")
        self.assertIn("secretKey", payload)
        self.assertEqual(
            "SK",
            payload["secretKey"],
            "secret key must be carried in payload, but hidden from repr",
        )
        self.assertNotIn("SK", repr(cfg))

    def test_mount_is_immutable(self):
        m = Mount(target="/mnt/data", image_url="file://local")
        self.assertTrue(type(m).__dataclass_params__.frozen)

    def test_mount_rejects_relative_target(self):
        with self.assertRaisesRegex(ValueError, "absolute"):
            Mount(target="mnt/data", image_url="file://local")

    def test_mount_rejects_empty_image_url(self):
        with self.assertRaisesRegex(ValueError, "non-empty"):
            Mount(target="/mnt/data", image_url="")

    def test_local_mount_carries_shared_workspace_contract(self):
        mount = Mount(
            target="/home/jiuwen", host_path="/home/agentos/users/user-1",
            options=("rw", "nodev"), create_if_missing=True,
            create_options=CreateOptions(uid=10001, gid=10001, mode="0700"),
        )
        self.assertEqual(mount.to_dict(), {
            "type": "bind", "target": "/home/jiuwen",
            "host_path": "/home/agentos/users/user-1",
            "options": ["rw", "nodev"], "create_if_missing": True,
            "create_options": {"uid": 10001, "gid": 10001, "mode": "0700"},
        })

    def test_local_mount_rejects_ambiguous_or_unsafe_sources(self):
        for kwargs in (
            {"host_path": "relative/user"},
            {"host_path": "/shared/../other"},
            {"host_path": "/shared/user", "image_url": "image:v1"},
            {"host_path": "/shared/user", "options": ("ro", "rw")},
            {"host_path": "/shared/user", "options": ("unknown",)},
            {"host_path": "/shared/user", "type": "erofs", "create_if_missing": True},
            {"image_url": "image:v1", "create_if_missing": True},
            {"host_path": "/shared/user", "create_options": CreateOptions(10001, 10001, "0700")},
            {"host_path": "/shared/user", "create_if_missing": True, "create_options": {}},
        ):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Mount(target="/workspace", **kwargs)

        for invalid in (
            {"uid": -1}, {"gid": 4294967295}, {"uid": True}, {"uid": "10001"},
            {"mode": "755"}, {"mode": "0788"}, {"mode": "1777"}, {"mode": 700},
        ):
            values = {"uid": 10001, "gid": 10001, "mode": "0700", **invalid}
            with self.subTest(values=values), self.assertRaises(ValueError):
                CreateOptions(**values)

    def test_mount_options_are_immutable_and_legacy_sources_remain_read_only(self):
        options = ["rw"]
        mount = Mount(target="/workspace", host_path="/shared/user", options=options)
        options.append("ro")
        self.assertEqual(mount.to_dict()["options"], ["rw"])
        self.assertEqual(Mount(target="/tool", image_url="image:v1").to_dict()["options"], ["ro"])

    def test_resources_uses_process_configuration(self):
        parameters = inspect.signature(resources).parameters
        self.assertEqual(list(parameters), ["connection"])
        self.assertIsNone(parameters["connection"].default)


if __name__ == "__main__":
    unittest.main()
