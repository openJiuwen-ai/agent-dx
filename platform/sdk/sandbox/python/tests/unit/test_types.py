import inspect
import os
import unittest
from dataclasses import fields
from unittest.mock import patch

from adx_sandbox import (
    CommandInfo,
    ConnectionConfig,
    Mount,
    NodeInfo,
    S3Config,
    SandboxInfo,
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
            [item.name for item in fields(SandboxInfo)],
            ["id", "state", "cpu", "memory", "image"],
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

    def test_resources_uses_process_configuration(self):
        parameters = inspect.signature(resources).parameters
        self.assertEqual(list(parameters), ["connection"])
        self.assertIsNone(parameters["connection"].default)


if __name__ == "__main__":
    unittest.main()
