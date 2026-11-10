"""Creation-time timeout propagation across the reverse-tunnel peers."""

import os
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import Mock, patch

from adx_environment import ConnectionConfig, Environment
from adx_environment._transport import EnvironmentClient


TIMEOUT_KEY = "ADX_TUNNEL_HTTP_TIMEOUT"


class TunnelTimeoutTests(unittest.TestCase):
    def setUp(self):
        self.client = Mock(spec=EnvironmentClient)
        self.client.create_info.return_value = {
            "environmentId": "timeout-test",
            "newlyCreated": True,
        }
        self.client.token = "test-token"
        self.client_patch = patch(
            "adx_environment.environment_api.EnvironmentClient", return_value=self.client
        )
        self.client_patch.start()
        self.addCleanup(self.client_patch.stop)
        self.tunnel_patch = patch("adx_environment.tunnel_client.TunnelClient")
        self.tunnel = self.tunnel_patch.start()
        self.addCleanup(self.tunnel_patch.stop)
        self.connection = ConnectionConfig(
            server_address="localhost:8888", token="test-token", use_tls=False
        )

    def create_environment(self, **kwargs):
        environment = Environment(
            image="python:3.12-slim",
            connection=self.connection,
            detached=True,
            **kwargs,
        )
        self.addCleanup(environment.close)
        return self.client.create_info.call_args.args[0]

    def test_fresh_create_uses_one_timeout_for_both_peers(self):
        cases = (
            (None, None, 600.0),
            ("43200", None, 43200.0),
            ("43200", "1200", 1200.0),
            ("43200", "0.25", 0.25),
            ("invalid", None, 600.0),
            ("43200", "invalid", 600.0),
            ("nan", None, 600.0),
            ("inf", None, 600.0),
            ("-1", None, 600.0),
            ("0", None, 600.0),
        )
        for process_value, explicit_value, expected in cases:
            with self.subTest(process=process_value, explicit=explicit_value):
                process_env = {} if process_value is None else {TIMEOUT_KEY: process_value}
                environment_env = {"USER_SETTING": "preserved"}
                if explicit_value is not None:
                    environment_env[TIMEOUT_KEY] = explicit_value
                original = dict(environment_env)
                with patch.dict(os.environ, process_env, clear=True):
                    body = self.create_environment(upstream="localhost:8000", env=environment_env)
                    self.assertEqual(dict(os.environ), process_env)
                self.assertEqual(environment_env, original)
                self.assertEqual(body["env"]["USER_SETTING"], "preserved")
                self.assertEqual(float(body["env"][TIMEOUT_KEY]), expected)
                self.assertEqual(self.tunnel.call_args.kwargs["http_timeout"], expected)

    def test_non_tunnel_creation_does_not_inherit_process_timeout(self):
        with patch.dict(os.environ, {TIMEOUT_KEY: "43200"}):
            body = self.create_environment(env={"USER_SETTING": "preserved"})
        self.assertEqual(body["env"], {"USER_SETTING": "preserved"})
        self.tunnel.assert_not_called()

    def test_snapshot_restore_does_not_inject_timeout_into_captured_server(self):
        with patch.dict(os.environ, {TIMEOUT_KEY: "43200"}):
            body = self.create_environment(snapshot_id="snapshot-1", upstream="localhost:8000")
        self.assertNotIn("env", body)

    def test_parallel_environments_keep_independent_timeouts(self):

        def create_info(body):
            return {
                "environmentId": "timeout-" + body["env"][TIMEOUT_KEY],
                "newlyCreated": True,
            }

        self.client.create_info.side_effect = create_info
        with patch.dict(os.environ, {TIMEOUT_KEY: "43200"}):
            with ThreadPoolExecutor(max_workers=2) as executor:
                environments = list(executor.map(self.create_with_timeout, (1200, 7200)))
            self.assertEqual(os.environ[TIMEOUT_KEY], "43200")
        bodies = [call.args[0] for call in self.client.create_info.call_args_list]
        self.assertEqual(sorted(float(body["env"][TIMEOUT_KEY]) for body in bodies), [1200, 7200])
        client_timeouts = {
            call.kwargs["environment_id"]: call.kwargs["http_timeout"]
            for call in self.tunnel.call_args_list
        }
        self.assertEqual(client_timeouts, {"timeout-1200.0": 1200, "timeout-7200.0": 7200})
        for environment in environments:
            environment.close()

    def create_with_timeout(self, seconds):
        return Environment(
            image="python:3.12-slim",
            connection=self.connection,
            detached=True,
            upstream="localhost:8000",
            env={TIMEOUT_KEY: str(seconds)},
        )
