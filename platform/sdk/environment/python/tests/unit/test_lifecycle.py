import gc
import inspect
import os
import unittest
from unittest.mock import Mock, call, patch

import adx_environment
from adx_environment import ConnectionConfig, Environment


class _CloseTracker:
    def __init__(self):
        self.closed = False
        self.close_count = 0
        self.deleted = []

    def close(self):
        self.closed = True
        self.close_count += 1

    def delete(self, environment_id):
        self.deleted.append(environment_id)


class _Shells:
    def close(self):
        pass


class _PTY:
    def _close(self):
        pass


class LifecycleTests(unittest.TestCase):
    def test_get_port_url_ignores_legacy_environment_router_overrides(self):
        environment = object.__new__(Environment)
        environment._sid = "default-environment-1"
        environment._forwarded_ports = {8080}

        class Client:
            @staticmethod
            def _safe_id(environment_id):
                return environment_id

        environment._client = Client()
        with patch.dict(
            os.environ,
            {
                "ADX_SERVER_ADDRESS": "api-control:8888",
                "ADX_TLS": "1",
                "ADX_GATEWAY_ADDRESS": "frontend-gateway:9443",
                "ADX_GATEWAY_TLS": "1",
                "ADX_ENVIRONMENT_ROUTER_ADDRESS": "environment-router:8080",
                "ADX_ENVIRONMENT_ROUTER_TLS": "0",
            },
            clear=True,
        ):
            self.assertEqual(
                environment.get_port_url(8080),
                "https://frontend-gateway:9443/default-environment-1/8080",
            )

    def test_get_port_url_uses_gateway_tls_without_changing_pty_routing(self):
        environment = object.__new__(Environment)
        environment._sid = "default-environment-1"
        environment._forwarded_ports = {8080}

        class Client:
            @staticmethod
            def _safe_id(environment_id):
                return environment_id

        environment._client = Client()
        with patch.dict(
            os.environ,
            {
                "ADX_SERVER_ADDRESS": "api-control:8888",
                "ADX_TLS": "1",
                "ADX_GATEWAY_ADDRESS": "environment-router:9443",
                "ADX_GATEWAY_TLS": "1",
            },
            clear=True,
        ):
            self.assertEqual(
                environment.get_port_url(8080),
                "https://environment-router:9443/default-environment-1/8080",
            )

    def test_get_port_url_falls_back_to_control_plane_tls_setting(self):
        environment = object.__new__(Environment)
        environment._sid = "default-environment-1"
        environment._forwarded_ports = {8080}

        class Client:
            @staticmethod
            def _safe_id(environment_id):
                return environment_id

        environment._client = Client()
        with patch.dict(
            os.environ,
            {"ADX_SERVER_ADDRESS": "frontend:8888", "ADX_TLS": "1"},
            clear=True,
        ):
            self.assertEqual(
                environment.get_port_url(8080),
                "https://frontend:8888/default-environment-1/8080",
            )

    def test_explicit_connection_without_gateway_uses_control_plane_tls(self):
        environment = object.__new__(Environment)
        environment._sid = "default-environment-1"
        environment._forwarded_ports = {8080}
        environment._connection = ConnectionConfig(
            server_address="frontend:8443",
            token="secret",
            use_tls=True,
        )

        class Client:
            @staticmethod
            def _safe_id(environment_id):
                return environment_id

        environment._client = Client()
        self.assertEqual(
            environment.get_port_url(8080),
            "https://frontend:8443/default-environment-1/8080",
        )

    def test_pause_and_resume_are_synchronous_without_public_request_id(self):
        pause = getattr(Environment, "pause", None)
        resume = getattr(Environment, "resume", None)
        self.assertTrue(callable(pause), "Environment.pause must exist")
        self.assertTrue(callable(resume), "Environment.resume must exist")
        self.assertEqual(
            list(inspect.signature(pause).parameters),
            ["self", "retention", "timeout_seconds"],
        )
        self.assertEqual(
            list(inspect.signature(resume).parameters),
            ["self"],
        )

    def test_permanent_pause_returns_no_expiry_and_sends_explicit_retention(self):
        class Client(_CloseTracker):
            def pause(self, environment_id, retention, timeout_seconds=300):
                self.retention = retention
                return {"environmentId": environment_id, "snapshotId": "pause-forever", "size": 8192,
                        "state": "paused", "expiresAt": None}
        environment = object.__new__(Environment)
        environment._client = Client()
        environment._sid = "default-environment-1"
        environment._closed = False
        result = environment.pause(adx_environment.RetentionPolicy.forever())
        self.assertIsNone(result.expires_at)
        self.assertEqual(environment._client.retention, {"kind": "forever"})

    def test_reload_returns_bool_without_replacing_facades(self):
        class Client(_CloseTracker):
            def reload(self, environment_id):
                self.reload_arg = environment_id
                return {"success": True}

        environment = object.__new__(Environment)
        environment._client = Client()
        environment._sid = "default-environment-1"
        environment._closed = False
        environment._commands = object()
        environment._files = object()
        environment._shells = object()
        before = (environment._client, environment._commands, environment._files, environment._shells)

        self.assertIs(environment.reload(), True)
        self.assertEqual(environment._client.reload_arg, "default-environment-1")
        self.assertEqual(
            (environment._client, environment._commands, environment._files, environment._shells),
            before,
        )

    def test_reload_returns_false_when_closed_or_transport_fails(self):
        class Client(_CloseTracker):
            def reload(self, _environment_id):
                raise adx_environment.EnvironmentError("reload failed")

        environment = object.__new__(Environment)
        environment._client = Client()
        environment._sid = "default-environment-1"
        environment._closed = True
        self.assertIs(environment.reload(), False)
        environment._closed = False
        self.assertIs(environment.reload(), False)

    def test_pause_and_resume_return_typed_authoritative_results(self):
        pause_result_type = getattr(adx_environment, "PauseResult", None)
        resume_result_type = getattr(adx_environment, "ResumeResult", None)
        self.assertIsNotNone(pause_result_type, "PauseResult must be public")
        self.assertIsNotNone(resume_result_type, "ResumeResult must be public")

        class Client(_CloseTracker):
            def pause(self, environment_id, retention, timeout_seconds=300):
                self.pause_args = (environment_id, retention, timeout_seconds)
                return {
                    "environmentId": environment_id,
                    "snapshotId": "pause-123",
                    "size": 8192,
                    "state": "paused",
                    "expiresAt": 1_800_000_000,
                }

            def resume(self, environment_id):
                self.resume_arg = environment_id
                return {
                    "environmentId": environment_id,
                    "state": "running",
                    "routeAddress": "10.0.0.8:9000",
                    "functionProxyId": "proxy-a",
                    "nodeId": "node-a",
                    "portMappings": {"8080": 41080},
                }

        environment = object.__new__(Environment)
        environment._client = Client()
        environment._sid = "default-environment-1"
        environment._closed = False

        pause_result = environment.pause()
        resume_result = environment.resume()

        self.assertEqual(
            pause_result,
            pause_result_type(
                environment_id="default-environment-1",
                snapshot_id="pause-123",
                size=8192,
                state="paused",
                expires_at=1_800_000_000,
            ),
        )
        self.assertEqual(
            environment._client.pause_args,
            ("default-environment-1", None, 300),
        )
        self.assertEqual(
            resume_result,
            resume_result_type(
                environment_id="default-environment-1",
                state="running",
                route_address="10.0.0.8:9000",
                function_proxy_id="proxy-a",
                node_id="node-a",
                port_mappings={"8080": 41080},
            ),
        )
        self.assertEqual(environment._client.resume_arg, "default-environment-1")

    def test_pause_rejects_invalid_ttl_before_transport(self):
        environment = object.__new__(Environment)
        environment._client = _CloseTracker()
        environment._sid = "default-environment-1"
        environment._closed = False

        for value in (True, 0, -1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(TypeError, "retention"):
                    environment.pause(value)

    def test_delete_uses_environment_id_without_name_namespace_variant(self):
        self.assertEqual(
            list(inspect.signature(Environment.delete).parameters),
            ["environment_id", "connection"],
        )

    def test_delete_accepts_explicit_connection_config(self):
        seen = {}

        class Client(_CloseTracker):
            def __init__(self, *, connection):
                super().__init__()
                seen["connection"] = connection
                seen["client"] = self

        connection = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
        )
        with (
            patch("adx_environment.environment_api.EnvironmentClient", Client),
            patch.dict(os.environ, {}, clear=True),
        ):
            Environment.delete("environment-1", connection=connection)

        self.assertIs(seen["connection"], connection)
        self.assertEqual(seen["client"].deleted, ["environment-1"])
        self.assertTrue(seen["client"].closed)

    def test_detached_kill_closes_local_client_without_deleting_remote(self):
        environment = object.__new__(Environment)
        environment._detached = True
        environment._tunnel_client = None
        environment._shells = _Shells()
        environment._pty = _PTY()
        environment._client = _CloseTracker()
        environment._sid = "environment-1"
        environment._closed = False

        environment.kill()

        self.assertTrue(environment._client.closed)

    def test_close_releases_local_resources_without_deleting_remote(self):
        environment = object.__new__(Environment)
        environment._detached = False
        environment._tunnel_client = None
        environment._shells = _Shells()
        environment._pty = _PTY()
        environment._client = _CloseTracker()
        environment._sid = "environment-1"
        environment._closed = False

        environment.close()
        environment.close()

        self.assertEqual(environment._client.deleted, [])
        self.assertEqual(environment._client.close_count, 1)

    def test_kill_is_idempotent(self):
        environment = object.__new__(Environment)
        environment._detached = False
        environment._tunnel_client = None
        environment._shells = _Shells()
        environment._pty = _PTY()
        environment._client = _CloseTracker()
        environment._sid = "environment-1"
        environment._closed = False

        environment.kill()
        environment.kill()

        self.assertEqual(environment._client.deleted, ["environment-1"])
        self.assertEqual(environment._client.close_count, 1)

    def test_garbage_collection_does_not_delete_remote_environment(self):
        environment = object.__new__(Environment)
        environment._detached = False
        environment._tunnel_client = None
        environment._shells = _Shells()
        environment._pty = _PTY()
        client = _CloseTracker()
        environment._client = client
        environment._sid = "environment-1"
        environment._closed = False
        del environment
        gc.collect()
        self.assertEqual(client.deleted, [])

    def test_context_exit_preserves_original_exception(self):
        environment = object.__new__(Environment)
        environment._detached = False
        environment._tunnel_client = None
        environment._shells = _Shells()
        environment._pty = _PTY()
        environment._sid = "environment-1"
        environment._closed = False

        class FailingDelete(_CloseTracker):
            def delete(self, _environment_id):
                raise RuntimeError("delete failed")

        client = FailingDelete()
        environment._client = client
        with self.assertRaisesRegex(ValueError, "body failed"):
            with environment:
                raise ValueError("body failed")
        self.assertTrue(client.closed)

    def test_tunnel_start_failure_rolls_back_created_environment(self):
        tracker = _CloseTracker()

        class Client:
            token = "token"

            def __init__(self):
                pass

            @staticmethod
            def create_info(_body):
                return {"environmentId": "environment-1", "status": "running"}

            @staticmethod
            def delete(environment_id):
                tracker.delete(environment_id)

            @staticmethod
            def close():
                tracker.close()

            @staticmethod
            def _safe_id(environment_id):
                return environment_id

        class Tunnel:
            def __init__(self, _target, token=None, environment_id=None):
                self.token = token
                self.environment_id = environment_id

            @staticmethod
            def start(_url, timeout=60):
                return False

            def stop(self):
                pass

        with (
            patch("adx_environment.environment_api.EnvironmentClient", Client),
            patch("adx_environment.tunnel_client.TunnelClient", Tunnel),
            patch.dict(
                os.environ,
                {"ADX_GATEWAY_ADDRESS": "frontend:8080", "ADX_GATEWAY_TLS": "0"},
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "connection timeout"):
                Environment(
                    image="ubuntu:22.04",
                    upstream="127.0.0.1:9000",
                    tunnel_connect_timeout=0.1,
                    detached=True,
                )

        self.assertEqual(tracker.deleted, ["environment-1"])
        self.assertEqual(tracker.close_count, 1)

    def test_tunnel_uses_explicit_gateway_and_token_without_environment(self):
        seen = {}

        class Client(_CloseTracker):
            def __init__(self, *, connection):
                super().__init__()
                self.token = connection.token

            def create_info(self, _body):
                return {
                    "environmentId": "environment-1",
                    "status": "running",
                    "tunnel": {"path": "/tunnel/environment-1"},
                }

            def set_direct_enabled(self, _enabled):
                pass

            @staticmethod
            def _safe_id(environment_id):
                return environment_id

        class Tunnel:
            def __init__(self, _target, token=None, environment_id=None):
                seen["token"] = token
                seen["environment_id"] = environment_id

            @staticmethod
            def start(url, timeout=60):
                seen["url"] = url
                return True

            def stop(self):
                pass

        connection = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
            gateway_address="gateway.example:8443",
            gateway_use_tls=True,
        )
        with (
            patch("adx_environment.environment_api.EnvironmentClient", Client),
            patch("adx_environment.tunnel_client.TunnelClient", Tunnel),
            patch.dict(os.environ, {}, clear=True),
        ):
            environment = Environment(
                image="ubuntu:22.04",
                upstream="127.0.0.1:9000",
                connection=connection,
                detached=True,
            )
            environment.close()

        self.assertEqual(seen["url"], "wss://gateway.example:8443/tunnel/environment-1")
        self.assertEqual(seen["token"], "secret")
        self.assertEqual(seen["environment_id"], "environment-1")

    def test_update_network_policy_replaces_and_clears_policy(self):
        class Client(_CloseTracker):
            def update_network_policy(self, environment_id, policy):
                self.updates = getattr(self, "updates", [])
                self.updates.append((environment_id, policy))
                return {"success": True}

        environment = object.__new__(Environment)
        environment._client = Client()
        environment._sid = "default-environment-1"
        environment._closed = False

        environment.update_network_policy(adx_environment.NetworkPolicy.block())
        environment.update_network_policy(adx_environment.NetworkPolicy())
        environment.update_network_policy(None)

        self.assertEqual(
            environment._client.updates,
            [
                ("default-environment-1", {"blockNetwork": True}),
                ("default-environment-1", {}),
                ("default-environment-1", {}),
            ],
        )
        with self.assertRaises(TypeError):
            environment.update_network_policy({"blockNetwork": True})
        environment._closed = True
        with self.assertRaises(RuntimeError):
            environment.update_network_policy(None)

    def test_network_update_reconnects_owned_tunnel_only_after_success(self):
        for outcome in ("success", "unacknowledged", "transport-error"):
            with self.subTest(outcome=outcome):
                self.check_network_update_reconnect(outcome)

    def check_network_update_reconnect(self, outcome):
        client = Mock()
        client.create_info.return_value = {"environmentId": "environment-1", "status": "running"}
        client.update_network_policy.return_value = {"success": outcome == "success"}
        if outcome == "transport-error":
            client.update_network_policy.side_effect = OSError("connection lost")
        tunnel = Mock()
        tunnel.start.return_value = True
        events = Mock()
        events.attach_mock(client.update_network_policy, "update")
        events.attach_mock(tunnel.reconnect, "reconnect")
        connection = ConnectionConfig(server_address="localhost:8889", token="test", use_tls=False)
        with (
            patch("adx_environment.environment_api.EnvironmentClient", return_value=client),
            patch("adx_environment.tunnel_client.TunnelClient", return_value=tunnel),
            Environment(image="test", upstream="127.0.0.1:9000", connection=connection, detached=True) as environment,
        ):
            if outcome == "success":
                environment.update_network_policy(None)
                self.assertEqual(events.mock_calls, [call.update("environment-1", {}), call.reconnect()])
            else:
                error = OSError if outcome == "transport-error" else adx_environment.EnvironmentError
                with self.assertRaises(error):
                    environment.update_network_policy(None)
                self.assertEqual(events.mock_calls, [call.update("environment-1", {})])


if __name__ == "__main__":
    unittest.main()
