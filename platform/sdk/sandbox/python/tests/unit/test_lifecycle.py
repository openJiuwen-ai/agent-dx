import inspect
import os
import unittest
from unittest.mock import patch

import adx_sandbox
from adx_sandbox import ConnectionConfig, Sandbox


class _CloseTracker:
    def __init__(self):
        self.closed = False
        self.close_count = 0
        self.deleted = []

    def close(self):
        self.closed = True
        self.close_count += 1

    def delete(self, sandbox_id):
        self.deleted.append(sandbox_id)


class _Shells:
    def close(self):
        pass


class _PTY:
    def _close(self):
        pass


class LifecycleTests(unittest.TestCase):
    def test_get_port_url_ignores_legacy_sandbox_router_overrides(self):
        sandbox = object.__new__(Sandbox)
        sandbox._sid = "default-sandbox-1"
        sandbox._forwarded_ports = {8080}

        class Client:
            @staticmethod
            def _safe_id(sandbox_id):
                return sandbox_id

        sandbox._client = Client()
        with patch.dict(
            os.environ,
            {
                "ADX_SERVER_ADDRESS": "api-control:8888",
                "ADX_TLS": "1",
                "ADX_GATEWAY_ADDRESS": "frontend-gateway:9443",
                "ADX_GATEWAY_TLS": "1",
                "ADX_SANDBOX_ROUTER_ADDRESS": "sandbox-router:8080",
                "ADX_SANDBOX_ROUTER_TLS": "0",
            },
            clear=True,
        ):
            self.assertEqual(
                sandbox.get_port_url(8080),
                "https://frontend-gateway:9443/default-sandbox-1/8080",
            )

    def test_get_port_url_uses_gateway_tls_without_changing_pty_routing(self):
        sandbox = object.__new__(Sandbox)
        sandbox._sid = "default-sandbox-1"
        sandbox._forwarded_ports = {8080}

        class Client:
            @staticmethod
            def _safe_id(sandbox_id):
                return sandbox_id

        sandbox._client = Client()
        with patch.dict(
            os.environ,
            {
                "ADX_SERVER_ADDRESS": "api-control:8888",
                "ADX_TLS": "1",
                "ADX_GATEWAY_ADDRESS": "sandbox-router:9443",
                "ADX_GATEWAY_TLS": "1",
            },
            clear=True,
        ):
            self.assertEqual(
                sandbox.get_port_url(8080),
                "https://sandbox-router:9443/default-sandbox-1/8080",
            )

    def test_get_port_url_falls_back_to_control_plane_tls_setting(self):
        sandbox = object.__new__(Sandbox)
        sandbox._sid = "default-sandbox-1"
        sandbox._forwarded_ports = {8080}

        class Client:
            @staticmethod
            def _safe_id(sandbox_id):
                return sandbox_id

        sandbox._client = Client()
        with patch.dict(
            os.environ,
            {"ADX_SERVER_ADDRESS": "frontend:8888", "ADX_TLS": "1"},
            clear=True,
        ):
            self.assertEqual(
                sandbox.get_port_url(8080),
                "https://frontend:8888/default-sandbox-1/8080",
            )

    def test_pause_and_resume_are_synchronous_without_public_request_id(self):
        pause = getattr(Sandbox, "pause", None)
        resume = getattr(Sandbox, "resume", None)
        self.assertTrue(callable(pause), "Sandbox.pause must exist")
        self.assertTrue(callable(resume), "Sandbox.resume must exist")
        self.assertEqual(
            list(inspect.signature(pause).parameters),
            ["self", "ttl_seconds", "timeout_seconds"],
        )
        self.assertEqual(
            list(inspect.signature(resume).parameters),
            ["self"],
        )

    def test_reload_returns_bool_without_replacing_facades(self):
        class Client(_CloseTracker):
            def reload(self, sandbox_id):
                self.reload_arg = sandbox_id
                return {"success": True}

        sandbox = object.__new__(Sandbox)
        sandbox._client = Client()
        sandbox._sid = "default-sandbox-1"
        sandbox._closed = False
        sandbox._commands = object()
        sandbox._files = object()
        sandbox._shells = object()
        before = (sandbox._client, sandbox._commands, sandbox._files, sandbox._shells)

        self.assertIs(sandbox.reload(), True)
        self.assertEqual(sandbox._client.reload_arg, "default-sandbox-1")
        self.assertEqual(
            (sandbox._client, sandbox._commands, sandbox._files, sandbox._shells),
            before,
        )

    def test_reload_returns_false_when_closed_or_transport_fails(self):
        class Client(_CloseTracker):
            def reload(self, _sandbox_id):
                raise adx_sandbox.SandboxError("reload failed")

        sandbox = object.__new__(Sandbox)
        sandbox._client = Client()
        sandbox._sid = "default-sandbox-1"
        sandbox._closed = True
        self.assertIs(sandbox.reload(), False)
        sandbox._closed = False
        self.assertIs(sandbox.reload(), False)

    def test_pause_and_resume_return_typed_authoritative_results(self):
        pause_result_type = getattr(adx_sandbox, "PauseResult", None)
        resume_result_type = getattr(adx_sandbox, "ResumeResult", None)
        self.assertIsNotNone(pause_result_type, "PauseResult must be public")
        self.assertIsNotNone(resume_result_type, "ResumeResult must be public")

        class Client(_CloseTracker):
            def pause(self, sandbox_id, ttl_seconds, timeout_seconds=300):
                self.pause_args = (sandbox_id, ttl_seconds, timeout_seconds)
                return {
                    "sandboxId": sandbox_id,
                    "snapshotId": "pause-123",
                    "size": 8192,
                    "state": "paused",
                    "expiresAt": 1_800_000_000,
                }

            def resume(self, sandbox_id):
                self.resume_arg = sandbox_id
                return {
                    "sandboxId": sandbox_id,
                    "state": "running",
                    "routeAddress": "10.0.0.8:9000",
                    "functionProxyId": "proxy-a",
                    "nodeId": "node-a",
                    "portMappings": {"8080": 41080},
                }

        sandbox = object.__new__(Sandbox)
        sandbox._client = Client()
        sandbox._sid = "default-sandbox-1"
        sandbox._closed = False

        pause_result = sandbox.pause()
        resume_result = sandbox.resume()

        self.assertEqual(
            pause_result,
            pause_result_type(
                sandbox_id="default-sandbox-1",
                snapshot_id="pause-123",
                size=8192,
                state="paused",
                expires_at=1_800_000_000,
            ),
        )
        self.assertEqual(
            sandbox._client.pause_args,
            ("default-sandbox-1", 90_000, 300),
        )
        self.assertEqual(
            resume_result,
            resume_result_type(
                sandbox_id="default-sandbox-1",
                state="running",
                route_address="10.0.0.8:9000",
                function_proxy_id="proxy-a",
                node_id="node-a",
                port_mappings={"8080": 41080},
            ),
        )
        self.assertEqual(sandbox._client.resume_arg, "default-sandbox-1")

    def test_pause_rejects_invalid_ttl_before_transport(self):
        sandbox = object.__new__(Sandbox)
        sandbox._client = _CloseTracker()
        sandbox._sid = "default-sandbox-1"
        sandbox._closed = False

        for value in (True, 0, -1):
            with self.subTest(value=value):
                with self.assertRaisesRegex(ValueError, "ttl_seconds"):
                    sandbox.pause(value)

    def test_delete_uses_sandbox_id_without_name_namespace_variant(self):
        self.assertEqual(
            list(inspect.signature(Sandbox.delete).parameters),
            ["sandbox_id", "connection"],
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
            patch("adx_sandbox.sandbox_api.SandboxClient", Client),
            patch.dict(os.environ, {}, clear=True),
        ):
            Sandbox.delete("sandbox-1", connection=connection)

        self.assertIs(seen["connection"], connection)
        self.assertEqual(seen["client"].deleted, ["sandbox-1"])
        self.assertTrue(seen["client"].closed)

    def test_detached_kill_closes_local_client_without_deleting_remote(self):
        sandbox = object.__new__(Sandbox)
        sandbox._detached = True
        sandbox._tunnel_client = None
        sandbox._shells = _Shells()
        sandbox._pty = _PTY()
        sandbox._client = _CloseTracker()
        sandbox._sid = "sandbox-1"
        sandbox._closed = False

        sandbox.kill()

        self.assertTrue(sandbox._client.closed)

    def test_close_releases_local_resources_without_deleting_remote(self):
        sandbox = object.__new__(Sandbox)
        sandbox._detached = False
        sandbox._tunnel_client = None
        sandbox._shells = _Shells()
        sandbox._pty = _PTY()
        sandbox._client = _CloseTracker()
        sandbox._sid = "sandbox-1"
        sandbox._closed = False

        sandbox.close()
        sandbox.close()

        self.assertEqual(sandbox._client.deleted, [])
        self.assertEqual(sandbox._client.close_count, 1)

    def test_kill_is_idempotent(self):
        sandbox = object.__new__(Sandbox)
        sandbox._detached = False
        sandbox._tunnel_client = None
        sandbox._shells = _Shells()
        sandbox._pty = _PTY()
        sandbox._client = _CloseTracker()
        sandbox._sid = "sandbox-1"
        sandbox._closed = False

        sandbox.kill()
        sandbox.kill()

        self.assertEqual(sandbox._client.deleted, ["sandbox-1"])
        self.assertEqual(sandbox._client.close_count, 1)

    def test_tunnel_start_failure_rolls_back_created_sandbox(self):
        tracker = _CloseTracker()

        class Client:
            token = "token"

            def __init__(self):
                pass

            def create_info(self, _body):
                return {"sandboxId": "sandbox-1", "status": "running"}

            def delete(self, sandbox_id):
                tracker.delete(sandbox_id)

            def close(self):
                tracker.close()

            @staticmethod
            def _safe_id(sandbox_id):
                return sandbox_id

        class Tunnel:
            def __init__(self, _target, token=None):
                self.token = token

            def start(self, _url, timeout=60):
                return False

            def stop(self):
                pass

        with (
            patch("adx_sandbox.sandbox_api.SandboxClient", Client),
            patch("adx_sandbox.tunnel_client.TunnelClient", Tunnel),
            patch.dict(
                os.environ,
                {"ADX_GATEWAY_ADDRESS": "frontend:8080", "ADX_GATEWAY_TLS": "0"},
            ),
        ):
            with self.assertRaisesRegex(RuntimeError, "connection timeout"):
                Sandbox(
                    image="ubuntu:22.04",
                    upstream="127.0.0.1:9000",
                    tunnel_connect_timeout=0.1,
                    detached=True,
                )

        self.assertEqual(tracker.deleted, ["sandbox-1"])
        self.assertEqual(tracker.close_count, 1)

    def test_tunnel_uses_explicit_gateway_and_token_without_environment(self):
        seen = {}

        class Client(_CloseTracker):
            def __init__(self, *, connection):
                super().__init__()
                self.token = connection.token

            def create_info(self, _body):
                return {
                    "sandboxId": "sandbox-1",
                    "status": "running",
                    "tunnel": {"path": "/tunnel/sandbox-1"},
                }

            def set_direct_enabled(self, _enabled):
                pass

            @staticmethod
            def _safe_id(sandbox_id):
                return sandbox_id

        class Tunnel:
            def __init__(self, _target, token=None):
                seen["token"] = token

            def start(self, url, timeout=60):
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
            patch("adx_sandbox.sandbox_api.SandboxClient", Client),
            patch("adx_sandbox.tunnel_client.TunnelClient", Tunnel),
            patch.dict(os.environ, {}, clear=True),
        ):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                upstream="127.0.0.1:9000",
                connection=connection,
                detached=True,
            )
            sandbox.close()

        self.assertEqual(seen["url"], "wss://gateway.example:8443/tunnel/sandbox-1")
        self.assertEqual(seen["token"], "secret")

    def test_update_network_policy_replaces_and_clears_policy(self):
        class Client(_CloseTracker):
            def update_network_policy(self, sandbox_id, policy):
                self.updates = getattr(self, "updates", [])
                self.updates.append((sandbox_id, policy))
                return {"success": True}

        sandbox = object.__new__(Sandbox)
        sandbox._client = Client()
        sandbox._sid = "default-sandbox-1"
        sandbox._closed = False

        sandbox.update_network_policy(adx_sandbox.NetworkPolicy.block())
        sandbox.update_network_policy(adx_sandbox.NetworkPolicy())
        sandbox.update_network_policy(None)

        self.assertEqual(
            sandbox._client.updates,
            [
                ("default-sandbox-1", {"blockNetwork": True}),
                ("default-sandbox-1", {}),
                ("default-sandbox-1", {}),
            ],
        )
        with self.assertRaises(TypeError):
            sandbox.update_network_policy({"blockNetwork": True})
        sandbox._closed = True
        with self.assertRaises(RuntimeError):
            sandbox.update_network_policy(None)

if __name__ == "__main__":
    unittest.main()
