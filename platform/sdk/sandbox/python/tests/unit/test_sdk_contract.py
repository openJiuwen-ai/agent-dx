import asyncio
import inspect
import unittest
from unittest.mock import patch

import adx_sandbox
from adx_sandbox import (
    ConnectionConfig,
    DataPlaneSecurityPolicy,
    DNSPolicy,
    DNSRule,
    NetworkPolicy,
    NetworkRule,
    PortForwarding,
    PortRange,
    S3Config,
    Sandbox,
    TrafficPolicy,
)
from adx_sandbox.commands import (
    CommandConflict,
    CommandHandle,
    CommandNotFound,
    CommandSubmissionError,
    Commands,
)
from adx_sandbox._transport import SandboxHTTPError
from adx_sandbox.shell import Shells
from adx_sandbox.shell.shell import Shell


class _FakeClient:
    created = []
    token = "sandbox-token"

    def __init__(self, *, connection=None):
        self.calls = []
        self.closed = False
        self.direct_enabled = True
        self.connection = connection

    def create_info(self, body):
        type(self).created.append(dict(body))
        return {"sandboxId": "sandbox-1", "status": "running"}

    def set_direct_enabled(self, enabled):
        self.direct_enabled = enabled

    def invoke(self, sandbox_id, action, args, **_kwargs):
        self.calls.append((sandbox_id, action, args))
        if action == "process.capabilities":
            return {
                "protocol_version": 1,
                "capabilities": [
                    "stable-command-id",
                    "recoverable-command-result",
                    "multiplexed-command-watch",
                ],
            }
        if action == "process.exec":
            return {"stdout": "", "stderr": "", "exit_code": 0}
        if action == "process.list":
            return {
                "processes": [
                    {"pid": 7, "cmd": "sleep 1", "running": True},
                    {"pid": 8, "cmd": "true", "running": False},
                ]
            }
        if action == "process.start":
            return {
                "command_id": args["command_id"],
                "pid": 42,
                "status": "running",
                "error": None,
            }
        if action == "process.get":
            return {
                "command_id": args["command_id"],
                "pid": 42,
                "cmd": "sleep 1",
                "status": "running",
                "started_at_ms": 123,
            }
        if action == "process.wait":
            return {
                "command_id": args["command_id"],
                "pid": 42,
                "status": "finished",
                "stdout": "done",
                "stderr": "",
                "exit_code": 0,
            }
        if action in ("shell.create", "shell.close"):
            return {}
        if action == "shell.run":
            return {}
        if action == "shell.poll":
            return {"status": "done", "stdout": "", "stderr": "", "exit_code": 0}
        if action == "entrypoint.poll":
            return {
                "status": "exited",
                "exit_code": None,
                "signal": 9,
                "shell_exit_code": 137,
                "message": "entrypoint exited after signal SIGKILL",
            }
        raise AssertionError(action)

    def instance_info(self, sandbox_id):
        return {
            "id": sandbox_id,
            "status": "running",
            "required_cpu": 1000,
            "required_mem": 4096,
            "image": "ubuntu:22.04",
        }

    def delete(self, _sandbox_id):
        pass

    def close(self):
        self.closed = True

    @staticmethod
    def _safe_id(sandbox_id):
        return sandbox_id


class SDKContractTests(unittest.TestCase):
    def setUp(self):
        _FakeClient.created.clear()

    def test_pause_resume_results_are_public_frozen_value_types(self):
        pause_result_type = getattr(adx_sandbox, "PauseResult", None)
        resume_result_type = getattr(adx_sandbox, "ResumeResult", None)
        sandbox_error_type = getattr(adx_sandbox, "SandboxError", None)
        self.assertIsNotNone(pause_result_type, "PauseResult must be public")
        self.assertIsNotNone(resume_result_type, "ResumeResult must be public")
        self.assertIsNotNone(sandbox_error_type, "SandboxError must be public")
        pause = pause_result_type(
            sandbox_id="sandbox-1",
            snapshot_id="pause-1",
            size=17,
            state="paused",
            expires_at=1_800_000_000,
        )
        resume = resume_result_type(
            sandbox_id="sandbox-1",
            state="running",
            route_address="10.0.0.8:9000",
            function_proxy_id="proxy-a",
            node_id="node-a",
            port_mappings={"8080": 41080},
        )

        with self.assertRaisesRegex(Exception, "cannot assign"):
            pause.state = "running"
        with self.assertRaisesRegex(Exception, "cannot assign"):
            resume.state = "paused"

    def test_sandbox_runtime_and_default_cwd_are_applied_without_frontend_cwd_field(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                runtime="kata",
                cwd="/workspace",
                detached=True,
            )
            sandbox.commands.run("pwd", timeout=10)

        body = _FakeClient.created[-1]
        self.assertNotIn("runtime", body)
        self.assertEqual(body["rootfs"]["runtime"], "kata")
        self.assertEqual(body["rootfs"]["imageurl"], "ubuntu:22.04")
        self.assertNotIn("image", body)
        self.assertNotIn("cwd", body)
        self.assertNotIn("cwdMode", body)
        self.assertEqual(
            sandbox._client.calls[-1][2]["cwd"],
            "/workspace",
        )

    def test_inherit_entrypoint_is_image_only_and_serialized_when_enabled(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(
                image="example/image:latest",
                inherit_entrypoint=True,
                detached=True,
            )

        self.assertTrue(_FakeClient.created[-1]["inheritEntrypoint"])
        self.assertEqual(sandbox.wait_entrypoint(), 137)
        self.assertEqual(sandbox.entrypoint_exit_info["signal"], 9)
        self.assertEqual(sandbox._client.calls[-1][1], "entrypoint.poll")

    def test_inherit_entrypoint_false_is_omitted_and_invalid_combinations_fail(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(image="ubuntu:22.04", detached=True)
            self.assertNotIn("inheritEntrypoint", _FakeClient.created[-1])
            with self.assertRaisesRegex(TypeError, "inherit_entrypoint"):
                Sandbox(image="ubuntu:22.04", inherit_entrypoint=1)
            with self.assertRaisesRegex(ValueError, "image rootfs"):
                Sandbox(
                    rootfs=S3Config("endpoint", "bucket", "object"),
                    inherit_entrypoint=True,
                )
            with self.assertRaisesRegex(ValueError, "snapshot_id"):
                Sandbox(
                    image="ubuntu:22.04",
                    snapshot_id="snapshot-1",
                    inherit_entrypoint=True,
                )

    def test_explicit_connection_config_is_shared_without_environment_state(self):
        connection = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
            use_tls=True,
            gateway_address="gateway.example:8080",
            gateway_use_tls=True,
        )
        with (
            patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient),
            patch.dict("os.environ", {}, clear=True),
        ):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                port_forwardings=[8080],
                connection=connection,
                detached=True,
            )

        self.assertIs(sandbox._client.connection, connection)
        self.assertIs(sandbox.pty._connection_config, connection)
        self.assertEqual(
            sandbox.get_port_url(8080),
            "https://gateway.example:8080/sandbox-1/8080",
        )

    def test_sandbox_forwards_runtime_without_owning_runtime_registry(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                runtime="gvisor-next",
                detached=True,
            )

        body = _FakeClient.created[-1]
        self.assertNotIn("runtime", body)
        self.assertEqual(body["rootfs"]["runtime"], "gvisor-next")

    def test_failover_defaults_false_and_forwards_true(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(image="ubuntu:22.04", detached=True)
            Sandbox(image="ubuntu:22.04", failover=True, detached=True)

        self.assertIs(_FakeClient.created[-2]["failover"], False)
        self.assertIs(_FakeClient.created[-1]["failover"], True)

    def test_failover_rejects_non_boolean_values(self):
        with (
            patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient),
            self.assertRaisesRegex(TypeError, "failover"),
        ):
            Sandbox(image="ubuntu:22.04", failover=1, detached=True)

    def test_affinity_groups_keep_weights_and_conjoin_node_id_in_every_alternative(self):
        groups = [{"kind": 0, "affinity": 2, "labelOps": [{"type": 0, "labelKey": "pool", "labelValues": [pool]}]} for pool in ("a", "b")]
        groups.append({"kind": 1, "affinity": 0, "weight": 9, "labelOps": [{"type": 2, "labelKey": "app"}]})
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(image="ubuntu", node_id="node-a", labels={"app": "worker"}, schedule_affinities=groups, detached=True)
        body = _FakeClient.created[-1]
        self.assertEqual(body["labels"], {"app": "worker"})
        self.assertEqual(body["scheduleAffinities"][2]["weight"], 9)
        for group in body["scheduleAffinities"][:2]:
            self.assertEqual(group["labelOps"][-1]["labelKey"], "NODE_ID")
        self.assertEqual(len(groups[0]["labelOps"]), 1)

    def test_node_id_is_encoded_as_frontend_affinity_semantics(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                node_id="node-a",
                detached=True,
            )

        body = _FakeClient.created[-1]
        self.assertNotIn("nodeId", body)
        self.assertEqual(
            body["scheduleAffinities"],
            [
                {
                    "kind": 0,
                    "affinity": 2,
                    "labelOps": [
                        {
                            "type": 0,
                            "labelKey": "NODE_ID",
                            "labelValues": ["node-a"],
                        }
                    ],
                }
            ],
        )

    def test_xpu_is_validated_and_forwarded_without_normalization(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                xpu="GPU:L20:2",
                detached=True,
            )

        self.assertEqual(_FakeClient.created[-1]["xpu"], "GPU:L20:2")

    def test_network_defaults_to_unrestricted_and_omits_wire_field(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(image="ubuntu:22.04", detached=True)

        self.assertNotIn("network", _FakeClient.created[-1])
        self.assertTrue(sandbox._client.direct_enabled)
        self.assertIsNone(inspect.signature(Sandbox).parameters["network"].default)

    def test_data_plane_security_policy_is_scoped_to_create_request(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                data_plane_security=DataPlaneSecurityPolicy(
                    tunnel_mode="tls-token",
                    port_forward_mode="tls",
                ),
                detached=True,
            )

        self.assertEqual(
            _FakeClient.created[-1]["dataPlane"],
            {
                "tunnelSecurityMode": "tls-token",
                "portForwardSecurityMode": "tls",
            },
        )

    def test_empty_data_plane_security_policy_inherits_cluster_defaults(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                data_plane_security=DataPlaneSecurityPolicy(),
                detached=True,
            )

        self.assertNotIn("dataPlane", _FakeClient.created[-1])

    def test_block_network_uses_canonical_field_and_keeps_direct(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                network=NetworkPolicy.block(),
                detached=True,
            )

        self.assertEqual(
            _FakeClient.created[-1]["network"],
            {"blockNetwork": True},
        )
        self.assertNotIn("extra_config", _FakeClient.created[-1])
        self.assertTrue(sandbox._client.direct_enabled)

    def test_dns_blacklist_is_normalized_and_forwarded(self):
        policy = NetworkPolicy.deny_dns(
            "GitHub.COM.", "*.GitHub.com", "github.com"
        )
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                network=policy,
                detached=True,
            )

        self.assertEqual(
            _FakeClient.created[-1]["network"],
            {"dnsBlacklist": ["github.com", "*.github.com"]},
        )
        self.assertNotIn("extra_config", _FakeClient.created[-1])
        self.assertTrue(sandbox._client.direct_enabled)

    def test_acl_v2_allowlist_is_normalized_and_forwarded(self):
        policy = NetworkPolicy.allowlist(
            [
                NetworkRule(
                    cidr="10.20.30.40",
                    protocol="tcp",
                    port_range=22773,
                    priority=110,
                ),
                NetworkRule(
                    domain="BÜCHER.example.",
                    protocol="tcp",
                    port_range=PortRange(80, 443),
                ),
                NetworkRule(protocol="udp", port_range=53),
            ]
        )
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                network=policy,
                detached=True,
            )

        self.assertEqual(
            _FakeClient.created[-1]["network"],
            {
                "schemaVersion": 2,
                "traffic": {
                    "ingressDefaultAction": "allow",
                    "egressDefaultAction": "deny",
                    "mode": "stateful",
                    "rules": [
                        {
                            "action": "allow",
                            "direction": "egress",
                            "protocol": "tcp",
                            "priority": 110,
                            "peer": {
                                "cidr": "10.20.30.40/32",
                                "portRange": {"first": 22773, "last": 22773},
                            },
                        },
                        {
                            "action": "allow",
                            "direction": "egress",
                            "protocol": "tcp",
                            "priority": 100,
                            "peer": {
                                "domain": "xn--bcher-kva.example",
                                "portRange": {"first": 80, "last": 443},
                            },
                        },
                        {
                            "action": "allow",
                            "direction": "egress",
                            "protocol": "udp",
                            "priority": 100,
                            "peer": {
                                "portRange": {"first": 53, "last": 53}
                            },
                        },
                    ],
                },
            },
        )

    def test_acl_v2_supports_independent_defaults_and_dns_policy(self):
        policy = NetworkPolicy(
            traffic=TrafficPolicy(
                ingress_default_action="deny",
                egress_default_action="allow",
                mode="stateless",
                rules=(
                    NetworkRule(
                        action="deny",
                        direction="ingress",
                        protocol="tcp",
                        cidr="192.0.2.129/24",
                        sandbox_port_range=PortRange(8000, 8010),
                        priority=200,
                    ),
                ),
            ),
            dns=DNSPolicy(
                default_action="deny",
                rules=(DNSRule("*.example.com", action="allow"),),
            ),
        )

        self.assertEqual(
            policy.to_dict(),
            {
                "schemaVersion": 2,
                "traffic": {
                    "ingressDefaultAction": "deny",
                    "egressDefaultAction": "allow",
                    "mode": "stateless",
                    "rules": [
                        {
                            "action": "deny",
                            "direction": "ingress",
                            "protocol": "tcp",
                            "priority": 200,
                            "peer": {"cidr": "192.0.2.0/24"},
                            "sandboxPortRange": {
                                "first": 8000,
                                "last": 8010,
                            },
                        }
                    ],
                },
                "dns": {
                    "defaultAction": "deny",
                    "rules": [
                        {"action": "allow", "pattern": "*.example.com"}
                    ],
                },
            },
        )

    def test_acl_v2_rejects_unsafe_or_unrepresentable_rules(self):
        invalid_factories = [
            lambda: PortRange(0),
            lambda: PortRange(100, 99),
            lambda: NetworkRule(cidr="2001:db8::/32"),
            lambda: NetworkRule(cidr="10.0.0.0/8", domain="example.com"),
            lambda: NetworkRule(domain="*.example.com", direction="both"),
            lambda: NetworkRule(protocol="any", port_range=443),
            lambda: NetworkRule(priority=(1 << 32) - 1),
            lambda: TrafficPolicy(rules=(NetworkRule(),) * 257),
            lambda: NetworkPolicy(
                block_network=True,
                traffic=TrafficPolicy(),
            ),
            lambda: NetworkPolicy.allowlist(()),
        ]
        for factory in invalid_factories:
            with self.subTest(factory=factory):
                with self.assertRaises((TypeError, ValueError)):
                    factory()

    def test_network_policy_preserves_user_extra_config(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                network=NetworkPolicy.block(),
                extra_config={"runtimeOption": "value"},
                detached=True,
            )

        self.assertEqual(
            _FakeClient.created[-1]["extra_config"],
            {"runtimeOption": "value"},
        )
        self.assertEqual(
            _FakeClient.created[-1]["network"],
            {"blockNetwork": True},
        )

    def test_empty_network_policy_is_omitted(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                network=NetworkPolicy(),
                detached=True,
            )

        self.assertNotIn("network", _FakeClient.created[-1])
        self.assertNotIn("extra_config", _FakeClient.created[-1])

    def test_invalid_network_policy_is_rejected_before_create(self):
        invalid_factories = [
            lambda: NetworkPolicy(block_network="yes"),
            lambda: NetworkPolicy(dns_blacklist="github.com"),
            lambda: NetworkPolicy.deny_dns(),
            lambda: NetworkPolicy.deny_dns("github.*"),
            lambda: NetworkPolicy.deny_dns("github..com"),
            lambda: NetworkPolicy.deny_dns("github.com.."),
            lambda: NetworkPolicy(
                block_network=True,
                dns_blacklist=("github.com",),
            ),
        ]
        for factory in invalid_factories:
            with self.subTest(factory=factory):
                with self.assertRaises((TypeError, ValueError)):
                    factory()
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(TypeError, "NetworkPolicy"):
                Sandbox(image="ubuntu:22.04", network={"blockNetwork": True})

        self.assertEqual(_FakeClient.created, [])

    def test_xpu_without_model_is_forwarded_without_normalization(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                xpu="GPU::2",
                detached=True,
            )

        self.assertEqual(_FakeClient.created[-1]["xpu"], "GPU::2")

    def test_npu_request_is_forwarded_without_normalization(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                runtime="runc",
                xpu="npu:ascend910b4:1",
                detached=True,
            )

        self.assertEqual(_FakeClient.created[-1]["xpu"], "npu:ascend910b4:1")

    def test_xpu_defaults_to_no_request(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(image="ubuntu:22.04", detached=True)

        self.assertNotIn("xpu", _FakeClient.created[-1])
        self.assertIsNone(inspect.signature(Sandbox).parameters["xpu"].default)

    def test_invalid_xpu_is_rejected_before_create(self):
        invalid_cases = [
            (1, TypeError, "string or None"),
            ("", ValueError, "exactly three fields"),
            ("gpu:l20", ValueError, "exactly three fields"),
            ("gpu:l20:1:0", ValueError, "exactly three fields"),
            (":l20:1", ValueError, "exactly three fields"),
            ("gpu:l20:", ValueError, "exactly three fields"),
            ("tpu:v5e:1", ValueError, "unsupported xpu type"),
            ("gpu:l20:0", ValueError, "positive integer"),
            ("gpu:l20:-1", ValueError, "positive integer"),
            ("gpu:l20:1.5", ValueError, "positive integer"),
            ("gpu:l20:1,gpu:h100:1", ValueError, "exactly three fields"),
            (" gpu:l20:1", ValueError, "whitespace"),
        ]
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for xpu, error_type, message in invalid_cases:
                with self.subTest(xpu=xpu):
                    with self.assertRaisesRegex(error_type, message):
                        Sandbox(image="ubuntu:22.04", xpu=xpu)
        self.assertEqual(_FakeClient.created, [])

    def test_storage_is_validated_and_forwarded(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                storage_mb=153600,
                storage_limit_mb=204800,
                detached=True,
            )

        body = _FakeClient.created[-1]
        self.assertEqual(body["storageMb"], 153600)
        self.assertEqual(body["storage_limit_mb"], 204800)

    def test_storage_default_is_omitted(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(image="ubuntu:22.04", detached=True)

        body = _FakeClient.created[-1]
        self.assertNotIn("storageMb", body)
        self.assertEqual(body["storage_limit_mb"], 0)
        signature = inspect.signature(Sandbox).parameters
        self.assertIsNone(signature["storage_mb"].default)
        self.assertEqual(signature["storage_limit_mb"].default, 0)

    def test_invalid_storage_is_rejected_before_create(self):
        invalid_cases = [
            (True, TypeError),
            ("1024", TypeError),
            (0, ValueError),
            (-1, ValueError),
        ]
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for storage_mb, error_type in invalid_cases:
                with self.subTest(storage_mb=storage_mb):
                    with self.assertRaises(error_type):
                        Sandbox(image="ubuntu:22.04", storage_mb=storage_mb)
        self.assertEqual(_FakeClient.created, [])

    def test_invalid_storage_limit_mb_is_rejected_before_create(self):
        invalid_cases = [
            (True, TypeError),
            ("1024", TypeError),
            (-1, ValueError),
        ]
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for storage_limit_mb, error_type in invalid_cases:
                with self.subTest(storage_limit_mb=storage_limit_mb):
                    with self.assertRaises(error_type):
                        Sandbox(
                            image="ubuntu:22.04",
                            storage_limit_mb=storage_limit_mb,
                        )
            with self.assertRaisesRegex(ValueError, "greater than or equal"):
                Sandbox(
                    image="ubuntu:22.04",
                    storage_mb=2048,
                    storage_limit_mb=1024,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_commands_list_reads_rrt_running_field(self):
        client = _FakeClient()
        processes = Commands(client, "sandbox-1").list()
        self.assertEqual(processes[0].command, "sleep 1")
        self.assertTrue(processes[0].running)
        self.assertFalse(processes[1].running)

    def test_commands_list_accepts_legacy_status_field(self):
        class _LegacyClient(_FakeClient):
            def invoke(self, sandbox_id, action, args, **kwargs):
                if action == "process.list":
                    return {
                        "processes": [
                            {"pid": 7, "cmd": "sleep 1", "status": "running"},
                            {"pid": 8, "cmd": "true", "status": "done"},
                        ]
                    }
                return super().invoke(sandbox_id, action, args, **kwargs)

        processes = Commands(_LegacyClient(), "sandbox-1").list()
        self.assertTrue(processes[0].running)
        self.assertFalse(processes[1].running)

    def test_background_command_has_stable_id_and_can_be_retrieved(self):
        client = _FakeClient()
        commands = Commands(client, "sandbox-1")
        handle = commands.run("sleep 1", background=True)
        self.assertIsInstance(handle, CommandHandle)
        self.assertTrue(handle.id)
        self.assertEqual(handle.pid, 42)
        self.assertEqual(client.calls[-1][2]["command_id"], handle.id)

        recovered = commands.get(handle.id)
        self.assertEqual(recovered.id, handle.id)
        self.assertEqual(recovered.pid, 42)
        self.assertEqual(recovered.wait().stdout, "done")

    def test_long_foreground_command_preserves_requested_remote_timeout(self):
        client = _FakeClient()

        result = Commands(client, "sandbox-1").run("sleep 31", timeout=31)

        start = next(call for call in client.calls if call[1] == "process.start")
        self.assertEqual(start[2]["timeout"], 31)
        self.assertEqual(result.stdout, "done")

    def test_submission_unknown_preserves_recoverable_command_id(self):
        class _FailingClient(_FakeClient):
            def invoke(self, sandbox_id, action, args, **kwargs):
                if action == "process.start":
                    raise TimeoutError("response lost")
                return super().invoke(sandbox_id, action, args, **kwargs)

        with self.assertRaises(CommandSubmissionError) as caught:
            Commands(_FailingClient(), "sandbox-1").run("true", background=True)
        self.assertTrue(caught.exception.command_id)
        self.assertTrue(caught.exception.may_have_started)

    def test_command_errors_preserve_request_and_resource_identity(self):
        class _ConflictResult(dict):
            request_id = "request-conflict"

        class _ConflictClient(_FakeClient):
            def invoke(self, sandbox_id, action, args, **kwargs):
                if action == "process.start":
                    return _ConflictResult(
                        error="same id has a different request",
                        error_code="COMMAND_CONFLICT",
                    )
                return super().invoke(sandbox_id, action, args, **kwargs)

        with self.assertRaises(CommandConflict) as conflict:
            Commands(_ConflictClient(), "sandbox-1").run(
                "true", background=True, command_id="stable-id"
            )
        self.assertEqual(conflict.exception.sandbox_id, "sandbox-1")
        self.assertEqual(conflict.exception.command_id, "stable-id")
        self.assertEqual(conflict.exception.request_id, "request-conflict")

        class _MissingClient(_FakeClient):
            def invoke(self, sandbox_id, action, args, **kwargs):
                if action == "process.get":
                    raise SandboxHTTPError(
                        404,
                        {"error": "missing"},
                        "missing",
                        request_id="request-missing",
                    )
                return super().invoke(sandbox_id, action, args, **kwargs)

        with self.assertRaises(CommandNotFound) as missing:
            Commands(_MissingClient(), "sandbox-1").get("missing-id")
        self.assertEqual(missing.exception.sandbox_id, "sandbox-1")
        self.assertEqual(missing.exception.command_id, "missing-id")
        self.assertEqual(missing.exception.request_id, "request-missing")

    def test_sandbox_from_id_does_not_create_or_delete_remote(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox.from_id("sandbox-existing")
            self.assertEqual(sandbox.id, "sandbox-existing")
            self.assertEqual(_FakeClient.created, [])
            sandbox.close()
            self.assertTrue(sandbox._client.closed)

    def test_ambiguous_connect_aliases_are_not_exposed(self):
        self.assertFalse(hasattr(Sandbox, "connect"))
        self.assertFalse(hasattr(Commands, "connect"))

    def test_shell_uses_sandbox_default_cwd(self):
        client = _FakeClient()
        shells = Shells(client, "sandbox-1", default_cwd="/workspace")
        asyncio.run(shells.create())
        shell_run = next(call for call in client.calls if call[1] == "shell.run")
        self.assertIn("cd /workspace", shell_run[2]["command"])

    def test_get_info_uses_frontend_instance_summary(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            sandbox = Sandbox(image="ubuntu:22.04", detached=True)
            info = sandbox.get_info()
            self.assertEqual(info.id, "sandbox-1")
            self.assertEqual(info.state, "running")
            self.assertTrue(sandbox.is_running())

    def test_sandbox_without_explicit_rootfs_preserves_cluster_default(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(detached=True)

        body = _FakeClient.created[-1]
        self.assertNotIn("image", body)
        self.assertEqual(body["rootfs"], {"runtime": "runsc"})

    def test_runtime_override_without_explicit_rootfs_preserves_cluster_default(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(runtime="kata", detached=True)

        body = _FakeClient.created[-1]
        self.assertNotIn("runtime", body)
        self.assertNotIn("image", body)
        self.assertEqual(body["rootfs"], {"runtime": "kata"})

    def test_s3_rootfs_uses_nested_runtime_only(self):
        rootfs = S3Config(
            endpoint="https://s3.example.com",
            bucket="rootfs",
            object="runtime.img",
        )
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(rootfs=rootfs, runtime="kata", detached=True)

        body = _FakeClient.created[-1]
        self.assertNotIn("runtime", body)
        self.assertEqual(body["rootfs"]["type"], "s3")
        self.assertEqual(body["rootfs"]["runtime"], "kata")

    def test_image_and_rootfs_are_mutually_exclusive(self):
        rootfs = S3Config(
            endpoint="https://s3.example.com",
            bucket="rootfs",
            object="rootfs.img",
        )
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "mutually exclusive"):
                Sandbox(
                    image="ubuntu:24.04",
                    rootfs=rootfs,
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_schedule_timeout_minus_one_is_rejected_before_create(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "positive integer"):
                Sandbox(
                    image="ubuntu:22.04",
                    schedule_timeout=-1,
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_sandbox_constructor_keeps_backend_reverse_tunnel_boundary(self):
        signature = inspect.signature(Sandbox)
        self.assertEqual(signature.parameters["schedule_timeout"].default, 30)
        self.assertNotIn("reverse_tunnel", signature.parameters)
        self.assertIn("upstream", signature.parameters)
        self.assertIn("proxy_port", signature.parameters)
        self.assertIn("tunnel_connect_timeout", signature.parameters)

    def test_proxy_port_is_validated_before_create(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for invalid in (True, "9001"):
                with self.subTest(proxy_port=invalid):
                    with self.assertRaisesRegex(TypeError, "proxy_port"):
                        Sandbox(
                            image="ubuntu:22.04",
                            upstream="127.0.0.1:9000",
                            proxy_port=invalid,
                            detached=True,
                        )
            for invalid in (1, 65536):
                with self.subTest(proxy_port=invalid):
                    with self.assertRaisesRegex(ValueError, "between 2 and 65535"):
                        Sandbox(
                            image="ubuntu:22.04",
                            upstream="127.0.0.1:9000",
                            proxy_port=invalid,
                            detached=True,
                        )
        self.assertEqual(_FakeClient.created, [])

    def test_custom_reverse_tunnel_ports_conflict_with_user_forwarding(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for port in (9000, 9001):
                with self.subTest(port=port):
                    with self.assertRaisesRegex(ValueError, str(port)):
                        Sandbox(
                            image="ubuntu:22.04",
                            upstream="127.0.0.1:8000",
                            proxy_port=9001,
                            port_forwardings=[port],
                            detached=True,
                        )
        self.assertEqual(_FakeClient.created, [])

    def test_duplicate_forwarded_ports_are_rejected_before_create(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "duplicate"):
                Sandbox(
                    image="ubuntu:22.04",
                    port_forwardings=[8080, 8080],
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_port_forwarding_descriptors_remain_compatible(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            Sandbox(
                image="ubuntu:22.04",
                port_forwardings=[PortForwarding(8080), 9090],
                detached=True,
            )

        self.assertEqual(_FakeClient.created[-1]["ports"], ["8080", "9090"])

    def test_port_forward_url_tls_and_optional_auth_header_are_explicit(self):
        with (
            patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient),
            patch.dict(
                "os.environ",
                {"ADX_GATEWAY_ADDRESS": "edge.example:443", "ADX_GATEWAY_TLS": "1"},
            ),
        ):
            sandbox = Sandbox(
                image="ubuntu:22.04",
                port_forwardings=[8080],
                detached=True,
            )
            self.assertEqual(
                sandbox.get_port_url(8080),
                "https://edge.example:443/sandbox-1/8080",
            )
            self.assertEqual(
                sandbox.get_port_auth_headers(),
                {"Authorization": "Bearer sandbox-token", "X-Auth": "sandbox-token"},
            )

    def test_duplicate_forwarded_ports_across_descriptor_and_integer_are_rejected(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "duplicate"):
                Sandbox(
                    image="ubuntu:22.04",
                    port_forwardings=[PortForwarding(8080), 8080],
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_reverse_tunnel_upstream_is_validated_before_create(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "upstream"):
                Sandbox(
                    image="ubuntu:22.04",
                    upstream="",
                    detached=True,
                )
        self.assertEqual(_FakeClient.created, [])

    def test_core_arguments_are_validated_before_create(self):
        invalid_cases = [
            ({"image": ""}, ValueError, "image"),
            ({"image": "ubuntu", "rootfs": "bad"}, TypeError, "S3Config"),
            ({"image": "ubuntu", "env": {"A": 1}}, TypeError, "env"),
            ({"image": "ubuntu", "name": ""}, ValueError, "name"),
            ({"image": "ubuntu", "cwd": 1}, TypeError, "cwd"),
            ({"image": "ubuntu", "mounts": ["bad"]}, TypeError, "Mount"),
            ({"image": "ubuntu", "detached": 1}, TypeError, "detached"),
            ({"image": "ubuntu", "node_id": 1}, TypeError, "node_id"),
        ]
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            for kwargs, error_type, message in invalid_cases:
                with self.subTest(kwargs=kwargs):
                    with self.assertRaisesRegex(error_type, message):
                        Sandbox(**kwargs)
        self.assertEqual(_FakeClient.created, [])

    def test_reverse_tunnel_ports_cannot_conflict_with_forwarded_ports(self):
        with patch("adx_sandbox.sandbox_api.SandboxClient", _FakeClient):
            with self.assertRaisesRegex(ValueError, "conflict"):
                Sandbox(
                    image="ubuntu:22.04",
                    port_forwardings=[8766],
                    upstream="127.0.0.1:9000",
                )
        self.assertEqual(_FakeClient.created, [])

    def test_shell_clean_output_preserves_output_without_trailing_newline(self):
        raw = "printf $SDK_E2E\r\nstateful__RRT_PROMPT__ echo __RRT_DONE_$?__\r\n"

        self.assertEqual(Shell._clean_output(raw), "stateful")

    def test_shell_clean_output_removes_reserved_prompt_after_newline(self):
        raw = "pwd\r\n/tmp\r\n__RRT_PROMPT__ echo __RRT_DONE_$?__\r\n"

        self.assertEqual(Shell._clean_output(raw), "/tmp")


if __name__ == "__main__":
    unittest.main()
