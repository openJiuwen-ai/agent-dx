"""Create-request contracts at the ADX HTTP boundary."""

import inspect
import json
import unittest
from unittest.mock import patch

import httpx
from adx_environment import (
    ConnectionConfig,
    EnvironmentError as SDKEnvironmentError,
    DNSPolicy,
    DNSRule,
    NetworkPolicy,
    NetworkRule,
    PortRange,
    Environment,
    LocalRootfs,
    TrafficPolicy,
)

_CONSTRAINED_CREATE = "/api/environment/v1/constrained-environments"


class CreateRequestTests(unittest.TestCase):
    def test_local_rootfs_sends_node_path_and_preserves_environment(self):
        with Environment(
            rootfs=LocalRootfs(path="/var/lib/agentos/images/tools/rootfs"),
            runtime="runsc", rootfs_readonly=True, env={"IMAGE_VALUE": "prepared"},
            detached=True, connection=self.connection,
        ):
            pass
        body = json.loads(self.exchanges[0].content)
        self.assertEqual(body["rootfs"], {
            "runtime": "runsc", "type": "local",
            "path": "/var/lib/agentos/images/tools/rootfs", "readonly": True,
        })
        self.assertEqual(body["env"], {"IMAGE_VALUE": "prepared"})
        self.assertNotIn("image", body)
        self.assertNotIn("inheritEntrypoint", body)

    def test_local_rootfs_rejects_image_and_snapshot_before_transport(self):
        for options in ({"image": "tools:v1"}, {"snapshot_id": "snapshot"}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                Environment(rootfs=LocalRootfs("/node/rootfs"), connection=self.connection, **options)
        self.assert_no_transport_setup()

    def test_local_rootfs_requires_external_entrypoint_resolution(self):
        with self.assertRaises(ValueError):
            Environment(rootfs=LocalRootfs("/node/rootfs"), inherit_entrypoint=True,
                        connection=self.connection)
        self.assert_no_transport_setup()

    def test_local_rootfs_path_is_a_node_absolute_path(self):
        for value in ("", "relative/rootfs", None, 12, "/node/\x00rootfs", "/node/../rootfs",
                      "/node/./rootfs", "/node/\nrootfs", "/node/\x85rootfs"):
            with self.subTest(path=value), self.assertRaises((ValueError, TypeError)):
                LocalRootfs(value)

    def test_explicit_forever_retention_and_typed_actions(self):
        import adx_environment as sdk
        for name in ("RetentionPolicy", "IdleAction", "CheckpointStorage"):
            self.assertTrue(hasattr(sdk, name), name)
        body = self.capture_create(
            idle_timeout=30,
            idle_action=sdk.IdleAction.PAUSE,
            pause_policy=sdk.PausePolicy(retention=sdk.RetentionPolicy.forever(), storage=sdk.CheckpointStorage.SHARED),
        )
        self.assertNotIn("autoResume", body)
        self.assertEqual(body["pauseOptions"]["retention"], {"kind": "forever"})

    def setUp(self):
        self.exchanges = []
        self.constrained_failures = []
        self.http_clients = []
        self.connection = ConnectionConfig(server_address="create.adx.test:7443", token="fixture-token")
        self.pool_patch = patch(
            "adx_environment._transport.acquire_shared_http_client",
            side_effect=self.open_http_client,
        )
        self.pool_factory = self.pool_patch.start()
        self.addCleanup(self.pool_patch.stop)
        self.addCleanup(self.close_http_clients)

    def close_http_clients(self):
        for client in self.http_clients:
            client.close()

    def open_http_client(self, *_args):
        client = httpx.Client(transport=httpx.MockTransport(self.answer_gateway_request))
        self.http_clients.append(client)
        return client

    def answer_gateway_request(self, request):
        self.exchanges.append(request)
        if request.url.path == _CONSTRAINED_CREATE and self.constrained_failures:
            failure = self.constrained_failures.pop(0)
            if failure is None:
                raise httpx.ReadTimeout("response was lost", request=request)
            if isinstance(failure, httpx.Response):
                return failure
            if isinstance(failure, dict):
                return httpx.Response(503, json={"error": failure})
            return httpx.Response(failure, json={
                "code": 404, "message": "route not found", "data": None,
                "error": {"code": "NOT_FOUND", "retry": "never", "outcome": "terminal"},
            })
        if request.method == "POST" and request.url.path in (
            "/api/environment/v1/environments", _CONSTRAINED_CREATE,
        ):
            final_event = json.dumps({"environmentId": "create-contract", "status": "running"})
            return httpx.Response(
                200,
                headers={"Content-Type": "text/event-stream"},
                text=f"event: final\ndata: {final_event}\n\n",
            )
        if request.method == "POST" and request.url.path == "/direct/create-contract/invoke":
            self.assertEqual(json.loads(request.content)["action"], "process.list")
            return httpx.Response(200, json={"processes": []})
        raise AssertionError(f"Unexpected gateway request: {request.method} {request.url}")

    def capture_create(self, **options):
        start = len(self.exchanges)
        with Environment(
            image="adx-contract-fixture:1", detached=True, connection=self.connection, **options
        ) as environment:
            self.assertEqual(environment.commands.list(), [])
        exchanged = self.exchanges[start:]
        self.assertEqual(
            [(request.method, request.url.path) for request in exchanged],
            [("POST", "/api/environment/v1/environments"), ("POST", "/direct/create-contract/invoke")],
        )
        return json.loads(exchanged[0].content)

    def assert_no_transport_setup(self):
        self.pool_factory.assert_not_called()
        self.assertEqual(self.exchanges, [])

    def test_node_capabilities_use_only_the_constrained_create_endpoint(self):
        with Environment(
            image="tools:1", runtime="runsc", required_node_capabilities=("checkpoint-restore",),
            detached=True, connection=self.connection,
        ):
            pass
        self.assertEqual(len(self.exchanges), 1)
        self.assertEqual(self.exchanges[0].url.path, _CONSTRAINED_CREATE)
        body = json.loads(self.exchanges[0].content)
        self.assertEqual(body["requiredNodeCapabilities"], ["checkpoint-restore"])
        self.assertNotIn("requiredNodeCapabilities", self.capture_create(required_node_capabilities=[]))

    def test_future_identifier_is_forwarded_for_server_validation(self):
        with Environment(image="tools:1", runtime="runsc", required_node_capabilities=["future.v2"],
                         detached=True, connection=self.connection):
            pass
        self.assertEqual(self.exchanges[0].url.path, _CONSTRAINED_CREATE)
        self.assertEqual(json.loads(self.exchanges[0].content)["requiredNodeCapabilities"], ["future.v2"])

    def test_invalid_node_capability_constraints_fail_before_transport(self):
        for options in (
            {"required_node_capabilities": "checkpoint-restore"},
            {"required_node_capabilities": ["checkpoint-restore"]},
            {"required_node_capabilities": ["not a token"], "runtime": "runsc"},
            {"required_node_capabilities": ["duplicate.v1", "duplicate.v1"], "runtime": "runsc"},
            {"required_node_capabilities": ["checkpoint-restore"], "runtime": "invalid/runtime"},
            {"required_node_capabilities": ["checkpoint-restore"], "runtime": "runsc", "snapshot_id": "snap"},
        ):
            with self.subTest(options=options), self.assertRaises((TypeError, ValueError)):
                Environment(image="tools:1", connection=self.connection, **options)
        self.assert_no_transport_setup()

    def test_old_server_rejection_never_falls_back_to_unconstrained_create(self):
        for response in (404, httpx.Response(404, text="route not found")):
            self.constrained_failures = [response]
            with self.subTest(response=response), self.assertRaises(SDKEnvironmentError) as raised:
                Environment(image="tools:1", runtime="runsc", required_node_capabilities=["checkpoint-restore"],
                            connection=self.connection)
            self.assertEqual((raised.exception.code, raised.exception.outcome), ("UNSUPPORTED", "not_started"))
        self.assertEqual([request.url.path for request in self.exchanges], [_CONSTRAINED_CREATE] * 2)

    def test_unrecognized_proxy_status_does_not_claim_no_allocation(self):
        for status in (404, 405, 501):
            self.constrained_failures = [httpx.Response(status, text="proxy response")]
            with self.subTest(status=status), self.assertRaises(SDKEnvironmentError) as raised:
                Environment(image="tools:1", runtime="runsc", required_node_capabilities=["checkpoint-restore"],
                            connection=self.connection)
            self.assertIsNone(raised.exception.outcome)
            self.assertIn(f"HTTP {status} proxy response", str(raised.exception))
        self.assertEqual([request.url.path for request in self.exchanges], [_CONSTRAINED_CREATE] * 3)

    def test_constrained_create_preserves_authoritative_errors_at_route_missing_statuses(self):
        for status, detail in (
            (404, {"code": "NOT_FOUND", "retry": "never", "outcome": "terminal"}),
            (405, {"code": "CONFLICT", "retry": "never", "outcome": "not_started"}),
            (501, {"code": "UNSUPPORTED", "retry": "same_operation", "outcome": "unknown",
                   "requestForwarded": True, "environmentId": "default-named", "operationId": "op-1"}),
        ):
            self.constrained_failures = [None] if status == 501 else []
            self.constrained_failures.append(httpx.Response(
                status, json={"message": "creation rejected", "error": detail}
            ))
            with (
                self.subTest(status=status), patch("adx_environment._transport.time.sleep"),
                self.assertRaises(SDKEnvironmentError) as raised,
            ):
                Environment(image="tools:1", runtime="runsc", required_node_capabilities=["checkpoint-restore"],
                            connection=self.connection)
            error = raised.exception
            self.assertEqual(
                (error.code, error.retry, error.outcome), (detail["code"], detail["retry"], detail["outcome"]),
            )
            self.assertEqual(error.request_forwarded, detail.get("requestForwarded"))
            self.assertEqual(error.environment_id, detail.get("environmentId"))
            self.assertEqual(error.operation_id, detail.get("operationId"))
        self.assertEqual([request.url.path for request in self.exchanges], [_CONSTRAINED_CREATE] * 4)

    def test_old_server_after_lost_response_preserves_the_same_operation(self):
        self.constrained_failures = [None, 404]
        with patch("adx_environment._transport.time.sleep"), self.assertRaises(SDKEnvironmentError) as raised:
            Environment(image="tools:1", runtime="runsc", required_node_capabilities=["checkpoint-restore"],
                        connection=self.connection)
        error = raised.exception
        self.assertEqual((error.code, error.retry, error.outcome), ("OUTCOME_UNKNOWN", "same_operation", "unknown"))
        self.assertIsInstance(error.first_error, httpx.ReadTimeout)
        self.assertEqual(len(self.exchanges), 2)
        self.assertEqual({request.url.path for request in self.exchanges}, {_CONSTRAINED_CREATE})
        self.assertEqual({request.headers["X-Request-Id"] for request in self.exchanges}, {error.request_id})
        self.assertEqual(self.exchanges[0].content, self.exchanges[1].content)
        self.assertEqual(error.environment_id, f"default-{json.loads(self.exchanges[0].content)['name']}")

    def test_mixed_server_retry_retains_earlier_authoritative_create_identity(self):
        self.constrained_failures = [{
            "code": "OUTCOME_UNKNOWN", "retry": "same_operation", "outcome": "unknown",
            "environmentId": "trusted-environment", "operationId": "operation-19",
        }, None, 404]
        with patch("adx_environment._transport.time.sleep"), self.assertRaises(SDKEnvironmentError) as raised:
            Environment(image="tools:1", runtime="runsc", required_node_capabilities=["checkpoint-restore"],
                        connection=self.connection)
        error = raised.exception
        self.assertEqual((error.code, error.retry, error.outcome), ("OUTCOME_UNKNOWN", "same_operation", "unknown"))
        self.assertEqual((error.environment_id, error.operation_id), ("trusted-environment", "operation-19"))
        self.assertEqual(len(self.exchanges), 3)
        self.assertEqual({request.headers["X-Request-Id"] for request in self.exchanges}, {error.request_id})

    def test_tool_identity_reaches_create_and_is_optional(self):
        body = self.capture_create(runtime="conch", user="1001", group="1002")
        self.assertEqual((body["user"], body["group"]), ("1001", "1002"))
        body = self.capture_create()
        self.assertNotIn("user", body)
        self.assertNotIn("group", body)

    def test_invalid_or_incomplete_tool_identity_never_opens_transport(self):
        for options in ({"user": "1001"}, {"group": "1002"},
                        {"user": "", "group": "1002"},
                        {"user": "a:b", "group": "1002"},
                        {"user": "4294967295", "group": "1002"}):
            with self.subTest(options=options), self.assertRaises(ValueError):
                Environment(image="tools:1", connection=self.connection, **options)
        self.assert_no_transport_setup()

    def test_omitted_options_inherit_cluster_defaults(self):
        body = self.capture_create()
        self.assertTrue({"network", "xpu", "storageMb", "extra_config"}.isdisjoint(body))
        self.assertEqual(body["storage_limit_mb"], 0)
        parameters = inspect.signature(Environment).parameters
        for option in ("network", "xpu", "storage_mb"):
            with self.subTest(option=option):
                self.assertIsNone(parameters[option].default)
        self.assertEqual(parameters["storage_limit_mb"].default, 0)

    def test_legacy_policies_keep_runtime_options_in_a_separate_field(self):
        cases = (
            ("empty", NetworkPolicy(), None),
            ("blocked", NetworkPolicy.block(), {"blockNetwork": True}),
            (
                "dns",
                NetworkPolicy.deny_dns("Artifacts.ADX.EXAMPLE.", "*.Artifacts.ADX.example", "artifacts.adx.example"),
                {"dnsBlacklist": ["artifacts.adx.example", "*.artifacts.adx.example"]},
            ),
        )
        runtime_options = {"overlayMode": "session"}
        for label, policy, expected in cases:
            with self.subTest(policy=label):
                body = self.capture_create(network=policy, extra_config=runtime_options)
                self.assertEqual(body.get("network"), expected)
                if expected is None:
                    self.assertNotIn("network", body)
                self.assertEqual(body["extra_config"], runtime_options)
                self.assertEqual(runtime_options, {"overlayMode": "session"})

    def test_allowlist_encodes_ordered_peer_and_port_selectors(self):
        inputs = (
            {"cidr": "198.51.100.7", "protocol": "tcp", "port_range": 3478, "priority": 17},
            {"domain": "CAFÉ.ADX.EXAMPLE.", "protocol": "tcp", "port_range": PortRange(8443, 8445)},
            {"protocol": "udp", "port_range": 123},
        )
        policy = NetworkPolicy.allowlist(tuple(NetworkRule(**options) for options in inputs))
        network = self.capture_create(network=policy)["network"]
        self.assertEqual(set(network), {"schemaVersion", "traffic"})
        self.assertEqual(network["schemaVersion"], 2)
        traffic = network["traffic"]
        self.assertEqual(
            (traffic["ingressDefaultAction"], traffic["egressDefaultAction"], traffic["mode"]),
            ("allow", "deny", "stateful"),
        )
        expected_peers = (
            {"cidr": "198.51.100.7/32", "portRange": {"first": 3478, "last": 3478}},
            {"domain": "xn--caf-dma.adx.example", "portRange": {"first": 8443, "last": 8445}},
            {"portRange": {"first": 123, "last": 123}},
        )
        self.assertEqual(len(traffic["rules"]), len(inputs))
        for options, expected_peer, encoded in zip(inputs, expected_peers, traffic["rules"]):
            with self.subTest(selector=options):
                self.assertEqual(set(encoded), {"action", "direction", "protocol", "priority", "peer"})
                self.assertEqual(
                    (encoded["action"], encoded["direction"], encoded["protocol"], encoded["priority"]),
                    ("allow", "egress", options["protocol"], options.get("priority", 100)),
                )
                self.assertEqual(encoded["peer"], expected_peer)

    def test_explicit_traffic_and_dns_sections_travel_in_one_create_request(self):
        policy = NetworkPolicy(
            traffic=TrafficPolicy(
                mode="stateless",
                ingress_default_action="deny",
                egress_default_action="allow",
                rules=[
                    NetworkRule(
                        cidr="203.0.113.29/27",
                        direction="ingress",
                        action="deny",
                        protocol="tcp",
                        priority=255,
                        environment_port_range=PortRange(9020, 9030),
                    )
                ],
            ),
            dns=DNSPolicy(rules=[DNSRule("*.Cache.ADX.EXAMPLE.", action="allow")], default_action="deny"),
        )
        encoded = self.capture_create(network=policy)["network"]
        self.assertEqual(set(encoded), {"schemaVersion", "traffic", "dns"})
        self.assertEqual(encoded["schemaVersion"], 2)
        traffic = encoded["traffic"]
        self.assertEqual(
            (traffic["mode"], traffic["ingressDefaultAction"], traffic["egressDefaultAction"]),
            ("stateless", "deny", "allow"),
        )
        self.assertEqual(len(traffic["rules"]), 1)
        rule = traffic["rules"][0]
        self.assertEqual(
            (rule["direction"], rule["action"], rule["protocol"], rule["priority"]),
            ("ingress", "deny", "tcp", 255),
        )
        self.assertEqual(rule["peer"], {"cidr": "203.0.113.0/27"})
        self.assertEqual(rule["environmentPortRange"], {"first": 9020, "last": 9030})
        self.assertEqual(encoded["dns"]["defaultAction"], "deny")
        self.assertEqual(encoded["dns"]["rules"], [{"pattern": "*.cache.adx.example", "action": "allow"}])

    def test_invalid_policy_models_fail_before_opening_a_client(self):
        cases = (
            (PortRange, (0,), {}, ValueError),
            (PortRange, (65536,), {}, ValueError),
            (PortRange, (True,), {}, TypeError),
            (PortRange, (51, 50), {}, ValueError),
            (NetworkRule, (), {"cidr": "2001:db8:1::/48"}, ValueError),
            (NetworkRule, (), {"cidr": "192.0.2.0/24", "domain": "api.adx.example"}, ValueError),
            (NetworkRule, (), {"domain": "*.adx.example", "direction": "both"}, ValueError),
            (NetworkRule, (), {"protocol": "any", "port_range": 8443}, ValueError),
            (NetworkRule, (), {"priority": 4294967295}, ValueError),
            (TrafficPolicy, (), {"rules": [NetworkRule()] * 257}, ValueError),
            (NetworkPolicy, (), {"block_network": True, "traffic": TrafficPolicy()}, ValueError),
            (NetworkPolicy, (), {"block_network": "enabled"}, TypeError),
            (NetworkPolicy, (), {"dns_blacklist": "api.adx.example"}, TypeError),
            (NetworkPolicy, (), {"block_network": True, "dns_blacklist": ("api.adx.example",)}, ValueError),
            (NetworkPolicy.allowlist, ([],), {}, ValueError),
            (NetworkPolicy.deny_dns, (), {}, ValueError),
        )
        for constructor, arguments, keywords, error_type in cases:
            with self.subTest(model=constructor.__name__, arguments=arguments, keywords=keywords):
                with self.assertRaises(error_type):
                    self.capture_create(network=constructor(*arguments, **keywords))
        self.assert_no_transport_setup()

    def test_malformed_dns_patterns_never_reach_the_gateway(self):
        for pattern in ("api.*", "api..adx.example", "api.adx.example.."):
            with self.subTest(pattern=pattern), self.assertRaises(ValueError):
                self.capture_create(network=NetworkPolicy.deny_dns(pattern))
        self.assert_no_transport_setup()

    def test_create_requires_a_typed_network_policy(self):
        with self.assertRaisesRegex(TypeError, "NetworkPolicy"):
            self.capture_create(network={"blockNetwork": True})
        self.assert_no_transport_setup()

    def test_accelerator_requests_preserve_spelling_and_runtime_selection(self):
        for request in ("GPU:A30:3", "GPU::4", "npu:ascend910b4:1"):
            with self.subTest(request=request):
                body = self.capture_create(xpu=request, runtime="runc")
                self.assertEqual((body["xpu"], body["rootfs"]["runtime"]), (request, "runc"))

    def test_invalid_accelerator_requests_fail_before_transport_setup(self):
        malformed = {
            "exactly three fields": ("", "gpu:a30", "gpu:a30:3:0", ":a30:3", "gpu:a30:", "gpu:a30:3,gpu:a30:4"),
            "positive integer": ("gpu:a30:0", "gpu:a30:-3", "gpu:a30:2.5"),
            "unsupported xpu type": ("tpu:v4:2",),
            "whitespace": (" gpu:a30:3",),
        }
        for diagnostic, requests in malformed.items():
            for request in requests:
                with self.subTest(request=request), self.assertRaisesRegex(ValueError, diagnostic):
                    self.capture_create(xpu=request)
        with self.assertRaisesRegex(TypeError, "string or None"):
            self.capture_create(xpu=3)
        self.assert_no_transport_setup()

    def test_storage_capacity_and_ceiling_have_distinct_wire_fields(self):
        cases = (
            ({"storage_mb": 8192}, (8192, 0)),
            ({"storage_mb": 8192, "storage_limit_mb": 12288}, (8192, 12288)),
            ({"storage_mb": 8192, "storage_limit_mb": 8192}, (8192, 8192)),
            ({"storage_limit_mb": 12288}, (None, 12288)),
        )
        for options, expected in cases:
            with self.subTest(options=options):
                body = self.capture_create(**options)
                self.assertEqual((body.get("storageMb"), body["storage_limit_mb"]), expected)
                self.assertNotIn("storage_mb", body)
                self.assertNotIn("storageLimitMb", body)

    def test_invalid_storage_options_fail_before_transport_setup(self):
        cases = (
            ({"storage_mb": True}, TypeError),
            ({"storage_mb": "8192"}, TypeError),
            ({"storage_mb": 0}, ValueError),
            ({"storage_mb": -8192}, ValueError),
            ({"storage_limit_mb": True}, TypeError),
            ({"storage_limit_mb": "8192"}, TypeError),
            ({"storage_limit_mb": -8192}, ValueError),
            ({"storage_mb": 8192, "storage_limit_mb": 4096}, ValueError),
        )
        for options, error_type in cases:
            with self.subTest(options=options), self.assertRaises(error_type):
                self.capture_create(**options)
        self.assert_no_transport_setup()

    def test_idle_pause_with_shared_storage_is_preserved(self):
        from adx_environment import PausePolicy, RetentionPolicy, IdleAction, CheckpointStorage
        body = self.capture_create(idle_timeout=30, idle_action=IdleAction.PAUSE,
                                   pause_policy=PausePolicy(retention=RetentionPolicy.for_seconds(3600),
                                       timeout_seconds=120, storage=CheckpointStorage.SHARED))
        self.assertEqual(body["idleAction"], "pause")
        self.assertNotIn("autoResume", body)
        self.assertEqual(body["pauseOptions"], {
            "retention": {"kind": "ttl", "ttlSeconds": 3600}, "timeoutSeconds": 120, "storage": "shared"})

    def test_invalid_idle_pause_options_do_not_open_transport(self):
        from adx_environment import PausePolicy, IdleAction
        for options in ({"idle_action": "missing"}, {"idle_action": IdleAction.PAUSE, "idle_timeout": 0},
                        {"auto_resume": 1}, {"pause_policy": {"storage": "shared"}}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                Environment(image="fixture", connection=self.connection, **options)
        self.assert_no_transport_setup()
        for options in ({"retention": 0}, {"timeout_seconds": 3601}, {"storage": "missing"}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                PausePolicy(**options)
