"""Create-request contracts at the ADX HTTP boundary."""

import inspect
import json
import unittest
from unittest.mock import patch

import httpx
from adx_environment import (
    ConnectionConfig,
    DNSPolicy,
    DNSRule,
    NetworkPolicy,
    NetworkRule,
    PortRange,
    Environment,
    TrafficPolicy,
)


class CreateRequestTests(unittest.TestCase):
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
        if request.method == "POST" and request.url.path == "/api/environment/v1/environments":
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
        with Environment(image="adx-contract-fixture:1", detached=True, connection=self.connection, **options) as environment:
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
                                   pause_policy=PausePolicy(retention=RetentionPolicy.for_seconds(3600), timeout_seconds=120, storage=CheckpointStorage.SHARED))
        self.assertEqual(body["idleAction"], "pause")
        self.assertNotIn("autoResume", body)
        self.assertEqual(body["pauseOptions"], {"retention": {"kind": "ttl", "ttlSeconds": 3600}, "timeoutSeconds": 120, "storage": "shared"})

    def test_invalid_idle_pause_options_do_not_open_transport(self):
        from adx_environment import PausePolicy, RetentionPolicy, IdleAction, CheckpointStorage
        for options in ({"idle_action": "missing"}, {"idle_action": IdleAction.PAUSE, "idle_timeout": 0},
                        {"auto_resume": 1}, {"pause_policy": {"storage": "shared"}}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                Environment(image="fixture", connection=self.connection, **options)
        self.assert_no_transport_setup()
        for options in ({"retention": 0}, {"timeout_seconds": 3601}, {"storage": "missing"}):
            with self.subTest(options=options), self.assertRaises((ValueError, TypeError)):
                PausePolicy(**options)
