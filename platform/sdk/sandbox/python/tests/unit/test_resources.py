import json
import unittest
from unittest.mock import patch

import httpx
from adx_sandbox import ConnectionConfig
from adx_sandbox._resources import resources
from adx_sandbox._transport import SandboxClient


class _ResourceClient:
    closed = False
    connection = None

    def __init__(self, *, connection=None):
        type(self).connection = connection

    def resources(self):
        return {
            "resource": {
                "fragment": {
                    "node-1": {
                        "id": "node-1",
                        "status": 0,
                        "capacity": {
                            "resources": {
                                "CPU": {"scalar": {"value": 4000}},
                                "Memory": {"scalar": {"value": 8192}},
                            }
                        },
                        "allocatable": {
                            "resources": {
                                "CPU": {"scalar": {"value": 3000}},
                                "Memory": {"scalar": {"value": 4096}},
                            }
                        },
                        "nodeLabels": {
                            "ARCH": {"items": {"arm64": 1}},
                        },
                    }
                }
            }
        }

    def close(self):
        type(self).closed = True


class ResourceTests(unittest.TestCase):
    def test_resources_maps_existing_scheduler_json_response(self):
        _ResourceClient.closed = False
        with patch("adx_sandbox._resources.SandboxClient", _ResourceClient):
            nodes = resources()
        self.assertTrue(_ResourceClient.closed)
        self.assertEqual(len(nodes), 1)
        self.assertEqual(nodes[0].id, "node-1")
        self.assertEqual(nodes[0].status, 0)
        self.assertEqual(nodes[0].capacity["CPU"], 4000.0)
        self.assertEqual(nodes[0].allocatable["Memory"], 4096.0)
        self.assertEqual(nodes[0].labels["ARCH"], "arm64")

    def test_client_uses_existing_global_scheduler_json_route(self):
        seen = {}

        def handle(request):
            seen["path"] = request.url.path
            seen["type"] = request.headers.get("Type")
            return httpx.Response(
                200,
                content=json.dumps({"resource": {"fragment": {}}}),
                headers={"Content-Type": "application/json"},
            )

        client = SandboxClient.__new__(SandboxClient)
        client._origin = "http://frontend"
        client._http = httpx.Client(transport=httpx.MockTransport(handle))
        try:
            payload = client.resources()
        finally:
            client.close()

        self.assertEqual(seen["path"], "/global-scheduler/resources")
        self.assertEqual(seen["type"], "json")
        self.assertIn("resource", payload)

    def test_client_accepts_explicit_connection_without_environment_state(self):
        connection = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
            use_tls=True,
        )
        with (
            patch(
                "adx_sandbox._transport.acquire_shared_http_client"
            ) as acquire,
            patch.dict("os.environ", {}, clear=True),
        ):
            client = SandboxClient(connection=connection)
            client.close()

        acquire.assert_called_once()
        scheme, server, verify_tls, token_provider = acquire.call_args.args
        self.assertEqual((scheme, server, verify_tls), ("https", "frontend.example:443", False))
        self.assertTrue(callable(token_provider))
        self.assertEqual(token_provider(), "secret")

    def test_resources_accepts_explicit_connection_config(self):
        connection = ConnectionConfig(
            server_address="frontend.example:443",
            token="secret",
        )
        _ResourceClient.connection = None
        with (
            patch("adx_sandbox._resources.SandboxClient", _ResourceClient),
            patch.dict("os.environ", {}, clear=True),
        ):
            resources(connection=connection)

        self.assertIs(_ResourceClient.connection, connection)


if __name__ == "__main__":
    unittest.main()
