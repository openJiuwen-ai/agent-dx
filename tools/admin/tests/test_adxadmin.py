import io
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest import mock

import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from adxadmin.cli import build_parser, run
from adxadmin.client import AdminClient, ClientOptions
from adxadmin.credentials import read_token, write_secret
from adxadmin.errors import ApiError, InvalidInput


class CliContractTests(unittest.TestCase):
    def test_create_does_not_require_an_output_file(self):
        parser = build_parser({})
        arguments = parser.parse_args(
            [
                "--endpoint",
                "https://adx.example.com",
                "--token-file",
                "/tmp/admin.key",
                "key",
                "create",
                "--tenant",
                "team-a",
            ]
        )
        self.assertEqual(arguments.tenant, "team-a")
        self.assertIsNone(arguments.output_file)
        self.assertEqual(arguments.expires_at, 0)

    def test_environment_supplies_connection_options(self):
        parser = build_parser(
            {
                "ADX_ENDPOINT": "https://adx.example.com",
                "ADX_ADMIN_TOKEN_FILE": "/tmp/admin.key",
                "ADX_ADMIN_OUTPUT": "json",
            }
        )
        arguments = parser.parse_args(["key", "list"])
        self.assertEqual(arguments.endpoint, "https://adx.example.com")
        self.assertEqual(arguments.output, "json")

    def test_no_expiry_is_an_explicit_alias_for_the_default(self):
        parser = build_parser({})
        arguments = parser.parse_args(
            [
                "--endpoint",
                "https://adx.example.com",
                "--token-file",
                "/tmp/admin.key",
                "key",
                "create",
                "--tenant",
                "team-a",
                "--no-expiry",
            ]
        )
        self.assertEqual(arguments.expires_at, 0)

    def test_create_prints_the_one_time_secret_by_default(self):
        created = {
            "key": {
                "id": "b" * 64,
                "tenantId": "team-a",
                "expiresAtUnixSeconds": 0,
            },
            "apiKey": "adx_" + "c" * 64,
        }
        with tempfile.TemporaryDirectory() as temporary:
            token_file = Path(temporary) / "admin.key"
            token_file.write_text("a" * 64)
            token_file.chmod(0o600)
            stdout = io.StringIO()
            with mock.patch("adxadmin.cli.AdminClient") as client_type:
                client_type.return_value.__enter__.return_value.create_key.return_value = created
                result = run(
                    [
                        "--endpoint",
                        "https://adx.example.com",
                        "--token-file",
                        str(token_file),
                        "--output",
                        "json",
                        "key",
                        "create",
                        "--tenant",
                        "team-a",
                    ],
                    environ={},
                    stdout=stdout,
                )
        self.assertEqual(result, 0)
        self.assertEqual(json.loads(stdout.getvalue())["apiKey"], created["apiKey"])

    def test_create_can_write_the_secret_without_printing_it(self):
        created = {
            "key": {
                "id": "b" * 64,
                "tenantId": "team-a",
                "expiresAtUnixSeconds": 0,
            },
            "apiKey": "adx_" + "c" * 64,
        }
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            token_file = root / "admin.key"
            token_file.write_text("a" * 64)
            token_file.chmod(0o600)
            output_file = root / "tenant.key"
            stdout = io.StringIO()
            with mock.patch("adxadmin.cli.AdminClient") as client_type:
                client_type.return_value.__enter__.return_value.create_key.return_value = created
                result = run(
                    [
                        "--endpoint",
                        "https://adx.example.com",
                        "--token-file",
                        str(token_file),
                        "--output",
                        "json",
                        "key",
                        "create",
                        "--tenant",
                        "team-a",
                        "--output-file",
                        str(output_file),
                    ],
                    environ={},
                    stdout=stdout,
                )
            self.assertEqual(output_file.read_text(), created["apiKey"] + "\n")
        self.assertEqual(result, 0)
        self.assertNotIn("apiKey", json.loads(stdout.getvalue()))


class CredentialTests(unittest.TestCase):
    def test_rejects_a_group_readable_administrator_key_on_posix(self):
        if os.name != "posix":
            self.skipTest("POSIX permission contract")
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "admin.key"
            path.write_text("a" * 64)
            path.chmod(0o640)
            with self.assertRaisesRegex(InvalidInput, "0600"):
                read_token(path)

    def test_writes_a_secret_once_with_private_permissions(self):
        with tempfile.TemporaryDirectory() as temporary:
            path = Path(temporary) / "credentials" / "tenant.key"
            secret = "adx_" + "b" * 64
            write_secret(path, secret)
            self.assertEqual(path.read_text(), secret + "\n")
            if os.name == "posix":
                self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o600)
                self.assertEqual(stat.S_IMODE(path.parent.stat().st_mode), 0o700)
            with self.assertRaises(FileExistsError):
                write_secret(path, "adx_" + "c" * 64)


class ClientContractTests(unittest.TestCase):
    def client(self, handler, endpoint="http://127.0.0.1:8000"):
        return AdminClient(
            ClientOptions(
                endpoint=endpoint,
                token="a" * 64,
                timeout_seconds=2,
                allow_loopback_http=True,
            ),
            transport=httpx.MockTransport(handler),
        )

    def test_create_uses_the_public_api_and_preserves_no_expiry(self):
        requests = []

        def handler(request):
            requests.append(request)
            return httpx.Response(
                201,
                json={
                    "key": {
                        "id": "b" * 64,
                        "tenantId": "team-a",
                        "expiresAtUnixSeconds": 0,
                    },
                    "apiKey": "adx_" + "c" * 64,
                },
            )

        with self.client(handler) as client:
            result = client.create_key("team-a", 0)
        self.assertEqual(result["key"]["expiresAtUnixSeconds"], 0)
        self.assertEqual(requests[0].method, "POST")
        self.assertEqual(requests[0].url.path, "/api/admin/v1/keys")
        self.assertEqual(json.loads(requests[0].content)["tenantId"], "team-a")
        self.assertEqual(requests[0].headers["authorization"], "Bearer " + "a" * 64)
        self.assertTrue(requests[0].headers["x-request-id"])

    def test_list_allows_multiple_keys_for_one_tenant(self):
        def handler(request):
            self.assertEqual(request.url.params["tenantId"], "team-a")
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "b" * 64,
                            "tenantId": "team-a",
                            "expiresAtUnixSeconds": 0,
                        },
                        {
                            "id": "c" * 64,
                            "tenantId": "team-a",
                            "expiresAtUnixSeconds": 2_000_000_000,
                        },
                    ],
                    "nextPageToken": "",
                },
            )

        with self.client(handler) as client:
            page = client.list_keys(tenant="team-a", page_size=100)
        self.assertEqual(len(page["items"]), 2)

    def test_create_rejects_a_response_for_another_tenant(self):
        def handler(_request):
            return httpx.Response(
                201,
                json={
                    "key": {
                        "id": "b" * 64,
                        "tenantId": "team-b",
                        "expiresAtUnixSeconds": 0,
                    },
                    "apiKey": "adx_" + "c" * 64,
                },
            )

        with self.client(handler) as client:
            with self.assertRaisesRegex(InvalidInput, "tenant"):
                client.create_key("team-a")

    def test_list_rejects_malformed_key_metadata(self):
        def handler(_request):
            return httpx.Response(
                200,
                json={
                    "items": [
                        {
                            "id": "not-a-digest",
                            "tenantId": "team-a",
                            "expiresAtUnixSeconds": 0,
                        }
                    ],
                    "nextPageToken": "",
                },
            )

        with self.client(handler) as client:
            with self.assertRaisesRegex(InvalidInput, "metadata"):
                client.list_keys()

    def test_revoke_uses_the_digest_identifier(self):
        key_id = "d" * 64

        def handler(request):
            self.assertEqual(request.method, "DELETE")
            self.assertEqual(request.url.path, f"/api/admin/v1/keys/{key_id}")
            return httpx.Response(204)

        with self.client(handler) as client:
            client.revoke_key(key_id)

    def test_structured_server_errors_are_preserved(self):
        def handler(_request):
            return httpx.Response(
                503,
                json={
                    "message": "authentication service unavailable",
                    "error": {
                        "code": "UNAVAILABLE",
                        "retry": "AFTER_BACKOFF",
                        "outcome": "NOT_STARTED",
                        "requestId": "request-a",
                    },
                },
            )

        with self.client(handler) as client:
            with self.assertRaises(ApiError) as raised:
                client.list_keys()
        self.assertEqual(raised.exception.code, "UNAVAILABLE")
        self.assertEqual(raised.exception.retry, "AFTER_BACKOFF")
        self.assertEqual(raised.exception.request_id, "request-a")

    def test_remote_plaintext_and_endpoint_paths_are_rejected(self):
        options = ClientOptions(
            endpoint="http://example.com",
            token="a" * 64,
            timeout_seconds=2,
            allow_loopback_http=True,
        )
        with self.assertRaises(InvalidInput):
            AdminClient(options)
        options.endpoint = "https://adx.example.com/base"
        with self.assertRaises(InvalidInput):
            AdminClient(options)


if __name__ == "__main__":
    unittest.main()
