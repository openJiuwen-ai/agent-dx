"""Regression tests for eventual Traefik route readiness in the port example."""

import importlib.util
import sys
import types
import urllib.error
from pathlib import Path
from unittest import mock


EXAMPLE_PATH = Path(__file__).parents[1] / "examples" / "port_forwarding.py"
SPEC = importlib.util.spec_from_file_location("port_forwarding_example", EXAMPLE_PATH)
port_forwarding = importlib.util.module_from_spec(SPEC)
assert SPEC.loader is not None
fake_sdk = types.ModuleType("adx_sandbox")
fake_sdk.Sandbox = object
with mock.patch.dict(sys.modules, {"adx_sandbox": fake_sdk}):
    SPEC.loader.exec_module(port_forwarding)


class _Response:
    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False

    def read(self):
        return b"ROUTER-PF-OK-808"


def test_fetch_text_retries_route_not_ready_404():
    not_ready = urllib.error.HTTPError(
        "http://gateway/sandbox/8080", 404, "Not Found", None, None
    )
    with mock.patch.object(
        port_forwarding.urllib.request,
        "urlopen",
        side_effect=[not_ready, _Response()],
    ) as urlopen, mock.patch.object(port_forwarding.time, "sleep") as sleep:
        body = port_forwarding.fetch_text("http://gateway/sandbox/8080")

    assert body == "ROUTER-PF-OK-808"
    assert urlopen.call_count == 2
    sleep.assert_called_once()
