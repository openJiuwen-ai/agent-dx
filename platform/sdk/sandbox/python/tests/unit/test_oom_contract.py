"""OOM classification must use committed state, not HTTP status or exit code alone."""

import httpx
import pytest
from adx_sandbox._transport import SandboxClient, SandboxError
from adx_sandbox.types import ConnectionConfig
from adx_sandbox.sandbox_api import Sandbox


def client(handler):
    c = SandboxClient(connection=ConnectionConfig(server_address="api:8443", token="test"))
    c._http = httpx.Client(transport=httpx.MockTransport(handler))
    return c


@pytest.mark.parametrize(
    "failure,expected",
    [
        (None, "OUTCOME_UNKNOWN"),
        ({"reason": "OomKilled", "exit_code": 137, "message": "memory limit exceeded"}, "SANDBOX_OOM"),
    ],
)
def test_direct_failure_uses_authoritative_environment_reason(failure, expected):

    def handler(request):
        if request.method == "GET":
            return httpx.Response(200, json=[{"id": "test", "status": "failed", "failure": failure}])
        return httpx.Response(503, text="route absent")

    c = client(handler)
    try:
        with pytest.raises(SandboxError) as raised:
            c.invoke("test", "process.exec", {"cmd": "true"}, timeout=1)
        assert raised.value.code == expected
        if expected == "SANDBOX_OOM":
            assert raised.value.retry == "never"
            assert raised.value.outcome == "terminal"
            assert raised.value.instance_id == "test"
    finally:
        c.close()


def test_get_info_retains_environment_failure():
    sb = object.__new__(Sandbox)
    sb._sid = "test"
    sb._cpu = sb._memory = sb._image = None

    class Catalog:

        def instance_info(self, _):
            return {
                "id": "test",
                "status": "failed",
                "failure": {"reason": "OomKilled", "exit_code": 137, "message": "memory limit exceeded"},
            }

    sb._client = Catalog()
    info = sb.get_info()
    assert info.failure.reason == "OomKilled"
    assert info.failure.exit_code == 137


def test_command_signal_does_not_imply_oom():
    from adx_sandbox.commands import _result, _info

    for signal in (None, 9):
        snapshot = {"status": "FAILED", "exit_code": -1, "exit_signal": signal}
        result = _result(snapshot)
        assert result.exit_signal == signal
        assert result.error_code is None
        assert _info(snapshot).exit_signal == signal


def test_background_submission_preserves_confirmed_sandbox_oom():
    from adx_sandbox.commands import Commands

    class DeadBackend:
        def invoke(self, *args, **kwargs):
            raise SandboxError("OOM", code="SANDBOX_OOM", retry="never", outcome="terminal")

    commands = Commands(DeadBackend(), "test")
    with pytest.raises(SandboxError) as raised:
        commands.run("true", background=True)
    assert raised.value.code == "SANDBOX_OOM"
