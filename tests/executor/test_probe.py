#!/usr/bin/env python3
# coding=UTF-8

import json
import logging
import socket

import pytest

from yr.agentexecutor import probe
from yr.agentexecutor.probe import (
    AggregateState,
    ProbeSpec,
    ProbeStartupError,
    _load_probe_set,
    _probe_once,
    _probe_tcp,
    probe_liveness,
    run_startup,
)


@pytest.fixture(autouse=True)
def _reset_liveness_state():
    setattr(probe, "_liveness_spec", None)
    setattr(probe, "_liveness_fail", 0)
    yield
    setattr(probe, "_liveness_spec", None)
    setattr(probe, "_liveness_fail", 0)


def test_from_mapping_tcp_action_picks_up_sandbox_ip():
    spec = ProbeSpec.from_mapping({"tcpSocket": {"port": 18092}}, sandbox_ip="10.0.0.1")

    assert spec.action == {"type": "tcp", "port": 18092, "host": "10.0.0.1"}
    assert spec.failure_threshold == 3


def test_from_mapping_falls_back_to_loopback_without_sandbox_ip():
    spec = ProbeSpec.from_mapping({"tcpSocket": {"port": 18092}})

    assert spec.action["host"] == "127.0.0.1"


def test_from_mapping_http_action_normalizes_defaults():
    spec = ProbeSpec.from_mapping({"httpGet": {"port": 8080}})

    assert spec.action == {
        "type": "http",
        "port": 8080,
        "host": "127.0.0.1",
        "path": "/",
        "scheme": "http",
    }


def test_from_mapping_spec_host_overrides_sandbox_ip():
    # A spec-provided host takes precedence over SANDBOX_IP and loopback, so the
    # caller can target an address other than the sandbox's own veth/loopback.
    spec = ProbeSpec.from_mapping({"tcpSocket": {"port": 18092, "host": "10.0.0.5"}},
                                  sandbox_ip="10.0.0.1")
    assert spec.action == {"type": "tcp", "port": 18092, "host": "10.0.0.5"}


def test_from_mapping_http_spec_host_overrides_sandbox_ip():
    spec = ProbeSpec.from_mapping(
        {"httpGet": {"port": 8080, "host": "10.0.0.5", "path": "/health", "scheme": "https"}},
        sandbox_ip="10.0.0.1",
    )
    assert spec.action == {
        "type": "http",
        "port": 8080,
        "host": "10.0.0.5",
        "path": "/health",
        "scheme": "https",
    }


def test_from_mapping_exec_action_keeps_command():
    spec = ProbeSpec.from_mapping({"exec": {"command": ["true"]}})

    assert spec.action == {"type": "exec", "command": ["true"]}


def test_from_mapping_rejects_multiple_actions():
    assert ProbeSpec.from_mapping(
        {"tcpSocket": {"port": 18092}, "exec": {"command": ["true"]}}
    ) is None


def test_from_mapping_rejects_missing_action():
    assert ProbeSpec.from_mapping({}) is None


def test_from_mapping_rejects_bad_port():
    assert ProbeSpec.from_mapping({"tcpSocket": {"port": 0}}) is None
    assert ProbeSpec.from_mapping({"tcpSocket": {"port": 70000}}) is None
    assert ProbeSpec.from_mapping({"tcpSocket": {"port": "18092"}}) is None


def test_from_mapping_rejects_bad_scheme():
    assert ProbeSpec.from_mapping({"httpGet": {"port": 8080, "scheme": "ftp"}}) is None


def test_load_probe_set_empty_env_returns_none_pair():
    assert _load_probe_set({}) == (None, None)
    assert _load_probe_set({probe.PROBE_SPEC_ENV: ""}) == (None, None)


def test_load_probe_set_invalid_json_returns_none_pair():
    assert _load_probe_set({probe.PROBE_SPEC_ENV: "not-json"}) == (None, None)


def test_load_probe_set_non_object_returns_none_pair():
    assert _load_probe_set({probe.PROBE_SPEC_ENV: "[1, 2]"}) == (None, None)


def test_load_probe_set_parses_both_roles_and_threads_sandbox_ip():
    raw = json.dumps(
        {
            "startup": {"tcpSocket": {"port": 18092}},
            "liveness": {"tcpSocket": {"port": 18092}},
        }
    )
    startup, liveness = _load_probe_set(
        {probe.PROBE_SPEC_ENV: raw, probe.SANDBOX_IP_ENV: "10.0.0.1"}
    )

    assert startup is not None and liveness is not None
    assert startup.action["host"] == "10.0.0.1"
    assert liveness.action["host"] == "10.0.0.1"


def test_load_probe_set_ignores_role_when_not_dict():
    raw = json.dumps({"startup": "bad", "liveness": {"tcpSocket": {"port": 18092}}})
    startup, liveness = _load_probe_set({probe.PROBE_SPEC_ENV: raw})

    assert startup is None
    assert liveness is not None


def test_probe_liveness_returns_disabled_when_unconfigured():
    assert probe_liveness() == AggregateState.DISABLED.value


def test_probe_liveness_returns_healthy_on_pass_and_resets_counter(monkeypatch):
    spec = ProbeSpec({"type": "tcp", "port": 1, "host": "127.0.0.1"}, failure_threshold=2)
    setattr(probe, "_liveness_spec", spec)
    setattr(probe, "_liveness_fail", 1)
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: (True, ""))

    assert probe_liveness() == AggregateState.HEALTHY.value
    assert getattr(probe, "_liveness_fail") == 0


def test_probe_liveness_subhealth_before_threshold(monkeypatch):
    spec = ProbeSpec({"type": "tcp", "port": 1, "host": "127.0.0.1"}, failure_threshold=3)
    setattr(probe, "_liveness_spec", spec)
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: (False, "refused"))

    assert probe_liveness() == AggregateState.SUBHEALTH.value
    assert getattr(probe, "_liveness_fail") == 1


def test_probe_liveness_failed_after_consecutive_threshold(monkeypatch):
    spec = ProbeSpec({"type": "tcp", "port": 1, "host": "127.0.0.1"}, failure_threshold=2)
    setattr(probe, "_liveness_spec", spec)
    setattr(probe, "_liveness_fail", 1)
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: (False, "refused"))

    assert probe_liveness() == AggregateState.FAILED.value
    assert getattr(probe, "_liveness_fail") == 2


def test_probe_liveness_recovery_clears_counter(monkeypatch):
    spec = ProbeSpec({"type": "tcp", "port": 1, "host": "127.0.0.1"}, failure_threshold=3)
    setattr(probe, "_liveness_spec", spec)
    results = iter([(False, "refused"), (True, "")])
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: next(results))

    assert probe_liveness() == AggregateState.SUBHEALTH.value
    assert probe_liveness() == AggregateState.HEALTHY.value
    assert getattr(probe, "_liveness_fail") == 0


def test_probe_liveness_exception_counts_as_failure(monkeypatch):
    spec = ProbeSpec({"type": "tcp", "port": 1, "host": "127.0.0.1"}, failure_threshold=2)
    setattr(probe, "_liveness_spec", spec)
    setattr(probe, "_liveness_fail", 1)

    def raise_(_action, _timeout):
        raise RuntimeError("boom")

    monkeypatch.setattr(probe, "_probe_once", raise_)

    assert probe_liveness() == AggregateState.FAILED.value
    assert getattr(probe, "_liveness_fail") == 2


def test_run_startup_returns_on_first_pass(monkeypatch):
    spec = ProbeSpec(
        {"type": "tcp", "port": 1, "host": "127.0.0.1"},
        initial_delay_seconds=0,
        period_seconds=1,
        failure_threshold=3,
    )
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: (True, ""))

    run_startup(spec)


def test_run_startup_raises_after_threshold(monkeypatch):
    spec = ProbeSpec(
        {"type": "tcp", "port": 1, "host": "127.0.0.1"},
        initial_delay_seconds=0,
        period_seconds=0,
        failure_threshold=2,
    )
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: (False, "refused"))
    monkeypatch.setattr(probe.time, "sleep", lambda _s: None)

    with pytest.raises(ProbeStartupError):
        run_startup(spec)


def test_probe_tcp_succeeds_against_listening_socket():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.bind(("127.0.0.1", 0))
    server.listen(1)
    port = server.getsockname()[1]
    try:
        assert _probe_tcp("127.0.0.1", port, timeout=1) == (True, "")
    finally:
        server.close()


def test_probe_tcp_fails_on_closed_port():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind(("127.0.0.1", 0))
    port = sock.getsockname()[1]
    sock.close()

    passed, reason = _probe_tcp("127.0.0.1", port, timeout=1)
    assert passed is False
    assert "Connection refused" in reason


def test_probe_once_dispatches_by_action_type(monkeypatch):
    seen = []

    def fake_tcp(host, port, timeout):
        seen.append(("tcp", host, port, timeout))
        return (True, "")

    monkeypatch.setattr(probe, "_probe_tcp", fake_tcp)
    action = {"type": "tcp", "port": 18092, "host": "127.0.0.1"}

    assert _probe_once(action, timeout=2) == (True, "")
    assert seen == [("tcp", "127.0.0.1", 18092, 2)]


def test_probe_liveness_logs_only_on_state_transition(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    spec = ProbeSpec({"type": "tcp", "port": 1, "host": "127.0.0.1"}, failure_threshold=3)
    setattr(probe, "_liveness_spec", spec)
    setattr(probe, "_liveness_last_state", None)
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: (True, ""))

    assert probe_liveness() == AggregateState.HEALTHY.value
    assert probe_liveness() == AggregateState.HEALTHY.value
    assert caplog.text.count("liveness probe HEALTHY") == 1


def test_probe_liveness_transition_back_to_healthy_is_logged(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    spec = ProbeSpec({"type": "tcp", "port": 1, "host": "127.0.0.1"}, failure_threshold=3)
    setattr(probe, "_liveness_spec", spec)
    setattr(probe, "_liveness_last_state", None)
    results = [(False, "refused"), (True, ""), (True, "")]
    monkeypatch.setattr(probe, "_probe_once", lambda _action, _timeout: results.pop(0))

    assert probe_liveness() == AggregateState.SUBHEALTH.value
    assert probe_liveness() == AggregateState.HEALTHY.value
    assert probe_liveness() == AggregateState.HEALTHY.value
    assert caplog.text.count("liveness probe HEALTHY") == 1
    assert caplog.text.count("liveness probe SUBHEALTH") == 1
