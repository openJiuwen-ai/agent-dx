#!/usr/bin/env python3
# coding=UTF-8

import threading
from types import SimpleNamespace

from yr.agentexecutor import handler
from yr.agentexecutor.probe import ProbeSpec, ProbeStartupError
from yr.agentexecutor.runtime import AgentExecutorRuntime, resolve_process_shutdown_grace


def test_handler_exposes_all_faas_lifecycle_entries(monkeypatch):
    events = []

    class Runtime:
        def start(self):
            events.append("start")

        def status(self):
            events.append("status")
            return {"ready": True}

        def stop(self):
            events.append("stop")

    monkeypatch.setattr(handler, "get_runtime", lambda: Runtime())

    assert handler.initialize(None) is None
    assert handler.handle({}, None) == {"ready": True}
    assert handler.pre_stop() is None
    assert events == ["start", "status", "stop"]


def test_shutdown_grace_uses_pre_stop_timeout_budget():
    assert resolve_process_shutdown_grace({"PRE_STOP_TIMEOUT": "10"}) == 8
    assert resolve_process_shutdown_grace({"PRE_STOP_TIMEOUT": "25"}) == 23


def test_shutdown_grace_honors_override_without_exceeding_budget():
    assert resolve_process_shutdown_grace({
        "PRE_STOP_TIMEOUT": "10",
        "YR_AGENT_EXECUTOR_SHUTDOWN_GRACE_SECONDS": "3",
    }) == 3
    assert resolve_process_shutdown_grace({
        "PRE_STOP_TIMEOUT": "10",
        "YR_AGENT_EXECUTOR_SHUTDOWN_GRACE_SECONDS": "20",
    }) == 8


def test_runtime_stops_http_before_user_processes(monkeypatch):
    events = []

    class Server:
        def stop(self):
            events.append("http")

    class ProcessManager:
        def stop(self, grace):
            events.append(("processes", grace))

    monkeypatch.setenv("PRE_STOP_TIMEOUT", "10")
    runtime = AgentExecutorRuntime()
    runtime._http_server = Server()
    runtime._process_manager = ProcessManager()
    runtime.stop()

    assert events == ["http", ("processes", 8)]


def test_start_raises_on_startup_failure_without_deadlock(monkeypatch):
    # Regression guard: start() runs _cleanup_on_startup_failure() inside its
    # own with-block. A non-reentrant Lock re-acquired in the cleanup path
    # would deadlock here. Run start() on a thread and join with a timeout so
    # a hang fails the test instead of hanging the suite.
    events = []

    def _pm_start():
        events.append("process_start")

    def _pm_stop(*_args):
        events.append("process_stop")

    pm = SimpleNamespace(start_from_env=_pm_start, stop=_pm_stop)
    # Patch the ProcessManager class so __init__ picks up the double without
    # touching the protected _process_manager attribute.
    monkeypatch.setattr("yr.agentexecutor.runtime.ProcessManager", lambda: pm)

    def _server_start():
        events.append("server_start")

    def _server_stop():
        events.append("server_stop")

    monkeypatch.setattr(
        "yr.agentexecutor.runtime.ExecutorHTTPServer",
        lambda *a, **kw: SimpleNamespace(start=_server_start, stop=_server_stop),
    )

    startup_spec = ProbeSpec(
        {"type": "tcp", "port": 1, "host": "127.0.0.1"},
        initial_delay_seconds=0,
        period_seconds=0,
        failure_threshold=1,
    )

    def _raise_startup(_spec):
        raise ProbeStartupError("forced startup failure")

    monkeypatch.setattr("yr.agentexecutor.runtime.run_startup", _raise_startup)
    monkeypatch.setattr("yr.agentexecutor.runtime._load_probe_set", lambda: (startup_spec, None))
    monkeypatch.setenv("PRE_STOP_TIMEOUT", "10")

    runtime = AgentExecutorRuntime()

    result = {}

    def run():
        try:
            runtime.start()
        except BaseException as exc:
            result["exc"] = exc

    t = threading.Thread(target=run)
    t.start()
    # startup failure path must return promptly; a deadlock would keep the
    # thread alive past the deadline.
    t.join(timeout=5.0)
    assert not t.is_alive(), "start() deadlocked instead of raising on startup failure"

    assert isinstance(result.get("exc"), ProbeStartupError)
    # cleanup ran before re-raising: http server + user processes both stopped.
    assert "server_stop" in events
    assert any(isinstance(e, str) and e == "process_stop" for e in events)
