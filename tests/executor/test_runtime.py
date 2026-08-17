#!/usr/bin/env python3
# coding=UTF-8

from yr.agentexecutor import handler
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
