#!/usr/bin/env python3
# coding=UTF-8

import json
import sys
import time

from yr.agentexecutor.process_manager import MAX_BOOTSTRAP_COMMANDS, ProcessManager, parse_bootstrap_commands


def test_parse_bootstrap_commands_ignores_invalid_values():
    assert parse_bootstrap_commands("{") == []
    assert parse_bootstrap_commands("{}") == []
    assert parse_bootstrap_commands('["cmd", [], ["ok", 1], ["valid"]]') == [["valid"]]


def test_process_manager_starts_and_stops_process(tmp_path):
    manager = ProcessManager()
    command = [sys.executable, "-c", "import time; time.sleep(60)"]
    manager.start([command], log_dir=str(tmp_path))
    try:
        assert manager.status()[0]["running"] is True
    finally:
        manager.stop(grace_period_seconds=0.1)
    assert manager.status() == []


def test_parse_bootstrap_commands_accepts_argv_lists():
    value = json.dumps([["python", "app.py"], ["worker", "--port", "8080"]])
    assert parse_bootstrap_commands(value) == [
        ["python", "app.py"],
        ["worker", "--port", "8080"],
    ]


def test_parse_bootstrap_commands_limits_command_count():
    commands = [["command", str(index)] for index in range(MAX_BOOTSTRAP_COMMANDS + 1)]
    assert parse_bootstrap_commands(json.dumps(commands)) == commands[:MAX_BOOTSTRAP_COMMANDS]


def test_process_manager_continues_after_one_command_fails(tmp_path):
    manager = ProcessManager()
    valid_command = [sys.executable, "-c", "import time; time.sleep(60)"]
    manager.start([["/definitely/not/a/command"], valid_command], log_dir=str(tmp_path))
    try:
        assert len(manager.status()) == 1
        assert manager.status()[0]["running"] is True
    finally:
        manager.stop(grace_period_seconds=0.1)


def test_process_manager_combines_stdout_and_stderr_in_compatible_log(tmp_path):
    manager = ProcessManager()
    command = [
        sys.executable,
        "-c",
        "import sys; print('stdout-line'); print('stderr-line', file=sys.stderr)",
    ]
    manager.start([command], log_dir=str(tmp_path), runtime_id="runtime-test")
    try:
        deadline = time.monotonic() + 5
        while manager.status()[0]["running"] and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        manager.stop(grace_period_seconds=0.1)

    output = (tmp_path / "runtime-test" / "bootstrap_cmd_0.log").read_text()
    assert "stdout-line" in output
    assert "stderr-line" in output


def test_process_manager_sanitizes_runtime_id_in_log_path(tmp_path):
    manager = ProcessManager()
    command = [sys.executable, "-c", "print('runtime-log')"]
    manager.start([command], log_dir=str(tmp_path), runtime_id="../runtime/id")
    try:
        deadline = time.monotonic() + 5
        while manager.status()[0]["running"] and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        manager.stop(grace_period_seconds=0.1)

    assert (tmp_path / ".._runtime_id" / "bootstrap_cmd_0.log").read_text().strip() == "runtime-log"


def test_process_manager_reads_runtime_id_from_env(tmp_path):
    manager = ProcessManager()
    environ = {
        "YR_RUNTIME_BOOTSTRAP_CMD": json.dumps([[sys.executable, "-c", "print('env-runtime-log')"]]),
        "GLOG_log_dir": str(tmp_path),
        "YR_RUNTIME_ID": "runtime-from-env",
    }
    manager.start_from_env(environ)
    try:
        deadline = time.monotonic() + 5
        while manager.status()[0]["running"] and time.monotonic() < deadline:
            time.sleep(0.01)
    finally:
        manager.stop(grace_period_seconds=0.1)

    output = tmp_path / "runtime-from-env" / "bootstrap_cmd_0.log"
    assert output.read_text().strip() == "env-runtime-log"
