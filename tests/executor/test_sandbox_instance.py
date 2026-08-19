#!/usr/bin/env python3
# coding=UTF-8

import os
import signal
import subprocess

import pytest

from yr.agentexecutor.sandbox_instance import SandboxInstance


def test_execute_uses_default_working_directory_and_environment(tmp_path):
    sandbox = SandboxInstance(str(tmp_path), env={"SANDBOX_VALUE": "expected"})

    result = sandbox.execute(
        ["/bin/sh", "-c", "printf '%s:%s' \"$SANDBOX_VALUE\" \"$PWD\""]
    )

    assert result == {
        "returncode": 0,
        "stdout": f"expected:{tmp_path}",
        "stderr": "",
    }


def test_execute_preserves_normal_command_failure(tmp_path):
    sandbox = SandboxInstance(str(tmp_path))

    result = sandbox.execute(["/bin/sh", "-c", "printf failed >&2; exit 23"])

    assert result == {"returncode": 23, "stdout": "", "stderr": "failed"}


def test_execute_uses_default_timeout_and_starts_a_process_group(monkeypatch, tmp_path):
    captured = {}

    class Process:
        returncode = 0

        @staticmethod
        def communicate(timeout=None):
            captured["timeout"] = timeout
            return "done", ""

    def popen(**kwargs):
        captured.update(kwargs)
        return Process()

    monkeypatch.setattr(subprocess, "Popen", popen)
    sandbox = SandboxInstance(str(tmp_path))

    result = sandbox.execute("command")

    assert result == {"returncode": 0, "stdout": "done", "stderr": ""}
    assert captured["timeout"] == 300
    assert captured["start_new_session"] is True


def test_execute_timeout_kills_the_process_group(monkeypatch, tmp_path):
    signals = []

    class Process:
        pid = 123
        returncode = None
        communicate_calls = 0

        def communicate(self, timeout=None):
            self.communicate_calls += 1
            if self.communicate_calls == 1:
                raise subprocess.TimeoutExpired("command", timeout, output=b"partial")
            self.returncode = -signal.SIGKILL
            return "partial", ""

        def poll(self):
            return self.returncode

    process = Process()
    monkeypatch.setattr(subprocess, "Popen", lambda **_kwargs: process)
    monkeypatch.setattr(os, "killpg", lambda pid, sig: signals.append((pid, sig)))
    sandbox = SandboxInstance(str(tmp_path))

    result = sandbox.execute("command", timeout=2)

    assert result == {
        "returncode": -1,
        "stdout": "partial",
        "stderr": "Command timed out after 2 seconds",
    }
    assert signals == [(123, signal.SIGKILL)]
    assert process.communicate_calls == 2


def test_read_write_list_and_search_files(tmp_path):
    sandbox = SandboxInstance(str(tmp_path))
    text_path = tmp_path / "nested" / "note.txt"
    binary_path = tmp_path / "nested" / "data.bin"

    sandbox.write_file(str(text_path), "hello", mode="w")
    sandbox.write_file(str(binary_path), b"\x00\x01", mode="wb")

    assert sandbox.read_file(str(text_path), mode="r") == "hello"
    assert sandbox.read_file(str(binary_path)) == b"\x00\x01"
    listed = sandbox.list_files(str(tmp_path), recursive=True)
    assert {item["name"] for item in listed} == {"nested", "note.txt", "data.bin"}
    assert [item["name"] for item in sandbox.search_files(str(tmp_path), "*.txt")] == [
        "note.txt"
    ]


def test_default_working_directory_is_cleaned_up():
    sandbox = SandboxInstance()
    working_dir = sandbox.working_dir

    sandbox.cleanup()

    with pytest.raises(RuntimeError, match="Sandbox is not initialized"):
        sandbox.execute("true")
    assert not os.path.exists(working_dir)
