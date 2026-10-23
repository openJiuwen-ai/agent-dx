"""Safety checks for privileged acceptance runners, without starting backends."""

import asyncio
import io
import json
import logging
import os
import sys
from pathlib import Path
import tempfile
from types import ModuleType, SimpleNamespace
import unittest
from unittest.mock import Mock, patch

ROOT = Path(__file__).resolve().parents[3]
SOURCE = ROOT / "build/e2e/jiuwenbox/accept.py"


def load_acceptance(optimize=0):
    namespace = {"__name__": "acceptance_fixture", "__file__": str(SOURCE)}
    # Process/root safety checks do not use the live runner's HTTP or YAML APIs.
    # Empty modules keep this unit suite stdlib-only and fail on accidental use.
    with patch.dict(sys.modules, {"httpx": ModuleType("httpx"), "yaml": ModuleType("yaml")}):
        exec(compile(SOURCE.read_text(), str(SOURCE), "exec", optimize=optimize), namespace)
    return SimpleNamespace(**namespace)


class JiuwenboxSafetyTests(unittest.TestCase):
    def test_optimized_runner_still_checks_root_before_host_mutation(self):
        module = load_acceptance(optimize=2)
        acceptance = module.Acceptance.__new__(module.Acceptance)
        with patch.object(module.os, "geteuid", return_value=123):
            with self.assertRaisesRegex(AssertionError, "root"):
                asyncio.run(acceptance.prepare())

    def test_spawn_closes_parent_log_on_success_and_failure(self):
        module = load_acceptance()
        with tempfile.TemporaryDirectory() as directory:
            acceptance = module.Acceptance.__new__(module.Acceptance)
            acceptance.run = Path(directory)
            acceptance.children = {}
            opened = []

            def spawn_process(*args, **kwargs):
                log = kwargs["stdout"]
                self.assertFalse(log.closed)
                opened.append(log)
                if len(opened) == 2:
                    raise OSError("spawn failed")
                return SimpleNamespace(pid=1)

            with patch.object(module.subprocess, "Popen", side_effect=spawn_process):
                acceptance.spawn("success", ["/test/program"])
                with self.assertRaisesRegex(OSError, "spawn failed"):
                    acceptance.spawn("failure", ["/test/program"])
            self.assertTrue(all(log.closed for log in opened))

    def test_driver_closes_parent_log_on_success_and_failure(self):
        module = load_acceptance()
        with tempfile.TemporaryDirectory() as directory:
            acceptance = module.Acceptance.__new__(module.Acceptance)
            acceptance.root = Path(directory)
            acceptance.drivers = []
            acceptance.driver_configs = {}
            opened = []

            async def ready():
                return b'{"ready":true}\n'

            async def start_driver(*args, **kwargs):
                log = kwargs["stderr"]
                self.assertFalse(log.closed)
                opened.append(log)
                if len(opened) == 2:
                    raise OSError("spawn failed")
                return SimpleNamespace(pid=1, stdout=SimpleNamespace(readline=ready))

            with patch.object(module.asyncio, "create_subprocess_exec", side_effect=start_driver):
                asyncio.run(acceptance.driver(Path(directory) / "config.json"))
                with self.assertRaisesRegex(OSError, "spawn failed"):
                    asyncio.run(acceptance.driver(Path(directory) / "config.json"))
            self.assertTrue(all(log.closed for log in opened))

    def test_sdk_json_lines_remain_machine_readable(self):
        import importlib.metadata

        sdk = SimpleNamespace(
            __file__="/sdk-venv/lib/adx_environment/__init__.py",
            ConnectionConfig=lambda **options: options,
            Environment=object,
            EnvironmentNotFound=LookupError,
            EnvironmentError=RuntimeError,
            RestartPolicy=object,
        )
        source = ROOT / "build/e2e/jiuwenbox/sdk_client.py"
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            token = root / "token"
            token.write_text("test-token")
            config = root / "config.json"
            config.write_text(
                json.dumps({"endpoint": "localhost:1", "ca": "test-ca", "token_file": str(token)})
            )
            output = io.StringIO()
            with (
                patch.dict(sys.modules, {"adx_environment": sdk}),
                patch.dict(os.environ),
                patch.object(sys, "argv", [str(source), str(config)]),
                patch.object(sys, "stdin", io.StringIO('{"action":"close"}\n')),
                patch.object(sys, "stdout", output),
                patch.object(importlib.metadata, "version", return_value="0.1.0"),
            ):
                namespace = {"__name__": "sdk_fixture"}
                exec(compile(source.read_text(), str(source), "exec"), namespace)
            messages = [json.loads(line) for line in output.getvalue().splitlines()]
            self.assertTrue(messages[0]["ready"])
            self.assertEqual(messages[1], {"ok": True, "value": True})
            failed_stream = SimpleNamespace(write=Mock(side_effect=BrokenPipeError("closed")), flush=Mock())
            handler_type = namespace.get("ProtocolHandler")
            self.assertIsNotNone(handler_type)
            handler = handler_type(failed_stream)
            with self.assertRaisesRegex(BrokenPipeError, "closed"):
                handler.emit(logging.makeLogRecord({"msg": "response", "args": ()}))


if __name__ == "__main__":
    unittest.main()
