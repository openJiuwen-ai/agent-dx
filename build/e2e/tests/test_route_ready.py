"""The file fault cases start only after the read-only route probe succeeds."""

import importlib.util
from pathlib import Path
import sys
import types
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[1]


class RouteReadyTests(unittest.TestCase):
    def load_case(self):
        class SandboxError(Exception):
            pass

        spec = importlib.util.spec_from_file_location('route_ready_case', ROOT / 'route_ready.py')
        module = importlib.util.module_from_spec(spec)
        with patch.dict(sys.modules, {'adx_sandbox': types.SimpleNamespace(SandboxError=SandboxError)}):
            spec.loader.exec_module(module)
        return module, SandboxError

    def test_retry_only_missing_synchronized_route(self):
        module, error_type = self.load_case()
        attempts = []

        def list_commands():
            attempts.append('read')
            if len(attempts) < 3:
                raise error_type('HTTP 503 route absent from synchronized cache')
            return []

        sandbox = types.SimpleNamespace(commands=types.SimpleNamespace(list=list_commands))
        module.wait_for_route(sandbox, sleep=lambda _: None)
        self.assertEqual(attempts, ['read'] * 3)

        def other_failure():
            raise error_type('HTTP 503 runtime unavailable')

        sandbox.commands.list = other_failure
        with self.assertRaisesRegex(error_type, 'runtime unavailable'):
            module.wait_for_route(sandbox, sleep=lambda _: None)


if __name__ == '__main__':
    unittest.main()
