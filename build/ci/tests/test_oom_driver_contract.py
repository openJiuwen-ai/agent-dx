"""The OOM acceptance driver's isolation checks survive Python optimization."""

import ast
import json
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest

DRIVER = Path(__file__).resolve().parents[2] / "e2e" / "oom_sdk.py"


def mapping_code():
    tree = ast.parse(DRIVER.read_text())
    for item in tree.body:
        if isinstance(item, ast.Assign) and any(
            isinstance(target, ast.Name) and target.id == "MAP_CODE" for target in item.targets
        ):
            return ast.literal_eval(item.value)
    raise RuntimeError("driver mapping helper is absent")


class OomDriverContractTests(unittest.TestCase):
    def test_isolation_guards_are_not_removable_assertions(self):
        tree = ast.parse(DRIVER.read_text())
        self.assertFalse(any(isinstance(node, ast.Assert) for node in ast.walk(tree)))
        for item in ast.walk(tree):
            if isinstance(item, ast.Constant) and isinstance(item.value, str) and "import sys" in item.value:
                helper = ast.parse(item.value)
                self.assertFalse(any(isinstance(node, ast.Assert) for node in ast.walk(helper)))

    def test_optimized_cli_help_does_not_require_a_live_fixture(self):
        result = subprocess.run([sys.executable, "-O", str(DRIVER), "--help"], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("--container", result.stdout)

    def test_unsafe_or_ambiguous_execution_is_rejected_under_optimization(self):
        for paths, message in [
            (["/"], "unsafe sandbox cgroup"),
            (["/../../tmp"], "unsafe sandbox cgroup"),
            (["/akernel/test/runc", "/akernel/test/runc"], "ambiguous sandbox identity"),
        ]:
            with self.subTest(paths=paths), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                for index, path in enumerate(paths):
                    backend = root / str(index)
                    backend.mkdir()
                    (backend / "config.json").write_text(
                        json.dumps(
                            {
                                "annotations": {"adx.environment_id": "owned"},
                                "linux": {"cgroupsPath": path},
                            }
                        )
                    )
                result = subprocess.run(
                    [sys.executable, "-O", "-c", mapping_code(), "owned", str(root)],
                    capture_output=True,
                    text=True,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)


if __name__ == "__main__":
    unittest.main()
