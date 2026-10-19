import subprocess
import sys
from pathlib import Path
import unittest


SMOKE = Path(__file__).with_name("smoke.py")


class SmokeUnitTest(unittest.TestCase):
    def test_require_remains_active_with_optimized_python(self):
        program = "\n".join(
            [
                "import importlib.util",
                f"spec = importlib.util.spec_from_file_location('smoke', {str(SMOKE)!r})",
                "module = importlib.util.module_from_spec(spec)",
                "spec.loader.exec_module(module)",
                "try:",
                "    module.require(False, 'must fail')",
                "except AssertionError:",
                "    pass",
                "else:",
                "    raise SystemExit('explicit check was optimized away')",
            ]
        )
        result = subprocess.run(
            [sys.executable, "-O", "-c", program],
            check=False,
            capture_output=True,
            text=True,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


if __name__ == "__main__":
    unittest.main()
