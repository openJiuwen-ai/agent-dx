import importlib.util
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


def _executable(name, environment=None, cwd=None):
    """Resolve an external command using the child's execution environment."""
    directory = os.getcwd() if cwd is None else os.path.abspath(cwd)
    search_path = os.pathsep.join(
        os.path.abspath(os.path.join(directory, entry)) for entry in os.get_exec_path(environment)
    )
    executable = shutil.which(name, path=search_path)
    if executable is None:
        raise FileNotFoundError(f"required executable not found: {name}")
    return os.path.abspath(executable)


SCRIPT = Path(__file__).resolve().parents[1] / "cargo_cache.py"


class CargoCacheTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        spec = importlib.util.spec_from_file_location("cargo_cache", SCRIPT)
        cls.cache = importlib.util.module_from_spec(spec)
        sys.modules[spec.name] = cls.cache
        spec.loader.exec_module(cls.cache)

    def test_parse_rustc_info_requires_host_and_release(self):
        self.assertEqual(
            self.cache.parse_rustc_info("host: aarch64-apple-darwin\nrelease: 1.95.0\n"),
            ("aarch64-apple-darwin", "1.95.0"),
        )
        with self.assertRaises(ValueError):
            self.cache.parse_rustc_info("release: 1.95.0\n")

    def test_worktrees_share_target_but_keep_isolated_targets_distinct(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp) / "source"
            linked = Path(temp) / "feature"
            subprocess.run([_executable("git"), "init", "-q", str(root)], check=True)
            subprocess.run(
                [
                    _executable("git"),
                    "-C",
                    str(root),
                    "-c",
                    "user.name=Test",
                    "-c",
                    "user.email=test@example.com",
                    "commit",
                    "--allow-empty",
                    "-qm",
                    "initial",
                ],
                check=True,
            )
            subprocess.run(
                [_executable("git"), "-C", str(root), "worktree", "add", "-q", "-b", "feature", str(linked)], check=True
            )
            rustc = Path(temp) / "rustc"
            rustc.write_text("#!/bin/sh\nprintf '%s\\n' 'host: aarch64-apple-darwin' 'release: 1.95.0'\n")
            rustc.chmod(0o755)

            primary = self.cache.discover(root, rustc=str(rustc))
            worktree = self.cache.discover(linked, rustc=str(rustc))
            self.assertEqual(primary.shared_target, worktree.shared_target)
            self.assertNotEqual(primary.isolated_target, worktree.isolated_target)
            self.assertEqual(primary.cache_key, "aarch64-apple-darwin-rust1.95.0")

    def test_shared_environment_disables_incremental(self):
        layout = self.cache.Layout(
            repo=Path("/tmp/worktree"),
            primary=Path("/tmp/source"),
            cache_root=Path("/tmp/source/.adx-cache"),
            cache_key="aarch64-apple-darwin-rust1.95.0",
        )
        values = self.cache.environment(layout, "shared")
        self.assertEqual(values["CARGO_TARGET_DIR"], str(layout.shared_target))
        self.assertEqual(values["CARGO_INCREMENTAL"], "0")
        self.assertEqual(values["SCCACHE_CACHE_SIZE"], "20G")


if __name__ == "__main__":
    unittest.main()
