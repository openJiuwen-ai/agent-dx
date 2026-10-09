"""Real Linux C-probe guards for optional, independently auditable IO samples."""
import json
from pathlib import Path
import platform
import subprocess
import tempfile
import unittest


@unittest.skipUnless(platform.system() == "Linux", "Linux integration only")
class IoSamples(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.build = tempfile.TemporaryDirectory(prefix="afs-io-build-", dir="/var/tmp")
        cls.binary = Path(cls.build.name) / "probe"
        source = Path(__file__).with_name("ownerfs_native_closeout_io.c")
        subprocess.run(["cc", "-O2", "-std=c11", "-Wall", "-Wextra", "-Werror",
                        "-pthread", str(source), "-o", str(cls.binary)], check=True)

    @classmethod
    def tearDownClass(cls):
        cls.build.cleanup()

    def setUp(self):
        self.scratch = tempfile.TemporaryDirectory(prefix="afs-io-case-", dir="/var/tmp")
        self.addCleanup(self.scratch.cleanup)
        self.file = Path(self.scratch.name) / "data"
        self.file.write_bytes(bytes([97]) * 1048576)

    def run_probe(self, *extra, operation="seq-read", cache="unobserved", mode="existing"):
        return subprocess.run([str(self.binary), str(self.file), operation, "1048576",
                               "16384", "1", "close", "1048576", "97", mode,
                               cache, *extra], capture_output=True, text=True)

    def test_legacy_output_and_corruption_propagation(self):
        result = self.run_probe()
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.assertNotIn("latency_samples_ns", value)
        self.assertEqual(value["operations"], 64)
        self.assertTrue(value["content_ok"])
        with self.file.open("r+b") as handle:
            handle.seek(1048575)
            handle.write(b"X")
        result = self.run_probe()
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("read content", result.stderr)

    def check_samples(self, value, interval):
        samples = value["latency_samples_ns"]
        self.assertEqual(len(samples), value["operations"])
        self.assertEqual(len(samples), 64)
        self.assertTrue(all(isinstance(ns, int) and ns > 0 for ns in samples))
        self.assertEqual(samples, sorted(samples))
        self.assertEqual(value["latency_order"], "sorted")
        self.assertEqual(value["latency_interval"], interval)
        for percentile in (50, 95, 99):
            self.assertEqual(value[f"p{percentile}_ns"], samples[len(samples) * percentile // 100])

    def test_all_read_samples_recompute_percentiles_with_observed_hot_cache(self):
        result = self.run_probe("samples", cache="hot")
        self.assertEqual(result.returncode, 0, result.stderr)
        value = json.loads(result.stdout)
        self.check_samples(value, "pread+count-check")
        self.assertTrue(value["residency_observed"])
        self.assertEqual(value["resident_before_bytes"], 1048576)
        self.assertEqual(value["cache_requested"], "hot")

    def test_write_samples_preserve_content_and_label_interval(self):
        self.file.unlink()
        result = self.run_probe("samples", operation="seq-write", mode="create")
        self.assertEqual(result.returncode, 0, result.stderr)
        self.check_samples(json.loads(result.stdout), "pwrite+count-check")
        self.assertEqual(self.file.read_bytes(), bytes([97]) * 1048576)

    def test_invalid_sample_mode_rejected_before_file_change(self):
        before = self.file.stat()
        result = self.run_probe("unknown", operation="seq-write")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("sample output mode", result.stderr)
        self.assertEqual(self.file.stat().st_mtime_ns, before.st_mtime_ns)
        self.assertEqual(self.file.read_bytes(), bytes([97]) * 1048576)

    def test_sample_request_cannot_hide_content_error(self):
        with self.file.open("r+b") as handle:
            handle.seek(1048575)
            handle.write(b"X")
        result = self.run_probe("samples")
        self.assertNotEqual(result.returncode, 0)
        self.assertEqual(result.stdout, "")
        self.assertIn("read content", result.stderr)


if __name__ == "__main__":
    unittest.main()
