import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest


SPEC = importlib.util.spec_from_file_location(
    "adx_obs", Path(__file__).resolve().parents[1] / "obs_upload.py"
)
obs = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(obs)


class Body:
    def __init__(self, content_length=0):
        self.contentLength = content_length
        self.etag = "fixture-etag"
        self.versionId = "fixture-version"


class Response:
    def __init__(self, status=200, content_length=0):
        self.status = status
        self.body = Body(content_length)
        self.requestId = "fixture-request"
        self.errorCode = "fixture-error"
        self.errorMessage = "fixture failure"


class Client:
    def __init__(self, failure=None, metadata_delta=0):
        self.failure = failure
        self.metadata_delta = metadata_delta
        self.objects = {}
        self.closed = False

    def putFile(self, bucket, key, path, progressCallback=None):
        if self.failure == key:
            return Response(500)
        self.objects[(bucket, key)] = Path(path).read_bytes()
        return Response(200)

    def getObjectMetadata(self, bucket, key):
        body = self.objects.get((bucket, key))
        length = 0 if body is None else len(body) + self.metadata_delta
        return Response(404 if body is None else 200, length)

    def close(self):
        self.closed = True


class ObsTests(unittest.TestCase):
    def test_cli_imports_external_obs_sdk_instead_of_uploader_module(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            (root / "obs.py").write_text(
                """\
import os

class Body:
    def __init__(self, length=0):
        self.contentLength = length

class Response:
    def __init__(self, status=200, length=0):
        self.status = status
        self.body = Body(length)

class ObsClient:
    def __init__(self, **_kwargs):
        self.sizes = {}
    def putFile(self, bucket, key, path):
        self.sizes[(bucket, key)] = os.path.getsize(path)
        return Response()
    def getObjectMetadata(self, bucket, key):
        return Response(200, self.sizes[(bucket, key)])
    def close(self):
        pass
"""
            )
            artifact = root / "adx-platform.tar.zst"
            artifact.write_bytes(b"platform")
            output = root / "manifest.json"
            env = dict(os.environ)
            env.update({
                "PYTHONPATH": str(root),
                "OBS_ACCESS_KEY_ID": "fixture-ak",
                "OBS_SECRET_ACCESS_KEY": "fixture-sk",
            })
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(obs.__file__)),
                    "--output", str(output),
                    "--arch", "amd64",
                    "--timestamp", "20260921123045",
                    "--commit", "d" * 40,
                    str(artifact),
                ],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            self.assertEqual(json.loads(output.read_text())["artifacts"][0]["name"], artifact.name)

    def test_daily_upload_uses_adx_namespace_and_publishes_verified_manifest(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            package = root / "adx-platform.tar.zst"
            checksum = root / "adx-platform.tar.zst.sha256"
            package.write_bytes(b"platform")
            checksum.write_text("digest  adx-platform.tar.zst\n")
            output = root / "obs" / "manifest.json"
            client = Client()

            manifest = obs.publish(
                client=client,
                files=[package, checksum],
                output=output,
                bucket="openyuanrong",
                endpoint="obs.example.test",
                channel="daily",
                version=None,
                platform="linux",
                arch="amd64",
                timestamp="20260921123045",
                commit="a" * 40,
                build_id="build-42",
                dry_run=False,
            )

            prefix = "adx/daily/20260921123045-aaaaaaaaaaaa/linux/amd64/"
            self.assertEqual([item["object"] for item in manifest["artifacts"]], [
                prefix + package.name,
                prefix + checksum.name,
            ])
            self.assertEqual(manifest["artifacts"][0]["bytes"], len(b"platform"))
            self.assertEqual(len(manifest["artifacts"][0]["sha256"]), 64)
            self.assertNotIn("source", manifest["artifacts"][0])
            self.assertNotIn(str(root), output.read_text())
            self.assertEqual(json.loads(output.read_text()), manifest)
            self.assertIn(("openyuanrong", prefix + "manifest.json"), client.objects)
            self.assertTrue(client.closed)

    def test_release_requires_version_and_uses_immutable_version_path(self):
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "adx-rrt.tar.zst"
            artifact.write_bytes(b"rrt")
            with self.assertRaisesRegex(ValueError, "version"):
                obs.plan(
                    [artifact], "release", None, "linux", "arm64",
                    "20260921123045", "b" * 40, "build-43", "bucket", "obs.test"
                )
            manifest = obs.plan(
                [artifact], "release", "0.2.0-rc.1", "linux", "arm64",
                "20260921123045", "b" * 40, "build-43", "bucket", "obs.test"
            )
            self.assertEqual(
                manifest["artifacts"][0]["object"],
                "adx/release/0.2.0-rc.1/linux/arm64/adx-rrt.tar.zst",
            )

    def test_rejects_symlinks_duplicate_names_and_failed_upload(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            one = root / "one"
            two = root / "two"
            one.mkdir()
            two.mkdir()
            first = one / "same.tar.zst"
            second = two / "same.tar.zst"
            first.write_bytes(b"one")
            second.write_bytes(b"two")
            invalid = root / "bad name.tar.zst"
            invalid.write_bytes(b"invalid")
            link = root / "link.tar.zst"
            link.symlink_to(first)
            common = ("daily", None, "linux", "amd64", "20260921123045", "c" * 40,
                      "build-44", "bucket", "obs.test")
            with self.assertRaisesRegex(ValueError, "symlink"):
                obs.plan([link], *common)
            with self.assertRaisesRegex(ValueError, "duplicate"):
                obs.plan([first, second], *common)
            with self.assertRaisesRegex(ValueError, "artifact filename"):
                obs.plan([invalid], *common)

            client = Client(failure="adx/daily/20260921123045-cccccccccccc/linux/amd64/same.tar.zst")
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                obs.publish(
                    client=client,
                    files=[first],
                    output=root / "manifest.json",
                    bucket="bucket",
                    endpoint="obs.test",
                    channel="daily",
                    version=None,
                    platform="linux",
                    arch="amd64",
                    timestamp="20260921123045",
                    commit="c" * 40,
                    build_id="build-44",
                    dry_run=False,
                )
            self.assertTrue(client.closed)

            client = Client(metadata_delta=1)
            with self.assertRaisesRegex(RuntimeError, "readback verification failed"):
                obs.publish(
                    client=client,
                    files=[first],
                    output=root / "mismatch.json",
                    bucket="bucket",
                    endpoint="obs.test",
                    channel="daily",
                    version=None,
                    platform="linux",
                    arch="amd64",
                    timestamp="20260921123045",
                    commit="c" * 40,
                    build_id="build-44",
                    dry_run=False,
                )
            self.assertTrue(client.closed)


if __name__ == "__main__":
    unittest.main()
