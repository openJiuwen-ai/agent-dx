import importlib.util
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from contextlib import redirect_stdout
from pathlib import Path
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location("adx_obs", Path(__file__).resolve().parents[1] / "obs_upload.py")
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

    def put_file(self, bucket, key, path):
        if self.failure == key:
            return Response(500)
        self.objects[(bucket, key)] = Path(path).read_bytes()
        return Response(200)

    def get_object_metadata(self, bucket, key):
        body = self.objects.get((bucket, key))
        length = 0 if body is None else len(body) + self.metadata_delta
        return Response(404 if body is None else 200, length)

    def get_object(self, bucket, key, **options):
        body = self.objects.get((bucket, key))
        if body is None:
            return Response(404)
        Path(options["downloadPath"]).write_bytes(body)
        return Response()

    # Match the Huawei OBS SDK methods called by the uploader.
    putFile = put_file
    getObjectMetadata = get_object_metadata
    getObject = get_object

    def close(self):
        self.closed = True


class ObsTests(unittest.TestCase):
    def test_rootfs_candidate_advances_after_verified_publication(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            artifact = root / "adx-execd.tar.gz"
            artifact.write_bytes(b"first")
            client = Client()
            bucket, endpoint = "agent-dx", "obs.example.test"
            key = "adx/rootfs-reuse/linux/amd64/manifest.json"

            def publish(number, commit, timestamp="20261118090000", arch="amd64", dry_run=False):
                return obs.publish(
                    client=client, files=[artifact], output=root / "manifest.json", bucket=bucket,
                    endpoint=endpoint, channel="daily", version=None, platform="linux", arch=arch,
                    timestamp=timestamp, commit=commit * 40, build_id=f"build-{number}", dry_run=dry_run,
                )

            first = publish(301, "a")
            self.assertIsNone(obs.read_rootfs_candidate(client, bucket, endpoint, "amd64"))
            obs.advance_rootfs_candidate(client, first, 301)
            first = first["artifacts"][0]
            candidate = obs.read_rootfs_candidate(client, bucket, endpoint, "amd64")
            self.assertEqual(candidate, {
                "schema_version": 1, "arch": "amd64", "build_number": 301, "commit": "a" * 40,
                "url": first["url"], "sha256": first["sha256"],
            })
            self.assertIsNone(obs.read_rootfs_candidate(client, bucket, endpoint, "arm64"))
            self.assertLess(list(client.objects).index((bucket, first["object"])),
                            list(client.objects).index((bucket, key)))

            artifact.write_bytes(b"second")
            second = publish(302, "b")
            obs.advance_rootfs_candidate(client, second, 302)
            second = second["artifacts"][0]
            candidate = obs.read_rootfs_candidate(client, bucket, endpoint, "amd64")
            self.assertEqual((candidate["url"], candidate["sha256"]), (second["url"], second["sha256"]))
            obs.advance_rootfs_candidate(client, publish(300, "c"), 300)
            obs.advance_rootfs_candidate(client, publish(302, "b", "20261118090100"), 302)
            publish(303, "d", dry_run=True)
            obs.advance_rootfs_candidate(client, publish(303, "e", arch="arm64"), 303)
            self.assertEqual(obs.read_rootfs_candidate(client, bucket, endpoint, "amd64"), candidate)

            client.failure = "adx/daily/20261118090000-ffffffffffff/linux/amd64/adx-execd.tar.gz"
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                publish(304, "f")
            self.assertEqual(obs.read_rootfs_candidate(client, bucket, endpoint, "amd64"), candidate)

            with patch.object(client, "getObject", return_value=Response(403)):
                released = publish(305, "f", "20261118090100")
                self.assertIn((bucket, released["manifest_object"]), client.objects)
                with self.assertRaisesRegex(RuntimeError, "read failed: status=403"):
                    obs.advance_rootfs_candidate(client, released, 305)
            self.assertEqual(obs.read_rootfs_candidate(client, bucket, endpoint, "amd64"), candidate)

            client.failure = key
            released = publish(306, "f", "20261118090200")
            self.assertIn((bucket, released["manifest_object"]), client.objects)
            with self.assertRaisesRegex(RuntimeError, "upload failed"):
                obs.advance_rootfs_candidate(client, released, 306)
            self.assertEqual(obs.read_rootfs_candidate(client, bucket, endpoint, "amd64"), candidate)

            client.objects[(bucket, key)] = json.dumps(dict(candidate, arch="arm64")).encode()
            with self.assertRaisesRegex(ValueError, "invalid EROFS reuse manifest"):
                obs.read_rootfs_candidate(client, bucket, endpoint, "amd64")

    def test_initial_rootfs_candidate_uses_only_the_pinned_amd64_seed(self):
        credentials = {
            "OBS_ACCESS_KEY_ID": "fixture", "OBS_SECRET_ACCESS_KEY": "fixture",
            "ADX_OBS_BUCKET": "agent-dx", "ADX_OBS_ENDPOINT": "obs.cn-southwest-2.myhuaweicloud.com",
        }
        with patch.object(obs, "obs_client", return_value=Client()), patch.dict(os.environ, credentials):
            with redirect_stdout(io.StringIO()) as output:
                obs.rootfs_candidate_main(["--arch", "amd64"])
            url, digest = output.getvalue().splitlines()
            self.assertTrue(url.endswith("20261117235108-4104c42e7783/linux/amd64/adx-execd.tar.gz"))
            self.assertEqual(digest, "bc073cceac7c09317ede57110ccbb93522177cba02f24a2ef79ba593c0920fa5")
            with redirect_stdout(io.StringIO()) as output:
                obs.rootfs_candidate_main(["--arch", "arm64"])
            self.assertEqual(output.getvalue(), "")

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
            env.pop("ADX_OBS_BUCKET", None)
            env.pop("ADX_OBS_ENDPOINT", None)
            env.update(
                {
                    "PYTHONPATH": str(root),
                    "OBS_ACCESS_KEY_ID": "fixture-ak",
                    "OBS_SECRET_ACCESS_KEY": "fixture-sk",
                }
            )
            result = subprocess.run(
                [
                    sys.executable,
                    str(Path(obs.__file__)),
                    "--output",
                    str(output),
                    "--arch",
                    "amd64",
                    "--timestamp",
                    "20260921123045",
                    "--commit",
                    "d" * 40,
                    str(artifact),
                ],
                capture_output=True,
                text=True,
                env=env,
                check=False,
            )
            self.assertEqual(result.returncode, 0, result.stderr)
            manifest = json.loads(output.read_text())
            self.assertEqual(manifest["artifacts"][0]["name"], artifact.name)
            self.assertEqual(manifest["bucket"], "agent-dx")
            self.assertTrue(manifest["manifest_url"].startswith(
                "https://agent-dx.obs.cn-southwest-2.myhuaweicloud.com/adx/daily/"
            ))
            self.assertIn(manifest["artifacts"][0]["url"], result.stdout)

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
                bucket="agent-dx",
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
            self.assertEqual(
                [item["object"] for item in manifest["artifacts"]],
                [
                    prefix + package.name,
                    prefix + checksum.name,
                ],
            )
            self.assertEqual(manifest["artifacts"][0]["bytes"], len(b"platform"))
            self.assertEqual(len(manifest["artifacts"][0]["sha256"]), 64)
            self.assertNotIn("source", manifest["artifacts"][0])
            self.assertNotIn(str(root), output.read_text())
            self.assertEqual(json.loads(output.read_text()), manifest)
            self.assertIn(("agent-dx", prefix + "manifest.json"), client.objects)
            self.assertTrue(client.closed)

    def test_release_requires_version_and_uses_immutable_version_path(self):
        with tempfile.TemporaryDirectory() as temp:
            artifact = Path(temp) / "adx-execd.tar.zst"
            artifact.write_bytes(b"execd")
            with self.assertRaisesRegex(ValueError, "version"):
                obs.plan(
                    [artifact],
                    "release",
                    None,
                    "linux",
                    "arm64",
                    "20260921123045",
                    "b" * 40,
                    "build-43",
                    "bucket",
                    "obs.test",
                )
            manifest = obs.plan(
                [artifact],
                "release",
                "0.2.0-rc.1",
                "linux",
                "arm64",
                "20260921123045",
                "b" * 40,
                "build-43",
                "bucket",
                "obs.test",
            )
            self.assertEqual(
                manifest["artifacts"][0]["object"],
                "adx/release/0.2.0-rc.1/linux/arm64/adx-execd.tar.zst",
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
            common = ("daily", None, "linux", "amd64", "20260921123045", "c" * 40, "build-44", "bucket", "obs.test")
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
