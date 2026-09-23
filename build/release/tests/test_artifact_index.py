import importlib.util
from pathlib import Path
import unittest


SPEC = importlib.util.spec_from_file_location(
    "artifact_index", Path(__file__).resolve().parents[1] / "artifact_index.py"
)


class ArtifactIndexTests(unittest.TestCase):
    def setUp(self):
        module = importlib.util.module_from_spec(SPEC)
        SPEC.loader.exec_module(module)
        self.index = module
        self.commit = "a" * 40
        self.build_id = "build-42"

    def manifest(self):
        return {
            "schema_version": 1,
            "commit": self.commit,
            "build_id": self.build_id,
            "bucket": "openyuanrong",
            "endpoint": "obs.cn-southwest-2.myhuaweicloud.com",
            "manifest_url": "https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/adx/daily/1/manifest.json",
            "artifacts": [
                {
                    "name": "adx-release.tar.gz",
                    "url": "https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/adx/daily/1/adx-release.tar.gz",
                    "bytes": 1024,
                    "sha256": "b" * 64,
                },
                {
                    "name": "adx-execd.tar.gz",
                    "url": "https://openyuanrong.obs.cn-southwest-2.myhuaweicloud.com/adx/daily/1/adx-execd.tar.gz",
                    "bytes": 512,
                    "sha256": "c" * 64,
                },
            ],
        }

    def test_render_summarizes_verified_obs_artifacts(self):
        page = self.index.render(
            self.manifest(), commit=self.commit, build_id=self.build_id,
            build_url="https://buildkite.com/agent-dx/agent-dx/builds/42",
        )
        self.assertIn("ADX Build Artifacts", page)
        self.assertIn("adx-release.tar.gz", page)
        self.assertIn("adx-execd.tar.gz", page)
        self.assertIn("1.0 KiB", page)
        self.assertIn("b" * 64, page)
        self.assertIn("manifest.json", page)
        self.assertIn("https://buildkite.com/agent-dx/agent-dx/builds/42", page)

    def test_rejects_stale_manifest_and_unsafe_url(self):
        with self.assertRaisesRegex(ValueError, "commit"):
            self.index.render(self.manifest(), commit="d" * 40,
                              build_id=self.build_id, build_url="https://buildkite.com/builds/42")
        with self.assertRaisesRegex(ValueError, "build ID"):
            self.index.render(self.manifest(), commit=self.commit,
                              build_id="another-build", build_url="https://buildkite.com/builds/42")
        manifest = self.manifest()
        manifest["artifacts"][0]["url"] = "javascript:alert(1)"
        with self.assertRaisesRegex(ValueError, "artifact URL"):
            self.index.render(manifest, commit=self.commit,
                              build_id=self.build_id, build_url="https://buildkite.com/builds/42")

    def test_disabled_publication_has_buildkite_artifact_link(self):
        page = self.index.render(
            None, commit=self.commit, build_id=self.build_id,
            build_url="https://buildkite.com/agent-dx/agent-dx/builds/42",
        )
        self.assertIn("OBS publication disabled", page)
        self.assertIn("https://buildkite.com/agent-dx/agent-dx/builds/42#artifacts", page)


if __name__ == "__main__":
    unittest.main()
