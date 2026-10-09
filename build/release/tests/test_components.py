import importlib.util
import json
from pathlib import Path
import tempfile
import unittest


MODULE_PATH = Path(__file__).resolve().parents[1] / "component.py"
SPEC = importlib.util.spec_from_file_location("component", MODULE_PATH)
component = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(component)


class ComponentManifestTests(unittest.TestCase):
    def test_legacy_build_manifest_cannot_silently_disable_afs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            path = root / "manifest.json"
            for mode in (True, False):
                path.write_text(json.dumps({"schema_version": 1, "commit": "a" * 40,
                                            "target": "test", "with_dfs": mode}))
                with self.assertRaisesRegex(ValueError, "legacy with_dfs"):
                    component.verify_build_manifest(path, "a" * 40, "test",
                                                    root, root, root, root, root)

    def test_gateway_component_contains_both_process_modes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            (root / "adx-apiserver").write_bytes(b"fixture")
            (root / "adx-ingress").write_bytes(b"fixture")
            (root / "adx-relay").write_bytes(b"fixture")
            manifest = component.create_manifest("gateway", root, "a" * 40,
                                                 "x86_64-unknown-linux-gnu")
            self.assertEqual(set(manifest["files"]), {"adx-apiserver", "adx-ingress", "adx-relay"})
            component.verify_manifest(root)
            (root / "adx-data-plane-forward").write_bytes(b"unexpected")
            with self.assertRaises(ValueError):
                component.create_manifest("gateway", root, "a" * 40,
                                          "x86_64-unknown-linux-gnu")

    @staticmethod
    def write_component(root, name):
        directory = root / name
        directory.mkdir(parents=True, exist_ok=True)
        for filename in component.REQUIRED_FILES[name]:
            (directory / filename).write_text(name + filename)
        return directory

    def test_component_manifest_binds_identity_and_file_hashes(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            for filename in component.REQUIRED_FILES["platform"]:
                (root / filename).write_text(filename)

            manifest = component.create_manifest(
                component="platform",
                directory=root,
                commit="a" * 40,
                target="x86_64-unknown-linux-gnu",
            )

            self.assertEqual(manifest["schema_version"], 1)
            self.assertEqual(manifest["component"], "platform")
            self.assertEqual(manifest["commit"], "a" * 40)
            self.assertEqual(set(manifest["files"]), component.REQUIRED_FILES["platform"])
            self.assertEqual(component.verify_manifest(root), manifest)

            (root / "adx-coordinator").write_bytes(b"tampered")
            with self.assertRaises(ValueError):
                component.verify_manifest(root)

    def test_release_manifest_summarizes_components_and_outputs(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            components = root / "components"
            for name in component.BASE_COMPONENTS:
                directory = self.write_component(components, name)
                component.create_manifest(
                    component=name,
                    directory=directory,
                    commit="b" * 40,
                    target="x86_64-unknown-linux-gnu",
                )

            release = root / "adx-release.tar.gz"
            package_manifest = root / "release-manifest.json"
            wheel = root / "adx_sandbox-0.1.0-py3-none-any.whl"
            backend_manifest = root / "backend-manifest.json"
            backend_archive = root / "backend.tar.gz"
            release.write_bytes(b"release")
            package_manifest.write_text(json.dumps({"commit": "b" * 40}))
            wheel.write_bytes(b"wheel")
            backend_manifest.write_text(json.dumps({"sandboxd_revision": "fixture"}))
            backend_archive.write_bytes(b"backend archive")

            manifest = component.create_build_manifest(
                component_root=components,
                commit="b" * 40,
                target="x86_64-unknown-linux-gnu",
                package_manifest=package_manifest,
                release_archive=release,
                wheel=wheel,
                backend_manifest=backend_manifest,
                backend_archive=backend_archive,
            )

            self.assertEqual(set(manifest["components"]), {"platform", "gateway", "execd"})
            self.assertFalse(manifest["with_afs"])
            self.assertEqual(manifest["package"]["archive"]["name"], release.name)
            self.assertEqual(manifest["sdk"]["name"], wheel.name)
            self.assertEqual(manifest["backend"]["manifest"]["name"], backend_manifest.name)
            self.assertEqual(manifest["backend"]["archive"]["name"], backend_archive.name)

            build_manifest = root / "build-manifest.json"
            build_manifest.write_text(json.dumps(manifest))
            extracted_package_manifest = root / "manifest.json"
            extracted_package_manifest.write_bytes(package_manifest.read_bytes())
            verified = component.verify_build_manifest(
                manifest_path=build_manifest,
                commit="b" * 40,
                target="x86_64-unknown-linux-gnu",
                package_manifest=extracted_package_manifest,
                release_archive=release,
                wheel=wheel,
                backend_manifest=backend_manifest,
                backend_archive=backend_archive,
            )
            self.assertEqual(verified, manifest)

            backend_archive.write_bytes(b"tampered")
            with self.assertRaisesRegex(ValueError, "backend archive"):
                component.verify_build_manifest(
                    manifest_path=build_manifest,
                    commit="b" * 40,
                    target="x86_64-unknown-linux-gnu",
                    package_manifest=extracted_package_manifest,
                    release_archive=release,
                    wheel=wheel,
                    backend_manifest=backend_manifest,
                    backend_archive=backend_archive,
                )

    def test_build_manifest_rejects_afs_component_when_off(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            components = root / "components"
            for name in component.COMPONENTS:
                directory = self.write_component(components, name)
                component.create_manifest(
                    component=name,
                    directory=directory,
                    commit="d" * 40,
                    target="x86_64-unknown-linux-gnu",
                )

            release = root / "adx-release.tar.gz"
            package_manifest = root / "release-manifest.json"
            wheel = root / "adx_sandbox-0.1.0-py3-none-any.whl"
            backend_manifest = root / "backend-manifest.json"
            backend_archive = root / "backend.tar.gz"
            release.write_bytes(b"release")
            package_manifest.write_text(json.dumps({"commit": "d" * 40}))
            wheel.write_bytes(b"wheel")
            backend_manifest.write_text(json.dumps({"sandboxd_revision": "fixture"}))
            backend_archive.write_bytes(b"backend archive")

            manifest = component.create_build_manifest(
                component_root=components,
                commit="d" * 40,
                target="x86_64-unknown-linux-gnu",
                package_manifest=package_manifest,
                release_archive=release,
                wheel=wheel,
                backend_manifest=backend_manifest,
                backend_archive=backend_archive,
            )

            self.assertEqual(set(manifest["components"]), {"platform", "gateway", "execd"})
            self.assertNotIn("afs", manifest["components"])

    def test_release_manifest_can_include_optional_afs_component(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            components = root / "components"
            for name in component.COMPONENTS:
                directory = self.write_component(components, name)
                component.create_manifest(
                    component=name,
                    directory=directory,
                    commit="c" * 40,
                    target="x86_64-unknown-linux-gnu",
                )

            release = root / "adx-release.tar.gz"
            package_manifest = root / "release-manifest.json"
            wheel = root / "adx_sandbox-0.1.0-py3-none-any.whl"
            backend_manifest = root / "backend-manifest.json"
            backend_archive = root / "backend.tar.gz"
            release.write_bytes(b"release")
            package_manifest.write_text(json.dumps({"commit": "c" * 40, "with_afs": True}))
            wheel.write_bytes(b"wheel")
            backend_manifest.write_text(json.dumps({"sandboxd_revision": "fixture"}))
            backend_archive.write_bytes(b"backend archive")

            manifest = component.create_build_manifest(
                component_root=components,
                commit="c" * 40,
                target="x86_64-unknown-linux-gnu",
                package_manifest=package_manifest,
                release_archive=release,
                wheel=wheel,
                backend_manifest=backend_manifest,
                backend_archive=backend_archive,
                with_afs=True,
            )

            self.assertEqual(set(manifest["components"]), {"platform", "gateway", "execd", "afs"})
            self.assertEqual(set(manifest["components"]["afs"]["files"]), {"afs-meta", "afs-node"})
            build_manifest = root / "build-manifest.json"
            build_manifest.write_text(json.dumps(manifest))
            extracted_package_manifest = root / "manifest.json"
            extracted_package_manifest.write_bytes(package_manifest.read_bytes())
            component.verify_build_manifest(
                manifest_path=build_manifest,
                commit="c" * 40,
                target="x86_64-unknown-linux-gnu",
                package_manifest=extracted_package_manifest,
                release_archive=release,
                wheel=wheel,
                backend_manifest=backend_manifest,
                backend_archive=backend_archive,
                with_afs=True,
            )

    def test_package_and_build_manifest_must_agree_on_afs_mode(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            components = root / "components"
            for name in component.COMPONENTS:
                directory = self.write_component(components, name)
                component.create_manifest(name, directory, "c" * 40, "test")
            paths = {}
            for name in ("package_manifest", "release_archive", "wheel",
                         "backend_manifest", "backend_archive"):
                paths[name] = root / name
                paths[name].write_bytes(b"fixture")
            arguments = dict(component_root=components, commit="c" * 40,
                             target="test", **paths)
            for enabled in (True, False):
                with self.subTest(with_afs=enabled):
                    package = {"commit": "c" * 40, "with_afs": enabled}
                    paths["package_manifest"].write_text(json.dumps(package))
                    manifest = component.create_build_manifest(**arguments, with_afs=enabled)
                    package["with_afs"] = not enabled
                    paths["package_manifest"].write_text(json.dumps(package))
                    with self.assertRaisesRegex(ValueError, "package AFS mode"):
                        component.create_build_manifest(**arguments, with_afs=enabled)
                    # Even a hash-consistent aggregate cannot contradict its package mode.
                    manifest["package"]["manifest"]["sha256"] = component.sha256(paths["package_manifest"])
                    path = root / "build-manifest.json"
                    path.write_text(json.dumps(manifest))
                    with self.assertRaisesRegex(ValueError, "package AFS mode"):
                        component.verify_build_manifest(path, "c" * 40, "test",
                                                        **paths, with_afs=enabled)

    def test_mixed_component_commits_are_rejected(self):
        with tempfile.TemporaryDirectory() as temporary:
            root = Path(temporary)
            components = root / "components"
            for index, name in enumerate(component.BASE_COMPONENTS):
                directory = self.write_component(components, name)
                component.create_manifest(
                    component=name,
                    directory=directory,
                    commit=("a" if index == 0 else "b") * 40,
                    target="x86_64-unknown-linux-gnu",
                )
            for name in ("package.json", "release.tgz", "wheel.whl", "backend.json"):
                (root / name).write_text(name)

            with self.assertRaises(ValueError):
                component.create_build_manifest(
                    component_root=components,
                    commit="b" * 40,
                    target="x86_64-unknown-linux-gnu",
                    package_manifest=root / "package.json",
                    release_archive=root / "release.tgz",
                    wheel=root / "wheel.whl",
                    backend_manifest=root / "backend.json",
                    backend_archive=root / "release.tgz",
                )


if __name__ == "__main__":
    unittest.main()
