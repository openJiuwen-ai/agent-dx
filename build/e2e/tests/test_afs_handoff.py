"""AFS test assets bind the selected product to independently versioned tests."""

import hashlib
import importlib.util
import json
import os
from pathlib import Path
import shutil
import tempfile
import subprocess
import sys
import unittest
from unittest import mock

E2E = Path(__file__).resolve().parents[1]


def load_module(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def elf(machine=62):
    header = bytearray(20)
    header[:6] = b"\x7fELF\x02\x01"
    header[18:20] = machine.to_bytes(2, "little")
    return bytes(header) + b"fixture only, never executed"


class AfsAssetTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.tool = load_module("afs_handoff_contract", E2E / "afs_handoff.py")
        self.source = self.root / "tests"
        for name in ("run.py", "fixtures/common.py", "fixtures/worker.py", "suites/dfs/test_basic.py"):
            path = self.source / name
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text("# fixture asset: " + name + "\n")
        (self.source / "README.md").write_text("fixture documentation\n")
        self.package = self.root / "package"
        (self.package / "bin").mkdir(parents=True)
        files = {}
        for name in ("afs-meta", "afs-node"):
            path = self.package / "bin" / name
            path.write_bytes(elf())
            path.chmod(0o755)
            files["bin/" + name] = hashlib.sha256(path.read_bytes()).hexdigest()
        self.product = {"commit": "a" * 40, "target": "x86_64-unknown-linux-gnu", "with_afs": True, "files": files}
        (self.package / "manifest.json").write_text(json.dumps(self.product))
        self.assets = self.root / "assets"

    def stage(self):
        return self.tool.stage(self.source, self.assets, self.product, "b" * 40)

    def materialize(self):
        return self.tool.materialize(self.assets, self.package, "b" * 40, "a" * 40, self.product["target"])

    def bundle(self):
        manifest = self.stage()
        directory = self.root / "bundle"
        directory.mkdir()
        (directory / "afs-tests.json").write_bytes((self.assets / "manifest.json").read_bytes())
        value = {
            "package": self.product,
            "afs_tests": {"manifest": "afs-tests.json", "sha256": self.tool.sha(directory / "afs-tests.json")},
        }
        return directory, value, manifest

    def test_asset_and_product_identities_are_distinct_and_complete(self):
        manifest = self.stage()
        self.assertEqual(manifest["harness_commit"], "b" * 40)
        self.assertEqual(manifest["product_commit"], "a" * 40)
        identity = json.loads((self.assets / "inputs/artifact-identity.json").read_text())
        self.assertEqual(identity["commit"], "a" * 40)
        self.assertEqual(identity["binaries"]["afs-node"], self.product["files"]["bin/afs-node"])
        self.assertNotIn("test_elf", identity)
        self.assertFalse((self.assets / "inputs/afs-node").exists())

    def test_sources_exclude_rust_cache_and_private_environment_files(self):
        (self.source / "fuse_contract.rs").write_text("not a compiled test ELF")
        (self.source / "fixtures/private.json").write_text("private environment")
        (self.source / "fixtures/singlevm.example.json").write_text("{}")
        (self.source / "fixtures/__pycache__").mkdir()
        (self.source / "fixtures/__pycache__/old.py").write_text("stale")
        manifest = self.stage()
        self.assertIn("fixtures/singlevm.example.json", manifest["files"])
        self.assertNotIn("fuse_contract.rs", manifest["files"])
        self.assertNotIn("fixtures/private.json", manifest["files"])
        self.assertFalse(any("__pycache__" in name for name in manifest["files"]))

    def test_off_candidate_cannot_create_afs_assets(self):
        self.product.pop("with_afs")
        with self.assertRaises(ValueError):
            self.stage()
        self.assertFalse(self.assets.exists())

    def test_missing_product_digest_and_invalid_commit_are_rejected(self):
        self.product["files"].pop("bin/afs-node")
        with self.assertRaises(ValueError):
            self.stage()
        with self.assertRaises(ValueError):
            self.tool.stage(self.source, self.assets, self.product, "HEAD")

    def test_symlinked_source_is_rejected_without_copying_its_contents(self):
        target = self.root / "private.py"
        target.write_text("private")
        (self.source / "fixtures/escape.py").symlink_to(target)
        with self.assertRaises(ValueError):
            self.stage()

    def test_runtime_inputs_reference_verified_product_without_copying_binaries(self):
        self.stage()
        self.materialize()
        for name in ("afs-meta", "afs-node"):
            link = self.assets / "inputs" / name
            self.assertTrue(link.is_symlink())
            self.assertEqual(link.resolve(), (self.package / "bin" / name).resolve())
        self.materialize()  # Rechecking the same owned inputs is idempotent.
        self.assertFalse((self.package / "bin/artifact-identity.json").exists())

    def test_product_tampering_fails_before_inputs_are_created(self):
        self.stage()
        (self.package / "bin/afs-node").write_bytes(b"wrong candidate")
        with self.assertRaises(ValueError):
            self.materialize()
        self.assertFalse((self.assets / "inputs/afs-meta").exists())

    def test_matching_digest_does_not_hide_wrong_elf_architecture(self):
        path = self.package / "bin/afs-node"
        path.write_bytes(elf(183))
        self.product["files"]["bin/afs-node"] = hashlib.sha256(path.read_bytes()).hexdigest()
        (self.package / "manifest.json").write_text(json.dumps(self.product))
        self.stage()
        with self.assertRaises(ValueError):
            self.materialize()

    def test_changed_sources_extra_files_and_identity_are_rejected(self):
        self.stage()
        for path in (self.assets / "run.py", self.assets / "inputs/artifact-identity.json"):
            original = path.read_bytes()
            path.write_bytes(original + b"tampered")
            with self.assertRaises(ValueError):
                self.materialize()
            path.write_bytes(original)
        (self.assets / "fixtures/unlisted.py").write_text("unlisted")
        with self.assertRaises(ValueError):
            self.materialize()

    def test_manifest_cannot_redirect_validation_outside_assets(self):
        self.stage()
        manifest_path = self.assets / "manifest.json"
        manifest = json.loads(manifest_path.read_text())
        manifest["files"]["../outside"] = "c" * 64
        manifest_path.write_text(json.dumps(manifest))
        with self.assertRaises(ValueError):
            self.materialize()

    def test_existing_inputs_must_point_to_the_selected_candidate(self):
        self.stage()
        (self.assets / "inputs/afs-node").symlink_to(self.root / "unrelated")
        with self.assertRaises(ValueError):
            self.materialize()
        self.assertFalse((self.assets / "inputs/afs-meta").exists())

    def test_harness_commit_and_product_commit_are_both_checked(self):
        self.stage()
        for harness_commit, product_commit in (("c" * 40, "a" * 40), ("b" * 40, "c" * 40)):
            with self.assertRaises(ValueError):
                self.tool.materialize(self.assets, self.package, harness_commit, product_commit, self.product["target"])

    def test_bundle_metadata_binds_the_exact_test_manifest(self):
        directory, value, manifest = self.bundle()
        self.assertEqual(self.tool.verify_bundle(directory, value), manifest)
        self.assertEqual(self.tool.verify_bundle(directory, value, True), manifest)
        with self.assertRaises(ValueError):
            self.tool.verify_bundle(directory, value, False)
        (directory / "afs-tests.json").write_text("{}")
        with self.assertRaises(ValueError):
            self.tool.verify_bundle(directory, value)

    def test_legacy_on_bundle_is_not_silently_treated_as_ready(self):
        with self.assertRaises(ValueError):
            self.tool.verify_bundle(self.root, {"package": self.product})

    def test_off_bundle_has_no_asset_requirement_or_fuse_dependency(self):
        self.assertIsNone(self.tool.verify_bundle(self.root, {"package": {}}))
        (self.root / "afs-tests.json").write_text("{}")
        with self.assertRaises(ValueError):
            self.tool.verify_bundle(self.root, {"package": {}})


class HarnessTransferTests(unittest.TestCase):
    def test_on_sync_replaces_only_test_assets_and_verifies_each_node(self):
        module = load_module("k8s_sync_afs", E2E / "kubernetes/run.py")
        product = {
            "commit": "a" * 40, "target": "x86_64-unknown-linux-gnu", "with_afs": True,
            "files": {"bin/afs-meta": "c" * 64, "bin/afs-node": "d" * 64},
        }
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory)
            run = module.KubernetesRun(output, Path("/fixture/kubeconfig"))
            run.nodes = ["node1", "node2"]
            calls = []

            def record(*args, **_kwargs):
                calls.append(args)
                return ""

            with mock.patch.object(run, "kube", side_effect=record), mock.patch.object(
                run, "execute", side_effect=record,
            ):
                run.sync_harness("b" * 40, "a" * 40, product)
            for node in run.nodes:
                replacement = calls.index((node, "rm", "-rf", "/opt/adx/e2e/afs-tests"))
                copy = next(i for i, call in enumerate(calls) if "cp" in call and node + ":/opt/adx/e2e" in call)
                materialize = next(i for i, call in enumerate(calls) if call[0] == node and "materialize" in call)
                self.assertLess(replacement, copy)
                self.assertLess(copy, materialize)
                self.assertIn("a" * 40, calls[materialize])
                self.assertIn("b" * 40, calls[materialize])
            manifest = json.loads((output / "afs-tests.json").read_text())
            self.assertEqual(manifest["product_commit"], "a" * 40)
            self.assertEqual(manifest["harness_commit"], "b" * 40)

    def test_on_harness_carries_assets_with_independent_test_identity(self):
        module = load_module("k8s_afs_handoff", E2E / "kubernetes/run.py")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "harness"
            product = {
                "commit": "a" * 40,
                "target": "x86_64-unknown-linux-gnu",
                "with_afs": True,
                "files": {"bin/afs-meta": "c" * 64, "bin/afs-node": "d" * 64},
            }
            files = module.stage_harness(output, package_manifest=product, harness_commit="b" * 40)
            self.assertIn("afs-tests/run.py", files)
            self.assertIn("afs-tests/fixtures/worker.py", files)
            self.assertIn("afs-tests/inputs/artifact-identity.json", files)
            self.assertNotIn("afs-tests/fuse_contract.rs", files)
            manifest = json.loads((output / "afs-tests/manifest.json").read_text())
            self.assertEqual(manifest["harness_commit"], "b" * 40)
            self.assertEqual(manifest["product_commit"], "a" * 40)

    def test_off_harness_remains_free_of_afs_component_assets(self):
        module = load_module("k8s_off_handoff", E2E / "kubernetes/run.py")
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "harness"
            files = module.stage_harness(output)
            self.assertFalse((output / "afs-tests").exists())
            self.assertFalse(any(name.startswith("afs-tests/") for name in files))

    def test_assets_cannot_be_accepted_from_the_wrong_product_or_source(self):
        module = load_module("k8s_assets_identity", E2E / "kubernetes/run.py")
        # The module must use candidate metadata without starting Kubernetes.
        with mock.patch.object(module.KubernetesRun, "deploy", side_effect=AssertionError("must not deploy")):
            with tempfile.TemporaryDirectory() as directory:
                product = {"with_afs": True, "commit": "a" * 40, "target": "x86_64-unknown-linux-gnu", "files": {}}
                with self.assertRaises(ValueError):
                    module.stage_harness(Path(directory) / "harness", package_manifest=product, harness_commit="b" * 40)


class CiBoundaryTests(unittest.TestCase):
    def test_direct_local_and_k8s_entrypoints_also_reject_legacy_on_bundle(self):
        driver = load_module("local_bundle_afs", E2E / "run.py")
        kubernetes = load_module("k8s_bundle_afs", E2E / "kubernetes/run.py")
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "bundle.json").write_text(json.dumps({"package": {"with_afs": True}}))
            with self.assertRaisesRegex(ValueError, "lacks its test asset handoff"):
                driver.verify_bundle(root)
            with self.assertRaisesRegex(ValueError, "lacks its test asset handoff"):
                kubernetes.identity(root / "bundle.json", root / "not-downloaded-registry.json")

    def test_mismatched_mode_or_missing_assets_stops_before_cluster_access(self):
        repository = E2E.parents[1]
        for mode, package, message in (
            ("0", {"with_afs": True}, "mode differs"),
            ("1", {"with_afs": False}, "mode differs"),
            ("1", {"with_afs": True}, "lacks its test asset handoff"),
        ):
            with self.subTest(mode=mode, package=package), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                bundle = root / "out/buildkite/bundle"
                bundle.mkdir(parents=True)
                (bundle / "bundle.json").write_text(json.dumps({"package": package}))
                tools = root / "bin"
                tools.mkdir()
                for name, source in {
                    "git": '#!/bin/sh\nif [ "$1" = rev-parse ]; then printf "%s\\n" "' + "b" * 40 + '"; fi\n',
                    "buildkite-agent": '#!/bin/sh\nexit 0\n',
                }.items():
                    path = tools / name
                    path.write_text(source)
                    path.chmod(0o755)
                (tools / "python3").symlink_to(sys.executable)
                target = root / "build/e2e"
                target.mkdir(parents=True)
                shutil.copyfile(E2E / "afs_handoff.py", target / "afs_handoff.py")
                # Actual wrapper and validator, fake artifact/Git commands; no Docker/kubectl on PATH.
                result = subprocess.run(
                    ["/bin/bash", str(repository / ".buildkite/run-e2e.sh")], cwd=root,
                    env={"PATH": str(tools), "BUILDKITE_COMMIT": "b" * 40, "ADX_WITH_AFS": mode},
                    capture_output=True, text=True, check=False, timeout=10,
                )
                self.assertNotEqual(result.returncode, 0)
                self.assertIn(message, result.stderr)
                self.assertNotIn("kubectl", result.stderr)


class ImageHandoffTests(unittest.TestCase):
    def test_prepare_stages_only_on_assets_and_checks_them_in_node_image(self):
        module = load_module("prepare_afs_assets", E2E / "prepare.py")
        for with_afs in (False, True):
            with self.subTest(with_afs=with_afs), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                product = {
                    "commit": "a" * 40, "target": "x86_64-unknown-linux-gnu",
                    "with_afs": with_afs, "dirty": False, "files": {},
                }
                package = root / "package"
                names = ["bin/" + name for name in module.package.BINARIES]
                names += ["bin/redis-server", "runtime/adx-execd"]
                if with_afs:
                    names += ["bin/afs-meta", "bin/afs-node"]
                for name in names:
                    path = package / name
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_bytes(elf())
                    product["files"][name] = module.sha(path)
                (package / "manifest.json").write_text(json.dumps(product))
                backend = root / "backend"
                backend.mkdir()
                revision = json.loads((module.ROOT / "third_party/sandboxd/source.json").read_text())["revision"]
                backend_manifest = {"sandboxd_revision": revision, "target": product["target"], "files": {}}
                for name in module.BACKEND_BINARIES:
                    (backend / name).write_bytes(b"fixture")
                    backend_manifest["files"][name] = module.sha(backend / name)
                (backend / "manifest.json").write_text(json.dumps(backend_manifest))
                wheel = root / "adx_environment-0.1.0-py3-none-any.whl"
                wheel.write_bytes(b"fixture wheel, never installed")
                candidate = root / "sdk-candidate.json"
                candidate.write_text(json.dumps({
                    "schema_version": 1, "commit": product["commit"], "files": {wheel.name: module.sha(wheel)},
                }))
                output = root / "bundle"
                argv = [
                    "prepare.py", "--package", str(package), "--backend", str(backend),
                    "--sdk-wheel", str(wheel), "--sdk-candidate", str(candidate), "--output", str(output),
                    "--runtime-base", "fixture@sha256:" + "e" * 64,
                    "--execd-base", "fixture@sha256:" + "f" * 64,
                ]
                inspected = []

                def docker(
                    command, selected_mode=with_afs, selected_target=product["target"], seen=inspected, **_kwargs,
                ):
                    self.assertEqual(command[0], "/fixture/docker")
                    if command[1] == "save":
                        Path(command[command.index("-o") + 1]).write_bytes(b"fixture archive")
                    elif command[1] == "build" and "Dockerfile.node" in command[command.index("-f") + 1]:
                        context = Path(command[-1])
                        assets = context / "e2e/afs-tests"
                        self.assertEqual(assets.exists(), selected_mode)
                        commands = [
                            json.loads(line[4:]) for line in (context / "Dockerfile.node").read_text().splitlines()
                            if line.startswith("RUN [")
                        ]
                        self.assertEqual(len(commands), int(selected_mode))
                        if selected_mode:
                            self.assertIn("materialize", commands[0])
                            module.afs_handoff.materialize(
                                assets, context / "package", "b" * 40, "a" * 40, selected_target,
                            )
                        seen.append(selected_mode)
                    return subprocess.CompletedProcess(command, 0)

                with mock.patch.object(sys, "argv", argv), mock.patch.dict(os.environ, {
                    "BUILDKITE": "1", "BUILDKITE_COMMIT": "b" * 40, "ADX_E2E_ARTIFACT_COMMIT": "a" * 40,
                }, clear=True), mock.patch.object(module.package, "verify", return_value=product), mock.patch.object(
                    module, "_executable", return_value="/fixture/docker",
                ), mock.patch.object(module.subprocess, "run", side_effect=docker), mock.patch.object(
                    module.subprocess, "check_output",
                    return_value=json.dumps([{"Id": "fixture", "Architecture": "amd64"}]),
                ):
                    module.main()
                value = json.loads((output / "bundle.json").read_text())
                self.assertEqual(inspected, [with_afs])
                manifest = module.afs_handoff.verify_bundle(output, value, with_afs)
                self.assertEqual(manifest is not None, with_afs)


if __name__ == "__main__":
    unittest.main()
