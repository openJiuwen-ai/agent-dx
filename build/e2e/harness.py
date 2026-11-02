"""Stage current test assets independently from fixed product images."""

import hashlib
import os
from pathlib import Path
import re
import shutil
import subprocess
import importlib.util

ROOT = Path(__file__).resolve().parents[2]
_spec = importlib.util.spec_from_file_location("harness_handoff", ROOT / "build/e2e/afs_handoff.py")
afs_handoff = importlib.util.module_from_spec(_spec)
_spec.loader.exec_module(afs_handoff)


def _executable(name):
    value = shutil.which(name)
    if value is None:
        raise FileNotFoundError(name)
    return os.path.abspath(value)


def source_commit():
    commit = os.getenv('ADX_E2E_HARNESS_COMMIT', os.getenv('BUILDKITE_COMMIT'))
    if commit is None:
        commit = subprocess.check_output(
            [_executable('git'), '-C', str(ROOT), 'rev-parse', 'HEAD'],
            text=True,
        ).strip()
    if not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('test harness commit must be a 40-character Git commit')
    return commit


def stage_harness(destination, package_manifest=None, harness_commit=None):
    shutil.copytree(
        ROOT / 'build/e2e',
        destination,
        ignore=shutil.ignore_patterns('__pycache__', 'tests', '*.pyc'),
    )
    shutil.copy2(ROOT / 'build/ci/rpc_certificates.py', destination / 'rpc_certificates.py')
    shutil.copy2(ROOT / 'build/release/package.py', destination / 'package.py')
    if package_manifest is not None and afs_handoff.enabled(package_manifest):
        afs_handoff.stage(ROOT / 'afs/tests', destination / 'afs-tests', package_manifest, harness_commit)
    files = {}
    for path in sorted(item for item in destination.rglob('*') if item.is_file()):
        files[str(path.relative_to(destination))] = hashlib.sha256(path.read_bytes()).hexdigest()
    return files


