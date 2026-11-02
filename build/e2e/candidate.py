"""Verify the same fixed product/SDK/backend and test-sidecar identity on every deployer."""
import importlib.util
import json
from pathlib import Path
import re

ROOT = Path(__file__).resolve().parents[2]


def load(name, path):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


afs_handoff = load('candidate_handoff', ROOT / 'build/e2e/afs_handoff.py')
afs_contracts = load('candidate_contracts', ROOT / 'build/e2e/afs_contracts.py')


def sha(path):
    return afs_handoff.sha(path)


def validate_identity(package, architecture, commit, ci):
    expected = {'x86_64-unknown-linux-gnu': 'amd64', 'aarch64-unknown-linux-gnu': 'arm64'}.get(package['target'])
    if not expected or architecture != expected:
        raise ValueError('runtime architecture differs from package')
    if ci and (package['dirty'] or package['commit'] != commit):
        raise ValueError('CI package is dirty or from another commit')


def identity(bundle, registry, commit=None, ci=False):
    m = json.loads(bundle.read_text())
    afs_handoff.verify_bundle(bundle.parent, m)
    afs_contracts.verify_bundle(bundle.parent, m)
    r = json.loads(registry.read_text())
    if m.get('schema_version') != 1 or r.get('schema_version') != 1:
        raise ValueError('invalid Kubernetes artifact manifest')
    if r.get('bundle_sha256') != sha(bundle) or r.get('image_ids') != m['image_ids']:
        raise ValueError('registry images do not match the build bundle')
    if set(r.get('references', {})) != {'node', 'execd', 'entrypoint'}:
        raise ValueError('node, EXECD and entrypoint test images required')
    for image in r['references'].values():
        if not re.fullmatch(r'[^\s]+@sha256:[0-9a-f]{64}', image):
            raise ValueError('immutable image references required')
    validate_identity(m['package'], m['architecture'], commit, ci)
    return m, r


def fixed_candidate(bundle, registry, commit, afs_required=False, contracts_required=False):
    """Apply the same artifact guards to a directly invoked fixed-package run."""
    if not isinstance(commit, str) or not re.fullmatch(r'[0-9a-f]{40}', commit):
        raise ValueError('fixed candidate requires a 40-character product commit')
    manifest, published = identity(bundle, registry)
    package_manifest = manifest['package']
    if package_manifest.get('dirty') is not False or package_manifest.get('commit') != commit:
        raise ValueError('fixed candidate is dirty or from another commit')
    sdk = manifest.get('sdk', {})
    if not isinstance(sdk, dict):
        raise ValueError('fixed candidate SDK identity must be an object')
    if sdk.get('schema_version') != 1 or sdk.get('commit') != commit:
        raise ValueError('fixed candidate SDK identity is missing or from another commit')
    sdk_files = sdk.get('files', {})
    if not isinstance(sdk_files, dict) or not any(name.endswith('.whl') for name in sdk_files):
        raise ValueError('fixed candidate SDK wheel digest is missing')
    backend = manifest.get('backend', {})
    if not isinstance(backend, dict):
        raise ValueError('fixed candidate backend identity must be an object')
    backend_pin = json.loads((ROOT / 'third_party/sandboxd/source.json').read_text())['revision']
    if backend.get('target') != package_manifest['target'] or backend.get('sandboxd_revision') != backend_pin:
        raise ValueError('fixed candidate backend target or pinned revision differs')
    backend_files = backend.get('files', {})
    if not isinstance(backend_files, dict) or not {'sandboxd', 'runc'} <= backend_files.keys():
        raise ValueError('fixed candidate backend binary digests are missing')
    for digest in list(sdk_files.values()) + list(backend_files.values()):
        afs_handoff.full_digest(digest, 64)
    afs_handoff.verify_bundle(bundle.parent, manifest, expected_mode=True if afs_required else None)
    afs_contracts.verify_bundle(bundle.parent, manifest, required=contracts_required)
    return manifest, published


