#!/usr/bin/env python3
"""Restore compact hash-bound AFS acceptance fixtures into a run directory."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path


FIXTURES_ROOT = Path(__file__).resolve().parent / "fixtures"
MANIFEST = FIXTURES_ROOT / "manifest.json"
STORE = FIXTURES_ROOT / "store"


def sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def load_manifest(path: Path = MANIFEST) -> dict:
    manifest = json.loads(path.read_text(encoding="utf-8"))
    if manifest.get("schema_version") != 1:
        raise ValueError("unsupported fixture manifest schema")
    return manifest


def restore(prefix: str, destination: Path, *, manifest_path: Path = MANIFEST) -> list[dict]:
    """Restore entries below prefix into destination and verify every byte."""

    normalized = prefix.strip("/")
    if not normalized or normalized.startswith("../") or "/../" in normalized:
        raise ValueError("invalid fixture prefix")
    manifest = load_manifest(manifest_path)
    destination.mkdir(parents=True, exist_ok=True)
    restored = []
    root = manifest_path.parent
    store = root / "store"
    needle = normalized + "/"
    for entry in manifest["files"]:
        path = entry["path"]
        if path == normalized or path.startswith(needle):
            relative = path[len(needle):]
            if not relative or relative.startswith("../") or "/../" in relative:
                raise ValueError(f"invalid fixture path: {path}")
            data = (store / entry["sha256"]).read_bytes()
            if sha256(data) != entry["sha256"] or len(data) != entry["bytes"]:
                raise ValueError(f"fixture content mismatch: {path}")
            target = destination / relative
            target.parent.mkdir(parents=True, exist_ok=True)
            target.write_bytes(data)
            restored.append(entry)
    if not restored:
        raise ValueError(f"fixture prefix not found: {prefix}")
    return restored
