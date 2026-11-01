#!/usr/bin/env python3
"""Direct non-K8s entry to the shared public/AFS required gate."""
import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location('shared_required_afs_gate',
                                            Path(__file__).resolve().parent / 'kubernetes/afs_gate.py')
GATE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(GATE)

if __name__ == '__main__':
    raise SystemExit(GATE.main(default_deployment='local-docker'))
