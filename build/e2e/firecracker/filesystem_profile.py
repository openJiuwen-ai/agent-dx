"""Diagnostic-only file-to-chunk metadata and client-wide read counters."""

import json
import os
from pathlib import Path
import subprocess


def file_layouts(evidence, phase, paths):
    if os.getenv('ADX_STORAGE_FILESYSTEM_TRACE') != '1' or not os.getenv('ADX_FC_JUICEFS_CONFIG'):
        return
    from juicefs_fixture import load_config
    binary = load_config(os.environ['ADX_FC_JUICEFS_CONFIG'])['binary']
    values = {}
    for label, root in paths.items():
        root = Path(root)
        if not root.exists():
            continue
        for path in sorted(root.rglob('*')):
            if path.is_file():
                answer = subprocess.run([binary, 'info', '--raw', str(path)],
                    capture_output=True, text=True, timeout=30)
                if answer.returncode:
                    raise RuntimeError('JuiceFS file layout probe failed: ' + str(path))
                values[label + '/' + str(path.relative_to(root))] = answer.stdout
    (Path(evidence) / (phase + '-file-layouts.json')).write_text(json.dumps(values, indent=2))
