#!/usr/bin/env python3
"""Direct fixed-package specials reuse the existing bounded targeted-suite scheduler."""

import importlib.util
from pathlib import Path

SPEC = importlib.util.spec_from_file_location('shared_targeted_suite',
                                             Path(__file__).with_name('kubernetes') / 'targeted_suite.py')
suite = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(suite)

if __name__ == '__main__':
    raise SystemExit(suite.main(default_deployment='local-docker'))
