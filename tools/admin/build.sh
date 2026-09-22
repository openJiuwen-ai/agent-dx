#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)
output=${1:-$script_dir/dist}
python=${PYTHON:-python3}

"$python" - <<'PY'
import sys
if sys.version_info < (3, 10):
    raise SystemExit("adxadmin requires Python 3.10 or newer")
PY
mkdir -p "$output"
clean_build_state() {
  "$python" - "$script_dir" <<'PY'
from pathlib import Path
import shutil
import sys

root = Path(sys.argv[1])
for path in (root / "build", root / "src/adxadmin.egg-info"):
    if path.exists():
        shutil.rmtree(path)
PY
}
clean_build_state
trap clean_build_state EXIT
"$python" -m build --wheel --sdist --outdir "$output" "$script_dir"
