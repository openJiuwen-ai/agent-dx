#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'USAGE'
Usage: ./install.sh [options]

Install this verified ADX release package on the current Linux host.

Options:
  --prefix DIR       ADX installation root (default: /opt/adx)
  --bin-dir DIR      Directory exposed through PATH (default: /usr/local/bin)
  --replace          Replace the same release if it is already installed
  -h, --help         Show this help
USAGE
}

prefix=/opt/adx
bin_dir=/usr/local/bin
replace=0
while (($#)); do
  case "$1" in
    --prefix)
      (($# >= 2)) || { echo "$1 requires a directory" >&2; exit 2; }
      prefix=$2
      shift 2
      ;;
    --bin-dir)
      (($# >= 2)) || { echo "$1 requires a directory" >&2; exit 2; }
      bin_dir=$2
      shift 2
      ;;
    --replace) replace=1; shift ;;
    -h|--help) usage; exit 0 ;;
    *) echo "unknown option: $1" >&2; usage >&2; exit 2 ;;
  esac
done

[[ $prefix == /* && $prefix != / ]] || {
  echo "installation prefix must be absolute and cannot be /: $prefix" >&2
  exit 2
}
[[ $bin_dir == /* && $bin_dir != / ]] || {
  echo "binary directory must be absolute and cannot be /: $bin_dir" >&2
  exit 2
}
[[ $(uname -s) == Linux ]] || { echo 'ADX release installation requires Linux' >&2; exit 1; }
command -v python3 >/dev/null || { echo 'python3 is required to verify the release package' >&2; exit 1; }

source_dir=$(cd "$(dirname "$0")" && pwd)
verify_package() {
  python3 - "$1" <<'PY'
import hashlib
import json
import platform
import sys
from pathlib import Path

root = Path(sys.argv[1]).resolve()
manifest_path = root / "manifest.json"
if not manifest_path.is_file():
    raise SystemExit("manifest.json is missing")
manifest = json.loads(manifest_path.read_text())
files = manifest.get("files")
if manifest.get("schema_version") != 1 or not isinstance(files, dict):
    raise SystemExit("invalid package manifest")
expected = set(files)
required = {
    "bin/adxctl",
    "bin/adx-coordinator",
    "bin/adxlet",
    "bin/adx-apiserver",
    "bin/redis-server",
    "bin/redis-cli",
    "install.sh",
    "runtime/adx-execd",
}
if manifest.get("profile") == "release" and "linux" in manifest.get("target", ""):
    required.add("runtime/adx-runtime-rootfs.img")
if not required.issubset(expected) or not any(
    name.startswith("sdk/adx_sandbox-") and name.endswith(".whl") for name in expected
):
    raise SystemExit("incomplete package")
actual = set()
for path in root.rglob("*"):
    if path.is_symlink():
        raise SystemExit(f"package contains a symlink: {path.relative_to(root)}")
    if path.is_file() and path != manifest_path:
        actual.add(path.relative_to(root).as_posix())
if actual != expected:
    raise SystemExit("package file list does not match manifest")
for name, digest in files.items():
    relative = Path(name)
    if relative.is_absolute() or ".." in relative.parts:
        raise SystemExit(f"invalid manifest path: {name}")
    value = hashlib.sha256((root / relative).read_bytes()).hexdigest()
    if value != digest:
        raise SystemExit(f"package integrity check failed: {name}")
machine = {"x86_64": "x86_64", "aarch64": "aarch64"}.get(platform.machine())
target = manifest.get("target", "")
if machine is None or not target.startswith(machine + "-") or "linux" not in target:
    raise SystemExit(f"package target {target!r} does not match Linux host {platform.machine()!r}")
commit = manifest.get("commit", "")
if len(commit) != 40 or any(character not in "0123456789abcdef" for character in commit):
    raise SystemExit("manifest commit must be a 40-character lowercase hexadecimal Git SHA")
print(f"package verified: {commit} ({target})")
PY
}

verify_package "$source_dir"
release_id=$(python3 - "$source_dir/manifest.json" <<'PY'
import json
import sys
from pathlib import Path
print(json.loads(Path(sys.argv[1]).read_text())["commit"])
PY
)

releases="$prefix/releases"
release="$releases/$release_id"
current="$prefix/current"
cli="$bin_dir/adxctl"
if [[ -e $current && ! -L $current ]]; then
  echo "installation path is not an ADX current symlink: $current" >&2
  exit 1
fi
if [[ -e $release && $replace != 1 ]]; then
  echo "ADX release is already installed: $release; use --replace to reinstall it" >&2
  exit 1
fi
if [[ -e $cli || -L $cli ]]; then
  if [[ ! -L $cli || $(readlink -- "$cli") != "$current/bin/adxctl" ]]; then
    echo "refusing to replace an unmanaged command: $cli" >&2
    exit 1
  fi
fi

install -d -m 0755 "$prefix" "$releases"
install -d -m 0755 "$bin_dir"
install -d -m 0700 "$prefix/config" "$prefix/config/tls" "$prefix/config/secrets"
install -d -m 0700 "$prefix/data" "$prefix/run"

stage=$(mktemp -d "$releases/.install.XXXXXX")
cli_link="$bin_dir/.adxctl.$$"
cleanup() {
  [[ -z ${stage:-} || ! -d $stage ]] || rm -rf -- "$stage"
  [[ -z ${cli_link:-} || ! -L $cli_link ]] || rm -f -- "$cli_link"
}
trap cleanup EXIT INT TERM
ln -s "$current/bin/adxctl" "$cli_link"
cp -a "$source_dir"/. "$stage"/
verify_package "$stage"

backup=
if [[ -e $release ]]; then
  backup="$releases/.previous.${release_id}.$(date -u +%Y%m%d%H%M%S)"
  mv -- "$release" "$backup"
fi
if ! mv -- "$stage" "$release"; then
  [[ -z $backup ]] || mv -- "$backup" "$release"
  exit 1
fi
stage=

link="$prefix/.current.$$"
ln -s "releases/$release_id" "$link"
if ! mv -Tf -- "$link" "$current"; then
  rm -f -- "$link"
  exit 1
fi
mv -Tf -- "$cli_link" "$cli"
cli_link=

echo "ADX release installed: $release"
echo "Current release: $current -> releases/$release_id"
[[ -z $backup ]] || echo "Previous copy retained at $backup"
echo "Persistent paths: $prefix/config, $prefix/data, $prefix/run"
echo "Command installed: $cli -> $current/bin/adxctl"
echo "Next: adxctl config init --profile standalone"
echo "Then edit $prefix/config/deployment.yaml and run: adxctl validate"
