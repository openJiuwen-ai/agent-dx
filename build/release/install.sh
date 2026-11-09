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
  python3 "$1/lib/package_manifest.py" --host-architecture "$1"
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
sbox="$bin_dir/sbox"
sbox_target="$current/runtime/sandboxd/sbox"
with_sandboxd=0
[[ ! -f "$source_dir/runtime/sandboxd/sbox" ]] || with_sandboxd=1
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

if [[ $with_sandboxd == 1 && ( -e $sbox || -L $sbox ) ]]; then
  if [[ ! -L $sbox || $(readlink -- "$sbox") != "$sbox_target" ]]; then
    echo "refusing to replace an unmanaged command: $sbox" >&2
    exit 1
  fi
fi

install -d -m 0755 "$prefix" "$releases"
install -d -m 0755 "$bin_dir"
install -d -m 0700 "$prefix/config" "$prefix/config/tls" "$prefix/config/secrets"
install -d -m 0700 "$prefix/data" "$prefix/run"

stage=$(mktemp -d "$releases/.install.XXXXXX")
cli_link="$bin_dir/.adxctl.$$"
sbox_link="$bin_dir/.sbox.$$"
cleanup() {
  [[ -z ${stage:-} || ! -d $stage ]] || rm -rf -- "$stage"
  [[ -z ${cli_link:-} || ! -L $cli_link ]] || rm -f -- "$cli_link"
  [[ -z ${sbox_link:-} || ! -L $sbox_link ]] || rm -f -- "$sbox_link"
}
trap cleanup EXIT INT TERM
ln -s "$current/bin/adxctl" "$cli_link"
if [[ $with_sandboxd == 1 ]]; then
  ln -s "$sbox_target" "$sbox_link"
fi
# Apply destination SELinux labels instead of copying labels from extraction.
cp -aZ "$source_dir"/. "$stage"/
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
if [[ $with_sandboxd == 1 ]]; then
  mv -Tf -- "$sbox_link" "$sbox"
  sbox_link=
elif [[ -L $sbox && $(readlink -- "$sbox") == "$sbox_target" ]]; then
  rm -f -- "$sbox"
fi

echo "ADX release installed: $release"
echo "Current release: $current -> releases/$release_id"
[[ -z $backup ]] || echo "Previous copy retained at $backup"
echo "Persistent paths: $prefix/config, $prefix/data, $prefix/run"
echo "Command installed: $cli -> $current/bin/adxctl"
[[ $with_sandboxd == 0 ]] || echo "Command installed: $sbox -> $sbox_target"
echo "Next: adxctl config init --profile standalone"
echo "Then edit $prefix/config/deployment.yaml and run: adxctl validate"
