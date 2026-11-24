#!/usr/bin/env bash
set -euo pipefail

if [[ $# != 1 ]]; then
  echo 'usage: build-hixl-module.sh /path/to/libafs_hixl.so' >&2
  exit 2
fi
output=$1
: "${ADX_CANN_PATH:?set ADX_CANN_PATH to a matching CANN SDK}"
if [[ $(uname -s) != Linux ]]; then
  echo 'building the HIXL module requires a native Linux builder' >&2
  exit 2
fi
arch=$(uname -m)
case "$arch" in
  aarch64|x86_64) ;;
  *) echo "unsupported HIXL module architecture: $arch" >&2; exit 2 ;;
esac
sdk=$(cd "$ADX_CANN_PATH" && pwd -P)
if [[ -d "$sdk/$arch-linux/include" ]]; then
  sdk="$sdk/$arch-linux"
fi
for file in include/hixl/hixl.h include/hixl/hixl_types.h include/acl/acl_rt.h \
  lib64/libcann_hixl.so lib64/libascendcl.so; do
  if [[ ! -f "$sdk/$file" ]]; then
    echo "CANN SDK file is missing: $sdk/$file" >&2
    exit 2
  fi
done
link_paths=()
for dir in "$sdk/lib64" "$sdk/devlib/linux/$arch" "$sdk/devlib/device"; do
  if [[ -d "$dir" ]]; then
    link_paths+=("-L$dir" "-Wl,-rpath-link,$dir")
  fi
done
native=$(cd "$(dirname "$0")" && pwd -P)
destination=$(dirname "$output")
mkdir -p "$destination"
stage=$(mktemp -d "$destination/.hixl-module.XXXXXX")
trap 'rm -rf "$stage"' EXIT
"${CXX:-c++}" -std=c++17 -fPIC -shared -Wl,-z,defs -Wl,-soname,libafs_hixl.so \
  -Wl,--version-script,"$native/hixl-module.map" \
  -Wall -Wextra -O2 -I"$sdk/include" "$native/hixl.cc" "$native/hixl_numa.cc" \
  "${link_paths[@]}" -lcann_hixl -lascendcl -lmetadef -lgraph -ldl \
  -o "$stage/libafs_hixl.so"
mv -f "$stage/libafs_hixl.so" "$output"
