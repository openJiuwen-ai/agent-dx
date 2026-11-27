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
cxx=${CXX:-c++}
fabric_flags=()
# Probe declarations only: linking against newer ACL APIs here would make the
# module unloadable with otherwise supported runtime/driver combinations.
if "$cxx" -std=c++17 -isystem "$sdk/include" -fsyntax-only "$native/hixl_fabric_vmm_probe.cc" \
    >"$stage/acl-fabric-vmm-probe.log" 2>&1
then
  fabric_flags+=(-DAFS_HIXL_HAVE_ACL_FABRIC_VMM)
else
  echo 'CANN SDK lacks ACL external FabricMem VMM declarations; building with ADXL only' >&2
fi
adxl_flags=()
if [[ -f "$sdk/include/adxl/adxl_engine.h" ]]; then
  adxl_flags+=(-DAFS_HIXL_HAVE_ADXL_HEADER)
fi

"$cxx" -std=c++17 -fPIC -shared -Wl,-z,defs -Wl,-soname,libafs_hixl.so \
  -Wl,--version-script,"$native/hixl-module.map" \
  -Wall -Wextra -O2 -I"$sdk/include" "${fabric_flags[@]}" "$native/hixl.cc" "$native/hixl_numa.cc" "$native/hixl_fabric.cc" \
  "${link_paths[@]}" -lcann_hixl -lascendcl -lmetadef -lgraph -ldl \
  -o "$stage/libafs_hixl.so"
"$cxx" -std=c++17 -fPIC -shared -Wl,-z,defs -Wl,-soname,libafs_hixl_fabric.so \
  -Wl,--version-script,"$native/hixl-fabric-module.map" \
  -Wall -Wextra -O2 -I"$sdk/include" "${adxl_flags[@]}" "$native/hixl_fabric_adxl.cc" \
  -ldl -o "$stage/libafs_hixl_fabric.so"
mv -f "$stage/libafs_hixl_fabric.so" "$destination/libafs_hixl_fabric.so"
mv -f "$stage/libafs_hixl.so" "$output"
