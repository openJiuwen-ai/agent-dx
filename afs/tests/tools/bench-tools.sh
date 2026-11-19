#!/usr/bin/env bash
# Prepare the industry benchmark tools used by suites/bench on one Linux role host.
#
#   bash afs/tests/tools/bench-tools.sh DEST_DIR
#
# Downloads pinned releases, verifies their SHA-256, builds fio and iperf3 from source
# (iperf3 without shared libraries, so no LD_LIBRARY_PATH is needed) and installs the
# official static elbencho for this machine's architecture. Nothing is installed into the
# system. DEST_DIR receives fio, elbencho, iperf3 and manifest.json; the `tools` object
# for the env file's `bench` section is printed at the end.
#
# Needs: Linux x86_64 or aarch64, curl, tar, gcc, make, sha256sum. Without network access,
# put the four archives below into $BENCH_TOOLS_CACHE (default DEST_DIR/.cache) first.
# Run it separately on every role host, or once per architecture and copy DEST_DIR.
set -euo pipefail

FIO_VERSION=3.43
FIO_URL=https://github.com/axboe/fio/archive/refs/tags/fio-${FIO_VERSION}.tar.gz
FIO_SHA256=efa49b3f36eda9adf29294f27a87d7e457747d676cd0f73996d6365357832cfe
IPERF_VERSION=3.16
IPERF_URL=https://github.com/esnet/iperf/releases/download/${IPERF_VERSION}/iperf-${IPERF_VERSION}.tar.gz
IPERF_SHA256=cc740c6bbea104398cc3e466befc515a25896ec85e44a662d5f4a767b9cf713e
ELBENCHO_VERSION=3.3-3
ELBENCHO_SHA256_x86_64=cd33b7dd53dde164800df8eaefff9de8d680f9dc10fa195cc50a7a63db589cb2
ELBENCHO_SHA256_aarch64=e3e0fcf7387e229a13387fa475572e6a142130f9079256cfa72bf0895c5d1ac1

DEST=${1:?usage: bench-tools.sh DEST_DIR}
case "$DEST" in /*) ;; *) echo "DEST_DIR must be absolute" >&2; exit 2 ;; esac
ARCH=$(uname -m)
case "$ARCH" in
  x86_64) ELBENCHO_SHA256=$ELBENCHO_SHA256_x86_64 ;;
  aarch64) ELBENCHO_SHA256=$ELBENCHO_SHA256_aarch64 ;;
  *) echo "unsupported architecture: $ARCH" >&2; exit 2 ;;
esac
ELBENCHO_URL=https://github.com/breuner/elbencho/releases/download/v${ELBENCHO_VERSION}/elbencho-static-${ARCH}.tar.gz
for tool in curl tar gcc make sha256sum; do
  command -v "$tool" >/dev/null || { echo "missing required tool: $tool" >&2; exit 2; }
done

CACHE=${BENCH_TOOLS_CACHE:-$DEST/.cache}
mkdir -p "$DEST" "$CACHE"
BUILD=$(mktemp -d)
trap 'rm -rf "$BUILD"' EXIT
JOBS=$(nproc 2>/dev/null || echo 2)

fetch() {  # fetch URL SHA256 -> path of verified archive in the cache
  local url=$1 sum=$2 file
  file=$CACHE/$(basename "$url")
  if [ ! -f "$file" ]; then
    curl -fsSL --retry 3 -o "$file.part" "$url"
    mv "$file.part" "$file"
  fi
  echo "$sum  $file" | sha256sum -c --quiet - >&2 || { echo "checksum mismatch: $file" >&2; exit 3; }
  echo "$file"
}

archive=$(fetch "$FIO_URL" "$FIO_SHA256")
tar -xzf "$archive" -C "$BUILD"
(cd "$BUILD/fio-fio-$FIO_VERSION" && ./configure --disable-native >/dev/null && make -j "$JOBS" >/dev/null)
install -m 0755 "$BUILD/fio-fio-$FIO_VERSION/fio" "$DEST/fio"

archive=$(fetch "$IPERF_URL" "$IPERF_SHA256")
tar -xzf "$archive" -C "$BUILD"
(cd "$BUILD/iperf-$IPERF_VERSION" && ./configure --disable-shared --without-sctp >/dev/null && make -j "$JOBS" >/dev/null)
install -m 0755 "$BUILD/iperf-$IPERF_VERSION/src/iperf3" "$DEST/iperf3"

archive=$(fetch "$ELBENCHO_URL" "$ELBENCHO_SHA256")
tar -xzf "$archive" -C "$BUILD" elbencho
install -m 0755 "$BUILD/elbencho" "$DEST/elbencho"

"$DEST/fio" --version >/dev/null
"$DEST/iperf3" --version >/dev/null
"$DEST/elbencho" --version >/dev/null

python3 - "$DEST" "$ARCH" <<'PY'
import hashlib, json, subprocess, sys
dest, arch = sys.argv[1], sys.argv[2]
manifest = {"arch": arch, "tools": {}}
for name in ("fio", "elbencho", "iperf3"):
    path = dest + "/" + name
    version = subprocess.run([path, "--version"], capture_output=True, text=True).stdout.strip().splitlines()
    manifest["tools"][name] = {"path": path, "sha256": hashlib.sha256(open(path, "rb").read()).hexdigest(),
                               "version": " ".join(l.strip(" *") for l in version[:2])}
json.dump(manifest, open(dest + "/manifest.json", "w"), indent=1)
print(json.dumps({"tools": {n: v["path"] for n, v in manifest["tools"].items()}}, indent=1))
PY
