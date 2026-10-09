#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

RUN_ID="${RUN_ID:-3fs-build-$(timestamp_utc)}"
GUEST_ROOT="${GUEST_ROOT:-$AFS_BASELINE_GUEST_ROOT}"
SRC_DIR="${SRC_DIR:-$GUEST_ROOT/3fs-src}"
BUILD_DIR="${BUILD_DIR:-$GUEST_ROOT/3fs-build}"
INSTALL_DIR="${INSTALL_DIR:-$GUEST_ROOT/3fs-install}"
EVIDENCE_DIR="$GUEST_ROOT/evidence"
LOG_FILE="$EVIDENCE_DIR/${RUN_ID}.log"
STATUS_FILE="$EVIDENCE_DIR/${RUN_ID}.status"
FDB_DIR="$GUEST_ROOT/foundationdb-${AFS_BASELINE_FDB_VERSION}"
CARGO_BIN_DIR="${CARGO_BIN_DIR:-$HOME/.cargo/bin}"
export CARGO_HOME="${CARGO_HOME:-$GUEST_ROOT/cargo-home}"
LIBFUSE_PREFIX="${LIBFUSE_PREFIX:-$GUEST_ROOT/libfuse-install}"
AFS_BASELINE_CC="${AFS_BASELINE_CC:-clang-14}"
AFS_BASELINE_CXX="${AFS_BASELINE_CXX:-clang++-14}"

export PATH="$CARGO_BIN_DIR:$PATH"
export PKG_CONFIG_PATH="$LIBFUSE_PREFIX/lib/aarch64-linux-gnu/pkgconfig:$LIBFUSE_PREFIX/lib/pkgconfig:${PKG_CONFIG_PATH:-}"
export LD_LIBRARY_PATH="$LIBFUSE_PREFIX/lib/aarch64-linux-gnu:$LIBFUSE_PREFIX/lib:${LD_LIBRARY_PATH:-}"
export CMAKE_PREFIX_PATH="$LIBFUSE_PREFIX:${CMAKE_PREFIX_PATH:-}"
export CARGO_NET_RETRY="${CARGO_NET_RETRY:-10}"
export CARGO_HTTP_LOW_SPEED_LIMIT="${CARGO_HTTP_LOW_SPEED_LIMIT:-1}"
export CARGO_HTTP_LOW_SPEED_TIME="${CARGO_HTTP_LOW_SPEED_TIME:-600}"


mkdir -p "$GUEST_ROOT" "$EVIDENCE_DIR" "$FDB_DIR"

download_fdb_deb() {
  local package="$1"
  local base="https://github.com/apple/foundationdb/releases/download/${AFS_BASELINE_FDB_VERSION}/${package}"
  if [[ ! -f "$FDB_DIR/$package" ]]; then
    curl -fsSL -o "$FDB_DIR/$package" "$base"
  fi
  if [[ ! -f "$FDB_DIR/${package}.sha256" ]]; then
    curl -fsSL -o "$FDB_DIR/${package}.sha256" "${base}.sha256"
  fi
  (cd "$FDB_DIR" && sha256sum -c "${package}.sha256")
}

{
  log_line "3FS build start"
  log_line "CARGO_HOME=$CARGO_HOME"
  log_line "C compiler=$AFS_BASELINE_CC"
  log_line "CXX compiler=$AFS_BASELINE_CXX"
  wait_for_package_manager
  for cmd in git cmake ninja "$AFS_BASELINE_CC" "$AFS_BASELINE_CXX" curl sha256sum pkg-config rustc cargo; do
    require_cmd "$cmd"
  done
  test -d "$SRC_DIR/.git"
  assert_git_head "$SRC_DIR" "$AFS_BASELINE_3FS_REF"
  if git -C "$SRC_DIR" submodule status --recursive | grep -q '^-'; then
    log_line "BLOCKED 3FS submodules are not initialized"
    git -C "$SRC_DIR" submodule status --recursive
    write_status "$STATUS_FILE" BLOCKED "3FS submodules are not initialized"
    exit 2
  fi

  download_fdb_deb "foundationdb-clients_${AFS_BASELINE_FDB_VERSION}-1_aarch64.deb"
  download_fdb_deb "foundationdb-server_${AFS_BASELINE_FDB_VERSION}-1_aarch64.deb"
  if [[ "${AFS_BASELINE_SKIP_APT_INSTALL:-0}" == "1" ]]; then
    log_line "skip apt install because AFS_BASELINE_SKIP_APT_INSTALL=1"
  else
    sudo apt-get update
    sudo apt-get install -y \
      "$FDB_DIR/foundationdb-clients_${AFS_BASELINE_FDB_VERSION}-1_aarch64.deb" \
      "$FDB_DIR/foundationdb-server_${AFS_BASELINE_FDB_VERSION}-1_aarch64.deb"
  fi

  pushd "$SRC_DIR" >/dev/null
  ./patches/apply.sh
  popd >/dev/null

  if [[ "${AFS_BASELINE_CLEAN_BUILD:-1}" == "1" && -e "$BUILD_DIR" ]]; then
    rm -rf "$BUILD_DIR"
  fi
  CARGO_BUILD_JOBS="$AFS_BASELINE_JOBS" cmake -S "$SRC_DIR" -B "$BUILD_DIR" \
    -G Ninja \
    -DCMAKE_CXX_COMPILER="$AFS_BASELINE_CXX" \
    -DCMAKE_C_COMPILER="$AFS_BASELINE_CC" \
    -DCMAKE_BUILD_TYPE=RelWithDebInfo \
    -DCMAKE_INSTALL_PREFIX="$INSTALL_DIR" \
    -DSHUFFLE_METHOD=g++11
  CARGO_BUILD_JOBS="$AFS_BASELINE_JOBS" cmake --build "$BUILD_DIR" -j "$AFS_BASELINE_JOBS"
  CARGO_BUILD_JOBS="$AFS_BASELINE_JOBS" cmake --install "$BUILD_DIR"

  git -C "$SRC_DIR" rev-parse HEAD >"$EVIDENCE_DIR/3fs-head.txt"
  git -C "$SRC_DIR" submodule status --recursive >"$EVIDENCE_DIR/3fs-submodules.txt"
  find "$INSTALL_DIR" -type f -perm -111 -print | sort >"$EVIDENCE_DIR/3fs-executables.txt"
  while IFS= read -r exe; do
    printf '### %s\n' "$exe"
    ldd "$exe" || true
  done <"$EVIDENCE_DIR/3fs-executables.txt" >"$EVIDENCE_DIR/3fs-ldd.txt"
  find "$INSTALL_DIR" -type f -print0 | sort -z | xargs -0 sha256sum >"$EVIDENCE_DIR/3fs-files.sha256"
  write_status "$STATUS_FILE" PASS "3FS built and installed"
  log_line "3FS build complete"
} 2>&1 | tee "$LOG_FILE"
