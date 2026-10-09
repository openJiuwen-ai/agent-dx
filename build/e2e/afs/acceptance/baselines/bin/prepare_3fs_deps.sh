#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

RUN_ID="${RUN_ID:-3fs-deps-$(timestamp_utc)}"
GUEST_ROOT="${GUEST_ROOT:-$AFS_BASELINE_GUEST_ROOT}"
EVIDENCE_DIR="$GUEST_ROOT/evidence"
LOG_FILE="$EVIDENCE_DIR/${RUN_ID}.log"
STATUS_FILE="$EVIDENCE_DIR/${RUN_ID}.status"
FDB_DIR="$GUEST_ROOT/foundationdb-${AFS_BASELINE_FDB_VERSION}"
LIBFUSE_VERSION="${LIBFUSE_VERSION:-3.16.2}"
LIBFUSE_SHA256="${LIBFUSE_SHA256:-f797055d9296b275e981f5f62d4e32e089614fc253d1ef2985851025b8a0ce87}"
LIBFUSE_DIR="$GUEST_ROOT/libfuse-${LIBFUSE_VERSION}"

mkdir -p "$EVIDENCE_DIR" "$FDB_DIR" "$LIBFUSE_DIR"

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
  log_line "3FS dependency download start"
  wait_for_package_manager
  for cmd in curl sha256sum tar; do
    require_cmd "$cmd"
  done

  download_fdb_deb "foundationdb-clients_${AFS_BASELINE_FDB_VERSION}-1_aarch64.deb"
  download_fdb_deb "foundationdb-server_${AFS_BASELINE_FDB_VERSION}-1_aarch64.deb"
  sha256sum "$FDB_DIR"/*.deb "$FDB_DIR"/*.sha256 >"$EVIDENCE_DIR/foundationdb-${AFS_BASELINE_FDB_VERSION}.sha256"

  local_fuse_tar="$LIBFUSE_DIR/fuse-${LIBFUSE_VERSION}.tar.gz"
  local_fuse_sig="$LIBFUSE_DIR/fuse-${LIBFUSE_VERSION}.tar.gz.sig"
  if [[ ! -f "$local_fuse_tar" ]]; then
    curl -fsSL -o "$local_fuse_tar" "https://github.com/libfuse/libfuse/releases/download/fuse-${LIBFUSE_VERSION}/fuse-${LIBFUSE_VERSION}.tar.gz"
  fi
  if [[ ! -f "$local_fuse_sig" ]]; then
    curl -fsSL -o "$local_fuse_sig" "https://github.com/libfuse/libfuse/releases/download/fuse-${LIBFUSE_VERSION}/fuse-${LIBFUSE_VERSION}.tar.gz.sig"
  fi
  printf '%s  %s\n' "$LIBFUSE_SHA256" "$local_fuse_tar" | sha256sum -c -
  sha256sum "$local_fuse_tar" "$local_fuse_sig" >"$EVIDENCE_DIR/libfuse-${LIBFUSE_VERSION}.sha256"
  tar -tzf "$local_fuse_tar" | sed -n '1,80p' >"$EVIDENCE_DIR/libfuse-${LIBFUSE_VERSION}.contents.txt"

  cat >"$EVIDENCE_DIR/3fs-deps-summary.txt" <<SUMMARY
3FS ref: $AFS_BASELINE_3FS_REF
FoundationDB: $AFS_BASELINE_FDB_VERSION, official release .sha256 verified
libfuse: $LIBFUSE_VERSION, GitHub release asset SHA256 verified against fixed acceptance script value
libfuse signature: downloaded for later key-based verification; not treated as a PASS gate here
SUMMARY
  write_status "$STATUS_FILE" PASS "3FS FDB and libfuse dependency artifacts downloaded and hashed; no 3FS C++ build started"
  log_line "3FS dependency download complete"
} 2>&1 | tee "$LOG_FILE"
