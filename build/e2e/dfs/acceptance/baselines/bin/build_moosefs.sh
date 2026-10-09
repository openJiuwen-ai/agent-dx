#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

RUN_ID="${RUN_ID:-moosefs-build-$(timestamp_utc)}"
GUEST_ROOT="${GUEST_ROOT:-$AFS_BASELINE_GUEST_ROOT}"
SRC_DIR="$GUEST_ROOT/moosefs-src"
PREFIX="$GUEST_ROOT/moosefs-install"
EVIDENCE_DIR="$GUEST_ROOT/evidence"
LOG_FILE="$EVIDENCE_DIR/${RUN_ID}.log"
STATUS_FILE="$EVIDENCE_DIR/${RUN_ID}.status"

mkdir -p "$GUEST_ROOT" "$EVIDENCE_DIR"

{
  log_line "MooseFS build start"
  wait_for_package_manager
  for cmd in git make gcc g++ pkg-config autoreconf libtoolize; do
    require_cmd "$cmd"
  done

  if [[ ! -d "$SRC_DIR/.git" ]]; then
    git clone https://github.com/moosefs/moosefs.git "$SRC_DIR"
  fi
  git -C "$SRC_DIR" fetch --tags origin "$AFS_BASELINE_MOOSEFS_REF"
  git -C "$SRC_DIR" checkout --detach "$AFS_BASELINE_MOOSEFS_REF"
  assert_git_head "$SRC_DIR" "$AFS_BASELINE_MOOSEFS_REF"
  git -C "$SRC_DIR" status --short
  git -C "$SRC_DIR" rev-parse HEAD >"$EVIDENCE_DIR/moosefs-head.txt"

  pushd "$SRC_DIR" >/dev/null
  if [[ -x ./bootstrap.sh ]]; then
    ./bootstrap.sh
  else
    autoreconf -fi
  fi
  ./configure \
    --prefix="$PREFIX" \
    --with-default-user="$(id -un)" \
    --with-default-group="$(id -gn)" \
    --with-systemdsystemunitdir=no
  make -j "$AFS_BASELINE_JOBS"
  make install
  popd >/dev/null

  find "$PREFIX" -type f -perm -111 -print | sort >"$EVIDENCE_DIR/moosefs-executables.txt"
  while IFS= read -r exe; do
    printf '### %s\n' "$exe"
    ldd "$exe" || true
  done <"$EVIDENCE_DIR/moosefs-executables.txt" >"$EVIDENCE_DIR/moosefs-ldd.txt"
  find "$PREFIX" -type f -print0 | sort -z | xargs -0 sha256sum >"$EVIDENCE_DIR/moosefs-files.sha256"
  write_status "$STATUS_FILE" PASS "MooseFS stock CE built; strong durability comparison remains blocked by B001"
  log_line "MooseFS build complete"
} 2>&1 | tee "$LOG_FILE"
