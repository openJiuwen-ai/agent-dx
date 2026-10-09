#!/usr/bin/env bash
set -Eeuo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
# shellcheck source=common.sh
source "$SCRIPT_DIR/common.sh"

RESEARCH_ROOT="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
REF_3FS="${REF_3FS:-/Users/lzc/workspace/code/agentruntime/ref/3FS}"
EVIDENCE_DIR="${EVIDENCE_DIR:-$RESEARCH_ROOT/evidence/afs-delivery/baseline-build}"
RUN_ID="${RUN_ID:-3fs-scratch-$(timestamp_utc)}"
LOG_FILE="$EVIDENCE_DIR/${RUN_ID}.log"
STATUS_FILE="$EVIDENCE_DIR/${RUN_ID}.status"

mkdir -p "$EVIDENCE_DIR"

{
  log_line "prepare 3FS scratch start"
  require_cmd limactl
  require_cmd git
  require_cmd tar
  assert_git_head "$REF_3FS" "$AFS_BASELINE_3FS_REF"
  if [[ -n "$(git -C "$REF_3FS" status --short)" ]]; then
    log_line "BLOCKED ref/3FS has local changes"
    git -C "$REF_3FS" status --short
    write_status "$STATUS_FILE" BLOCKED "ref/3FS is dirty"
    exit 2
  fi

  limactl shell --workdir=/tmp "$AFS_BASELINE_LIMA_INSTANCE" bash -lc "
    set -Eeuo pipefail
    mkdir -p '$AFS_BASELINE_GUEST_ROOT'
    test -w '$AFS_BASELINE_GUEST_ROOT'
  "

  export COPYFILE_DISABLE=1
  TAR_FLAGS=("--no-xattrs")
  if tar --help 2>&1 | grep -q -- "--disable-copyfile"; then
    TAR_FLAGS+=("--disable-copyfile")
  fi
  if tar --help 2>&1 | grep -q -- "--no-mac-metadata"; then
    TAR_FLAGS+=("--no-mac-metadata")
  fi

  log_line "copying $REF_3FS to $AFS_BASELINE_LIMA_INSTANCE:$AFS_BASELINE_GUEST_ROOT/3fs-src"
  tar "${TAR_FLAGS[@]}" -C "$(dirname "$REF_3FS")" -cf - "$(basename "$REF_3FS")" |
    limactl shell --workdir=/tmp "$AFS_BASELINE_LIMA_INSTANCE" bash -lc "
      set -Eeuo pipefail
      cd '$AFS_BASELINE_GUEST_ROOT'
      test ! -e 3fs-src || mv 3fs-src \"3fs-src.previous.\$(date -u +%Y%m%dT%H%M%SZ)\"
      tar -xf -
      mv '$(basename "$REF_3FS")' 3fs-src
      find 3fs-src -name '._*' -type f -delete
      git -C 3fs-src rev-parse HEAD
    "

  log_line "initializing 3FS submodules in guest scratch"
  limactl shell --workdir="$AFS_BASELINE_GUEST_ROOT/3fs-src" "$AFS_BASELINE_LIMA_INSTANCE" bash -lc "
    set -Eeuo pipefail
    git rev-parse HEAD
    test \"\$(git rev-parse HEAD)\" = '$AFS_BASELINE_3FS_REF'
    git submodule sync --recursive
    git submodule update --init --recursive --jobs 2
    find . -name '._*' -type f -delete
    git submodule status --recursive > '$AFS_BASELINE_GUEST_ROOT/3fs-submodules.txt'
    git status --short > '$AFS_BASELINE_GUEST_ROOT/3fs-status.txt'
    test ! -s '$AFS_BASELINE_GUEST_ROOT/3fs-status.txt'
  "

  limactl shell --workdir="$AFS_BASELINE_GUEST_ROOT" "$AFS_BASELINE_LIMA_INSTANCE" bash -lc "
    set -Eeuo pipefail
    find 3fs-src -maxdepth 1 -type f -name 'CMakeLists.txt' -print
    wc -l 3fs-submodules.txt
    cat 3fs-status.txt
  "
  write_status "$STATUS_FILE" PASS "3FS scratch copied and submodules initialized"
  log_line "prepare 3FS scratch complete"
} 2>&1 | tee "$LOG_FILE"
