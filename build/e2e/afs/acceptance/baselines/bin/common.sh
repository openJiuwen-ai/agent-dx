#!/usr/bin/env bash
set -Eeuo pipefail

AFS_BASELINE_MOOSEFS_REF="${AFS_BASELINE_MOOSEFS_REF:-ac106b2ec8661ff00def725d042cb67d3ca2184d}"
AFS_BASELINE_3FS_REF="${AFS_BASELINE_3FS_REF:-22fca04564c7cc230fd8b9523b8b92864e1dad47}"
AFS_BASELINE_FDB_VERSION="${AFS_BASELINE_FDB_VERSION:-7.3.63}"
AFS_BASELINE_GUEST_ROOT="${AFS_BASELINE_GUEST_ROOT:-/home/lzc.guest/afs-build/baselines}"
AFS_BASELINE_LIMA_INSTANCE="${AFS_BASELINE_LIMA_INSTANCE:-afs-build}"
AFS_BASELINE_JOBS="${AFS_BASELINE_JOBS:-2}"

timestamp_utc() {
  date -u +%Y%m%dT%H%M%SZ
}

log_line() {
  printf '%s %s\n' "$(timestamp_utc)" "$*"
}

require_cmd() {
  local cmd="$1"
  if ! command -v "$cmd" >/dev/null 2>&1; then
    log_line "BLOCKED missing command: $cmd"
    return 1
  fi
}

write_status() {
  local status_file="$1"
  local status="$2"
  local message="$3"
  {
    printf 'status=%s\n' "$status"
    printf 'time=%s\n' "$(timestamp_utc)"
    printf 'message=%s\n' "$message"
  } >"$status_file"
}

sha256_one() {
  if command -v sha256sum >/dev/null 2>&1; then
    sha256sum "$1"
  else
    shasum -a 256 "$1"
  fi
}

assert_git_head() {
  local dir="$1"
  local want="$2"
  local got
  got="$(git -C "$dir" rev-parse HEAD)"
  if [[ "$got" != "$want" ]]; then
    log_line "BLOCKED git head mismatch dir=$dir want=$want got=$got"
    return 1
  fi
}

wait_for_package_manager() {
  local waited=0
  while pgrep -x apt >/dev/null 2>&1 || pgrep -x apt-get >/dev/null 2>&1 || pgrep -x dpkg >/dev/null 2>&1; do
    if (( waited >= 1800 )); then
      log_line "BLOCKED apt/dpkg still running after ${waited}s"
      return 1
    fi
    sleep 10
    waited=$((waited + 10))
  done
}
