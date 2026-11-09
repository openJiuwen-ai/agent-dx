#!/usr/bin/env bash
# Build-identity regression: exercises the real afs/build.rs in the four shapes
# that broke, or were never covered, by the original implementation.
#
#   1. linked worktree                 - HEAD lives under .git/worktrees/<name>
#   2. same-branch commit in that tree - symbolic HEAD never changes, so the
#                                        loose branch ref has to be watched
#   3. uncommitted tracked change      - the id must carry the `-dirty` suffix
#   4. source archive without .git     - identity must be supplied by the build
#                                        entry point or reported as `unknown`
#
# build.rs is compiled standalone and run with the package directory as cwd,
# which is exactly how Cargo invokes it, so this covers the identity logic
# without paying for a full workspace build.
set -euo pipefail

repo_root=$(cd "$(dirname "$0")/../.." && pwd)
scratch=$(mktemp -d)
trap 'rm -rf "$scratch"' EXIT

fail() { echo "FAIL: $*" >&2; exit 1; }

rustc --edition 2024 "$repo_root/afs/build.rs" -o "$scratch/build-id" >/dev/null 2>&1 ||
    fail "cannot compile afs/build.rs standalone"

export GIT_AUTHOR_NAME=t GIT_AUTHOR_EMAIL=t@example.com
export GIT_COMMITTER_NAME=t GIT_COMMITTER_EMAIL=t@example.com

git init -q -b main "$scratch/repo"
mkdir -p "$scratch/repo/afs"
cp "$repo_root/afs/build.rs" "$scratch/repo/afs/build.rs"
cd "$scratch/repo"
git add -A
git commit -qm one
git worktree add -q "$scratch/wt" -b feature

# Identity plus the watch set Cargo will record for this build.
run_identity() {
    local dir=$1
    shift
    ( cd "$dir/afs" && env "$@" "$scratch/build-id" )
}

id_of() { ( cd "$1" && git rev-parse --short=9 HEAD ); }

# Fingerprint of every path build.rs asks Cargo to watch: if this changes after
# a commit, Cargo rebuilds and the identity cannot go stale.
watch_fingerprint() {
    run_identity "$1" | sed -n 's/^cargo:rerun-if-changed=//p' | while read -r path; do
        [ -e "$1/afs/$path" ] || [ -e "$path" ] || continue
        local resolved
        resolved=$path
        [ -e "$1/afs/$path" ] && resolved="$1/afs/$path"
        printf '%s %s\n' "$resolved" "$(stat -c %Y-%s "$resolved" 2>/dev/null || echo missing)"
    done | sort
}

# ---- scenario 1: linked worktree reports its own HEAD ----------------------
id1=$(run_identity "$scratch/wt" | sed -n 's/^cargo:rustc-env=AFS_BUILD_ID=//p')
head1=$(id_of "$scratch/wt")
[ "${id1%-dirty}" = "$head1" ] || fail "linked worktree id $id1 != HEAD $head1"
echo "ok: linked worktree id=$id1"

watch_list=$(run_identity "$scratch/wt" | sed -n 's/^cargo:rerun-if-changed=//p')
echo "$watch_list" | grep -q 'refs/heads/feature' ||
    fail 'the loose branch ref is not watched; symbolic HEAD never changes on commit'
echo "ok: loose branch ref is watched"

fingerprint1=$(watch_fingerprint "$scratch/wt")

# ---- scenario 2: a commit on the same branch must change a watched path ----
( cd "$scratch/wt" && echo x >> afs/build.rs && git commit -qam two )
head2=$(id_of "$scratch/wt")
[ "$head2" != "$head1" ] || fail "test setup did not move HEAD"

id2=$(run_identity "$scratch/wt" | sed -n 's/^cargo:rustc-env=AFS_BUILD_ID=//p')
[ "${id2%-dirty}" = "$head2" ] || fail "same-branch commit kept stale id $id2 (HEAD $head2)"

fingerprint2=$(watch_fingerprint "$scratch/wt")
[ "$fingerprint1" != "$fingerprint2" ] ||
    fail "no watched path changed after a same-branch commit; Cargo would keep the stale id"
echo "ok: same-branch commit moved id=$id2 and changed a watched input"

# ---- scenario 3: uncommitted tracked change must mark the id dirty ---------
( cd "$scratch/wt" && echo y >> afs/build.rs )
dirty_id=$(run_identity "$scratch/wt" | sed -n 's/^cargo:rustc-env=AFS_BUILD_ID=//p')
[ "$dirty_id" = "$(id_of "$scratch/wt")-dirty" ] ||
    fail "uncommitted tracked change reported $dirty_id instead of <HEAD>-dirty"
echo "ok: uncommitted tracked change reports $dirty_id"

# ---- scenario 4: archive without .git --------------------------------------
mkdir -p "$scratch/archive"
tar -C "$scratch/repo" --exclude=.git -cf - . | tar -C "$scratch/archive" -xf -
[ ! -e "$scratch/archive/.git" ] || fail "archive copy still contains .git"

unknown=$(run_identity "$scratch/archive" | sed -n 's/^cargo:rustc-env=AFS_BUILD_ID=//p')
[ "$unknown" = "unknown" ] || fail "archive without identity reported $unknown instead of unknown"
run_identity "$scratch/archive" | grep -q 'cargo:warning=.*unknown' ||
    fail "archive build did not warn about the missing identity"
echo "ok: archive without .git reports unknown and warns"

supplied=$(run_identity "$scratch/archive" AFS_BUILD_ID=v1.2.3-archive |
    sed -n 's/^cargo:rustc-env=AFS_BUILD_ID=//p')
[ "$supplied" = "v1.2.3-archive" ] || fail "AFS_BUILD_ID override ignored: $supplied"
echo "ok: archive honours AFS_BUILD_ID=$supplied"

echo "build identity: all scenarios passed"
