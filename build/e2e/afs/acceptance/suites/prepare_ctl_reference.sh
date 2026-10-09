#!/usr/bin/env bash
set -euo pipefail

STATE_ROOT="${STATE_ROOT:-/mnt/lima-afsctlstate/afs-acceptance/suites-reference}"
RUN_ID="${RUN_ID:-$(date -u +%Y%m%dT%H%M%SZ)-$$}"
EVIDENCE_ROOT="${EVIDENCE_ROOT:-$STATE_ROOT/evidence/runs/$RUN_ID}"
SCRIPT_DIR="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"
WORK_ROOT="$STATE_ROOT/work"
SRC_ROOT="$STATE_ROOT/src"
EXT4_ROOT="$STATE_ROOT/ext4-smoke"

PJDFS_REPO="https://github.com/sanwan/pjdfstest.git"
PJDFS_REV="d25636a227606f8960e5179741d8f4ad7030ef41"
LTP_REPO="https://github.com/linux-test-project/ltp.git"
LTP_REV="20260529"
SECFS_REPO="https://github.com/billziss-gh/secfs.test.git"
SECFS_REV="edf5eb4a108bfb41073f765aef0cdd32bb3ee1ed"
JUICEFS_REPO="https://github.com/juicedata/juicefs.git"
JUICEFS_REV="adcca1cc61bb4d668a945d64b2e176b44ac8e5b5"

mkdir -p "$EVIDENCE_ROOT"/{logs,inventory,applicability,accounting,short-ext4,full-ext4,bin,raw-exit,diagnostics} "$WORK_ROOT" "$SRC_ROOT" "$EXT4_ROOT"

exec > >(tee "$EVIDENCE_ROOT/logs/prepare_ctl_reference.log") 2>&1

log() {
  printf '[%s] %s\n' "$(date -Is)" "$*"
}

run_json() {
  local name="$1"
  shift
  "$@" >"$EVIDENCE_ROOT/logs/${name}.stdout" 2>"$EVIDENCE_ROOT/logs/${name}.stderr"
}

capture_step() {
  local name="$1"
  shift
  set +e
  "$@" >"$EVIDENCE_ROOT/logs/${name}.log" 2>&1
  local status=$?
  set -e
  printf '%s\n' "$status" >"$EVIDENCE_ROOT/raw-exit/${name}.exit"
  return "$status"
}

capture_optional_step() {
  local name="$1"
  shift
  if ! capture_step "$name" "$@"; then
    log "Optional step $name failed with exit $(cat "$EVIDENCE_ROOT/raw-exit/${name}.exit")"
  fi
}

require_ext4_state() {
  local fstype source target
  fstype="$(findmnt -n -o FSTYPE -T "$STATE_ROOT")"
  source="$(findmnt -n -o SOURCE -T "$STATE_ROOT")"
  target="$(findmnt -n -o TARGET -T "$STATE_ROOT")"
  if [[ "$fstype" != "ext4" ]]; then
    echo "STATE_ROOT must be on ext4; got $fstype at $target" >&2
    exit 2
  fi
  if [[ "$target" == *"/workspace"* || "$source" == *"virtiofs"* ]]; then
    echo "STATE_ROOT must not be on shared workspace/virtiofs; got source=$source target=$target" >&2
    exit 2
  fi
}

install_deps() {
  if [[ "${SKIP_APT:-0}" == "1" ]]; then
    log "Skipping apt dependency installation because SKIP_APT=1"
    printf '0\n' >"$EVIDENCE_ROOT/raw-exit/install-deps.exit"
    return 0
  fi
  log "Installing ctl-only suite dependencies when missing"
  export DEBIAN_FRONTEND=noninteractive
  sudo apt-get update
  sudo apt-get install -y \
    git ca-certificates build-essential autoconf automake libtool pkg-config \
    bison flex m4 perl python3 python3-venv python3-pip acl attr xfsprogs \
    libacl1-dev libattr1-dev libaio-dev libcap-dev libkeyutils-dev libnuma-dev \
    libssl-dev libtirpc-dev linux-tools-common
  python3 -m pip --version >"$EVIDENCE_ROOT/logs/pip-version.log" 2>&1 || true
}

clone_checkout() {
  local repo="$1" rev="$2" dest="$3"
  if [[ ! -d "$dest/.git" ]]; then
    git clone "$repo" "$dest"
  fi
  git -C "$dest" fetch --tags --force origin
  git -C "$dest" checkout --force "$rev"
  git -C "$dest" rev-parse HEAD
}

write_identity() {
  python3 - "$EVIDENCE_ROOT" "$SRC_ROOT" <<'PY'
import json, subprocess, sys
from pathlib import Path
evidence = Path(sys.argv[1])
src = Path(sys.argv[2])
repos = {
  "pjdfstest": src / "pjdfstest",
  "ltp": src / "ltp",
  "secfs.test": src / "secfs.test",
  "juicefs": src / "juicefs",
}
out = {
  "created_at": subprocess.check_output(["date", "-Is"], text=True).strip(),
  "host": subprocess.check_output(["hostname"], text=True).strip(),
  "uname": subprocess.check_output(["uname", "-a"], text=True).strip(),
    "mount": json.loads(subprocess.check_output(["findmnt", "-T", str(evidence), "-o", "TARGET,SOURCE,FSTYPE,OPTIONS", "--json"], text=True)),
  "repos": {},
  "tools": {},
}
for name, path in repos.items():
  out["repos"][name] = {
    "path": str(path),
    "head": subprocess.check_output(["git", "-C", str(path), "rev-parse", "HEAD"], text=True).strip(),
    "describe": subprocess.run(["git", "-C", str(path), "describe", "--tags", "--always", "--dirty"], text=True, stdout=subprocess.PIPE).stdout.strip(),
    "remote": subprocess.run(["git", "-C", str(path), "remote", "get-url", "origin"], text=True, stdout=subprocess.PIPE).stdout.strip(),
  }
for cmd in ["git", "gcc", "make", "python3"]:
  out["tools"][cmd] = subprocess.run([cmd, "--version"], text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT).stdout.splitlines()[0]
(evidence / "upstream-identities.json").write_text(json.dumps(out, indent=2, sort_keys=True) + "\n")
PY
}

discover_pjdfstest() {
  log "Discovering pjdfstest inventory"
  local src="$SRC_ROOT/pjdfstest"
  find "$src/tests" -type f | sort >"$EVIDENCE_ROOT/inventory/pjdfstest-files.txt"
  python3 - "$src" "$EVIDENCE_ROOT" <<'PY'
import json, os, sys
from pathlib import Path
src = Path(sys.argv[1])
evidence = Path(sys.argv[2])
tests = []
for path in sorted((src / "tests").rglob("*.t")):
  if path.is_file():
    tests.append(str(path.relative_to(src / "tests")))
(evidence / "inventory/pjdfstest-executable-tests.txt").write_text("\n".join(tests) + "\n")
(evidence / "accounting/pjdfstest-inventory.json").write_text(json.dumps({
  "suite": "pjdfstest",
  "discovered": len(tests),
  "pass": 0,
  "fail": 0,
  "pre_reviewed_excluded": 0,
  "incomplete": len(tests),
  "note": "Inventory only; short smoke is separate and full suite is NOT_RUN."
}, indent=2, sort_keys=True) + "\n")
PY
  cat >"$EVIDENCE_ROOT/applicability/pjdfstest.tsv" <<'EOF'
case_id	test_or_area	applicability	exclusion_reason
STD-01	pjdfstest full tests	REVIEW_REQUIRED
EOF
}

discover_ltp() {
  log "Discovering LTP filesystem subset inventory"
  local src="$SRC_ROOT/ltp"
  cp "$SCRIPT_DIR/ltp-filesystem-selectors.txt" "$EVIDENCE_ROOT/inventory/ltp-filesystem-selectors.txt"
  python3 - "$src" "$EVIDENCE_ROOT" <<'PY'
import json, re, sys
from pathlib import Path
src = Path(sys.argv[1])
evidence = Path(sys.argv[2])
runtest = src / "runtest"
selector_files = ["fs", "fs_bind", "fs_perms_simple", "fcntl-locktests"]
records = []
for name in selector_files:
  path = runtest / name
  if path.exists():
    for line in path.read_text(errors="replace").splitlines():
      s = line.strip()
      if not s or s.startswith("#"):
        continue
      records.append({"selector": name, "test_id": s.split()[0], "command": s})
syscall_file = runtest / "syscalls"
file_related = re.compile(r"(open|creat|read|write|pread|pwrite|close|fsync|fdatasync|sync_file_range|truncate|ftruncate|lseek|stat|lstat|fstat|statx|rename|unlink|link|symlink|mkdir|rmdir|getdents|readdir|chmod|chown|xattr|flock|fcntl|mmap|msync|fallocate|copy_file_range|sendfile|splice)")
if syscall_file.exists():
  for line in syscall_file.read_text(errors="replace").splitlines():
    s = line.strip()
    if not s or s.startswith("#"):
      continue
    test_id = s.split()[0]
    if file_related.search(test_id):
      records.append({"selector": "syscalls-file-related", "test_id": test_id, "command": s})
out_lines = [f"{r['selector']}\t{r['test_id']}\t{r['command'].replace(chr(9), ' ')}" for r in records]
(evidence / "inventory/ltp-filesystem-expanded.tsv").write_text("selector\ttest_id\tcommand\n" + "\n".join(out_lines) + "\n")
(evidence / "accounting/ltp-inventory.json").write_text(json.dumps({
  "suite": "LTP-filesystem",
  "discovered": len(records),
  "pass": 0,
  "fail": 0,
  "pre_reviewed_excluded": 0,
  "incomplete": len(records),
  "selector_counts": {sel: sum(1 for r in records if r["selector"] == sel) for sel in sorted(set(r["selector"] for r in records))},
  "note": "Inventory only; selected syscalls are a frozen file-related expansion requiring review before full gate."
}, indent=2, sort_keys=True) + "\n")
with (evidence / "applicability/ltp-filesystem.tsv").open("w") as f:
  f.write("case_id\tselector\ttest_id\tapplicability\texclusion_reason\n")
  for r in records:
    f.write(f"STD-02\t{r['selector']}\t{r['test_id']}\tREVIEW_REQUIRED\t\n")
PY
}

discover_fsx() {
  log "Discovering FSx inventory"
  local src="$SRC_ROOT/secfs.test"
  find "$src" -type f | sort >"$EVIDENCE_ROOT/inventory/secfs-files.txt"
  find "$src" -type f \( -iname '*fsx*' -o -iname '*fsstress*' \) | sort >"$EVIDENCE_ROOT/inventory/fsx-candidates.txt"
  python3 - "$EVIDENCE_ROOT" <<'PY'
import json, sys
from pathlib import Path
evidence = Path(sys.argv[1])
candidates = [line for line in (evidence / "inventory/fsx-candidates.txt").read_text().splitlines() if line]
(evidence / "accounting/fsx-inventory.json").write_text(json.dumps({
  "suite": "FSx",
  "discovered": len(candidates),
  "pass": 0,
  "fail": 0,
  "pre_reviewed_excluded": 0,
  "incomplete": len(candidates),
  "note": "Inventory of FSx candidate files only; full 900s x 3 seeds is NOT_RUN. Build status for tools/bin/fsx is tracked separately from unrelated secfs.test tools."
}, indent=2, sort_keys=True) + "\n")
(evidence / "applicability/fsx.tsv").write_text("case_id\ttest_or_area\tapplicability\texclusion_reason\nSTD-03\tFSx read/write/truncate/mmap\tREVIEW_REQUIRED\t\n")
(evidence / "full-ext4/fsx-900s-plan.md").write_text("""# FSx full-run plan

Pinned source: secfs.test edf5eb4. Build target: `make tools/bin/fsx`, not top-level `make`, because top-level also builds unrelated tools such as iozone.

Full release gate remains NOT_RUN in this preparation slice. Reproducible command shape for each fixed seed:

```bash
timeout --preserve-status 930s ./tools/bin/fsx -d 900s -S <seed> -P <evidence-dir> <mount>/fsx-seed-<seed>.dat
```

Required seeds: 1, 2, 3. The 930s timeout gives the tool's own 900s duration a bounded cleanup margin and must be reported as BLOCKED/FAIL if it fires.
""")
PY
}

discover_random_model() {
  log "Preparing random model inventory"
  cp "$SCRIPT_DIR/random_model.py" "$EVIDENCE_ROOT/bin/random_model.py"
  chmod +x "$EVIDENCE_ROOT/bin/random_model.py"
  cat >"$EVIDENCE_ROOT/applicability/differential-random.tsv" <<'EOF'
case_id	test_or_area	applicability	exclusion_reason
STD-04	random create/write/pread/truncate/rename/unlink/mkdir/rmdir/symlink/readdir model	REVIEW_REQUIRED
EOF
  python3 - "$EVIDENCE_ROOT" <<'PY'
import json, sys
from pathlib import Path
evidence = Path(sys.argv[1])
(evidence / "accounting/differential-random-inventory.json").write_text(json.dumps({
  "suite": "differential-random",
  "discovered": 10,
  "unit": "fixed full seeds",
  "full_seeds": list(range(10)),
  "full_operations_per_seed": 10000,
  "pass": 0,
  "fail": 0,
  "pre_reviewed_excluded": 0,
  "incomplete": 10,
  "note": "Model prepared; full ext4-vs-AFS comparison and shrink retention are NOT_RUN."
}, indent=2, sort_keys=True) + "\n")
PY
}


write_ltp_installed_manifest() {
  log "Writing LTP selected-test installed-binary manifest"
  python3 - "$EVIDENCE_ROOT" "$WORK_ROOT/ltp-install/testcases/bin" <<'PY'
import csv, json, os, shlex, shutil, sys
from pathlib import Path
evidence = Path(sys.argv[1])
bin_dir = Path(sys.argv[2])
source = evidence / "inventory/ltp-filesystem-expanded.tsv"
out = evidence / "inventory/ltp-filesystem-installed.tsv"
shell_builtins = {"export", "cd", "ulimit", "umask", "shift", "test", "[", "true", "false"}
records = []

def first_token(segment):
  try:
    parts = shlex.split(segment, comments=False, posix=True)
  except ValueError:
    parts = segment.split()
  while parts and "=" in parts[0] and not parts[0].startswith(("/", "./")):
    parts = parts[1:]
  return parts[0] if parts else ""

if source.exists():
  with source.open() as f:
    reader = csv.DictReader(f, delimiter="\t")
    for row in reader:
      command = row.get("command", "")
      tokens = command.split()
      payload = " ".join(tokens[1:]) if tokens and tokens[0] == row.get("test_id") else command
      ltp_bins = []
      system_cmds = []
      missing_tokens = []
      for segment in payload.split(";"):
        token = first_token(segment.strip())
        if not token:
          continue
        token_name = Path(token).name
        if (bin_dir / token_name).is_file() and os.access(bin_dir / token_name, os.X_OK):
          if token_name not in ltp_bins:
            ltp_bins.append(token_name)
        elif token_name in shell_builtins or shutil.which(token_name):
          if token_name not in system_cmds:
            system_cmds.append(token_name)
        else:
          if token_name not in missing_tokens:
            missing_tokens.append(token_name)
      status = "INSTALLED" if ltp_bins else ("SYSTEM_ONLY" if system_cmds and not missing_tokens else "MISSING")
      reason = "" if status != "MISSING" else f"no executable LTP binary found in {bin_dir}; missing tokens={missing_tokens}"
      records.append({
        "selector": row.get("selector", ""),
        "test_id": row.get("test_id", ""),
        "command": command,
        "ltp_binaries": ",".join(ltp_bins),
        "system_commands": ",".join(system_cmds),
        "missing_tokens": ",".join(missing_tokens),
        "status": status,
        "reason": reason,
      })
with out.open("w") as f:
  f.write("selector\ttest_id\tstatus\tltp_binaries\tsystem_commands\tmissing_tokens\treason\tcommand\n")
  for r in records:
    f.write(f"{r['selector']}\t{r['test_id']}\t{r['status']}\t{r['ltp_binaries']}\t{r['system_commands']}\t{r['missing_tokens']}\t{r['reason']}\t{r['command']}\n")
counts = {}
for r in records:
  counts[r["status"]] = counts.get(r["status"], 0) + 1
(evidence / "accounting/ltp-installed-binaries.json").write_text(json.dumps({
  "suite": "LTP-filesystem-installed-binaries",
  "total_selected": len(records),
  "counts": counts,
  "missing": [r for r in records if r["status"] == "MISSING"],
  "note": "Frozen selector-to-command manifest. A record is INSTALLED when at least one LTP test binary in the command is present; shell/system helpers are tracked separately. Missing entries are reported, not silently filtered into PASS."
}, indent=2, sort_keys=True) + "\n")
PY
}


run_short_ext4() {
  log "Running short ext4 smoke checks"
  sudo rm -rf "$EXT4_ROOT"
  mkdir -p "$EXT4_ROOT"/{pjdfstest-root,pjdfstest-full-root,ltp,fsx,random}
  findmnt -T "$EXT4_ROOT" -o TARGET,SOURCE,FSTYPE,OPTIONS --json >"$EVIDENCE_ROOT/short-ext4/mount.json"

  local pj="$SRC_ROOT/pjdfstest"
  if [[ -f "$pj/tests/open/00.t" ]]; then
    capture_optional_step pjdfstest-short-root bash -lc "cd '$EXT4_ROOT/pjdfstest-root' && sudo prove -e /bin/sh -r '$pj/tests/open' '$pj/tests/mkdir' '$pj/tests/rename'"
  else
    echo "pjdfstest executable tests not found after build" >"$EVIDENCE_ROOT/logs/pjdfstest-short-root.log"
    echo 127 >"$EVIDENCE_ROOT/raw-exit/pjdfstest-short-root.exit"
  fi

  cat >"$EVIDENCE_ROOT/diagnostics/pjdfstest-nonroot.json" <<'EOF'
{
  "status": "NOT_APPLICABLE_AS_WHOLE_SUITE",
  "reason": "Upstream pjdfstest README requires root. JuiceFS runs pjdfstest with sudo prove -rv tests/. Non-root permission coverage must be selected separately; a whole-suite non-root run is a harness error, not a filesystem baseline.",
  "raw_nonroot_suite_run": "not executed by this fixed runner"
}
EOF

  if [[ "${RUN_FULL_PJDFSTEST_ROOT:-1}" == "1" ]]; then
    capture_optional_step pjdfstest-full-root bash -lc "cd '$EXT4_ROOT/pjdfstest-full-root' && sudo prove -e /bin/sh -rv '$pj/tests'"
  else
    echo "Full pjdfstest root harness disabled by RUN_FULL_PJDFSTEST_ROOT=0" >"$EVIDENCE_ROOT/logs/pjdfstest-full-root.log"
    echo 125 >"$EVIDENCE_ROOT/raw-exit/pjdfstest-full-root.exit"
  fi

  if [[ -x "$WORK_ROOT/ltp-install/kirk" ]]; then
    "$WORK_ROOT/ltp-install/kirk" --version >"$EVIDENCE_ROOT/logs/kirk-version.log" 2>&1 || true
  elif [[ -x "$WORK_ROOT/ltp-install/bin/kirk" ]]; then
    "$WORK_ROOT/ltp-install/bin/kirk" --version >"$EVIDENCE_ROOT/logs/kirk-version.log" 2>&1 || true
  elif command -v kirk >/dev/null 2>&1; then
    kirk --version >"$EVIDENCE_ROOT/logs/kirk-version.log" 2>&1 || true
  else
    echo "kirk not found after LTP install" >"$EVIDENCE_ROOT/logs/kirk-version.log"
  fi

  if [[ -x "$WORK_ROOT/ltp-install/kirk" ]]; then
    cat >"$EVIDENCE_ROOT/short-ext4/ltp-requested-tests.txt" <<'EOF'
creat01 creat01
open01 open01
rename01 rename01
unlink05 unlink05
fsync01 fsync01
fcntl01 fcntl01
EOF
    : >"$EVIDENCE_ROOT/short-ext4/ltp-missing-binaries.txt"
    : >"$EVIDENCE_ROOT/short-ext4/ltp-short-afs-smoke"
    while read -r id cmd rest; do
      [[ -z "$id" ]] && continue
      if [[ -x "$WORK_ROOT/ltp-install/testcases/bin/$cmd" ]]; then
        printf '%s %s%s\n' "$id" "$cmd" "${rest:+ $rest}" >>"$EVIDENCE_ROOT/short-ext4/ltp-short-afs-smoke"
      else
        printf '%s\t%s\n' "$id" "$cmd" >>"$EVIDENCE_ROOT/short-ext4/ltp-missing-binaries.txt"
      fi
    done <"$EVIDENCE_ROOT/short-ext4/ltp-requested-tests.txt"
    sudo cp "$EVIDENCE_ROOT/short-ext4/ltp-short-afs-smoke" "$WORK_ROOT/ltp-install/runtest/short-afs-smoke"
    mkdir -p "$EXT4_ROOT/ltp/tmp" "$EVIDENCE_ROOT/short-ext4/ltp-kirk"
    capture_optional_step ltp-short-ext4 bash -lc "cd '$EXT4_ROOT/ltp' && sudo env LTPROOT='$WORK_ROOT/ltp-install' TMPDIR='$EXT4_ROOT/ltp' PATH='$WORK_ROOT/ltp-install/testcases/bin':\$PATH '$WORK_ROOT/ltp-install/kirk' --run-suite short-afs-smoke --json-report '$EVIDENCE_ROOT/short-ext4/ltp-kirk/report.json' --tmp-dir '$EXT4_ROOT/ltp/tmp'"
  else
    echo "kirk not available; LTP short ext4 smoke not run" >"$EVIDENCE_ROOT/logs/ltp-short-ext4.log"
    echo 127 >"$EVIDENCE_ROOT/raw-exit/ltp-short-ext4.exit"
  fi

  local fsx_bin
  fsx_bin="$(find "$SRC_ROOT/secfs.test" -type f -perm -111 -iname '*fsx*' | head -n 1 || true)"
  if [[ -n "$fsx_bin" ]]; then
    capture_optional_step fsx-short-ext4 timeout 30s "$fsx_bin" -N 1000 -S 1 "$EXT4_ROOT/fsx/fsxfile"
  else
    echo "No executable FSx binary discovered after build" >"$EVIDENCE_ROOT/logs/fsx-short-ext4.log"
    echo 127 >"$EVIDENCE_ROOT/raw-exit/fsx-short-ext4.exit"
  fi

  python3 "$SCRIPT_DIR/random_model.py" \
    --root "$EXT4_ROOT/random/seed0" \
    --seed 0 \
    --operations 100 \
    --output "$EVIDENCE_ROOT/short-ext4/differential-random-seed0.json"
}

write_summary() {
  log "Writing summary and suite accounting"
  python3 - "$EVIDENCE_ROOT" <<'PY'
import json, re, sys
from pathlib import Path
evidence = Path(sys.argv[1])
def load_json(path):
  p = evidence / path
  return json.loads(p.read_text()) if p.exists() else None
def tap_summary(path):
  p = evidence / "logs" / path
  if not p.exists():
    return {"exists": False}
  text = p.read_text(errors="replace")
  tap_ok = 0
  tap_not_ok = 0
  tap_skip = 0
  tap_todo = 0
  tap_unexpected_fail = 0
  for line in text.splitlines():
    stripped = line.strip()
    if re.match(r"^ok\s+\d+", stripped):
      tap_ok += 1
      if re.search(r"#\s*SKIP", stripped, re.I):
        tap_skip += 1
      if re.search(r"#\s*TODO", stripped, re.I):
        tap_todo += 1
    elif re.match(r"^not ok\s+\d+", stripped):
      tap_not_ok += 1
      if re.search(r"#\s*TODO", stripped, re.I):
        tap_todo += 1
      else:
        tap_unexpected_fail += 1
  prove_files = None
  prove_tests = None
  prove_result = None
  m = re.search(r"Files=(\d+),\s+Tests=(\d+).+?Result:\s+(\w+)", text, re.S)
  if m:
    prove_files = int(m.group(1))
    prove_tests = int(m.group(2))
    prove_result = m.group(3)
  return {
    "exists": True,
    "bytes": len(text.encode()),
    "tap_ok": tap_ok,
    "tap_not_ok": tap_not_ok,
    "tap_skip": tap_skip,
    "tap_todo": tap_todo,
    "tap_unexpected_fail": tap_unexpected_fail,
    "prove_files": prove_files,
    "prove_tests": prove_tests,
    "prove_result": prove_result,
    "note": "TAP/prove log summary; inspect raw log for per-file diagnostics."
  }
def log_summary(path):
  p = evidence / "logs" / path
  if not p.exists():
    return {"exists": False}
  text = p.read_text(errors="replace")
  return {
    "exists": True,
    "bytes": len(text.encode()),
    "pass_like": len(re.findall(r"\\bok\\b|\\bpass\\b|PASS|Passed:", text)),
    "fail_like": len(re.findall(r"not ok|\\bfail\\b|FAIL|Failed:|Failures:|ERROR", text)),
    "note": "Short smoke log only; not a full acceptance result."
  }
accounting = {}
for p in sorted((evidence / "accounting").glob("*.json")):
  accounting[p.stem] = json.loads(p.read_text())
raw_exit = {}
for p in sorted((evidence / "raw-exit").glob("*.exit")):
  raw_exit[p.stem] = p.read_text().strip()
ltp_missing = []
missing_path = evidence / "short-ext4/ltp-missing-binaries.txt"
if missing_path.exists():
  ltp_missing = [line for line in missing_path.read_text().splitlines() if line]
summary = {
  "created_at": __import__("subprocess").check_output(["date", "-Is"], text=True).strip(),
  "full_acceptance": False,
  "status": "REFERENCE_SMOKE_ONLY",
  "run_id": evidence.name,
  "evidence_root": str(evidence),
  "raw_exit": raw_exit,
  "inventories": sorted(str(p.relative_to(evidence)) for p in (evidence / "inventory").glob("*")),
  "applicability": sorted(str(p.relative_to(evidence)) for p in (evidence / "applicability").glob("*")),
  "accounting": accounting,
  "diagnostics": {
    "pjdfstest_nonroot": load_json("diagnostics/pjdfstest-nonroot.json"),
    "ltp_missing_binaries": ltp_missing,
  },
  "short_ext4_logs": {
    "pjdfstest_root": tap_summary("pjdfstest-short-root.log"),
    "ltp": log_summary("ltp-short-ext4.log"),
    "fsx": log_summary("fsx-short-ext4.log"),
  },
  "full_ext4_logs": {
    "pjdfstest_root": tap_summary("pjdfstest-full-root.log"),
  },
  "known_gaps": [
    "Full AFS pjdfstest gate not run; ext4 root reference may have been run depending on raw exits.",
    "Whole-suite non-root pjdfstest is not an upstream-valid harness mode; root harness covers internal -u/-g non-root subcases and selected non-root probes still need separate design.",
    "Full LTP subset not run.",
    "Full FSx 900 seconds x 3 seeds not run.",
    "Full differential random 10 x 10000 ext4-vs-AFS comparison not run.",
    "No AFS mount was tested in P0.2b reference preparation."
  ]
}
ltp_report = evidence / "short-ext4/ltp-kirk/report.json"
if ltp_report.exists():
  stats = json.loads(ltp_report.read_text()).get("stats", {})
  summary["short_ext4_logs"]["ltp"].update({
    "pass_like": stats.get("passed", summary["short_ext4_logs"]["ltp"].get("pass_like", 0)),
    "fail_like": stats.get("failed", summary["short_ext4_logs"]["ltp"].get("fail_like", 0)),
    "broken": stats.get("broken"),
    "skipped": stats.get("skipped"),
  })
(evidence / "summary.json").write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n")
PY
}

main() {
  require_ext4_state
  install_deps
  log "Cloning pinned upstream suites"
  clone_checkout "$PJDFS_REPO" "$PJDFS_REV" "$SRC_ROOT/pjdfstest" | tee "$EVIDENCE_ROOT/logs/pjdfstest-head.log"
  clone_checkout "$LTP_REPO" "$LTP_REV" "$SRC_ROOT/ltp" | tee "$EVIDENCE_ROOT/logs/ltp-head.log"
  git -C "$SRC_ROOT/ltp" submodule update --init tools/kirk/kirk-src | tee "$EVIDENCE_ROOT/logs/ltp-kirk-submodule.log"
  clone_checkout "$SECFS_REPO" "$SECFS_REV" "$SRC_ROOT/secfs.test" | tee "$EVIDENCE_ROOT/logs/secfs-head.log"
  clone_checkout "$JUICEFS_REPO" "$JUICEFS_REV" "$SRC_ROOT/juicefs" | tee "$EVIDENCE_ROOT/logs/juicefs-head.log"
  write_identity
  discover_pjdfstest
  discover_ltp
  discover_fsx
  discover_random_model
  capture_optional_step pjdfstest-build bash -lc "cd '$SRC_ROOT/pjdfstest' && autoreconf -ifs && ./configure && make -j\"\$(nproc)\""
  capture_optional_step ltp-build-install bash -lc "cd '$SRC_ROOT/ltp' && make autotools && ./configure --prefix='$WORK_ROOT/ltp-install' && make -j\"\$(nproc)\" && sudo make install"
  write_ltp_installed_manifest
  capture_optional_step fsx-build bash -lc "cd '$SRC_ROOT/secfs.test' && make tools/bin/fsx"
  run_short_ext4
  write_summary
  log "Done. Evidence root: $EVIDENCE_ROOT"
}

main "$@"
