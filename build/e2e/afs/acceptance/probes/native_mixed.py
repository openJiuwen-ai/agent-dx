#!/usr/bin/env python3
"""Short mixed FUSE/native OwnerFs semantic probe.

This probe is intentionally small. It exercises a running managed OwnerFs
workspace through two paths that should name the same directory:

* primary: the host FUSE workspace path
* secondary: the container final view, usually /proc/<container-pid>/root/workspace

It records raw artifacts under --out and prints one JSON summary to stdout.
It does not stop the container or claim full POSIX coverage.
"""
from __future__ import annotations

import argparse
import ctypes
import errno
import hashlib
import json
import mmap
import os
import platform
import shutil
import struct
import subprocess
import sys
import time
import traceback
from pathlib import Path
from typing import Any, Callable


IN_CREATE = 0x00000100
IN_MODIFY = 0x00000002
IN_CLOSE_WRITE = 0x00000008
IN_MOVED_FROM = 0x00000040
IN_MOVED_TO = 0x00000080
IN_DELETE = 0x00000200
IN_ALL_NEEDED = IN_CREATE | IN_MODIFY | IN_CLOSE_WRITE | IN_MOVED_FROM | IN_MOVED_TO | IN_DELETE
IN_NONBLOCK = 0o00004000


def now() -> str:
    return time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime())


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def json_write(path: Path, value: Any) -> None:
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")


def read_text(path: Path, limit: int = 64 * 1024) -> str:
    try:
        data = path.read_bytes()
    except OSError as exc:
        return f"<read error {type(exc).__name__}: {exc}>"
    if len(data) > limit:
        return data[:limit].decode("utf-8", "replace") + f"\n<truncated {len(data) - limit} bytes>"
    return data.decode("utf-8", "replace")


def stat_record(path: Path) -> dict[str, Any]:
    st = path.stat()
    return {
        "path": str(path),
        "dev": st.st_dev,
        "ino": st.st_ino,
        "mode": oct(st.st_mode & 0o7777),
        "uid": st.st_uid,
        "gid": st.st_gid,
        "size": st.st_size,
        "mtime_ns": st.st_mtime_ns,
    }


def errno_record(exc: OSError) -> dict[str, Any]:
    return {"errno": exc.errno, "errno_name": errno.errorcode.get(exc.errno, f"ERRNO_{exc.errno}"), "message": str(exc)}


def absolute_path(path: Path) -> Path:
    return Path(os.path.abspath(os.fspath(path)))


def expect(condition: bool, message: str, details: Any = None) -> None:
    if not condition:
        raise AssertionError(json.dumps({"message": message, "details": details}, sort_keys=True))


def expected_append_lines() -> set[str]:
    return {f"primary:{i:02d}:native-mixed-append" for i in range(64)} | {
        f"secondary:{i:02d}:native-mixed-append" for i in range(64)
    }


def concurrent_append_status(lines: list[str], mirror_lines: list[str] | None) -> dict[str, Any]:
    expected = expected_append_lines()
    counts = {line: lines.count(line) for line in set(lines)}
    duplicates = sorted(line for line, count in counts.items() if count > 1)
    missing = sorted(expected - set(lines))
    extra = sorted(set(lines) - expected)
    return {
        "status": "PASS" if len(lines) == 128 and not duplicates and not missing and not extra and mirror_lines == lines else "FAIL",
        "records": len(lines),
        "unique_records": len(set(lines)),
        "missing": missing,
        "extra": extra,
        "duplicates": duplicates,
        "mirror_matches": mirror_lines == lines,
        "sample": lines[:5] + lines[-5:],
    }


def correlate_concurrent_offsets(content: bytes, child_records: list[dict[str, Any]]) -> dict[str, Any]:
    actual: dict[str, int] = {}
    position = 0
    for raw in content.splitlines(keepends=True):
        line = raw.rstrip(b"\n").decode("utf-8", "replace")
        position += len(raw)
        actual[line] = position
    mismatches = []
    parsed_children = []
    coverage = []
    roles = []
    for child in child_records:
        if child.get("returncode") != 0 or child.get("timed_out"):
            mismatches.append({"child": child.get("index"), "error": "child did not exit cleanly"})
        try:
            payload = json.loads(child.get("stdout", "") or "{}")
        except json.JSONDecodeError:
            payload = {}
        rows = payload.get("offsets", [])
        role = payload.get("role")
        roles.append(role)
        parsed_children.append({"index": child.get("index"), "role": payload.get("role"), "offsets": rows})
        if role not in ("primary", "secondary") or len(rows) != 64:
            mismatches.append({"child": child.get("index"), "role": role, "rows": len(rows), "error": "expected 64 offsets for one role"})
        for row in rows:
            line = row.get("line")
            offset = row.get("offset")
            coverage.append(line)
            if not isinstance(offset, int) or offset <= 0:
                mismatches.append({"line": line, "offset": offset, "error": "offset must be a positive integer"})
                continue
            if actual.get(line) != offset:
                mismatches.append({"line": line, "offset": offset, "actual_end": actual.get(line)})
    expected = expected_append_lines()
    duplicates = sorted(line for line in set(coverage) if coverage.count(line) > 1)
    missing = sorted(expected - set(coverage))
    extra = sorted(set(coverage) - expected)
    if sorted(roles) != ["primary", "secondary"] or len(coverage) != 128 or duplicates or missing or extra:
        mismatches.append({
            "error": "offset coverage mismatch",
            "roles": roles,
            "rows": len(coverage),
            "duplicates": duplicates,
            "missing": missing,
            "extra": extra,
        })
    if set(actual) != expected:
        mismatches.append({"error": "final content records mismatch", "missing": sorted(expected - set(actual)), "extra": sorted(set(actual) - expected)})
    return {"status": "PASS" if not mismatches else "FAIL", "mismatches": mismatches, "children": parsed_children}


def append_offset_status(offsets: list[dict[str, Any]]) -> dict[str, Any]:
    failures = [row for row in offsets if row.get("offset") != row.get("expected_eof")]
    status = "FAIL" if failures else ("PASS" if len(offsets) == 4 else "PARTIAL")
    return {"status": status, "failures": failures, "offsets": offsets}


def find_command_artifacts(control_dir: Path, before: set[Path]) -> list[dict[str, Any]]:
    rows = []
    after = set(control_dir.glob("command-*.*"))
    stems = sorted({control_dir / path.name.split(".", 1)[0] for path in after - before})
    for stem in stems:
        row: dict[str, Any] = {"stem": str(stem)}
        for suffix in ("command.json", "exit.json", "stdout", "stderr"):
            path = stem.with_suffix("." + suffix)
            if not path.exists():
                continue
            entry: dict[str, Any] = {"path": str(path), "sha256": sha256_file(path), "bytes": path.stat().st_size}
            if suffix in ("stdout", "stderr", "command.json", "exit.json"):
                entry["text"] = read_text(path)
            row[suffix.replace(".", "_")] = entry
        rows.append(row)
    return rows


def run_command(argv: list[str], timeout: float, out_dir: Path, name: str, allowed: tuple[int, ...] = (0,)) -> dict[str, Any]:
    started = time.time()
    try:
        proc = subprocess.run(argv, text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout, check=False)
        record = {
            "argv": argv,
            "returncode": proc.returncode,
            "timed_out": False,
            "timeout_seconds": timeout,
            "duration_ms": round((time.time() - started) * 1000, 3),
            "stdout": proc.stdout,
            "stderr": proc.stderr,
        }
    except subprocess.TimeoutExpired as exc:
        stdout = exc.stdout if isinstance(exc.stdout, str) else (exc.stdout or b"").decode("utf-8", "replace")
        stderr = exc.stderr if isinstance(exc.stderr, str) else (exc.stderr or b"").decode("utf-8", "replace")
        record = {
            "argv": argv,
            "returncode": None,
            "timed_out": True,
            "timeout_seconds": timeout,
            "duration_ms": round((time.time() - started) * 1000, 3),
            "stdout": stdout,
            "stderr": stderr,
        }
    json_write(out_dir / f"{name}.command.json", record)
    (out_dir / f"{name}.stdout").write_text(str(record["stdout"]), encoding="utf-8")
    (out_dir / f"{name}.stderr").write_text(str(record["stderr"]), encoding="utf-8")
    if record["timed_out"] or record["returncode"] not in allowed:
        raise RuntimeError(json.dumps({"command": argv, "returncode": record["returncode"], "timed_out": record["timed_out"], "stderr": str(record["stderr"])[-2000:]}, sort_keys=True))
    return record


class Inotify:
    def __init__(self) -> None:
        self.libc = ctypes.CDLL(None, use_errno=True)
        self.fd = self.libc.inotify_init1(IN_NONBLOCK)
        if self.fd < 0:
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err))
        self.watches: dict[int, str] = {}

    def add(self, name: str, path: Path) -> None:
        wd = self.libc.inotify_add_watch(self.fd, os.fsencode(path), IN_ALL_NEEDED)
        if wd < 0:
            err = ctypes.get_errno()
            raise OSError(err, os.strerror(err), str(path))
        self.watches[int(wd)] = name

    def collect(self, seconds: float) -> list[dict[str, Any]]:
        deadline = time.time() + seconds
        events: list[dict[str, Any]] = []
        while time.time() < deadline:
            try:
                data = os.read(self.fd, 65536)
            except BlockingIOError:
                time.sleep(0.02)
                continue
            offset = 0
            while offset + 16 <= len(data):
                wd, mask, cookie, name_len = struct.unpack_from("iIII", data, offset)
                offset += 16
                raw_name = data[offset : offset + name_len]
                offset += name_len
                name = raw_name.rstrip(b"\0").decode("utf-8", "replace")
                events.append({
                    "watch": self.watches.get(wd, str(wd)),
                    "mask": mask,
                    "mask_names": mask_names(mask),
                    "cookie": cookie,
                    "name": name,
                })
        return events

    def close(self) -> None:
        os.close(self.fd)


def mask_names(mask: int) -> list[str]:
    names = []
    for bit, name in (
        (IN_CREATE, "CREATE"),
        (IN_MODIFY, "MODIFY"),
        (IN_CLOSE_WRITE, "CLOSE_WRITE"),
        (IN_MOVED_FROM, "MOVED_FROM"),
        (IN_MOVED_TO, "MOVED_TO"),
        (IN_DELETE, "DELETE"),
    ):
        if mask & bit:
            names.append(name)
    return names


class Probe:
    def __init__(self, args: argparse.Namespace) -> None:
        self.args = args
        self.primary = absolute_path(args.primary)
        self.secondary = absolute_path(args.secondary)
        self.out = absolute_path(args.out)
        self.run_id = f"native-mixed-{os.getpid()}-{time.time_ns()}"
        self.prefix = f".afs-native-mixed-{os.getpid()}-{time.time_ns()}"
        self.created: list[Path] = []
        self.steps: list[dict[str, Any]] = []
        self.failures: list[dict[str, Any]] = []

    def owned(self, root: Path, suffix: str) -> Path:
        return root / f"{self.prefix}-{suffix}"

    def remember(self, *paths: Path) -> None:
        self.created.extend(paths)

    def cleanup(self) -> dict[str, Any]:
        rows = []
        for path in sorted(set(self.created), key=lambda p: len(str(p)), reverse=True):
            row: dict[str, Any] = {"path": str(path)}
            try:
                if path.is_dir():
                    shutil.rmtree(path)
                else:
                    path.unlink()
                row["status"] = "removed"
            except FileNotFoundError:
                row["status"] = "absent"
            except OSError as exc:
                row["status"] = "error"
                row["error"] = errno_record(exc)
            rows.append(row)
        return {"owned_paths": rows}

    def step(self, name: str, func: Callable[[Path], dict[str, Any]]) -> None:
        step_dir = self.out / name
        step_dir.mkdir(parents=True, exist_ok=True)
        started = time.time()
        record: dict[str, Any] = {"name": name, "status": "FAIL", "started_at": now(), "out": str(step_dir)}
        try:
            details = func(step_dir)
            record.update({"status": "PASS", "details": details})
        except Exception as exc:  # noqa: BLE001 - serialize probe failures
            record.update({
                "status": "FAIL",
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            })
            self.failures.append(record)
        finally:
            record["duration_ms"] = round((time.time() - started) * 1000, 3)
            json_write(step_dir / "step.json", record)
            self.steps.append(record)

    def locks_probe(self, step_dir: Path) -> dict[str, Any]:
        script = (self.args.locks_probe or Path(__file__).with_name("locks_smoke.py")).resolve()
        json_write(step_dir / "locks-tool.json", {"path": str(script), "sha256": sha256_file(script)})
        primary_file = self.owned(self.primary, "locks.bin")
        secondary_file = self.owned(self.secondary, "locks.bin")
        self.remember(primary_file, secondary_file)
        argv = [
            sys.executable,
            str(script),
            "--path",
            str(primary_file),
            "--second-path",
            str(secondary_file),
            "--evidence",
            str(step_dir / "locks-smoke"),
            "--child-timeout",
            "5",
        ]
        command = run_command(argv, 45, step_dir, "locks-smoke", allowed=(0, 1))
        report_path = step_dir / "locks-smoke" / "report.json"
        expect(report_path.exists(), "locks_smoke report exists", str(report_path))
        report = json.loads(report_path.read_text(encoding="utf-8"))
        expect(command["returncode"] == 0 and report.get("summary", {}).get("status") == "PASS", "locks_smoke passes", report.get("summary"))
        return {
            "tool": str(script),
            "tool_sha256": sha256_file(script),
            "command": command,
            "report_path": str(report_path),
            "report_sha256": sha256_file(report_path),
            "summary": report.get("summary"),
            "primary_path": str(primary_file),
            "secondary_path": str(secondary_file),
        }

    def append_probe(self, step_dir: Path) -> dict[str, Any]:
        primary_file = self.owned(self.primary, "append.log")
        secondary_file = self.owned(self.secondary, "append.log")
        concurrent_primary = self.owned(self.primary, "append-concurrent.log")
        concurrent_secondary = self.owned(self.secondary, "append-concurrent.log")
        start_primary = self.owned(self.primary, "append-start.flag")
        start_secondary = self.owned(self.secondary, "append-start.flag")
        self.remember(primary_file, secondary_file, concurrent_primary, concurrent_secondary, start_primary, start_secondary)
        fd_init = os.open(primary_file, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
        os.close(fd_init)
        pfd = os.open(primary_file, os.O_WRONLY | os.O_APPEND)
        sfd = os.open(secondary_file, os.O_WRONLY | os.O_APPEND)
        records = [
            ("primary-1", b"primary-one\n", pfd),
            ("secondary-1", b"secondary-one\n", sfd),
            ("primary-2", b"primary-two\n", pfd),
            ("secondary-2", b"secondary-two\n", sfd),
        ]
        proof: dict[str, Any] = {
            "offsets": {"status": "NOT_RUN", "failures": [], "offsets": []},
            "sequential_data": {"status": "NOT_RUN"},
            "concurrent_data": {"status": "NOT_RUN"},
        }
        offsets: list[dict[str, Any]] = []
        expected = b""
        try:
            try:
                for label, payload, fd in records:
                    written = os.write(fd, payload)
                    expect(written == len(payload), "complete append write", {"label": label, "written": written})
                    expected += payload
                    row = {"label": label, "offset": os.lseek(fd, 0, os.SEEK_CUR), "expected_eof": len(expected)}
                    offsets.append(row)
                    proof["offsets"] = append_offset_status(offsets)
                    json_write(step_dir / "append-offsets.json", proof["offsets"])
                os.fsync(pfd)
                os.fsync(sfd)
            finally:
                os.close(pfd)
                os.close(sfd)
            primary_content = primary_file.read_bytes()
            secondary_content = secondary_file.read_bytes()
            proof["sequential_data"] = {
                "status": "PASS" if primary_content == expected and secondary_content == expected else "FAIL",
                "expected_sha256": hashlib.sha256(expected).hexdigest(),
                "expected_bytes": len(expected),
                "primary_sha256": hashlib.sha256(primary_content).hexdigest(),
                "secondary_sha256": hashlib.sha256(secondary_content).hexdigest(),
                "primary_bytes": len(primary_content),
                "secondary_bytes": len(secondary_content),
                "primary": stat_record(primary_file),
                "secondary": stat_record(secondary_file),
            }
            fd_init = os.open(concurrent_primary, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            os.close(fd_init)
            child_code = (
                "import json, os, sys, time\n"
                "target, role, start = sys.argv[1:4]\n"
                "fd = os.open(target, os.O_WRONLY | os.O_APPEND)\n"
                "try:\n"
                "    deadline = time.monotonic() + 5.0\n"
                "    while not os.path.exists(start):\n"
                "        if time.monotonic() >= deadline:\n"
                "            raise TimeoutError('append start flag not observed')\n"
                "        time.sleep(0.005)\n"
                "    offsets = []\n"
                "    for i in range(64):\n"
                "        line = f'{role}:{i:02d}:native-mixed-append'\n"
                "        payload = (line + '\\n').encode()\n"
                "        n = os.write(fd, payload)\n"
                "        if n != len(payload):\n"
                "            raise RuntimeError(f'short write {n}/{len(payload)}')\n"
                "        offsets.append({'line': line, 'offset': os.lseek(fd, 0, os.SEEK_CUR)})\n"
                "    os.fsync(fd)\n"
                "    print(json.dumps({'role': role, 'records': 64, 'offsets': offsets}))\n"
                "finally:\n"
                "    os.close(fd)\n"
            )
            children = [
                subprocess.Popen([sys.executable, "-c", child_code, str(concurrent_primary), "primary", str(start_primary)], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE),
                subprocess.Popen([sys.executable, "-c", child_code, str(concurrent_secondary), "secondary", str(start_secondary)], text=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE),
            ]
            fd_start = os.open(start_primary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            os.close(fd_start)
            child_records = []
            for index, child in enumerate(children):
                try:
                    stdout, stderr = child.communicate(timeout=5)
                except subprocess.TimeoutExpired:
                    child.kill()
                    stdout, stderr = child.communicate(timeout=2)
                    child_records.append({"index": index, "returncode": child.returncode, "timed_out": True, "stdout": stdout, "stderr": stderr})
                    continue
                child_records.append({"index": index, "returncode": child.returncode, "timed_out": False, "stdout": stdout, "stderr": stderr})
            json_write(step_dir / "append-concurrent-children.json", child_records)
            if all(row["returncode"] == 0 and not row["timed_out"] for row in child_records):
                content = concurrent_primary.read_bytes()
                lines = content.decode("utf-8").splitlines()
                mirror_lines = concurrent_secondary.read_text(encoding="utf-8").splitlines()
                proof["concurrent_data"] = concurrent_append_status(lines, mirror_lines)
                proof["concurrent_data"].update({
                    "offset_correlation": correlate_concurrent_offsets(content, child_records),
                    "primary": stat_record(concurrent_primary),
                    "secondary": stat_record(concurrent_secondary),
                    "content_sha256": sha256_file(concurrent_primary),
                    "children": child_records,
                })
            else:
                proof["concurrent_data"] = {"status": "FAIL", "children": child_records}
            json_write(step_dir / "append-proof.json", proof)
            sections = [proof["offsets"], proof["sequential_data"], proof["concurrent_data"]]
            if proof["concurrent_data"].get("offset_correlation", {}).get("status") != "PASS":
                sections.append({"status": "FAIL", "name": "concurrent_offset_correlation"})
            if any(section["status"] != "PASS" for section in sections):
                raise AssertionError(json.dumps(proof, sort_keys=True))
            return proof
        except Exception as exc:
            proof["failure"] = {"exception": type(exc).__name__, "message": str(exc)}
            json_write(step_dir / "append-proof.json", proof)
            raise

    def mmap_inotify_probe(self, step_dir: Path) -> dict[str, Any]:
        primary_file = self.owned(self.primary, "mmap.bin")
        secondary_file = self.owned(self.secondary, "mmap.bin")
        self.remember(primary_file, secondary_file)
        primary_watch_file = self.owned(self.primary, "watch-primary.tmp")
        primary_watch_renamed = self.owned(self.primary, "watch-primary.done")
        secondary_watch_file = self.owned(self.secondary, "watch-secondary.tmp")
        secondary_watch_renamed = self.owned(self.secondary, "watch-secondary.done")
        self.remember(primary_watch_file, primary_watch_renamed, secondary_watch_file, secondary_watch_renamed)

        watcher = Inotify()
        try:
            same_watch_dir = self.primary.stat().st_dev == self.secondary.stat().st_dev and self.primary.stat().st_ino == self.secondary.stat().st_ino
            watcher.add("primary", self.primary)
            if not same_watch_dir:
                watcher.add("secondary", self.secondary)
            fd = os.open(primary_file, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
            try:
                os.write(fd, b"\0" * 4096)
                os.fsync(fd)
            finally:
                os.close(fd)

            nfd = os.open(secondary_file, os.O_RDWR)
            try:
                with mmap.mmap(nfd, 4096, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ | mmap.PROT_WRITE) as view:
                    native_payload = b"NATIVE-MAP-SHARED"
                    view[128 : 128 + len(native_payload)] = native_payload
                    view.flush()
                    os.fsync(nfd)
            finally:
                os.close(nfd)
            expect(primary_file.read_bytes()[128 : 128 + len(native_payload)] == native_payload, "FUSE read sees native MAP_SHARED write")

            nfd = os.open(secondary_file, os.O_RDWR)
            try:
                with mmap.mmap(nfd, 4096, flags=mmap.MAP_SHARED, prot=mmap.PROT_READ) as view:
                    fuse_payload = b"FUSE-WRITE-SEEN-BY-MAP"
                    pfd = os.open(primary_file, os.O_WRONLY)
                    try:
                        os.pwrite(pfd, fuse_payload, 512)
                        os.fsync(pfd)
                    finally:
                        os.close(pfd)
                    expect(view[512 : 512 + len(fuse_payload)] == fuse_payload, "native MAP_SHARED read sees FUSE write")
            finally:
                os.close(nfd)
            json_write(step_dir / "mmap-proof.json", {"status": "PASS", "bytes": 4096,
                "native_map_to_primary_read": native_payload.decode(),
                "primary_write_to_native_map": fuse_payload.decode()})

            self.create_write_rename(primary_watch_file, primary_watch_renamed, b"primary-watch\n")
            self.create_write_rename(secondary_watch_file, secondary_watch_renamed, b"secondary-watch\n")
            events = watcher.collect(2.0)
        finally:
            watcher.close()

        json_write(step_dir / "inotify-events.json", events)
        by_watch: dict[str, list[dict[str, Any]]] = {"primary": [], "secondary": []}
        for event in events:
            if event.get("watch") in by_watch:
                by_watch[event["watch"]].append(event)
        if same_watch_dir:
            by_watch["secondary"] = list(by_watch["primary"])
        for watch_name, watch_events in by_watch.items():
            # Each view must observe both origins, including the other path.
            for path, renamed in ((primary_watch_file, primary_watch_renamed),
                                  (secondary_watch_file, secondary_watch_renamed)):
                self.expect_watch_events(watch_name, watch_events, path.name, renamed.name)
        return {
            "primary": stat_record(primary_file),
            "secondary": stat_record(secondary_file),
            "events": {"primary": by_watch["primary"], "secondary": by_watch["secondary"]},
            "event_count": len(events),
            "same_watch_dir": same_watch_dir,
        }

    def expect_watch_events(self, watch_name: str, events: list[dict[str, Any]], tmp_name: str, done_name: str) -> None:
        def has(name: str, mask: str) -> bool:
            return any(event.get("name") == name and mask in event.get("mask_names", []) for event in events)

        expect(has(tmp_name, "CREATE"), f"{watch_name} watch observes CREATE for {tmp_name}", events)
        expect(has(tmp_name, "MODIFY") or has(tmp_name, "CLOSE_WRITE"), f"{watch_name} watch observes write for {tmp_name}", events)
        expect(has(tmp_name, "MOVED_FROM"), f"{watch_name} watch observes MOVED_FROM for {tmp_name}", events)
        expect(has(done_name, "MOVED_TO"), f"{watch_name} watch observes MOVED_TO for {done_name}", events)

    def create_write_rename(self, path: Path, renamed: Path, payload: bytes) -> None:
        fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(fd, payload)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.rename(path, renamed)

    def permissions_errno_probe(self, step_dir: Path) -> dict[str, Any]:
        control_dir = self.args.socket.resolve().parent
        root_file = self.owned(self.primary, "root-owned-0600.txt")
        secondary_root_file = self.owned(self.secondary, "root-owned-0600.txt")
        missing_primary = self.owned(self.primary, "missing.txt")
        missing_secondary = self.owned(self.secondary, "missing.txt")
        exist_primary = self.owned(self.primary, "exclusive.txt")
        exist_secondary = self.owned(self.secondary, "exclusive.txt")
        self.remember(root_file, secondary_root_file, exist_primary, exist_secondary)

        secret = b"root-secret-native-mixed\n"
        fd = os.open(root_file, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        try:
            os.write(fd, secret)
            os.fsync(fd)
        finally:
            os.close(fd)
        os.chown(root_file, 0, 0)
        os.chmod(root_file, 0o600)

        before = set(control_dir.glob("command-*.*"))
        script = (
            "set +e\n"
            "uid=$(/bin/busybox id -u)\n"
            "/bin/busybox echo uid=$uid\n"
            "test \"$uid\" = 501 || exit 7\n"
            f"/bin/busybox cat /workspace/{root_file.name}\n"
            "read_rc=$?\n"
            f"/bin/busybox sh -c 'printf blocked >> /workspace/{root_file.name}'\n"
            "write_rc=$?\n"
            "/bin/busybox echo read_rc=$read_rc\n"
            "/bin/busybox echo write_rc=$write_rc\n"
            "test \"$read_rc\" != 0 && test \"$write_rc\" != 0\n"
        )
        argv = [
            sys.executable,
            str(self.args.controller),
            "--socket",
            str(self.args.socket),
            "--id",
            f"{self.run_id}-perm",
            "exec",
            "--",
            "/bin/sh",
            "-ec",
            script,
        ]
        command = run_command(argv, 20, step_dir, "container-permission")
        artifacts = find_command_artifacts(control_dir, before)
        stdout_text = "\n".join(str(item.get("stdout", {}).get("text", "")) for item in artifacts)
        stderr_text = "\n".join(str(item.get("stderr", {}).get("text", "")) for item in artifacts)
        expect(command["returncode"] == 0, "control exec command succeeds", command)
        response = json.loads(command["stdout"])["response"]
        expect(response.get("status") == "Executed", "container permission script executed", response)
        expect("uid=501" in stdout_text, "container command runs as UID 501", stdout_text)
        expect(root_file.read_bytes() == secret, "root-owned file content unchanged")
        expect(hashlib.sha256(root_file.read_bytes()).hexdigest() == hashlib.sha256(secret).hexdigest(), "root-owned file content hash unchanged")
        expect(root_file.stat().st_uid == 0 and root_file.stat().st_gid == 0 and (root_file.stat().st_mode & 0o777) == 0o600, "root-owned mode unchanged", stat_record(root_file))
        expect((stdout_text + stderr_text).count("Permission denied") >= 2, "raw container output records read and write Permission denied", {"stdout": stdout_text, "stderr": stderr_text})

        missing = {}
        for label, path in (("primary", missing_primary), ("secondary", missing_secondary)):
            try:
                os.open(path, os.O_RDONLY)
                raise AssertionError(f"{label} missing open unexpectedly succeeded")
            except OSError as exc:
                missing[label] = errno_record(exc)
                expect(exc.errno == errno.ENOENT, f"{label} missing path returns ENOENT", missing[label])

        fd = os.open(exist_primary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        os.close(fd)
        try:
            os.open(exist_secondary, os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
            raise AssertionError("secondary O_EXCL unexpectedly succeeded")
        except OSError as exc:
            exist = errno_record(exc)
            expect(exc.errno == errno.EEXIST, "O_CREAT|O_EXCL through secondary returns EEXIST", exist)

        json_write(step_dir / "control-artifacts.json", artifacts)
        return {
            "parent": stat_record(self.primary),
            "root_file": stat_record(root_file),
            "root_file_sha256": sha256_file(root_file),
            "expected_sha256": hashlib.sha256(secret).hexdigest(),
            "container_response": response,
            "control_artifacts": artifacts,
            "missing": missing,
            "exclusive": exist,
        }

    def run(self) -> dict[str, Any]:
        self.out.mkdir(parents=True, exist_ok=False)
        result: dict[str, Any] = {
            "schema": "afs.native_mixed.v1",
            "status": "FAIL",
            "created_at": now(),
            "run_id": self.run_id,
            "scope": "short managed OwnerFs mixed FUSE/native probe; not full POSIX or production ON qualification",
            "tool": {"path": str(Path(__file__).resolve()), "sha256": sha256_file(Path(__file__).resolve())},
            "platform": {
                "system": platform.system(),
                "release": platform.release(),
                "machine": platform.machine(),
                "python": platform.python_version(),
                "uid": os.geteuid(),
                "gid": os.getegid(),
            },
            "paths": {"primary": str(self.primary), "secondary": str(self.secondary)},
            "selected_groups": self.args.groups,
        }
        try:
            expect(platform.system() == "Linux", "native_mixed runs only on Linux")
            expect(self.primary.is_dir(), "--primary is a directory", str(self.primary))
            expect(self.secondary.is_dir(), "--secondary is a directory", str(self.secondary))
            result["path_identity"] = {"primary": stat_record(self.primary), "secondary": stat_record(self.secondary)}
            for name, probe in (("locks", self.locks_probe), ("append", self.append_probe),
                                ("mmap_inotify", self.mmap_inotify_probe),
                                ("permissions_errno", self.permissions_errno_probe)):
                if name in self.args.groups:
                    self.step(name, probe)
        except Exception as exc:  # noqa: BLE001
            failure = {
                "name": "setup_or_runner",
                "status": "FAIL",
                "exception": type(exc).__name__,
                "message": str(exc),
                "traceback": traceback.format_exc(),
            }
            self.failures.append(failure)
            self.steps.append(failure)
        finally:
            cleanup = self.cleanup()
            cleanup_errors = [row for row in cleanup.get("owned_paths", []) if row.get("status") == "error"]
            if cleanup_errors:
                failure = {"name": "cleanup", "status": "FAIL", "errors": cleanup_errors}
                self.failures.append(failure)
                self.steps.append(failure)
            result["cleanup"] = cleanup
            result["steps"] = self.steps
            result["failures"] = self.failures
            result["summary"] = {
                "total": len(self.steps),
                "passed": sum(1 for step in self.steps if step.get("status") == "PASS"),
                "failed": sum(1 for step in self.steps if step.get("status") != "PASS"),
            }
            result["status"] = "PASS" if self.steps and not self.failures and all(step.get("status") == "PASS" for step in self.steps) else "FAIL"
            json_write(self.out / "result.json", result)
        return result


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--primary", type=Path, required=True, help="host FUSE workspace path")
    parser.add_argument("--secondary", type=Path, required=True, help="native container workspace path")
    parser.add_argument("--controller", type=Path, required=True, help="native workspace control CLI")
    parser.add_argument("--socket", type=Path, required=True, help="native workspace control socket")
    parser.add_argument("--out", type=Path, required=True, help="new output directory")
    parser.add_argument("--locks-probe", type=Path, help="optional fixed locks_smoke.py path")
    parser.add_argument("--groups", nargs="+", choices=("locks", "append", "mmap_inotify", "permissions_errno"),
                        default=["locks", "append", "mmap_inotify", "permissions_errno"],
                        help="explicit affected groups; unselected groups are not tested")
    return parser


def main(argv: list[str]) -> int:
    args = build_parser().parse_args(argv)
    result = Probe(args).run()
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") == "PASS" else 1


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
