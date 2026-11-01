"""Shared discovery, bounded worker transport and identities for AFS integration."""

from __future__ import annotations

import base64
from collections import deque
from dataclasses import dataclass, replace
import fnmatch
import hashlib
import importlib.util
import json
from pathlib import Path
import queue
import subprocess
import threading
import time

HERE = Path(__file__).resolve().parent
TESTS = HERE.parent


class Blocked(RuntimeError):
    pass


def is_integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


@dataclass(frozen=True)
class DfsProfile:
    """Small, typed per-case product inputs; never arbitrary TOML or host policy."""

    copies: int = 1
    node_count: int = 2
    write_queue_bytes: int | None = None
    read_crc_mode: str | None = None
    readahead_max_bytes: int | None = None
    keep_cache: bool | None = None
    kernel_readahead_bytes: int | None = None
    native_open_authority: bool | None = None

    def __post_init__(self):
        if not is_integer(self.node_count) or self.node_count not in (2, 3):
            raise ValueError("DFS profile needs two or three Nodes")
        if not is_integer(self.copies) or not 1 <= self.copies <= self.node_count:
            raise ValueError("DFS copies must fit the declared Node count")
        for name, value, minimum, maximum in (
            ("write_queue_bytes", self.write_queue_bytes, 8 * 1024**2, 1024**4),
            ("readahead_max_bytes", self.readahead_max_bytes, 0, 32 * 1024**2),
            ("kernel_readahead_bytes", self.kernel_readahead_bytes, 0, 16 * 1024**2),
        ):
            if value is not None and (not is_integer(value) or not minimum <= value <= maximum):
                raise ValueError("invalid DFS profile " + name)
        if self.read_crc_mode not in (None, "off", "receiver", "both"):
            raise ValueError("invalid DFS read CRC mode")
        for name, value in (("keep_cache", self.keep_cache), ("native_open_authority", self.native_open_authority)):
            if value is not None and not isinstance(value, bool):
                raise ValueError(name + " must be a boolean")

    def meta_config(self):
        return (
            f"dfs_desired_copies = {self.copies}\ndfs_sync_required_copies = {self.copies}\n"
            f"dfs_min_distinct_nodes = {self.copies}\ndfs_min_distinct_failure_domains = 1\n"
            'dfs_local_copy = "required"\n'
        )

    def node_config(self):
        values = (
            ("dfs_write_queue_bytes", self.write_queue_bytes),
            ("dfs_read_crc_mode", self.read_crc_mode),
            ("dfs_readahead_max_bytes", self.readahead_max_bytes),
            ("dfs_fuse_keep_cache", self.keep_cache),
            ("fuse_max_readahead_bytes", self.kernel_readahead_bytes),
            ("dfs_native_open_read_authority", self.native_open_authority),
        )
        return "".join(f"{key} = {json.dumps(value)}\n" for key, value in values if value is not None)


@dataclass(frozen=True)
class Case:
    id: str
    tags: tuple[str, ...]
    mode: str
    groups: tuple[str, ...]
    function: object
    rust: bool = False
    profile: DfsProfile | None = None
    purpose: str = "functional"
    observe_cache: bool = False


def is_single_host(config):
    return config["kind"] == "singlevm" or (
        config["kind"] == "kubernetes" and config.get("topology") == "local"
    )


def needs_reader_c(case, kind, topology=None):
    return case.id.endswith(".many_readers") or (
        (kind == "singlevm" or (kind == "kubernetes" and topology == "local"))
        and case.mode == "dfs" and (case.profile is None or case.profile.node_count == 3)
    )


def performance_parameters(config):
    values = config.get("performance")
    if not isinstance(values, dict):
        raise Blocked("explicit performance input requires bytes and request_bytes; no implicit experiment")
    size, request = values.get("bytes"), values.get("request_bytes")
    if not is_integer(size) or not 4096 <= size <= 1024**3 or size % 4096:
        raise Blocked("performance bytes must be declared, 4KiB..1GiB and page aligned")
    if not is_integer(request) or not 4096 <= request <= 1024**2 or request % 4096:
        raise Blocked("performance request_bytes must be declared, 4KiB..1MiB and page aligned")
    return size, request


def discover():
    cases = []
    seen = set()
    for path in sorted((TESTS / "suites").rglob("test_*.py")):
        name = "afs_suite_" + "_".join(path.relative_to(TESTS).with_suffix("").parts)
        spec = importlib.util.spec_from_file_location(name, path)
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        for case in getattr(module, "CASES", ()):
            if case.id in seen:
                raise ValueError("duplicate case ID: " + case.id)
            if "local-basic" in case.tags or "cluster-manyread" in case.tags:
                case = replace(case, tags=case.tags + ("all-basic",))
            if any(tag in case.tags for tag in ("local-basic", "local-extended", "cluster-manyread")):
                case = replace(case, tags=case.tags + ("ci-full",))
            special_tags = (
                "local-write-special", "cluster-write-special", "local-read-special", "cluster-read-special",
            )
            if any(tag in case.tags for tag in special_tags):
                case = replace(case, tags=case.tags + ("all-special",))
            seen.add(case.id)
            cases.append(case)
    return cases


def select(cases, *, all_cases=False, suite=None, patterns=None):
    if sum(bool(x) for x in (all_cases, suite, patterns)) > 1:
        raise ValueError("choose --all, --suite or --case")
    if patterns:
        selected = set()
        for pattern in patterns:
            matches = {c.id for c in cases if fnmatch.fnmatchcase(c.id, pattern)}
            if not matches:
                raise ValueError("case selector matched zero cases: " + pattern)
            selected.update(matches)
        return [c for c in cases if c.id in selected]
    if all_cases:
        return list(cases)
    tag = suite or "local-basic"
    found = [c for c in cases if tag in c.tags]
    if not found:
        raise ValueError("unknown or empty suite: " + tag)
    return found


def sha256(path):
    h = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def write_json(path, value):
    path = Path(path)
    temporary = path.with_suffix(path.suffix + ".tmp")
    temporary.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n")
    temporary.replace(path)


class Channel:
    def __init__(self, argv, timeout=30):
        self.timeout = timeout
        self.proc = subprocess.Popen(
            argv, stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True, bufsize=1
        )
        self.replies = queue.Queue()
        self.errors = deque(maxlen=100)
        self.lock = threading.Lock()
        self.sequence = 0
        self.reader = threading.Thread(target=self._read, daemon=True)
        self.error_reader = threading.Thread(target=self._stderr, daemon=True)
        self.reader.start()
        self.error_reader.start()
        try:
            response = self._next(timeout)
            if not response.get("ready") or response.get("protocol") != 1:
                raise Blocked("worker did not complete protocol handshake")
        except Exception:
            try:
                self.close()
            except Exception:
                pass
            raise

    def _read(self):
        for line in self.proc.stdout:
            try:
                self.replies.put(json.loads(line))
            except ValueError:
                self.replies.put({"ok": False, "error": "invalid worker JSON: " + line[:200]})
        self.replies.put(None)

    def _stderr(self):
        for line in self.proc.stderr:
            self.errors.append(line)

    def _next(self, timeout):
        try:
            response = self.replies.get(timeout=timeout)
        except queue.Empty as exc:
            raise RuntimeError("worker response timed out; cleanup is unconfirmed") from exc
        if response is None:
            raise RuntimeError("worker disconnected: " + "".join(self.errors))
        return response

    def call(self, action, **data):
        with self.lock:
            self.sequence += 1
            identifier = self.sequence
            self.proc.stdin.write(json.dumps(dict(data, action=action, id=identifier)) + "\n")
            self.proc.stdin.flush()
            deadline = time.monotonic() + data.get("timeout", self.timeout) + 10
            while True:
                left = deadline - time.monotonic()
                if left <= 0:
                    raise RuntimeError("worker response timed out; cleanup is unconfirmed")
                response = self._next(left)
                if response.get("id") != identifier:
                    # A prior timed-out/interrupted command may finish during cleanup.
                    self.errors.append("late worker reply: " + repr(response.get("id")))
                    continue
                if not response.get("ok"):
                    raise RuntimeError(response.get("error", "worker failure"))
                return response["value"]

    def upload(self, local, remote, mode=0o600):
        append = False
        with Path(local).open("rb") as source:
            while True:
                chunk = source.read(512 * 1024)
                self.call("put", path=str(remote), data=base64.b64encode(chunk).decode(), append=append, mode=mode)
                append = True
                if not chunk:
                    break
        actual = self.call("hash", path=str(remote))["sha256"]
        if actual != sha256(local):
            raise RuntimeError("transferred input SHA mismatch: " + str(local))
        return actual

    def close(self):
        try:
            self.proc.stdin.close()
        except BrokenPipeError:
            pass
        try:
            code = self.proc.wait(timeout=40)
        except subprocess.TimeoutExpired as exc:
            raise RuntimeError("worker still running; preserving owned process") from exc
        self.reader.join(timeout=2)
        self.error_reader.join(timeout=2)
        self.proc.stdout.close()
        self.proc.stderr.close()
        if code:
            raise RuntimeError("worker exited nonzero: " + str(code))
