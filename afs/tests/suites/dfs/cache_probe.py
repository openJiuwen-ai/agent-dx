"""Linux-only cache observations; zero new FUSE READ remains a hard contract."""

import ctypes
import errno
import hashlib
import json
import mmap
import os
from pathlib import Path
import sys
import tempfile
import time
import urllib.request

SIZE = 16 * 1024**2
PAGE = mmap.PAGESIZE


def integer(value):
    return isinstance(value, int) and not isinstance(value, bool)


def parse_cache_open(line):
    if "dfs.fuse_cache_open" not in line:
        return None
    event = json.loads(line)
    if event.get("msg") != "dfs.fuse_cache_open":
        return None
    assert all(isinstance(event.get(k), bool) for k in ("cached", "keep_cache", "versioned_read")), event
    assert all(integer(event.get(k)) and event[k] > 0 for k in ("ino", "fh")), event
    return event


def assert_open(event, cached, keep=None):
    assert event["versioned_read"] is True and event["cached"] is cached, event
    if keep is not None:
        assert event["keep_cache"] is keep, event


def assert_resident(value):
    assert all(integer(value.get(k)) for k in ("pages", "resident", "page_bytes", "size_bytes")), value
    assert value["page_bytes"] > 0 and value["size_bytes"] > 0, value
    pages = (value["size_bytes"] + value["page_bytes"] - 1) // value["page_bytes"]
    vector = bytes.fromhex(value["vector_hex"])
    assert value["pages"] == pages == len(vector) > 0, value
    assert value["resident"] == sum(v & 1 for v in vector) == pages, value


def assert_cached_reuse(evidence):
    assert_open(evidence["warm_open"], True)
    assert_open(evidence["reopen"], True, True)
    for stage in ("before_open", "after_open", "after_read"):
        assert_resident(evidence[stage])
    before, after = evidence["read_callbacks_before"], evidence["read_callbacks_after"]
    assert integer(before) and integer(after) and before >= 0, evidence
    assert after == before, ("same-version cached reopen added FUSE READ", before, after)


def assert_conflict_direct(evidence):
    assert_open(evidence["open"], False, False)
    before, after = evidence["read_callbacks_before"], evidence["read_callbacks_after"]
    assert integer(before) and integer(after) and 0 <= before < after, evidence


def residency(fd, size):
    """mincore observes owned pages without faulting the mapping or warming it."""
    assert os.fstat(fd).st_uid == os.geteuid(), "mincore requires an owned test file"
    region = mmap.mmap(fd, size, access=mmap.ACCESS_COPY)
    anchor = ctypes.c_char.from_buffer(region)
    try:
        count = (size + PAGE - 1) // PAGE
        vector = (ctypes.c_ubyte * count)()
        libc = ctypes.CDLL(None, use_errno=True)
        libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.POINTER(ctypes.c_ubyte)]
        libc.mincore.restype = ctypes.c_int
        result = libc.mincore(ctypes.addressof(anchor), size, vector)
        if result != 0:
            error = ctypes.get_errno()
            raise OSError(error, os.strerror(error))
        return {"size_bytes": size, "page_bytes": PAGE, "pages": count,
                "resident": sum(v & 1 for v in vector), "vector_hex": bytes(vector).hex(),
                "method": "mincore on owned MAP_PRIVATE; mapping not touched"}
    finally:
        del anchor
        region.close()


def emit(stage, value):
    print(json.dumps({"stage": stage, "value": value}), flush=True)
    return value


def memory_context():
    wanted = ("MemAvailable:", "Cached:", "SwapFree:")
    return {line.split(":", 1)[0]: line.split(":", 1)[1].strip()
            for line in Path("/proc/meminfo").read_text().splitlines() if line.startswith(wanted)}


class Probe:
    def __init__(self, writer, reader, metrics, log):
        self.writer, self.reader = Path(writer), Path(reader)
        self.metrics, self.log = metrics, Path(log)
        self.http = urllib.request.build_opener(urllib.request.ProxyHandler({}))
        self.log_offset = self.log.stat().st_size
        self.events = []

    def counter(self):
        with self.http.open(self.metrics, timeout=3) as response:
            body = response.read(2 * 1024**2 + 1)
        assert len(body) <= 2 * 1024**2
        values = [int(line.rsplit(" ", 1)[1]) for line in body.decode().splitlines()
                  if line.startswith("afs_fuse_callbacks_total{")
                  and 'filesystem="dfs"' in line and 'operation="read"' in line]
        assert len(values) == 1, values
        return values[0]

    def opened(self, path, label):
        fd = os.open(path, os.O_RDONLY)
        try:
            inode = os.fstat(fd).st_ino
            deadline = time.monotonic() + 10
            while True:
                with self.log.open("rb") as stream:
                    stream.seek(self.log_offset)
                    raw = stream.read(8 * 1024**2 + 1)
                assert len(raw) <= 8 * 1024**2, "cache observation log overflow"
                complete = raw.rsplit(b"\n", 1)[0] if b"\n" in raw else b""
                if complete:
                    self.log_offset += len(complete) + 1
                    for line in complete.decode().splitlines():
                        event = parse_cache_open(line)
                        if event is not None:
                            self.events.append(event)
                matching = [e for e in self.events if e["ino"] == inode]
                if matching:
                    event = matching[0]
                    self.events.remove(event)
                    event = dict(event, requested_os_flags="O_RDONLY (no O_DIRECT)", label=label)
                    emit(label + ".open", event)
                    return fd, event
                assert time.monotonic() < deadline, ("missing actual cache-open event", label, inode)
                time.sleep(0.02)
        except BaseException:
            os.close(fd)
            raise

    @staticmethod
    def durable(path, body, create):
        flags = os.O_WRONLY | (os.O_CREAT | os.O_EXCL if create else 0)
        fd = os.open(path, flags, 0o600)
        try:
            for offset in range(0, len(body), 1024**2):
                part = body[offset:offset + 1024**2]
                assert os.pwrite(fd, part, offset) == len(part)
            os.fsync(fd)
        finally:
            os.close(fd)

    @staticmethod
    def full(fd, body):
        digest = hashlib.sha256()
        offset = 0
        while offset < len(body):
            part = os.pread(fd, min(1024**2, len(body) - offset), offset)
            assert part == body[offset:offset + len(part)] and part, ("bad body", offset, len(part))
            digest.update(part)
            offset += len(part)
        assert os.pread(fd, 1, offset) == b""
        assert digest.hexdigest() == hashlib.sha256(body).hexdigest()
        return {"bytes": offset, "sha256": digest.hexdigest(), "eof": True}

    def conflict(self, a, b, body):
        self.durable(a, body, True)
        inode = a.stat().st_ino
        first, old = self.opened(b, "H1.V1")
        second = third = None
        try:
            assert_open(old, True)
            assert os.pread(first, 4096, 0) == body[:4096]
            emit("V1.first_page", {"bytes": 4096, "sha256": hashlib.sha256(body).hexdigest()})
            newer = body.translate(bytes(255 - i for i in range(256)))
            self.durable(a, newer, False)
            assert a.stat().st_ino == inode
            second, event = self.opened(b, "H2.V2.conflict")
            before = self.counter()
            checked = self.full(second, newer)
            evidence = emit("H2.conflict", {"open": event, "read_callbacks_before": before,
                                           "read_callbacks_after": self.counter(), "content": checked})
            assert_conflict_direct(evidence)
            # Legacy (open-authority=false) path retains its existing uncached-tail behavior.
            tail = 12 * 1024**2
            assert os.pread(first, 4096, tail) == newer[tail:tail + 4096]
            emit("H1.legacy_tail", {"bytes": 4096, "matches_V2": True, "snapshot_promise_added": False})
            third, event = self.opened(b, "H3.V2.conflict")
            before = self.counter()
            checked = self.full(third, newer)
            evidence = emit("H3.conflict", {"open": event, "read_callbacks_before": before,
                                           "read_callbacks_after": self.counter(), "content": checked})
            assert_conflict_direct(evidence)
        finally:
            for fd in (third, second, first):
                if fd is not None:
                    os.close(fd)

    def reuse(self, a, b, body):
        self.durable(a, body, True)
        warm, warm_event = self.opened(b, "warm")
        try:
            assert_open(warm_event, True)
            emit("warm.content", self.full(warm, body))
            before_open = emit("warm.residency", residency(warm, len(body)))
            assert_resident(before_open)
            before_repeat = self.counter()
            emit("same_fd.content", self.full(warm, body))
            after_repeat = self.counter()
            emit("same_fd.reads", {"before": before_repeat, "after": after_repeat})
            assert after_repeat == before_repeat, (before_repeat, after_repeat)
        finally:
            os.close(warm)
        before = self.counter()
        reopened, event = self.opened(b, "reopen")
        try:
            after_open = emit("reopen.residency_before_read", residency(reopened, len(body)))
            evidence = {"warm_open": warm_event, "reopen": event, "before_open": before_open,
                        "after_open": after_open, "after_read": after_open,
                        "read_callbacks_before": before, "read_callbacks_after": self.counter()}
            # Catch loss at OPEN before a read can warm discarded pages again.
            emit("reopen.before_read", evidence)
            assert_cached_reuse(evidence)
            emit("reopen.content", self.full(reopened, body))
            evidence.update(after_read=emit("reopen.residency_after_read", residency(reopened, len(body))),
                            read_callbacks_after=self.counter())
            emit("reopen.verified", evidence)
            assert_cached_reuse(evidence)
        finally:
            os.close(reopened)

    def permission(self, a, b, body):
        self.durable(a, body, True)
        warm, event = self.opened(b, "permission.warm")
        try:
            assert_open(event, True)
            emit("permission.warm_content", self.full(warm, body))
            pages = emit("permission.residency", residency(warm, len(body)))
            assert_resident(pages)
            os.chmod(a, 0)
            denied = None
            try:
                fd = os.open(b, os.O_RDONLY)
            except OSError as error:
                denied = error.errno
            else:
                os.close(fd)
            finally:
                os.chmod(a, 0o600)
            emit("permission.fresh_open", {"errno": denied, "expected": errno.EACCES})
            assert denied == errno.EACCES, denied
        finally:
            os.close(warm)
        restored, _ = self.opened(b, "permission.restored")
        try:
            emit("permission.restored_content", self.full(restored, body))
        finally:
            os.close(restored)


def main():
    action, writer, reader, metrics, log = sys.argv[1:]
    probe = Probe(writer, reader, metrics, log)
    assert os.geteuid() != 0
    assert probe.writer.stat().st_dev != probe.reader.stat().st_dev
    emit("memory.before", memory_context())
    with tempfile.TemporaryDirectory(prefix="cache-contract-", dir=writer) as directory:
        a = Path(directory) / "data"
        b = Path(reader) / a.relative_to(writer)
        body = bytes(range(256)) * (SIZE // 256)
        {"conflict": probe.conflict, "reuse": probe.reuse, "permission": probe.permission}[action](a, b, body)
    emit("complete", {"action": action, "passed": True, "performance_measurement": False})


if __name__ == "__main__":
    main()
