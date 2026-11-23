"""Page-cache control and evidence for benchmark rounds (runs on a role host, Linux only).

    bench_probe.py evict PATH...     fdatasync + POSIX_FADV_DONTNEED every regular file (recursively)
    bench_probe.py resident PATH...  measured resident bytes via mmap + mincore (like util-linux fincore)
    bench_probe.py list PATH...      regular files and their sizes only (no open, no page-cache effect)

Output is one JSON document. Eviction is advisory: callers must check `resident`
afterwards instead of trusting that eviction happened. Files that cannot be mapped
(for example FUSE direct-I/O handles) are reported as errors, never as zero.
"""

import ctypes
import json
import logging
import os
import sys

PAGE = os.sysconf("SC_PAGE_SIZE")
PROT_READ, MAP_SHARED = 0x1, 0x01
libc = ctypes.CDLL(None, use_errno=True)
libc.mmap.restype = ctypes.c_void_p
libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int, ctypes.c_int, ctypes.c_int, ctypes.c_long]
libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_char_p]
MAP_FAILED = ctypes.c_void_p(-1).value


def regular_files(paths):
    for path in paths:
        if os.path.isdir(path) and not os.path.islink(path):
            for parent, dirs, names in os.walk(path):
                # Never descend into another filesystem (for example a FUSE mount under test).
                dirs[:] = [d for d in dirs if not os.path.ismount(os.path.join(parent, d))]
                for name in sorted(names):
                    full = os.path.join(parent, name)
                    if os.path.isfile(full) and not os.path.islink(full):
                        yield full
        elif os.path.isfile(path):
            yield path


def evict(paths):
    files = size = 0
    errors = []
    for path in regular_files(paths):
        try:
            fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW, 0o600)
            try:
                os.fdatasync(fd)
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
                size += os.fstat(fd).st_size
                files += 1
            finally:
                os.close(fd)
        except OSError as error:
            errors.append({"path": path, "errno": error.errno, "error": error.strerror})
    return {"files": files, "bytes": size, "errors": errors}


def resident_one(path):
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW, 0o600)
    try:
        size = os.fstat(fd).st_size
        if size == 0:
            return {"path": path, "bytes": 0, "resident_bytes": 0}
        address = libc.mmap(None, size, PROT_READ, MAP_SHARED, fd, 0)
        if address in (None, MAP_FAILED):
            code = ctypes.get_errno()
            raise OSError(code, "mmap: " + os.strerror(code))
        try:
            pages = (size + PAGE - 1) // PAGE
            vector = ctypes.create_string_buffer(pages)
            if libc.mincore(address, size, vector) != 0:
                code = ctypes.get_errno()
                raise OSError(code, "mincore: " + os.strerror(code))
            pages_resident = sum(byte & 1 for byte in vector.raw)
        finally:
            libc.munmap(address, size)
        return {"path": path, "bytes": size, "resident_bytes": min(size, pages_resident * PAGE)}
    finally:
        os.close(fd)


def resident(paths):
    files, errors = [], []
    for path in regular_files(paths):
        try:
            files.append(resident_one(path))
        except OSError as error:
            errors.append({"path": path, "errno": error.errno, "error": error.strerror})
    return {"files": files, "bytes": sum(f["bytes"] for f in files),
            "resident_bytes": sum(f["resident_bytes"] for f in files), "errors": errors, "page_size": PAGE}


def listing(paths):
    files, errors = [], []
    for path in regular_files(paths):
        try:
            files.append({"path": path, "bytes": os.lstat(path).st_size})
        except OSError as error:
            errors.append({"path": path, "errno": error.errno, "error": error.strerror})
    return {"files": files, "errors": errors}


def main(argv):
    actions = {"evict": evict, "resident": resident, "list": listing}
    if len(argv) < 2 or argv[0] not in actions:
        raise SystemExit("usage: bench_probe.py evict|resident|list PATH...")
    result = actions[argv[0]](argv[1:])
    logging.basicConfig(level=logging.INFO, format="%(message)s", stream=sys.stdout)
    logging.info("%s", json.dumps(result))


if __name__ == "__main__":
    main(sys.argv[1:])
