"""Linux no-touch file-range residency observation; not a cache warmer.

Only the requested backing-file range is counted. Client/FUSE cache layers are
separate observations. mincore is a snapshot, not a pin or a durability proof.
"""
import ctypes
import mmap
import os
from pathlib import Path
import platform
import stat


def identity(info):
    return {"device": info.st_dev, "inode": info.st_ino, "size": info.st_size,
            "mtime_ns": info.st_mtime_ns, "ctime_ns": info.st_ctime_ns}


def observe(path, offset, length, expected=None):
    if platform.system() != "Linux":
        raise RuntimeError("physical cache observation requires Linux")
    if type(offset) is not int or type(length) is not int or offset < 0 or length <= 0:
        raise ValueError("invalid file range")
    path = Path(path)
    if not path.is_absolute():
        raise ValueError("absolute backing path required")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
    try:
        info = os.fstat(fd)
        before = identity(info)
        if not stat.S_ISREG(info.st_mode) or offset + length > info.st_size:
            raise ValueError("range must be inside a regular file")
        if expected is not None and before != expected:
            raise ValueError("backing file identity changed")
        page = os.sysconf("SC_PAGE_SIZE")
        aligned = offset // page * page
        mapped_length = offset + length - aligned
        pages = (mapped_length + page - 1) // page
        libc = ctypes.CDLL(None, use_errno=True)
        libc.mmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_int,
                              ctypes.c_int, ctypes.c_int, ctypes.c_long]
        libc.mmap.restype = ctypes.c_void_p
        libc.mincore.argtypes = [ctypes.c_void_p, ctypes.c_size_t, ctypes.c_void_p]
        libc.mincore.restype = ctypes.c_int
        libc.munmap.argtypes = [ctypes.c_void_p, ctypes.c_size_t]
        libc.munmap.restype = ctypes.c_int
        address = libc.mmap(None, mapped_length, mmap.PROT_READ, mmap.MAP_SHARED, fd, aligned)
        if address == ctypes.c_void_p(-1).value:
            raise OSError(ctypes.get_errno(), "residency mmap", str(path))
        try:
            vector = (ctypes.c_ubyte * pages)()
            if libc.mincore(address, mapped_length, vector):
                raise OSError(ctypes.get_errno(), "mincore", str(path))
            resident = sum(max(0, min(aligned + (i + 1) * page, offset + length)
                               - max(aligned + i * page, offset))
                           for i, bit in enumerate(vector) if bit & 1)
        finally:
            if libc.munmap(address, mapped_length):
                raise OSError(ctypes.get_errno(), "residency munmap", str(path))
        if identity(os.fstat(fd)) != before or identity(path.stat()) != before:
            raise ValueError("backing file changed during observation")
        return {"path": str(path), "identity": before, "offset": offset,
                "length": length, "page_size": page, "pages": pages,
                "resident_bytes": resident, "fully_resident": resident == length,
                "method": "read-only MAP_SHARED + mincore; no mapped payload access"}
    finally:
        os.close(fd)
