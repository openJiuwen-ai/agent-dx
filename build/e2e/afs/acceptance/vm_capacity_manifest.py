#!/usr/bin/env python3
"""Read-only Linux volume identity for an offline-backed VM resize.

The full manifest stays outside Git. No image, filesystem or service is changed.
Compare two manifests before accepting restoration; sockets are recorded by type,
not read. A nested mount is an error rather than an omitted subtree.
"""
import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import stat


def entry(path):
    before = path.lstat()
    result = {"mode": stat.S_IMODE(before.st_mode), "uid": before.st_uid,
              "gid": before.st_gid, "type": stat.S_IFMT(before.st_mode),
              "mtime_ns": before.st_mtime_ns}
    result["xattrs"] = {name: os.getxattr(path, name, follow_symlinks=False).hex()
                        for name in sorted(os.listxattr(path, follow_symlinks=False))}
    if stat.S_ISREG(before.st_mode):
        digest = hashlib.sha256()
        fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
        with os.fdopen(fd, "rb") as stream:
            opened = os.fstat(stream.fileno())
            if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
                raise RuntimeError(f"file replaced before open: {path}")
            for block in iter(lambda: stream.read(1024 * 1024), b""):
                digest.update(block)
        result.update(size=before.st_size, sha256=digest.hexdigest())
    elif stat.S_ISLNK(before.st_mode):
        result["target"] = os.readlink(path)
    elif stat.S_ISBLK(before.st_mode) or stat.S_ISCHR(before.st_mode):
        result["rdev"] = before.st_rdev
    after = path.lstat()
    if (before.st_dev, before.st_ino, before.st_mode, before.st_size,
            before.st_mtime_ns, before.st_ctime_ns) != (
            after.st_dev, after.st_ino, after.st_mode, after.st_size,
            after.st_mtime_ns, after.st_ctime_ns):
        raise RuntimeError(f"file changed while recording: {path}")
    return result


def manifest(root):
    root = Path(root).resolve(strict=True)
    device = root.stat().st_dev
    records = {}
    pending = [root]
    while pending:
        path = pending.pop()
        identity = path.lstat()
        if identity.st_dev != device:
            raise RuntimeError(f"nested mount requires separate backup: {path}")
        records[str(path.relative_to(root))] = entry(path)
        if stat.S_ISDIR(identity.st_mode):
            pending.extend(sorted(path.iterdir(), reverse=True))
    return records


def compare(before, after):
    missing = sorted(before.keys() - after.keys())
    added = sorted(after.keys() - before.keys())
    changed = sorted(key for key in before.keys() & after.keys()
                     if before[key] != after[key])
    return {"status": "PASS" if not (missing or added or changed) else "FAIL",
            "entries": len(before), "missing": missing, "added": added,
            "changed": changed,
            "regular_bytes": sum(v.get("size", 0) for v in before.values())}


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument("--root", type=Path)
    group.add_argument("--compare", nargs=2, type=Path)
    args = parser.parse_args()
    if platform.system() != "Linux":
        parser.error("volume identity must be collected and checked on Linux")
    if args.root:
        print(json.dumps(manifest(args.root), sort_keys=True))
    else:
        result = compare(*(json.loads(p.read_text()) for p in args.compare))
        print(json.dumps(result, sort_keys=True))
        return 0 if result["status"] == "PASS" else 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
