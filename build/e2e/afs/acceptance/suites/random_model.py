#!/usr/bin/env python3
"""Small deterministic filesystem operation model for AFS STD-04 preparation.

This is a preparation smoke model. Full STD-04 still requires ext4 vs AFS
comparison for 10 fixed seeds x 10,000 operations and shrink corpus retention.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import random
import shutil
from pathlib import Path


OPS = ["create", "write", "pread", "truncate", "rename", "unlink", "mkdir", "rmdir", "symlink", "readdir"]


def digest_tree(root: Path) -> dict[str, str]:
    out: dict[str, str] = {}
    for path in sorted(root.rglob("*")):
        rel = str(path.relative_to(root))
        if path.is_symlink():
            out[rel] = "symlink:" + os.readlink(path)
        elif path.is_dir():
            out[rel] = "dir"
        elif path.is_file():
            out[rel] = "file:" + hashlib.sha256(path.read_bytes()).hexdigest() + f":{path.stat().st_size}"
    return out


def run(root: Path, seed: int, operations: int) -> dict[str, object]:
    if root.exists():
        shutil.rmtree(root)
    root.mkdir(parents=True)
    rng = random.Random(seed)
    files = [Path("f0")]
    dirs = [Path(".")]
    trace: list[dict[str, object]] = []

    for index in range(operations):
        op = rng.choice(OPS)
        event: dict[str, object] = {"index": index, "op": op}
        try:
            if op == "create":
                rel = Path(f"f{rng.randrange(16)}")
                (root / rel).write_bytes(b"")
                if rel not in files:
                    files.append(rel)
                event["path"] = str(rel)
            elif op == "write" and files:
                rel = rng.choice(files)
                data = hashlib.sha256(f"{seed}:{index}:{rel}".encode()).digest()[: rng.randrange(1, 33)]
                with (root / rel).open("r+b" if (root / rel).exists() else "w+b") as handle:
                    handle.seek(rng.randrange(0, 96))
                    handle.write(data)
                event.update({"path": str(rel), "bytes": len(data)})
            elif op == "pread" and files:
                rel = rng.choice(files)
                if (root / rel).exists():
                    with (root / rel).open("rb") as handle:
                        handle.seek(rng.randrange(0, 96))
                        data = handle.read(rng.randrange(1, 33))
                    event.update({"path": str(rel), "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})
            elif op == "truncate" and files:
                rel = rng.choice(files)
                if (root / rel).exists():
                    os.truncate(root / rel, rng.randrange(0, 128))
                    event.update({"path": str(rel), "size": (root / rel).stat().st_size})
            elif op == "rename" and files:
                rel = rng.choice(files)
                dst = Path(f"f{rng.randrange(16)}")
                if (root / rel).exists():
                    os.replace(root / rel, root / dst)
                    if rel in files:
                        files.remove(rel)
                    if dst not in files:
                        files.append(dst)
                    event.update({"src": str(rel), "dst": str(dst)})
            elif op == "unlink" and files:
                rel = rng.choice(files)
                if (root / rel).exists() and not (root / rel).is_dir():
                    (root / rel).unlink()
                    files.remove(rel)
                    event["path"] = str(rel)
            elif op == "mkdir":
                rel = Path(f"d{rng.randrange(4)}")
                (root / rel).mkdir(exist_ok=True)
                if rel not in dirs:
                    dirs.append(rel)
                event["path"] = str(rel)
            elif op == "rmdir" and len(dirs) > 1:
                rel = rng.choice([d for d in dirs if d != Path(".")])
                try:
                    (root / rel).rmdir()
                    dirs.remove(rel)
                    event["path"] = str(rel)
                except OSError as exc:
                    event["errno"] = exc.errno
            elif op == "symlink":
                link = root / f"l{rng.randrange(4)}"
                if link.exists() or link.is_symlink():
                    link.unlink()
                os.symlink("f0", link)
                event["path"] = link.name
            elif op == "readdir":
                event["entries"] = sorted(p.name for p in root.iterdir())
        except OSError as exc:
            event["errno"] = exc.errno
        trace.append(event)

    return {
        "seed": seed,
        "operations": operations,
        "root": str(root),
        "operation_alphabet": OPS,
        "trace_sha256": hashlib.sha256(json.dumps(trace, sort_keys=True).encode()).hexdigest(),
        "tree": digest_tree(root),
        "trace": trace,
    }


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--seed", required=True, type=int)
    parser.add_argument("--operations", required=True, type=int)
    parser.add_argument("--output", required=True, type=Path)
    args = parser.parse_args()
    result = run(args.root, args.seed, args.operations)
    args.output.parent.mkdir(parents=True, exist_ok=True)
    args.output.write_text(json.dumps(result, indent=2, sort_keys=True) + "\n")


if __name__ == "__main__":
    main()
