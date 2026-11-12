#!/usr/bin/env python3
"""Count AFS layer-direction violations on product lines (R47 quality metric).

Usage: dep_edges.py [afs-dir] [--json]    (afs-dir defaults to this script's parent)

Product lines exclude test files, top-level `#[cfg(test)]` items and `//` comment lines,
so inline tests and prose cannot move a count. A plain `grep | grep -v tests` does not
give these numbers. Read-only; needs nothing but the Python standard library.
"""

import json
import re
import sys
from pathlib import Path

TEST_FILE = re.compile(
    r"(_tests\.rs$|/tests\.rs$|/tests/|_fixture\.rs$|_perf\.rs$|_invariants\.rs$|test_hooks\.rs$)"
)
TEST_ATTR = re.compile(r"#\[cfg\((all\()?test\b")
RPC = re.compile(r"node::rpc::")

# Data-layer modules directly under src/node: a name prefix matches both `x.rs` and `x/`.
DATA_PREFIXES = ("dfs_read", "peer_cache", "chunk", "read", "external_chunk")
DATA_NAMES = ("dfs_fetch", "direct_prefetch", "replication", "gc", "fault")


def under(*prefixes):
    return lambda rel: any(rel == p or rel.startswith(p + "/") for p in prefixes)


def data_layer(rel):
    parts = rel.split("/")
    if len(parts) < 3 or parts[:2] != ["src", "node"]:
        return False
    name = parts[2].removesuffix(".rs")
    return name in DATA_NAMES or name.startswith(DATA_PREFIXES)


# name -> (selects the file, pattern on product lines, target).
# D-file-access stays at 1: `vfs/ownerfs/fuse_policy` is a known OwnerFs exception (S1e M7).
EDGES = {
    "D-vfs-rpc": (under("src/node/vfs"), RPC, 0),
    "D-node-metastore": (
        lambda rel: rel == "src/node.rs" or rel.startswith("src/node/"),
        re.compile(r"crate::meta::store"),
        0,
    ),
    "D-shared-meta": (
        lambda rel: rel == "src/dfs.rs" or rel.startswith("src/dfs/"),
        re.compile(r"crate::meta::"),
        0,
    ),
    "D-meta-node": (under("src/meta"), re.compile(r"crate::node::"), 0),
    "D-data-rpc": (data_layer, RPC, 0),
    "D-file-access": (under("src/node/vfs"), re.compile(r"node::fuse::"), 1),
    # The fault loops reach the file layer only through what `run_node` hands
    # them (a `gc::Collector`, a renewal closure); `vfs/dfs -> fault` stays.
    "D-fault-vfs": (under("src/node/fault"), re.compile(r"\bvfs::"), 0),
    # GC execution lives in `node/gc`; the read engine only evicts its caches.
    "D-read-gc": (under("src/node/read"), re.compile(r"\bfn (gc_\w+|collect_generation)\b"), 0),
    # Peer proofs: only the two decoder receipts in `rpc/peer` may implement the
    # data-layer proof traits (pinned, not a ceiling to grow into).
    "N-peer-proof-impls": (
        under("src"),
        re.compile(r"^\s*impl\b.*\bPeer(Object|Stream)Proof\b.*\bfor\b"),
        2,
    ),
}


def test_lines(lines):
    """Indexes covered by top-level `#[cfg(test)]` items (attribute through closing brace)."""
    covered = set()
    i = 0
    while i < len(lines):
        if not TEST_ATTR.match(lines[i]):
            i += 1
            continue
        j = i + 1
        while j < len(lines) and lines[j].lstrip().startswith("#["):
            j += 1
        end = j
        if j < len(lines) and not lines[j].rstrip().endswith(";"):
            depth = 0
            opened = False
            while end < len(lines):
                depth += lines[end].count("{") - lines[end].count("}")
                opened = opened or "{" in lines[end]
                if opened and depth <= 0:
                    break
                end += 1
        covered.update(range(i, end + 1))
        i = end + 1
    return covered


def count(afs):
    """Return {metric: {"count", "target", "hits"}}; hits are `path:line` of each match."""
    src = afs / "src"
    product = {}
    for path in sorted(src.rglob("*.rs")):
        rel = path.relative_to(afs).as_posix()
        if TEST_FILE.search("/" + rel):
            continue
        lines = path.read_text(errors="ignore").split("\n")
        hidden = test_lines(lines)
        product[rel] = [
            (i + 1, line)
            for i, line in enumerate(lines)
            if i not in hidden and not line.lstrip().startswith("//")
        ]
    result = {}
    for name, (selected, pattern, target) in EDGES.items():
        hits = [
            f"{rel}:{number}"
            for rel, lines in product.items()
            if selected(rel)
            for number, line in lines
            if pattern.search(line)
        ]
        result[name] = {"count": len(hits), "target": target, "hits": hits}
    same_name = sorted(p.relative_to(afs).as_posix() for p in src.rglob("read_pipeline*.rs"))
    result["N-read_pipeline"] = {"count": len(same_name), "target": 0, "hits": same_name}
    return result


def main():
    args = sys.argv[1:]
    as_json = "--json" in args
    paths = [arg for arg in args if not arg.startswith("--")]
    afs = Path(paths[0]).resolve() if paths else Path(__file__).resolve().parent.parent
    if not (afs / "src").is_dir():
        sys.exit(f"{afs} has no src directory")
    result = count(afs)
    if as_json:
        print(json.dumps(result, indent=2))
        return
    for name, metric in result.items():
        print(f"{name:18} {metric['count']:3}  target {metric['target']}")
        for hit in metric["hits"]:
            print(f"    {hit}")


if __name__ == "__main__":
    main()
