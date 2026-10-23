#!/usr/bin/env python3
"""Compare successful storage runs only when runtime and workload identities match."""

import argparse
import json
import math
from pathlib import Path
import statistics

from storage_contract import summarize, verify


def comparable(left, right):
    if (
        left["package"]["files"] != right["package"]["files"]
        or left["image"] != right["image"]
        or left["workload"] != right["workload"]
    ):
        raise ValueError("package, workload image or guest workload differs")


def load(directory):
    root = Path(directory)
    result = json.loads((root / "result.json").read_text())
    verify(result)
    if result.get("diagnostic_only"):
        raise ValueError("traced diagnostic run is excluded from performance comparison")
    if result.get("meta_profile_enabled"):
        raise ValueError("Meta profiling run is excluded from performance comparison")
    if result.get("filesystem_profile_enabled"):
        raise ValueError("filesystem profiling run is excluded from performance comparison")
    cleanup = json.loads((root / "storage-cleanup.json").read_text())
    if cleanup["exit_code"] != 0 or cleanup.get("errors") != []:
        raise ValueError("storage suite did not finish successfully")
    result["fixture_cleanup"] = cleanup
    if result["storage"]["kind"] == "juicefs":
        fixture = json.loads((root / "juicefs/juicefs-identities.json").read_text())
        parameter_names = (
            "max_readahead_mib",
            "cache_size_mib",
            "cache_dir",
            "writeback",
            "binary_sha256",
            "ceph",
            "postgres",
            "postgres_synchronous_commit",
        )
        result["filesystem_parameters"] = {
            "block_size_kib": fixture.get("juicefs_block_size_kib"),
            **{key: fixture.get(key) for key in parameter_names},
        }
    identities = {
        "package": json.loads((root / "package-manifest.json").read_text()),
        "image": json.loads((root / "execd-image.json").read_text()),
        "workload": result["workload"],
    }
    hashes = json.loads((root / "runtime-hashes.json").read_text())
    identities["runtime"] = {}
    for key, digest in hashes.items():
        name = Path(key).name
        if name in ("sandboxd", "firecracker", "Image", "initrd.img"):
            identities["runtime"][name] = digest
    return result, identities


def direct_samples(run):
    direct = run.get("direct_baseline")
    if direct is None:
        return None
    if (
        direct.get("status") != "passed"
        or direct.get("cleanup_errors") != []
        or direct.get("workload") != run["workload"]
    ):
        raise ValueError("incomplete direct sandboxd baseline")
    if [s["snapshot_type"] for s in direct.get("samples", [])] != ["Full", "Incremental"]:
        raise ValueError("incomplete direct sandboxd baseline")
    for row in direct["samples"]:
        digest = (
            "expected_full_sha256" if row["snapshot_type"] == "Full" else "expected_dirty_sha256"
        )
        if (
            row.get("continuity") is not True
            or row.get("checked_bytes") != run["workload"]["payload_bytes"]
            or row.get("sha256") != run["workload"][digest]
        ):
            raise ValueError("missing direct full-memory or timing proof")
        timings_valid = True
        for field in ("pause_seconds", "rpc_seconds", "first_response_seconds", "checksum_seconds"):
            if not math.isfinite(row[field]) or row[field] < 0:
                timings_valid = False
                break
        if not timings_valid:
            raise ValueError("missing direct full-memory or timing proof")
    return direct


def aggregate(runs):
    groups = {}
    probes = []
    for run, _ in runs:
        direct = direct_samples(run)
        probes.append(
            None if direct is None else (direct["first_response_probe"], direct["probe_sha256"])
        )
        key = (run["storage"]["kind"], run["storage"].get("alias"), bool(run.get("benchmark_only")))
        groups.setdefault(key, []).append(run)
    if any(probe != probes[0] for probe in probes[1:]):
        raise ValueError("direct probe identities differ")
    backends = []
    for trials in groups.values():
        parameters = trials[0].get("filesystem_parameters")
        if any(t.get("filesystem_parameters") != parameters for t in trials[1:]):
            raise ValueError(
                "backend filesystem parameters differ; do not pool different geometry or caching"
            )
        samples = [sample for trial in trials for sample in trial["samples"]]
        backend = {
            "storage": trials[0]["storage"],
            "summary": summarize(samples, sum(t["repetitions"] for t in trials)),
            "samples": samples,
            "fixture_cleanup": {"exit_code": 0, "errors": []},
            "verification_scope": "benchmark" if trials[0].get("benchmark_only") else "lifecycle",
            "filesystem_parameters": parameters,
            "lifecycle_cases": trials[0]["cases"],
            "trials": [
                {"storage": t["storage"], "fixture_cleanup": t["fixture_cleanup"]} for t in trials
            ],
        }
        if probes[0] is not None:
            direct = [sample for trial in trials for sample in trial["direct_baseline"]["samples"]]
            summary = {}
            for flavor in ("Full", "Incremental"):
                rows = [s for s in direct if s["snapshot_type"] == flavor]
                summary[flavor] = {"samples": len(rows)}
                for field in (
                    "pause_seconds",
                    "rpc_seconds",
                    "first_response_seconds",
                    "checksum_seconds",
                ):
                    values = [r[field] for r in rows]
                    summary[flavor][field] = {
                        "median": statistics.median(values),
                        "min": min(values),
                        "max": max(values),
                    }
            backend["direct_baseline"] = {"probe": probes[0], "summary": summary, "samples": direct}
        backends.append(backend)
    return backends


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("evidence", type=Path, nargs="+")
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--markdown", type=Path)
    args = parser.parse_args()
    if len({path.resolve() for path in args.evidence}) != len(args.evidence):
        raise ValueError("duplicate evidence directory")
    runs = [load(path) for path in args.evidence]
    if len(runs) < 2:
        raise ValueError("at least two successful backend runs are required")
    baseline = runs[0][1]
    for _, identity in runs[1:]:
        comparable(baseline, identity)
        if baseline["runtime"] != identity["runtime"]:
            raise ValueError("external runtime identities differ")
    result = {
        "scope": "one KVM host, two isolated execution nodes; cold peer / warm owner; no machine-wide cache flush",
        "timing_contract": {
            "first_http_seconds": "minimum of 12 request durations",
            "remote_read_bytes.first": "after all 12 wake responses, before checksum",
            "direct_vs_adx": (
                "same payload; Python initial process versus Execd-launched service; "
                "totals are not subtractable"
            ),
        },
        "identities": baseline,
        "backends": [],
    }
    result["backends"] = aggregate(runs)
    args.output.write_text(json.dumps(result, indent=2))
    if args.markdown:
        lines = [
            "# AFS / JuiceFS / S3 Firecracker 存储验证",
            "",
            "同一专用 KVM 主机、两个隔离执行节点、两个 Ingress；同一发布包、外部运行时、guest 镜像和工作负载。",
            (
                f"每个 Environment 为 {baseline['workload']['cpu_millis'] / 1000:g} vCPU / "
                f"{baseline['workload']['memory_mib']} MiB，工作集 "
                f"{baseline['workload']['payload_bytes'] // 1024**2} MiB。"
            ),
            (
                f"磁盘 I/O 使用 {baseline['workload']['writable_io_engine']} / "
                f"{baseline['workload']['writable_cache_type']}。冷恢复为独立节点、无该制品缓存；"
                "热恢复为原节点。未执行全机缓存清理。"
            ),
            "",
            "每种后端冷、热恢复各至少三次，表中使用各组实际样本中位数。每次 12 个请求经过两个 Ingress，只恢复一次，并校验 guest PID、内存状态及文件。",
            "",
            "| 后端 | 暂停发布中位数 (s) | 冷恢复首 HTTP (s) | 热恢复首 HTTP (s) |",
            "| --- | ---: | ---: | ---: |",
        ]
        for backend in result["backends"]:
            samples = backend["samples"]
            kind = backend["storage"]["kind"]
            label = {
                "afs": "AFS",
                "juicefs": "JuiceFS / PostgreSQL / Ceph RGW",
                "minio": "S3 / MinIO",
                "real_obs": "OBS / " + backend["storage"].get("region", ""),
            }.get(kind, kind)
            pause = statistics.median(s["pause_seconds"] for s in samples)
            cold = statistics.median(
                s["first_http_seconds"] for s in samples if s["cache"] == "cold"
            )
            warm = statistics.median(
                s["first_http_seconds"] for s in samples if s["cache"] == "warm"
            )
            lines.append(f"| {label} | {pause:.3f} | {cold:.3f} | {warm:.3f} |")
        lines.extend(
            [
                "",
                "AFS 为 local-file Meta、三个客户端、单份必需持久副本；不代表存储 HA 或跨物理主机故障域。",
                "JuiceFS 使用三个独立客户端、PostgreSQL 同步提交、Ceph RGW；缓存和物理介质以导出的 fixture 身份记录为准。",
                "本机后端与 OBS 远端链路具有不同网络成本，不能用这组结果分离纯后端性能。",
                "",
                "## 生命周期与清理",
                "",
            ]
        )
        for backend in result["backends"]:
            lines.append(
                f"- {backend['storage']['kind']}: {len(backend['lifecycle_cases'])} 项集合检查通过；执行节点最终 inventory 均为 0。"
            )
            for error in backend["fixture_cleanup"]["errors"]:
                lines.append("  存储服务清理未通过：" + error)
        lines.extend(
            [
                "",
                "原始样本、每阶段后端读流量、VMM 映射及回收证据见同目录 JSON 和导出的 evidence。",
                "",
            ]
        )
        args.markdown.write_text("\n".join(lines))


if __name__ == "__main__":
    main()
