#!/usr/bin/env python3
"""AFS component integration: one entry for one Linux VM and independent SSH VMs."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import signal
import sys
import tempfile
import time
import traceback
import uuid

from fixtures.common import Blocked, discover, select, sha256, write_json
from fixtures.multivm import Cluster, validate


def parser():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("command", choices=["list", "check-env", "run"])
    choice = p.add_mutually_exclusive_group()
    choice.add_argument("--all", action="store_true")
    choice.add_argument("--suite")
    choice.add_argument("--case", action="append")
    p.add_argument("--env", type=Path)
    p.add_argument("--binary-dir", type=Path)
    p.add_argument("--work-root", type=Path)
    p.add_argument("--case-timeout", type=int, default=600)
    return p


def configurations(path, cases):
    raw = json.loads(path.read_text())
    if raw.get("kind") != "matrix":
        validate(raw, cases)
        return raw, {case.id: raw for case in cases}
    result = {}
    for name in ("local", "cluster"):
        chosen = [case for case in cases if any(tag.startswith("local-") for tag in case.tags) == (name == "local")]
        if not chosen:
            continue
        entry = raw.get("environments", {}).get(name)
        if entry is None:
            raise Blocked("matrix lacks selected environment: " + name)
        config = json.loads((path.parent / entry).read_text()) if isinstance(entry, str) else entry
        if not isinstance(config, dict) or config.get("kind") == "matrix":
            raise ValueError("matrix entries must be one concrete environment")
        validate(config, chosen)
        result.update({case.id: config for case in chosen})
    return raw, result


def interrupted(signum, _frame):
    raise InterruptedError("test interrupted by signal " + str(signum))


def timed_out(_signum, _frame):
    raise TimeoutError("case exceeded its explicit time budget")


def main(argv=None):
    p = parser()
    args = p.parse_args(argv)
    if args.case_timeout < 60 or args.case_timeout > 3600:
        p.error("--case-timeout must be 60..3600 seconds")
    try:
        cases = discover()
        selected = select(cases, all_cases=args.all, suite=args.suite, patterns=args.case)
        required = {
            "local-basic": {f"LB{i:02}" for i in range(1, 9)},
            "cluster-basic": {f"MV{i:02}" for i in range(1, 8)},
            "cluster-manyread": {f"MV{i:02}" for i in range(1, 9)},
        }
        required["all-basic"] = required["local-basic"] | required["cluster-manyread"]
        tag = args.suite or ("local-basic" if not (args.all or args.case) else None)
        missing = required.get(tag, set()) - {g for c in selected for g in c.groups}
        if missing:
            raise ValueError("incomplete registered suite: " + ",".join(sorted(missing)))
    except ValueError as exc:
        p.error(str(exc))
    if args.command == "list":
        # With no selector list the whole registry, while run defaults to local-basic.
        if not (args.all or args.suite or args.case):
            selected = cases
        print(
            json.dumps(
                [
                    {"id": c.id, "suites": c.tags, "mode": c.mode, "groups": c.groups, "requires_rust_test_elf": c.rust}
                    for c in selected
                ],
                indent=2,
            )
        )
        return 0
    if args.env is None or args.binary_dir is None:
        p.error("--env and --binary-dir are required")
    try:
        config, configs = configurations(args.env, selected)
    except (OSError, ValueError, Blocked) as exc:
        print(json.dumps({"status": "BLOCKED", "error": str(exc)}), file=sys.stderr)
        return 2
    for sig in (signal.SIGTERM, signal.SIGINT):
        signal.signal(sig, interrupted)
    signal.signal(signal.SIGALRM, timed_out)
    token = "afs-test-" + uuid.uuid4().hex
    if args.command == "check-env":
        try:
            groups = {}
            for case in selected:
                group = groups.setdefault(id(configs[case.id]), {"config": configs[case.id], "cases": []})
                group["cases"].append(case)
            observations = []
            for group in groups.values():
                subset = group["cases"]
                with tempfile.TemporaryDirectory(prefix="afs-preflight-") as directory:
                    case = next(
                        (c for c in subset if c.id.endswith(".many_readers")),
                        next((c for c in subset if c.rust), subset[0]),
                    )
                    cluster = Cluster(group["config"], args.binary_dir, directory, case, token)
                    try:
                        info = cluster.connect()
                        identity = cluster.inputs(info)
                        observations.append(
                            {"kind": group["config"]["kind"], "environment": info, "identity": identity}
                        )
                    finally:
                        result = cluster.cleanup()
                        if result["status"] != "PASS":
                            raise RuntimeError("preflight worker cleanup failed: " + repr(result))
            print(
                json.dumps(
                    {"status": "READY", "scope": "environment only, no product PASS", "observations": observations},
                    indent=2,
                )
            )
            return 0
        except Exception as exc:
            print(json.dumps({"status": "BLOCKED", "error": str(exc)}), file=sys.stderr)
            return 2
    if args.work_root is None or not args.work_root.is_absolute() or args.work_root.exists():
        p.error("--work-root must be a new absolute directory")
    args.work_root.mkdir(parents=True, mode=0o755)
    report = {
        "status": "NOT_RUN",
        "suite": args.suite or ("all" if args.all else "targeted" if args.case else "local-basic"),
        "partial": bool(args.case),
        "kind": config["kind"],
        "run_id": token,
        "selected_cases": [c.id for c in selected],
        "cases": [],
        "cleanup_errors": [],
    }
    sources = {
        str(f.relative_to(Path(__file__).parent)): sha256(f)
        for folder in ("fixtures", "suites")
        for f in (Path(__file__).parent / folder).rglob("*.py")
    }
    sources["run.py"] = sha256(__file__)
    write_json(args.work_root / "identity.json", {"test_source_sha256": sources})
    write_json(args.work_root / "environment.json", config)
    stop = False
    for case in selected:
        entry = {
            "id": case.id,
            "groups": case.groups,
            "mode": case.mode,
            "kind": configs[case.id]["kind"],
            "status": "NOT_RUN",
        }
        report["cases"].append(entry)
        if stop:
            continue
        out = args.work_root / case.id
        out.mkdir()
        cluster = Cluster(configs[case.id], args.binary_dir, out, case, token)
        start = time.monotonic()
        print("[RUN] " + case.id, flush=True)
        try:
            signal.alarm(args.case_timeout)
            if any(sha256(Path(__file__).parent / name) != digest for name, digest in sources.items()):
                raise RuntimeError("test input changed after run identity was frozen")
            cluster.prepare()
            case.function(cluster)
            if any(sha256(Path(__file__).parent / name) != digest for name, digest in sources.items()):
                raise RuntimeError("test input changed during case execution")
            entry["status"] = "PASS"
        except Blocked as exc:
            entry.update(status="BLOCKED", error=str(exc))
            stop = True
        except Exception as exc:
            entry.update(status="FAIL", error=f"{type(exc).__name__}: {exc}", traceback=traceback.format_exc())
            if isinstance(exc, (InterruptedError, TimeoutError)):
                stop = True
        finally:
            signal.alarm(0)
            # A second interrupt must not bypass owned-resource cleanup.
            previous = {sig: signal.signal(sig, signal.SIG_IGN) for sig in (signal.SIGINT, signal.SIGTERM)}
            try:
                cleanup = cluster.cleanup()
                if cleanup["status"] != "PASS":
                    report["cleanup_errors"].append({"case": case.id, "errors": cleanup["errors"]})
                    entry["status"] = "FAIL"
                    stop = True
            except Exception as exc:
                entry["status"] = "FAIL"
                entry["cleanup_error"] = str(exc)
                stop = True
                report["cleanup_errors"].append({"case": case.id, "errors": [str(exc)]})
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)
        entry["seconds"] = time.monotonic() - start
        write_json(out / "result.json", entry)
        write_json(args.work_root / "result.json", report)
        print("[" + entry["status"] + "] " + case.id, flush=True)
    states = {c["status"] for c in report["cases"]}
    report["status"] = "FAIL" if "FAIL" in states else "BLOCKED" if states != {"PASS"} else "PASS"
    report["selected_group_results"] = {}
    for group in sorted({g for c in selected for g in c.groups}):
        contributions = [c["status"] for c in report["cases"] if group in c["groups"]]
        report["selected_group_results"][group] = {
            "status": "FAIL"
            if "FAIL" in contributions
            else "PASS"
            if set(contributions) == {"PASS"}
            else "NOT_COMPLETE",
            "complete_suite_scope": not report["partial"],
        }

    write_json(args.work_root / "result.json", report)
    # Preserve per-case actual process exit receipts and cleanup, not just the aggregate exit code.
    write_json(
        args.work_root / "waits.json",
        {
            e["id"]: json.loads((args.work_root / e["id"] / "waits.json").read_text())
            for e in report["cases"]
            if (args.work_root / e["id"] / "waits.json").exists()
        },
    )
    write_json(
        args.work_root / "cleanup.json",
        {"errors": report["cleanup_errors"], "status": "FAIL" if report["cleanup_errors"] else "PASS"},
    )
    return {"PASS": 0, "FAIL": 1, "BLOCKED": 2}[report["status"]]


if __name__ == "__main__":
    raise SystemExit(main())
