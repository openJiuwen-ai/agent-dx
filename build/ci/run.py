#!/usr/bin/env python3
"""Local component, Socket interoperability, and packaging checks."""
import argparse
from datetime import datetime, timezone
import hashlib
import json
import os
from pathlib import Path
import platform
import signal
import shutil
import subprocess
import sys
import time
import uuid

ROOT = Path(__file__).resolve().parents[2]
SUITES = ("harness", "rust", "api-server", "agent", "sandbox-sdk", "interop", "package", "storage", "control-rpc", "api-control")


def commands_for(suite, output, jobs):
    python = sys.executable
    make = ["make", f"JOBS={jobs}", f"PYTHON={python}"]
    if suite == "harness":
        return [[python, "-m", "unittest", "discover", "-s", "build/ci/tests", "-v"]]
    if suite == "rust":
        cargo = os.environ.get("CARGO", "cargo")
        return [[cargo, "--version"], make + ["rust-check", "rust-test"]]
    if suite in ("control-rpc", "api-control"):
        if suite == "api-control" and (not Path(os.environ.get("ADX_TEST_API_SERVER", "")).is_file() or not os.access(os.environ.get("ADX_TEST_API_SERVER", ""), os.X_OK)):
            raise ValueError("api-control requires ADX_TEST_API_SERVER executable")
        cargo = os.environ.get("CARGO", "cargo")
        redis = os.environ.get("ADX_TEST_REDIS_SERVER", "redis-server")
        return [[redis, "--version"],
                [python, "build/ci/rpc_certificates.py", str(output / "tls")],
                ["env", f"ADX_TEST_REDIS_SERVER={redis}", f"ADX_TEST_TLS_DIR={output / 'tls'}", f"ADX_TEST_EVIDENCE={output}",
                 cargo, "test", "--locked", "-p", "adx-master", "--test", "rpc",
                 "-j", str(jobs), "--", "--ignored", "--nocapture"]]
    if suite == "storage":
        cargo = os.environ.get("CARGO", "cargo")
        redis = os.environ.get("ADX_TEST_REDIS_SERVER", "redis-server")
        return [[redis, "--version"],
                ["env", f"ADX_TEST_REDIS_SERVER={redis}", f"ADX_TEST_EVIDENCE={output}",
                 cargo, "test", "--locked", "-p", "adx-master", "--test", "storage",
                 "-j", str(jobs), "--", "--ignored", "--nocapture"]]
    if suite == "api-server":
        return [[os.environ.get("CARGO", "cargo"), "test", "--locked", "-p", "adx-api-server", "-j", str(jobs)]]
    if suite == "agent":
        return [make + ["agent-test", f"PYTEST_ARGS=--junitxml={output / 'junit.xml'}"]]
    if suite == "sandbox-sdk":
        return [make + ["sandbox-sdk-test", f"PYTEST_ARGS=--junitxml={output / 'junit.xml'}"]]
    if suite == "interop":
        cargo = os.environ.get("CARGO", "cargo")
        target = Path(os.environ.get("CARGO_TARGET_DIR", ROOT / "target")).resolve()
        return [[cargo, "build", "--locked", "-p", "rrt-daemon", "--bin", "rrt-runtime", "-j", str(jobs)],
                ["env", "ADX_TUNNEL_PROTOCOL_VERSION=2", "ADX_TUNNEL_FAST_PATH_BODY_BYTES=65536",
                 f"RRT_RUNTIME={target / 'debug/rrt-runtime'}",
                 f"PYTHONPATH={ROOT / 'platform/sdk/sandbox/python'}", python,
                 "platform/runtime/rrt/tests/tunnel_interop.py"],
                [python, "build/ci/rpc_certificates.py", str(output / "tls")],
                ["env", f"PYTHONPATH={ROOT / 'platform/sdk/sandbox/python'}", python,
                 "build/ci/command_watch_tls.py", "--tls", str(output / "tls")]]
    if suite == "package":
        packages = ["agent/cli", "agent/sdk/python", "agent/executor", "platform/sdk/sandbox/python"]
        return [[python, "-m", "build", "--no-isolation", "--wheel", "--sdist",
                 "--outdir", str(output / "wheels"), package] for package in packages]
    raise ValueError(f"Suite is not implemented: {suite}")


def execute(commands, output, timeout, metadata):
    if not commands:
        raise ValueError("A suite must execute at least one command")
    output.mkdir(parents=True, exist_ok=True)
    if (output / "result.json").exists():
        raise FileExistsError(f"Evidence already exists: {output}")
    # A failed or canceled run must not be silently replaced by a retry.
    (output / ".run.lock").touch(exist_ok=False)
    report = dict(metadata, status="running", commands=[])
    code = 0
    interrupted = False
    try:
        for index, command in enumerate(commands, 1):
            log = output / f"{index:02}.log"
            entry = {"argv": command, "log": log.name, "timed_out": False}
            report["commands"].append(entry)
            started = time.monotonic()
            print(f"[{index}/{len(commands)}] {' '.join(command)} -> {log}", flush=True)
            process = None
            with log.open("wb") as stream:
                try:
                    process = subprocess.Popen(command, stdout=stream, stderr=subprocess.STDOUT,
                                               start_new_session=True)
                    code = process.wait(timeout=timeout)
                except subprocess.TimeoutExpired:
                    entry["timed_out"] = True
                    code = 124
                except OSError as exc:
                    stream.write(str(exc).encode())
                    code = 127
                except KeyboardInterrupt:
                    code = 130
                    interrupted = True
                finally:
                    if process is not None and (entry["timed_out"] or interrupted):
                        try:
                            os.killpg(process.pid, signal.SIGKILL)
                        except ProcessLookupError:
                            pass
                        process.wait()
            entry.update(exit_code=code, duration_seconds=round(time.monotonic() - started, 3))
            if code:
                print(log.read_text(errors="replace")[-6000:], file=sys.stderr)
                break
    except KeyboardInterrupt:
        code = 130
        interrupted = True
    except Exception:
        code = 1
        raise
    finally:
        report["status"] = "canceled" if interrupted else ("passed" if code == 0 else "failed")
        report["exit_code"] = code
        report["artifacts"] = {
            str(path.relative_to(output)): hashlib.sha256(path.read_bytes()).hexdigest()
            for path in sorted((output / "wheels").glob("*")) if path.is_file()
        }
        (output / "result.json").write_text(json.dumps(report, indent=2) + "\n")
    return code if code >= 0 else 128 - code


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=SUITES)
    parser.add_argument("--jobs", type=int, default=int(os.environ.get("JOBS", "2")))
    parser.add_argument("--timeout", type=float, default=1800, help="seconds per command")
    parser.add_argument("--output", type=Path, help="new evidence directory for this run")
    parser.add_argument("--list", action="store_true", help="print commands without running")
    args = parser.parse_args()
    if args.jobs < 1 or args.timeout <= 0:
        parser.error("jobs and timeout must be positive")
    os.chdir(ROOT)
    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ") + "-" + uuid.uuid4().hex[:8]
    output = (args.output or ROOT / "out/ci" / args.suite / run_id).resolve()
    commands = commands_for(args.suite, output, args.jobs)
    if args.list:
        print(json.dumps(commands, indent=2))
        return 0
    commit = subprocess.check_output(["git", "rev-parse", "HEAD"], text=True).strip()
    dirty = bool(subprocess.check_output(["git", "status", "--porcelain"], text=True).strip())
    if os.environ.get("BUILDKITE") == "true" and dirty:
        parser.error("Buildkite requires a clean checkout before running the suite")
    metadata = {"suite": args.suite, "commit": commit, "dirty": dirty,
                "platform": platform.platform(), "python": sys.version,
                "build_url": os.environ.get("BUILDKITE_BUILD_URL"),
                "image": os.environ.get("ADX_CI_IMAGE"), "jobs": args.jobs,
                "caches": {key: os.environ.get(key) for key in
                           ("CARGO_TARGET_DIR", "GOCACHE", "GOMODCACHE", "PIP_CACHE_DIR")}}
    if args.suite in ("storage", "control-rpc", "api-control"):
        binary = shutil.which(os.environ.get("ADX_TEST_REDIS_SERVER", "redis-server"))
        metadata["redis_binary_sha256"] = hashlib.sha256(Path(binary).read_bytes()).hexdigest() if binary else None
    if args.suite == "api-control":
        binary = Path(os.environ["ADX_TEST_API_SERVER"])
        metadata["api_server_binary_sha256"] = hashlib.sha256(binary.read_bytes()).hexdigest()
    def interrupt(*_):
        raise KeyboardInterrupt

    signal.signal(signal.SIGTERM, interrupt)
    code = execute(commands, output, args.timeout, metadata)
    print(f"{args.suite}: {'passed' if code == 0 else 'failed'}; evidence: {output}")
    return code


if __name__ == "__main__":
    sys.exit(main())
