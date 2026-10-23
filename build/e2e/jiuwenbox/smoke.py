#!/usr/bin/env python3
"""Run the installed-SDK smoke against a verified precompiled Jiuwenbox supervisor backend."""

import argparse
import fcntl
import hashlib
import json
import logging
import os
from pathlib import Path
import platform
import shutil
import subprocess
import sys
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[3]
LOCK = Path(__file__).with_name("prebuilt-lock.json")
EXPECTED_CASES = (
    "supervisor-create-exec-files-erofs",
    "adxlet-restart",
    "coordinator-restart",
    "apiserver-restart",
    "ingress-restart",
    "jiuwenbox-restart",
    "supervisor-delete",
    "native-resource-audit",
)


def sha(path):
    digest = hashlib.sha256()
    with Path(path).open("rb") as source:
        for chunk in iter(lambda: source.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def verify_identity(manifest, commit):
    if manifest.get("target") != "x86_64-unknown-linux-gnu":
        raise ValueError("Jiuwenbox smoke requires a Linux x86_64 package")
    if commit and manifest.get("commit") != commit:
        raise ValueError("package commit differs from expected commit")
    if commit and manifest.get("dirty") is not False:
        raise ValueError("expected-commit validation rejects a dirty package")


def verify_wheel(wheel, site, prefixes):
    count = 0
    with zipfile.ZipFile(wheel) as archive:
        for name in archive.namelist():
            if name.startswith(prefixes) and not name.endswith("/"):
                if (site / name).read_bytes() != archive.read(name):
                    raise ValueError("installed wheel payload differs: " + name)
                count += 1
    if count == 0:
        raise ValueError("wheel contains no selected package files")
    return count


def verify_backend(root, lock):
    archive = next(a for a in lock["archives"] if a["name"] == "jiuwenswarm-daily")
    wheel = lock["jiuwenbox"]["artifact"]
    for artifact in (archive, wheel):
        relative = Path(artifact["cache_path"])
        if relative.is_absolute() or ".." in relative.parts:
            raise ValueError("invalid pinned cache path")
        path = root / "pinned" / relative
        if path.stat().st_size != artifact["size"] or sha(path) != artifact["sha256"]:
            raise ValueError("pinned artifact differs: " + artifact["file"])
    sites = list((root / "venv/lib").glob("python*/site-packages"))
    if len(sites) != 1:
        raise ValueError("exactly one backend site-packages directory is required")
    if (root / "jiuwenbox/src/jiuwenbox").resolve() != (sites[0] / "jiuwenbox").resolve():
        raise ValueError("backend policy path differs from the verified wheel")
    installed = verify_wheel(root / "pinned" / wheel["cache_path"], sites[0], ("jiuwenbox/",))
    return {"unchanged": True, "pinned_files": 2, "installed_wheel_files": installed}


def verify_results(run_dir):
    result = json.loads((run_dir / "results.json").read_text())
    cases = result.get("cases", [])
    if result.get("status") != "passed" or [case["case"] for case in cases] != list(EXPECTED_CASES):
        raise ValueError("SDK cases are failed, missing, duplicated or out of order")
    if any(case.get("status") != "passed" for case in cases):
        raise ValueError("SDK cases did not all pass")
    host = json.loads((run_dir / "host-audit.json").read_text())
    runtime = json.loads((run_dir / "runtime/audit.json").read_text())
    if host.get("leftovers") != [] or host.get("conch_bridge_ports") != [] or runtime.get("leftovers") != []:
        raise ValueError("native cleanup audit failed")
    return result


def export_evidence(run_dir, output, acceptance_log=None):
    secrets = [p.read_bytes().strip() for p in (run_dir / "api-key", run_dir / "redis-key") if p.is_file()]
    paths = list(run_dir.rglob("*"))
    if acceptance_log is not None and acceptance_log.is_file():
        paths.append(acceptance_log)
    for path in paths:
        if not path.is_file():
            continue
        relative = Path("acceptance.log") if path == acceptance_log else path.relative_to(run_dir)
        if "bootstrap" in relative.parts:
            continue
        allowed = path.suffix == ".log" or (
            path.suffix == ".json"
            and path.name not in ("config.json", "recovery.json", "sdk-client.json")
            and (len(relative.parts) == 1 or path.name == "audit.json")
        )
        if not allowed:
            continue
        data = path.read_bytes()
        for secret in secrets:
            if secret:
                data = data.replace(secret, b"<redacted>")
        if b"PRIVATE KEY-----" in data:
            raise ValueError("private key found in selected evidence")
        target = output / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        target.write_bytes(data)


def cleanup_fixture(private):
    # The SDK audit already proved the mount survives environment deletion.
    # The runner now tears down its temporary node; other nodes' shared mounts stay.
    digest = sha(private / "package-sdk/runtime/adx-runtime-rootfs.img")
    target = private / "runs/smoke/runtime/state/bootstrap" / digest
    findmnt = shutil.which("findmnt")
    unmount = shutil.which("umount")
    if not findmnt or not unmount:
        raise FileNotFoundError("findmnt and umount are required for fixture cleanup")
    result = subprocess.run([findmnt, "-J", "-M", str(target)], capture_output=True, text=True, check=True)
    mounts = json.loads(result.stdout)["filesystems"]
    if len(mounts) != 1 or mounts[0]["target"] != str(target) or mounts[0]["fstype"] != "erofs":
        raise ValueError("unexpected fixture mount during teardown")
    subprocess.run([unmount, "--", str(target)], check=True)
    shutil.rmtree(private)
    return {"fixture_mount_unmounted": True, "private_directory_removed": True}


def preflight():
    if sys.platform != "linux" or platform.machine() != "x86_64" or os.geteuid() != 0:
        raise ValueError("smoke requires a privileged Linux x86_64 worker")
    for tool in ("busybox", "bwrap", "ip", "iptables", "mount", "findmnt", "losetup"):
        if not shutil.which(tool):
            raise ValueError("missing backend prerequisite: " + tool)
    cgroup = Path("/sys/fs/cgroup")
    if not (
        (cgroup / "cgroup.controllers").is_file()
        or all((cgroup / controller).is_dir() for controller in ("memory", "cpu", "pids"))
    ):
        raise ValueError("cgroup v1 controllers or cgroup v2 are required")


def prepare_fixture(backend, package, private):
    private.mkdir(mode=0o700)
    for name in ("venv", "jiuwenbox"):
        (private / name).symlink_to(backend / name)
    (private / "adx").symlink_to(ROOT)
    # A regular package directory is required by the process-restart PID check.
    shutil.copytree(package, private / "package-sdk")
    artifacts = private / "artifacts"
    artifacts.mkdir()
    owned = backend / "artifacts/host-config-owned"
    if owned.exists():
        (artifacts / owned.name).symlink_to(owned)
    (artifacts / "execd.erofs").symlink_to(private / "package-sdk/runtime/adx-runtime-rootfs.img")
    sdk = private / "sdk-venv"
    subprocess.run([str(backend / "venv/bin/python"), "-m", "venv", str(sdk)], check=True)
    wheels = list((package / "sdk").glob("adx_environment-*.whl"))
    if len(wheels) != 1:
        raise ValueError("exactly one package SDK wheel is required")
    subprocess.run(
        [str(sdk / "bin/python"), "-m", "pip", "install", "--disable-pip-version-check", str(wheels[0])],
        check=True,
    )
    return wheels[0]


def run(args):
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    private = output.with_name(output.name + "-private")
    run_dir = private / "runs/smoke"
    summary = {"passed": False, "cases": [], "error": None}
    try:
        preflight()
        # All callers on this host share Jiuwenbox/Conch's fixed global paths.
        with Path("/var/lock/adx-jiuwenbox-smoke.lock").open("a") as guard:
            fcntl.flock(guard, fcntl.LOCK_EX | fcntl.LOCK_NB)
            backend, package = args.backend_root.resolve(), args.package.resolve()
            subprocess.run(
                [sys.executable, str(ROOT / "build/release/package.py"), "verify", str(package)], check=True
            )
            manifest = json.loads((package / "manifest.json").read_text())
            verify_identity(manifest, args.commit)
            lock = json.loads(LOCK.read_text())
            integrity = verify_backend(backend, lock)
            (output / "integrity-before.json").write_text(json.dumps(integrity, indent=2) + "\n")
            wheel = prepare_fixture(backend, package, private)
            sdk_site = next((private / "sdk-venv/lib").glob("python*/site-packages"))
            summary["sdk_files_verified"] = verify_wheel(wheel, sdk_site, ("adx_environment/",))
            summary["identity"] = {
                "commit": manifest["commit"],
                "dirty": manifest["dirty"],
                "package_manifest_sha256": sha(package / "manifest.json"),
                "sdk_wheel_sha256": sha(wheel),
                "execd_sha256": sha(package / "runtime/adx-execd"),
                "execd_erofs_sha256": sha(package / "runtime/adx-runtime-rootfs.img"),
                "prebuilt_lock_sha256": sha(LOCK),
            }
            shutil.copy2(LOCK, output / "prebuilt-lock.json")
            # Do not inherit caller credentials or a host-only TMPDIR into sandboxes.
            environment = {
                "PATH": os.environ.get("PATH", os.defpath),
                "LANG": "C.UTF-8",
                "PYTHONDONTWRITEBYTECODE": "1",
                "TMPDIR": "/tmp",
            }
            command = [
                str(backend / "venv/bin/python"),
                "-u",
                str(Path(__file__).with_name("full_sdk.py")),
                "--root",
                str(private),
                "--run",
                "smoke",
                "--runtime",
                "supervisor",
            ]
            with (private / "acceptance.log").open("w") as log:
                process = subprocess.Popen(
                    command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, env=environment
                )
                for line in process.stdout:
                    log.write(line)
                    log.flush()
                    if line.startswith(("RUN ", "PASS ")):
                        logging.info("%s", line.rstrip("\n"))
                code = process.wait()
            if run_dir.exists():
                export_evidence(run_dir, output, private / "acceptance.log")
            try:
                after = verify_backend(backend, lock)
                (output / "integrity-after.json").write_text(json.dumps(after, indent=2) + "\n")
            finally:
                if code:
                    raise RuntimeError("SDK acceptance failed; see exported component logs/results")
            result = verify_results(run_dir)
            summary["cleanup"] = cleanup_fixture(private)
            summary.update(passed=True, cases=result["cases"])
    except Exception as error:
        summary["error"] = f"{type(error).__name__}: {error}"
        if run_dir.exists():
            export_evidence(run_dir, output, private / "acceptance.log")
    finally:
        (output / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        suite = ET.Element(
            "testsuite", name="jiuwenbox-smoke", tests="1", failures="0" if summary["passed"] else "1"
        )
        case = ET.SubElement(suite, "testcase", name="supervisor-public-sdk")
        if not summary["passed"]:
            ET.SubElement(case, "failure", message=summary["error"] or "smoke failed")
        ET.ElementTree(suite).write(output / "junit.xml", encoding="utf-8", xml_declaration=True)
        logging.info("%s", json.dumps(summary))
    return 0 if summary["passed"] else 1


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--backend-root", type=Path, required=True)
    parser.add_argument("--package", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--commit", help="Require this exact clean package revision")
    args = parser.parse_args()
    raise SystemExit(run(args))


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
