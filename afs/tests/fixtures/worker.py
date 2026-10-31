"""Private mount-namespace worker. Only the invoking test session owns its children."""

from __future__ import annotations

import base64
import hashlib
import json
import os
from pathlib import Path
import platform
import shutil
import socket
import subprocess
import sys
import traceback


def digest(path):
    h = hashlib.sha256()
    with open(path, "rb") as source:
        for block in iter(lambda: source.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


class Worker:
    def __init__(self):
        self.root = None
        self.processes = {}
        self.receipts = []
        self.listeners = []
        self.allow_meta = True
        self.mounts_before = {line.split()[0] for line in Path("/proc/self/mountinfo").read_text().splitlines()}

    def owned(self, value):
        path = Path(value)
        if self.root is None or not path.is_absolute() or not path.is_relative_to(self.root):
            raise ValueError("path is outside this worker run")
        if not path.resolve().is_relative_to(self.root.resolve()):
            raise ValueError("resolved path escapes this worker run")
        if ".." in path.parts:
            raise ValueError("parent traversal is forbidden")
        return path

    def inspect(self, req):
        parent = Path(req["base"])
        if not parent.is_absolute() or not parent.is_dir():
            raise ValueError("base must be an existing absolute Linux directory")
        tools = {
            name: shutil.which(name) for name in ("python3", "openssl", "findmnt", "fusermount3", "mount", "unshare")
        }
        findmnt = tools["findmnt"]
        if findmnt is None:
            raise ValueError("missing required worker tool: findmnt")
        fs = subprocess.check_output(
            [str(Path(findmnt).resolve()), "-n", "-o", "FSTYPE", "-T", str(parent)], text=True
        ).strip()
        for address, port in req.get("listeners", []):
            with socket.socket() as sock:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                sock.bind((address, port))
        machine = Path("/etc/machine-id").read_text().strip()
        dmi = Path("/sys/class/dmi/id/product_uuid")
        uuid = dmi.read_text().strip() if dmi.exists() else None
        return dict(
            system=platform.system(),
            machine=platform.machine(),
            uid=os.geteuid(),
            machine_id=machine,
            product_uuid=uuid,
            boot_id=Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            filesystem=fs,
            free=shutil.disk_usage(parent).free,
            fuse=Path("/dev/fuse").exists(),
            tools=tools,
            mount_namespace=os.readlink("/proc/self/ns/mnt"),
            network_namespace=os.readlink("/proc/self/ns/net"),
            pid_namespace=os.readlink("/proc/self/ns/pid"),
        )

    def dispatch(self, req):
        action = req["action"]
        if action == "inspect":
            return self.inspect(req)
        if action == "initialize":
            root = Path(req["root"])
            if not root.is_absolute() or ".." in root.parts or len(root.parts) < 4:
                raise ValueError("unsafe run directory")
            root.mkdir(parents=True, exist_ok=False, mode=0o755)
            self.root = root
            (root / ".afs-test-owner").write_text(req["token"])
            return {"root": str(root)}
        if action == "put":
            p = self.owned(req["path"])
            p.parent.mkdir(parents=True, exist_ok=True)
            with p.open("ab" if req.get("append") else "wb") as output:
                output.write(base64.b64decode(req["data"], validate=True))
            os.chmod(p, req.get("mode", 0o600))
            return {"size": p.stat().st_size}
        if action == "hash":
            p = self.owned(req["path"])
            return {"sha256": digest(p), "size": p.stat().st_size}
        if action == "command":
            env = os.environ.copy()
            env.update(req.get("env", {}))
            argv = req["argv"]
            if req.get("uid") is not None:
                # Drop identities inside a fresh interpreter, never in the threaded controller.
                drop = (
                    "import os,sys;os.setgroups([]);os.setgid(int(sys.argv[2]));os.setuid"
                    "(int(sys.argv[1]));os.execvpe(sys.argv[3],sys.argv[3:],os.environ)"
                )
                argv = ["python3", "-c", drop, str(req["uid"]), str(req["gid"])] + argv
            p = subprocess.run(
                argv,
                input=req.get("stdin"),
                text=True,
                capture_output=True,
                env=env,
                timeout=req.get("timeout", 30),
                check=False,
            )
            return {"returncode": p.returncode, "stdout": p.stdout, "stderr": p.stderr}
        if action == "parallel":
            from concurrent.futures import ThreadPoolExecutor

            if not 1 <= len(req["requests"]) <= 8:
                raise ValueError("parallel commands must have 1..8 bounded entries")
            if any(item["action"] != "command" for item in req["requests"]):
                raise ValueError("parallel is restricted to bounded commands")
            with ThreadPoolExecutor(max_workers=len(req["requests"])) as pool:
                return list(pool.map(self.dispatch, req["requests"]))
        if action == "start":
            role = req["role"]
            if role in self.processes:
                raise ValueError("role already running")
            log = self.owned(req["log"])
            log.parent.mkdir(parents=True, exist_ok=True)
            with log.open("ab") as stream:
                proc = subprocess.Popen(req["argv"], stdout=stream, stderr=stream, start_new_session=True)
            self.processes[role] = proc
            self.listeners.extend(req.get("listeners", []))
            return {"pid": proc.pid, "role": role}
        if action in ("stop", "wait"):
            return self.stop(req["role"], req.get("timeout", 30), terminate=action == "stop")
        if action == "status":
            return {role: {"pid": proc.pid, "returncode": proc.poll()} for role, proc in self.processes.items()}
        if action == "snapshot":
            mounts = [
                line
                for line in Path("/proc/self/mountinfo").read_text().splitlines()
                if line.split()[0] not in self.mounts_before
            ]
            return {"processes": self.dispatch({"action": "status"}), "mounts": mounts, "waits": self.receipts}
        if action == "collect":
            result = {}
            for path in self.root.glob("*.log"):
                size = path.stat().st_size
                with path.open("rb") as stream:
                    stream.seek(max(0, size - 4 * 1024 * 1024))
                    result[path.name] = {"data": base64.b64encode(stream.read()).decode(), "original_bytes": size}
            return result
        if action == "cleanup":
            self.allow_meta = req.get("allow_meta", self.allow_meta)
            return self.cleanup()
        raise ValueError("unknown worker action: " + str(action))

    def stop(self, role, timeout=30, terminate=True):
        proc = self.processes.get(role)
        if proc is None:
            return {"role": role, "not_started": True}
        if terminate and proc.poll() is None:
            proc.terminate()
        try:
            code = proc.wait(timeout=timeout)
        except subprocess.TimeoutExpired:
            record = {"role": role, "pid": proc.pid, "timeout": True}
            self.receipts.append(record)
            raise RuntimeError("normal stop timed out: " + role)
        record = {"role": role, "pid": proc.pid, "wait_status": code}
        self.receipts.append(record)
        del self.processes[role]
        if code != 0:
            raise RuntimeError("product did not exit normally: " + repr(record))
        return record

    def cleanup(self):
        errors = []
        for role in list(self.processes):
            if role == "M":
                continue
            try:
                self.stop(role)
            except Exception as exc:
                errors.append(str(exc))
        if self.allow_meta and not any(role != "M" for role in self.processes):
            try:
                self.stop("M")
            except Exception as exc:
                errors.append(str(exc))
        snap = self.dispatch({"action": "snapshot"})
        if snap["processes"] or snap["mounts"]:
            errors.append("owned processes or mounts remain")
        for host, port in set(tuple(pair) for pair in self.listeners):
            try:
                with socket.socket() as sock:
                    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                    sock.bind((host, port))
            except OSError as exc:
                errors.append(f"listener remains at {host}:{port}: {exc}")
        return dict(snap, errors=errors, status="FAIL" if errors else "PASS")


def main():
    worker = Worker()
    print(json.dumps({"ready": True, "protocol": 1}), flush=True)
    try:
        for line in sys.stdin:
            request = None
            try:
                request = json.loads(line)
                value = worker.dispatch(request)
                print(json.dumps({"id": request.get("id"), "ok": True, "value": value}), flush=True)
            except Exception as exc:
                print(
                    json.dumps(
                        {
                            "id": request.get("id") if isinstance(request, dict) else None,
                            "ok": False,
                            "error": f"{type(exc).__name__}: {exc}",
                            "traceback": traceback.format_exc(),
                        }
                    ),
                    flush=True,
                )
    finally:
        if worker.processes:
            worker.cleanup()


if __name__ == "__main__":
    main()
