#!/usr/bin/env python3
"""Privileged Linux acceptance against unmodified Jiuwenbox and Conch services.

Requires prepared artifacts under --root. Runs only on a dedicated host without
other Jiuwenbox/Conch services; retains logs and a JSON result on failure.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import signal
import shutil
import subprocess
import time

import httpx
import yaml


def command(*args, check=True):
    program = shutil.which(args[0])
    if program is None:
        raise FileNotFoundError(f"required executable not found: {args[0]}")
    return subprocess.run((program, *args[1:]), text=True, capture_output=True, check=check).stdout


def cgroup_roots(root=Path("/sys/fs/cgroup")):
    # Audit both supported layouts, including remnants on hybrid hosts.
    return [root / "jiuwenbox"] + [
        root / controller / "jiuwenbox" for controller in ("memory", "cpu", "pids")
    ]


class Acceptance:
    def __init__(self, root, run, backends=None):
        self.backends = tuple(backends or ("supervisor", "conch"))
        self.root = root
        self.run = root / "runs" / run
        self.run.mkdir(parents=True, exist_ok=False)
        self.name = run
        self.template_binding = {}
        if "conch" in self.backends:
            kernel_sha = hashlib.sha256((root / "artifacts/vmlinux").read_bytes()).hexdigest()
            self.template = "localhost/adx-acceptance:" + kernel_sha[:16]
            self.template_binding = {"template_name": self.template}
            self.template_ready = root / "artifacts" / ("template-ready-" + kernel_sha[:16])
        self.jw_socket = self.run / "j.sock"
        self.conch_socket = Path("/var/run/conch/conchd.sock")
        self.jw = None
        self.conch = None
        self.cni_path = None
        self.children = {}
        self.results = []
        self.drivers = []
        self.driver_configs = {}
        self.expected = {}
        self.sha = hashlib.sha256((root / "artifacts/execd.erofs").read_bytes()).hexdigest()

    def spawn(self, name, args, env=None):
        with (self.run / f"{name}.log").open("ab") as log:
            process = subprocess.Popen(
                args,
                stdin=subprocess.DEVNULL,
                stdout=log,
                stderr=subprocess.STDOUT,
                env=env,
                start_new_session=True,
            )
        self.children[name] = process
        return process

    async def uds(self, socket, method, path, **kwargs):
        async with httpx.AsyncClient(
            transport=httpx.AsyncHTTPTransport(uds=str(socket)), trust_env=False, timeout=120
        ) as client:
            return await client.request(method, "http://localhost" + path, **kwargs)

    async def wait_health(self, socket, process, timeout=120):
        end = time.monotonic() + timeout
        while time.monotonic() < end:
            if process.poll() is not None:
                raise AssertionError(f"service exited {process.returncode}: {socket}")
            try:
                if (await self.uds(socket, "GET", "/health")).is_success:
                    return
            except (httpx.HTTPError, OSError):
                pass
            await asyncio.sleep(0.2)
        raise TimeoutError(f"health not ready: {socket}")

    async def start_jw(self):
        env = dict(
            os.environ,
            JIUWENBOX_LISTEN=f"unix://{self.jw_socket}",
            JIUWENBOX_POLICY_PATH=str(self.run / "policy.yaml"),
            JIUWENBOX_NO_SUPERVISOR="1",
        )
        p = self.spawn(f"jiuwenbox-{len(self.children)}", [str(self.root / "venv/bin/jiuwenbox-server")], env)
        await self.wait_health(self.jw_socket, p)
        self.jw = p

    async def start_conch(self):
        p = self.spawn(
            f"conch-{len(self.children)}",
            [str(self.root / "conch/bin/conchd"), "--config", str(self.run / "conch.yaml")],
            dict(os.environ, PATH=f"{self.root}/tools/bin:{self.root}/bin:" + os.environ["PATH"]),
        )
        await self.wait_health(self.conch_socket, p)
        self.conch = p

    async def stop(self, process, sig=signal.SIGTERM):
        if process.poll() is None:
            process.send_signal(sig)
            await asyncio.to_thread(process.wait, 45)

    async def prepare(self):
        if not (os.geteuid() == 0):
            raise AssertionError("acceptance requires root")
        if "conch" in self.backends:
            if not (not self.conch_socket.exists()):
                raise AssertionError("existing Conch socket must be preserved")
        if not (
            not Path("/root/.jiuwenbox").exists() or (self.root / "artifacts/host-config-owned").exists()
        ):
            raise AssertionError("existing Jiuwenbox state must be preserved")
        if "conch" in self.backends:
            cni = Path("/etc/conch/cni/net.d")
            if not (
                not cni.exists()
                or not list(cni.iterdir())
                or (
                    (self.root / "artifacts/host-config-owned").exists()
                    and [p.name for p in cni.iterdir()] == ["10-adx-acceptance.conf"]
                )
            ):
                raise AssertionError("existing CNI configuration must be preserved")
            cni.mkdir(parents=True, exist_ok=True)
            self.cni_path = cni / "10-adx-acceptance.conf"
            self.cni_path.write_text(
                json.dumps(
                    {
                        "cniVersion": "1.0.0",
                        "name": "adxjbx-acceptance",
                        "type": "bridge",
                        "bridge": "adxjbc0",
                        "isGateway": True,
                        "ipMasq": True,
                        "ipam": {
                            "type": "host-local",
                            "subnet": "10.83.0.0/24",
                            "routes": [{"dst": "0.0.0.0/0"}],
                        },
                    }
                )
            )
        (self.root / "artifacts/host-config-owned").touch()
        policy = yaml.safe_load(
            (self.root / "jiuwenbox/src/jiuwenbox/configs/default-policy.yaml").read_text()
        )
        policy["process"] = {"run_as_user": "root", "run_as_group": "root"}
        policy["namespace"]["user"] = False
        policy["network"]["ingress"] = {"default": "allow"}
        policy["network"]["egress"] = {"default": "allow"}
        (self.run / "policy.yaml").write_text(yaml.safe_dump(policy))
        if "conch" in self.backends:
            conch = yaml.safe_load((self.root / "conch/config/config.yaml").read_text())
            conch["server"] = {"work_dir": "/var/run/conch", "state_dir": str(self.root / "conch-state")}
            fixture_path = self.root / "backend-fixture.json"
            fixture = json.loads(fixture_path.read_text()) if fixture_path.exists() else {}
            backend = fixture.get("conch_backend", "cloud-hypervisor")
            if backend not in ("cloud-hypervisor", "stratovirt"):
                raise AssertionError(backend)
            conch["sandbox"]["backend"] = backend
            conch["sandbox"][backend.replace("-", "_")]["binary"] = str(self.root / "bin" / backend)
            conch["network"]["warm_pool_size"] = 2
            conch["network"]["refill_threshold"] = 1
            conch["network"]["cni"]["plugin_bin_dirs"] = ["/usr/lib/cni", "/usr/libexec/cni", "/opt/cni/bin"]
            conch["volume"]["virtiofs"]["binary"] = str(self.root / "tools/bin/virtiofsd")
            (self.run / "conch.yaml").write_text(yaml.safe_dump(conch))
            (self.run / "conch.yaml").chmod(0o600)
            await self.start_conch()
            if not self.template_ready.exists():
                result = await asyncio.to_thread(
                    command,
                    str(self.root / "conch/bin/conch"),
                    "template",
                    "create",
                    "--config",
                    str(self.run / "conch.yaml"),
                    "--name",
                    self.template,
                    "--source",
                    "docker.io/library/ubuntu:24.04",
                    "--kernel",
                    str(self.root / "artifacts/vmlinux"),
                    "--initrd",
                    str(self.root / "conch/build-artifacts/conch-init-initramfs.cpio.gz"),
                )
                (self.run / "template.log").write_text(result)
                self.template_ready.write_text(result)
            if fixture.get("conch_template_selector") == "id":
                lines = self.template_ready.read_text().splitlines()
                template_id = next(
                    line.removeprefix("Template ID: ").strip()
                    for line in lines
                    if line.startswith("Template ID: ")
                )
                if not (template_id.startswith("sha256:") and len(template_id) == 71):
                    raise AssertionError(template_id)
                self.template_binding = {"template_id": template_id}
        await self.start_jw()
        recovered = []
        for previous in (self.root / "runs").glob("*/*/config.json"):
            if self.run in previous.parents:
                continue
            config = json.loads(previous.read_text())
            config["socket"] = str(self.jw_socket)
            recovery = previous.parent / "recovery.json"
            recovery.write_text(json.dumps(config))
            p = await self.driver(recovery)
            await self.ok(p, "reconcile", catalog=[])
            await self.kill_driver(p)
            recovered.append(recovery)
        for recovery in recovered:
            await self.audit(recovery)

    def config(self, case, socket=None):
        config = {
            "socket": str(socket or self.jw_socket),
            "conch_socket": str(self.conch_socket) if "conch" in self.backends else None,
            "state_dir": str(self.run / case / "state"),
            "bootstrap": {
                "image": str(self.root / "artifacts/execd.erofs"),
                "sha256": self.sha,
                "target": "/__adx",
                "command": ["/__adx/usr/local/bin/adx-execd"],
            },
            "classes": {},
        }
        for backend in self.backends:
            image = {"process": {"args": ["/bin/sleep", "600"], "cwd": "/", "user": "0:0"}}
            if backend == "conch":
                image.update(self.template_binding)
            config["classes"][backend] = {"images": {"acceptance:v1": image}}
        path = self.run / case / "config.json"
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(config))
        return path

    async def driver(self, config):
        with (config.parent / "driver.log").open("ab") as log:
            p = await asyncio.create_subprocess_exec(
                str(self.root / "target/debug/examples/jiuwenbox_acceptance"),
                str(config),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=log,
            )
        ready = await asyncio.wait_for(p.stdout.readline(), 30)
        if not (ready and json.loads(ready).get("ready")):
            raise AssertionError(f"driver start failed: {ready!r}")
        self.drivers.append(p)
        self.driver_configs[p.pid] = config
        return p

    async def rpc(self, driver, action, environment_id="", **kwargs):
        config = self.driver_configs[driver.pid]
        if action == "start":
            self.expected.setdefault(config.parent, {})[environment_id] = {
                "runtime_id": environment_id,
                "sandbox_id": "adx-" + hashlib.sha256(environment_id.encode()).hexdigest()[:28],
                "spec": kwargs["spec"],
                "ip": None,
            }
        driver.stdin.write((json.dumps(dict(action=action, id=environment_id, **kwargs)) + "\n").encode())
        await driver.stdin.drain()
        line = await asyncio.wait_for(driver.stdout.readline(), 180)
        if not (line):
            raise AssertionError(f"driver exited {driver.returncode}")
        result = json.loads(line)
        if action == "start" and result["ok"]:
            self.expected[config.parent][environment_id]["ip"] = result["value"]["ip"]
        return result

    async def ok(self, *args, **kwargs):
        result = await self.rpc(*args, **kwargs)
        if not (result["ok"]):
            raise AssertionError(result)
        return result["value"]

    @staticmethod
    def spec(environment_id, backend):
        return {
            "id": environment_id,
            "tenant_id": "acceptance",
            "image": "acceptance:v1",
            "runtime_class": backend,
            "resources": {"cpu_millis": 1000, "memory_bytes": 536870912, "disk_bytes": 0},
            "priority": 0,
            "execution": {
                "failover": False,
                "inherit_entrypoint": True,
                "rootfs": {"readonly": False, "source": {"Image": "acceptance:v1"}},
            },
        }

    async def create(self, driver, case, backend):
        environment = f"jbxp0-{self.name}-{case}"
        environment_id = environment + "-1"
        value = await self.ok(driver, "start", environment_id, spec=self.spec(environment, backend))
        ip = value["ip"]
        async with httpx.AsyncClient(trust_env=False, timeout=5) as client:
            end = time.monotonic() + 90
            while time.monotonic() < end:
                try:
                    status = (await client.get(f"http://{ip}:50090/control/v1/status")).json()
                    if status["phase"] == "running":
                        if not (status["identity"]["runtime_id"] == environment_id):
                            raise AssertionError(status)
                        break
                except (httpx.HTTPError, ValueError, KeyError):
                    pass
                await asyncio.sleep(0.2)
            else:
                raise TimeoutError(f"Execd not ready: {ip}")
            probe = (
                "sha256sum /__adx/usr/local/bin/adx-execd; grep ' /__adx ' /proc/mounts; "
                "if touch /__adx/SHOULD_NOT_WRITE 2>/dev/null; then exit 9; fi; echo EROFS_READONLY_OK"
            )
            response = await client.post(
                f"http://{ip}:50090/invoke",
                json={"action": "process.exec", "args": {"cmd": probe, "timeout": 10}},
            )
            response.raise_for_status()
            result = response.json()
            if not (result["exit_code"] == 0 and "EROFS_READONLY_OK" in result["stdout"]):
                raise AssertionError(result)
            expected = hashlib.sha256(
                (self.root / "target/x86_64-unknown-linux-musl/release/adx-execd").read_bytes()
            ).hexdigest()
            if expected not in result["stdout"]:
                raise AssertionError(result)
            (self.run / case / "guest-erofs.json").write_text(json.dumps(result, indent=2))
        mounts = command("findmnt", "-J", "-T", str(self.run / case / "state/bootstrap" / self.sha))
        if not ("erofs" in mounts and "ro," in mounts):
            raise AssertionError(mounts)
        (self.run / case / "host-erofs.json").write_text(mounts)
        return environment_id

    def records(self, config):
        return list(self.expected.get(config.parent, {}).values())

    def runtime_catalog(self, config):
        # Test-owned authority input, never a production driver recovery file.
        return [
            dict(
                spec=r["spec"],
                assignment=dict(
                    environment_id=r["spec"]["id"], node_id="acceptance", shard_id=0, generation=1, devices=[]
                ),
                state="Running",
                revision=1,
                runtime=dict(id=r["runtime_id"], ip=r["ip"]),
                checkpoint=None,
                last_operation=None,
                resources_held=True,
                failure=None,
                restart_attempts=0,
                restart_pending=False,
            )
            for r in self.records(config)
            if r["ip"]
        ]

    async def managed_backends_absent(self):
        sockets = [self.jw_socket] + ([self.conch_socket] if "conch" in self.backends else [])
        for socket in sockets:
            response = await self.uds(socket, "GET", "/api/v1/sandboxes?limit=5000")
            response.raise_for_status()
            values = response.json()
            if not (len(values) < 5000):
                raise AssertionError("possibly truncated native inventory")
            if any(v.get("sandboxID", v.get("id", "")).startswith("adx-") for v in values):
                return False
        for directory, pattern in [(root, "adx-*") for root in cgroup_roots()] + [
            (Path("/run/netns"), "jbx-adx-*")
        ]:
            if any(directory.glob(pattern)):
                return False
        return True

    async def audit(self, config):
        records = self.records(config)
        ids = {r["sandbox_id"] for r in records}
        runtime_ids = {r["runtime_id"] for r in records}
        leftovers = []
        for record in records:
            environment_id = record["sandbox_id"]
            for path in [root / environment_id for root in cgroup_roots()] + [
                Path("/run/netns") / ("jbx-" + environment_id),
                Path("/sys/class/net") / ("jwbH" + hashlib.sha256(environment_id.encode()).hexdigest()[:8]),
            ]:
                if path.exists():
                    leftovers.append(str(path))
            digest = hashlib.sha256(record["runtime_id"].encode()).hexdigest()[:20]
            if f"ADXJI{digest}" in command("iptables-save", "-t", "filter") or f"ADXJE{digest}" in command(
                "iptables-save", "-t", "filter"
            ):
                leftovers.append("iptables:" + environment_id)
            if "conch" in self.backends:
                native = await self.uds(self.conch_socket, "GET", f"/api/v1/sandboxes/{environment_id}")
                if native.status_code != 404:
                    leftovers.append("conch:" + environment_id + ":" + str(native.status_code))
        for proc in Path("/proc").iterdir():
            if not proc.name.isdigit() or int(proc.name) == os.getpid():
                continue
            try:
                cmdline = (proc / "cmdline").read_bytes().decode(errors="replace")
                # Select task-owned backend processes without leaking other environments.
                if any(environment_id in cmdline for environment_id in ids):
                    leftovers.append("process:" + proc.name)
                if "adx-execd" in cmdline:
                    environment = (proc / "environ").read_bytes().split(b"\0")
                    if any(
                        ("ADX_RUNTIME_ID=" + environment_id).encode() in environment
                        for environment_id in runtime_ids
                    ):
                        leftovers.append("execd:" + proc.name)
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                pass
        mount = config.parent / "state/bootstrap" / self.sha
        retained = command("findmnt", "-J", "-M", str(mount), check=False)
        if records:
            if not ("erofs" in retained and "ro," in retained):
                raise AssertionError(retained)
        if self.run in config.parents:
            if not (not (config.parent / "state/ownership.sqlite").exists()):
                raise AssertionError("acceptance condition failed")
        listed = await self.uds(self.jw_socket, "GET", "/api/v1/sandboxes")
        listed.raise_for_status()
        leftovers.extend("jiuwenbox:" + r["id"] for r in listed.json() if r["id"].startswith("adx-"))
        if "conch" in self.backends:
            native = await self.uds(self.conch_socket, "GET", "/api/v1/sandboxes?limit=5000")
            native.raise_for_status()
            if not (len(native.json()) < 5000):
                raise AssertionError("possibly truncated native inventory")
            leftovers.extend(
                "conch:" + r.get("sandboxID", r.get("id", ""))
                for r in native.json()
                if r.get("sandboxID", r.get("id", "")).startswith("adx-")
            )
        output = {
            "expected": records,
            "retained_bootstrap": json.loads(retained) if retained else None,
            "leftovers": leftovers,
        }
        (config.parent / "audit.json").write_text(json.dumps(output, indent=2))
        if not (not leftovers):
            raise AssertionError(output)

    async def kill_driver(self, p):
        if p.returncode is None:
            p.kill()
            await p.wait()

    async def normal(self, backend, mode):
        case = f"{backend}-{mode}"
        config = self.config(case)
        p = await self.driver(config)
        await self.ok(p, "classes")
        environment_id = await self.create(p, case, backend)
        if mode == "driver-restart":
            await self.kill_driver(p)
            p = await self.driver(config)
            if not ((await self.ok(p, "reconcile", catalog=self.runtime_catalog(config)))["count"] == 1):
                raise AssertionError("acceptance condition failed")
            if not ((await self.ok(p, "observe", environment_id))["running"]):
                raise AssertionError("acceptance condition failed")
        elif mode == "jiuwenbox-restart":
            await self.stop(self.jw, signal.SIGKILL)
            await self.start_jw()
            if not ((await self.ok(p, "reconcile", catalog=self.runtime_catalog(config)))["count"] == 0):
                raise AssertionError("acceptance condition failed")
        elif mode == "conchd-restart":
            await self.stop(self.conch, signal.SIGKILL)
            await self.start_conch()
        await self.ok(p, "remove", environment_id)
        await self.ok(p, "inventory")
        await self.audit(config)
        await self.kill_driver(p)

    async def late(self, backend):
        case = f"{backend}-late"
        ready, release = asyncio.Event(), asyncio.Event()
        proxy_socket = self.run / (case + ".sock")

        async def proxy(reader, writer):
            try:
                head = await reader.readuntil(b"\r\n\r\n")
                length = next(
                    (
                        int(line.split(b":", 1)[1])
                        for line in head.split(b"\r\n")
                        if line.lower().startswith(b"content-length:")
                    ),
                    0,
                )
                body = await reader.readexactly(length)
                method, path, _ = head.split(b"\r\n", 1)[0].decode().split(" ")
                response = await self.uds(
                    self.jw_socket, method, path, content=body, headers={"Content-Type": "application/json"}
                )
                if method == "POST" and path == "/api/v1/sandboxes":
                    if not (response.status_code == 201 and response.json()["phase"] == "ready"):
                        raise AssertionError(response.text)
                    ready.set()
                    await release.wait()
                payload = response.content
                writer.write(
                    (
                        f"HTTP/1.1 {response.status_code} Test\r\n"
                        "Content-Type: application/json\r\n"
                        f"Content-Length: {len(payload)}\r\nConnection: close\r\n\r\n"
                    ).encode()
                    + payload
                )
                await writer.drain()
            except (asyncio.IncompleteReadError, ConnectionError):
                pass
            finally:
                writer.close()

        server = await asyncio.start_unix_server(proxy, str(proxy_socket))
        config = self.config(case, proxy_socket)
        p = await self.driver(config)
        environment = f"jbxp0-{self.name}-{case}"
        environment_id = environment + "-1"
        result = await self.rpc(
            p, "start", environment_id, spec=self.spec(environment, backend), deadline_ms=1000
        )
        if not (not result["ok"] and "DeadlineExceeded" in result["debug"]):
            raise AssertionError(result)
        await asyncio.wait_for(ready.wait(), 120)
        if not ((await self.uds(self.jw_socket, "GET", "/api/v1/sandboxes")).json()):
            raise AssertionError("backend must actually exist after caller timeout")
        pending = asyncio.create_task(self.ok(p, "remove", environment_id))
        await asyncio.sleep(0.2)
        if not (not pending.done()):
            raise AssertionError("delete crossed the create barrier")
        release.set()
        await pending
        await self.ok(p, "inventory")
        await self.audit(config)
        await self.kill_driver(p)
        server.close()
        await server.wait_closed()

    def host_audit(self):
        leftovers = []
        directories = [(Path("/run/netns"), "jbx-*")] + [(root, "*") for root in cgroup_roots()]
        if "conch" in self.backends:
            directories.append((Path("/run/conch/netns"), "slot-*"))
        for directory, pattern in directories:
            leftovers.extend(
                str(path)
                for path in directory.glob(pattern)
                if path.is_dir() or directory not in cgroup_roots()
            )
        for proc in Path("/proc").iterdir():
            if not proc.name.isdigit():
                continue
            try:
                executable = (proc / "exe").resolve().name
                cmdline = (proc / "cmdline").read_bytes().decode(errors="replace")
                if (
                    executable in ["cloud-hypervisor", "stratovirt", "virtiofsd", "jiuwenbox_acceptance"]
                    and str(self.root) in cmdline
                ):
                    leftovers.append("backend-process:" + proc.name)
            except (FileNotFoundError, PermissionError, ProcessLookupError):
                pass
        retained_loops = command("losetup", "-j", str(self.root / "artifacts/execd.erofs")).splitlines()
        rules = command("iptables-save", "-t", "filter")
        leftovers.extend(line for line in rules.splitlines() if line.startswith((":ADXJI", ":ADXJE")))
        output = {
            "leftovers": leftovers,
            "retained_bootstrap_loops": retained_loops,
            "conch_bridge_ports": json.loads(command("ip", "-j", "link", "show", "master", "adxjbc0"))
            if "conch" in self.backends
            else [],
        }
        (self.run / "host-audit.json").write_text(json.dumps(output, indent=2))
        return output

    async def run_all(self):
        try:
            await self.prepare()
            for backend in ["supervisor", "conch"]:
                for mode in ["normal", "driver-restart", "jiuwenbox-restart", "late"]:
                    name = backend + "-" + mode
                    started = time.monotonic()
                    logging.info("%s %s", "RUN", name)
                    try:
                        if mode == "late":
                            await self.late(backend)
                        else:
                            await self.normal(backend, mode)
                        self.results.append(
                            {"case": name, "passed": True, "seconds": round(time.monotonic() - started, 3)}
                        )
                        logging.info("%s %s", "PASS", name)
                    except Exception as error:
                        self.results.append({"case": name, "passed": False, "error": repr(error)})
                        raise
            await self.normal("conch", "conchd-restart")
            self.results.append({"case": "conch-conchd-restart", "passed": True})
        finally:
            for driver in self.drivers:
                await self.kill_driver(driver)
            # Failure evidence is captured before shutdown; external sources are untouched.
            for process in reversed(list(self.children.values())):
                try:
                    await self.stop(process)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait()
            (self.run / "results.json").write_text(json.dumps(self.results, indent=2))
            host = self.host_audit()
        if not (len(self.results) == 9 and all(r["passed"] for r in self.results)):
            raise AssertionError("acceptance condition failed")
        if not (not host["leftovers"] and not host["conch_bridge_ports"]):
            raise AssertionError(host)
        logging.info("%s %s", "ACCEPTANCE_OK", json.dumps(self.results))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run", required=True)
    args = parser.parse_args()
    asyncio.run(Acceptance(args.root, args.run).run_all())


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    main()
