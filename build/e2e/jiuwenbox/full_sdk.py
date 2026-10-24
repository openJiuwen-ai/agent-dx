#!/usr/bin/env python3
"""Single-host ADX deployment and installed public SDK acceptance on real backends."""

import argparse
import asyncio
import hashlib
import json
import logging
import os
from pathlib import Path
import secrets
import signal
import socket
import subprocess
import time
import traceback

import httpx
import yaml
from accept import Acceptance, cgroup_roots, command as backend_command


def command(*command_args):
    try:
        return backend_command(*command_args)
    except subprocess.CalledProcessError as error:
        raise RuntimeError(f"{Path(command_args[0]).name} failed: {error.stderr}") from error


class FullSDK(Acceptance):
    async def prepare(self):
        # Check every listener before reserving the backends' fixed global paths.
        for port in (26379, 27000, 27001, 27003, 27090, 27091, 27092, 28444, 28888, 29444, 30443, 30880, 30881):
            with socket.socket() as s:
                s.bind(("127.0.0.1", port))
        with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as route:
            route.connect(("192.0.2.1", 1))
            control_host = route.getsockname()[0]
        await super().prepare()
        await self.stop(self.jw)
        workspace = self.run / "workspace"
        workspace.mkdir()
        policy = yaml.safe_load((self.run / "policy.yaml").read_text())
        mount = {"host_path": str(workspace), "sandbox_path": "/workspace", "mode": "rw"}
        policy["filesystem_policy"]["bind_mounts"].append(mount)
        policy["filesystem_policy"]["read_write"].append("/workspace")
        if "conch" in self.backends:
            policy["conch"]["filesystem_policy"]["bind_mounts"].append(mount)
        (self.run / "policy.yaml").write_text(yaml.safe_dump(policy))
        await self.start_jw()
        self.package = self.root / "package-sdk"
        await asyncio.to_thread(
            command, "python3", str(self.root / "adx/build/release/package.py"), "verify", str(self.package)
        )
        manifest = json.loads((self.package / "manifest.json").read_text())
        if not (manifest["files"]["runtime/adx-runtime-rootfs.img"] == self.sha):
            raise AssertionError("bootstrap differs from verified package")
        self.config_path = self.run / "deployment.yaml"
        self.namespace = "jbx-" + self.name
        self.tls_dir = self.run / "tls"
        await asyncio.to_thread(
            command, "python3", str(self.root / "adx/build/ci/rpc_certificates.py"), str(self.tls_dir)
        )
        for name in ("api-key", "redis-key"):
            path = self.run / name
            path.write_text(secrets.token_hex(32))
            path.chmod(0o600)

        def tls(name, peers):
            t = self.tls_dir
            return {
                "ca": str(t / "ca.pem"),
                "certificate": str(t / (name + ".pem")),
                "private_key": str(t / (name + ".key")),
                "server_name": "localhost",
                "peers": {key: str(t / (value + ".der")) for key, value in peers.items()},
            }

        services = []

        def add(environment_id, role, config, env=None):
            services.append(
                {
                    "id": environment_id,
                    "role": role,
                    "config": config,
                    "env": dict(RUST_LOG="info", ADX_LOG_FORMAT="json", **(env or {})),
                }
            )

        add(
            "redis",
            "redis",
            {
                "bind": "127.0.0.1",
                "port": 26379,
                "data_dir": str(self.run / "redis"),
                "appendfsync": "always",
                "password_file": str(self.run / "redis-key"),
            },
        )
        add(
            "coordinator",
            "coordinator",
            {
                "listen": "127.0.0.1:27000",
                "advertised_address": "https://127.0.0.1:27000",
                "scheduler_shards": 1,
                "placement": "pack",
                "rpc_timeout_seconds": 120,
                "heartbeat_timeout_seconds": 90,
                "metrics_listen": "127.0.0.1:27090",
                "autoscaling_listen": "127.0.0.1:27092",
                "tls": tls(
                    "coordinator", {"apiserver": "apiserver", "ingress": "ingress", "node:node1": "node"}
                ),
                "bootstrap_credentials": [
                    {
                        "key_file": str(self.run / "api-key"),
                        "tenant_id": "acceptance",
                        "administrator": True,
                        "expires_at_unix_seconds": 0,
                    }
                ],
            },
        )
        jw = json.loads(self.config("runtime").read_text())
        for klass in jw["classes"].values():
            klass["images"]["acceptance:v1"]["process"]["args"] = ["/bin/sleep", "3600"]
        self.runtime_config = self.run / "runtime/config.json"
        self.runtime_config.write_text(json.dumps(jw))
        common = {"ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE": "mtls"}
        node_env = {
            **common,
            "ADX_DATA_PLANE_RELAY_BIND": "127.0.0.1:28444",
            "ADX_DATA_PLANE_RELAY_HEALTH_BIND": "127.0.0.1:29444",
            "ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS": "10.83.0.0/24,100.64.0.0/10",
            "ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS": "127.0.0.1/32",
            "ADX_DATA_PLANE_RELAY_TLS_CERT": str(self.tls_dir / "node.pem"),
            "ADX_DATA_PLANE_RELAY_TLS_KEY": str(self.tls_dir / "node.key"),
            "ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA": str(self.tls_dir / "ca.pem"),
            "ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR": str(self.run / "proxy"),
        }
        add(
            "node1",
            "adxlet",
            {
                "node_id": "node1",
                "listen": "127.0.0.1:27001",
                "advertised_address": "127.0.0.1:27001",
                "proxy_address": "127.0.0.1:28444",
                "proxy_mode": "embedded",
                "proxy_socket": str(self.run / "proxy/route.sock"),
                "tls": tls("node", {"coordinator": "coordinator", "apiserver": "apiserver"}),
                "jiuwenbox": jw,
                "resource_source": {"kind": "auto", "disk_path": str(self.run), "valid_for_seconds": 30},
                "report_interval_seconds": 1,
                "rpc_timeout_seconds": 120,
                "runtime_ready_timeout_seconds": 120,
                "execd_port": 50090,
                "runtime_control": {
                    "listen": "0.0.0.0:27003",
                    "advertised_address": f"http://{control_host}:27003",
                    "key_file": str(self.run / "runtime-control-key"),
                },
                "execd_health_failure_threshold": 3,
                "execd_env": {},
                "metrics_listen": "127.0.0.1:27091",
            },
            node_env,
        )
        add(
            "api",
            "apiserver",
            {
                "listen": "127.0.0.1:28888",
                "loopback_http": True,
                "discovery": {"poll_seconds": 1},
                "ca": str(self.tls_dir / "ca.pem"),
                "certificate": str(self.tls_dir / "apiserver.pem"),
                "private_key": str(self.tls_dir / "apiserver.key"),
                "server_name": "localhost",
                "rpc_timeout_seconds": 120,
                "cache_entries": 1000,
                "auth_cache_ttl_seconds": 1,
                "ingress_mode": "standalone",
            },
        )
        ingress_env = {
            **common,
            "ADX_DATA_PLANE_INGRESS_TLS_BIND": "127.0.0.1:30443",
            "ADX_DATA_PLANE_INGRESS_PLAIN_BIND": "127.0.0.1:30881",
            "ADX_DATA_PLANE_INGRESS_HEALTH_BIND": "127.0.0.1:30880",
            "ADX_DATA_PLANE_INGRESS_TLS_CERT": str(self.tls_dir / "ingress.pem"),
            "ADX_DATA_PLANE_INGRESS_TLS_KEY": str(self.tls_dir / "ingress.key"),
            "ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS": "127.0.0.1/32",
            "ADX_DATA_PLANE_INGRESS_NODE_TLS_CA": str(self.tls_dir / "ca.pem"),
            "ADX_DATA_PLANE_INGRESS_NODE_TLS_SERVER_NAME": "localhost",
            "ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_CERT": str(self.tls_dir / "ingress.pem"),
            "ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_KEY": str(self.tls_dir / "ingress.key"),
            "ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS": "127.0.0.1:28888",
        }
        add(
            "ingress",
            "ingress",
            {
                "tls": tls("ingress", {"coordinator": "coordinator"}),
                "rpc_timeout_seconds": 5,
                "refresh_seconds": 1,
                "auth_cache_seconds": 1,
                "auth_cache_entries": 1000,
            },
            ingress_env,
        )
        image = str(self.root / "artifacts/execd.erofs")
        config = {
            "schema_version": 1,
            "package_dir": str(self.package),
            "state_dir": str(self.run / "ctl"),
            "redis_url": "redis://:" + (self.run / "redis-key").read_text() + "@127.0.0.1:26379/",
            "namespace": self.namespace,
            "restart_delay_ms": 1000,
            "stop_timeout_seconds": 90,
            "services": services,
            "runtime_profile": {
                "rootfs": {"runtime_class": "supervisor", "type": "local", "path": image, "readonly": False},
                "bootstrap": {
                    "type": "erofs",
                    "root": image,
                    "target": "/__adx",
                    "entrypoint": ["/__adx/usr/local/bin/adx-execd"],
                },
                "env": {"PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"},
            },
        }
        self.config_path.write_text(json.dumps(config, indent=2))
        self.config_path.chmod(0o600)
        (self.run / "proxy").mkdir(mode=0o700)
        for action in ("validate", "render"):
            argv = [str(self.package / "bin/adxctl"), "--config", str(self.config_path), action]
            if action == "render":
                argv += ["--output", str(self.run / "render")]
            (self.run / (action + ".log")).write_text(await asyncio.to_thread(command, *argv))
        self.deployment = self.spawn(
            "deployment", [str(self.package / "bin/adxctl"), "--config", str(self.config_path), "run"]
        )
        await self.until(lambda: self.node_ready(), "node admission and public TLS API ready")
        client_config = {
            "endpoint": "127.0.0.1:30443",
            "ca": str(self.tls_dir / "ca.pem"),
            "token_file": str(self.run / "api-key"),
            "execd_sha": hashlib.sha256((self.package / "runtime/adx-execd").read_bytes()).hexdigest(),
        }
        (self.run / "sdk-client.json").write_text(json.dumps(client_config))
        await self.start_sdk()

    async def start_sdk(self):
        with (self.run / "sdk-client.log").open("ab") as log:
            self.sdk = await asyncio.create_subprocess_exec(
                str(self.root / "sdk-venv/bin/python"),
                "-I",
                "-u",
                str(self.root / "adx/build/e2e/jiuwenbox/sdk_client.py"),
                str(self.run / "sdk-client.json"),
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=log,
            )
        self.sdk_info = json.loads(await asyncio.wait_for(self.sdk.stdout.readline(), 30))
        if not (self.sdk_info["ready"] and "/sdk-venv/" in self.sdk_info["sdk_location"]):
            raise AssertionError(self.sdk_info)
        (self.run / "sdk-installation.json").write_text(json.dumps(self.sdk_info, indent=2))

    def catalog(self):
        env = dict(
            os.environ,
            REDISCLI_AUTH=(self.run / "redis-key").read_text(),
        )
        return json.loads(
            subprocess.check_output(
                [
                    str(self.package / "bin/redis-cli"),
                    "-p",
                    "26379",
                    "--json",
                    "HGETALL",
                    f"adx:{{{self.namespace}}}:control:v1",
                ],
                env=env,
                text=True,
                timeout=5,
            )
        )

    def record(self, environment_id):
        value = self.catalog().get("environment:" + environment_id)
        record = json.loads(value) if value else None
        if record and record.get("result"):
            runtime = record["result"]["runtime"]
            runtime_id = runtime["id"]
            self.expected.setdefault(self.runtime_config.parent, {})[runtime_id] = dict(
                runtime_id=runtime_id,
                sandbox_id="adx-" + hashlib.sha256(runtime_id.encode()).hexdigest()[:28],
                spec=record["spec"],
                ip=runtime["ip"],
            )
        return record

    async def backend_evidence(self, records):
        evidence = []
        running = set()
        if "conch" in self.backends:
            native = await self.uds(self.conch_socket, "GET", "/api/v1/sandboxes?state=running&limit=5000")
            native.raise_for_status()
            if not (len(native.json()) < 5000):
                raise AssertionError("acceptance condition failed")
            running = {v.get("sandboxID", v.get("id")) for v in native.json()}
        for record in records:
            runtime = record["result"]["runtime"]
            environment_id = "adx-" + hashlib.sha256(runtime["id"].encode()).hexdigest()[:28]
            response = await self.uds(self.jw_socket, "GET", f"/api/v1/sandboxes/{environment_id}")
            response.raise_for_status()
            ref = response.json()
            if not (ref["phase"] == "ready" and ref["ip_address"] == runtime["ip"]):
                raise AssertionError(ref)
            if record["spec"]["runtime_class"] == "conch":
                if environment_id not in running:
                    raise AssertionError(environment_id)
            else:
                unified, memory, cpu, pids = [root / environment_id for root in cgroup_roots()]
                groups = [unified] if unified.is_dir() else [memory, cpu]
                if not unified.is_dir() and pids.is_dir():
                    groups.append(pids)
                for group in groups:
                    if not ((group / "cgroup.procs").read_text().strip()):
                        raise AssertionError(str(group))
            evidence.append(dict(runtime_id=runtime["id"], sandbox_id=environment_id, backend=ref))
        return evidence

    async def node_ready(self):
        try:
            catalog = self.catalog()
            node = json.loads(catalog.get("node:node1", "null"))
            if not node:
                return False
            if (
                not node["node"]["available"]
                or set(node["node"]["runtime_classes"]) != set(self.backends)
                or not node["session"]["routable"]
            ):
                return False
            async with httpx.AsyncClient(
                verify=str(self.tls_dir / "ca.pem"), trust_env=False, timeout=3
            ) as client:
                response = await client.get(
                    "https://127.0.0.1:30443/api/environment/v1/resources",
                    headers={"Authorization": "Bearer " + (self.run / "api-key").read_text()},
                )
                return response.is_success
        except (subprocess.CalledProcessError, ValueError, httpx.HTTPError):
            return False

    async def until(self, predicate, label, timeout=180):
        end = time.monotonic() + timeout
        last = None
        while time.monotonic() < end:
            try:
                value = predicate()
                if hasattr(value, "__await__"):
                    value = await value
                if value:
                    return value
            except Exception as error:
                last = repr(error)
            await asyncio.sleep(0.5)
        raise TimeoutError(f"{label}: {last}")

    async def sdk_rpc(self, action, backend=None, **options):
        self.sdk.stdin.write((json.dumps({"action": action, "backend": backend, **options}) + "\n").encode())
        await self.sdk.stdin.drain()
        line = await asyncio.wait_for(self.sdk.stdout.readline(), 210)
        if not (line):
            raise AssertionError("SDK client exited")
        result = json.loads(line)
        if not (result["ok"]):
            raise AssertionError(result)
        return result["value"]

    async def check_all(self, label, ids):
        for backend, environment_id in ids.items():
            value = await self.until(
                lambda: self.sdk_rpc("check", backend), label + " public SDK recovery", 60
            )
            if not (value["id"] == environment_id):
                raise AssertionError(value)
            value["record"] = self.record(environment_id)
            (self.run / (label + "-" + backend + ".json")).write_text(
                json.dumps(value, indent=2, ensure_ascii=False)
            )
        self.results.append({"case": label, "status": "passed"})
        logging.info("%s %s", "PASS", label)

    async def restart_service(self, service):
        status = json.loads(
            await asyncio.to_thread(
                command, str(self.package / "bin/adxctl"), "--config", str(self.config_path), "status"
            )
        )
        (self.run / ("status-before-" + service + ".json")).write_text(json.dumps(status, indent=2))
        # Inspect the supervisor's public process listing, never kill by name.
        entries = status["services"]
        entry = next(item for item in entries if item["id"] == service)
        pid = entry["pid"]
        if not (pid and str(self.package / "bin") in Path(f"/proc/{pid}/exe").resolve().as_posix()):
            raise AssertionError(entry)
        os.kill(pid, signal.SIGKILL)

        async def replaced():
            current = json.loads(
                await asyncio.to_thread(
                    command, str(self.package / "bin/adxctl"), "--config", str(self.config_path), "status"
                )
            )
            item = next(item for item in current["services"] if item["id"] == service)
            return item.get("pid") and item["pid"] != pid

        await self.until(replaced, "supervisor restarts " + service)

    async def scenarios(self, ids):
        for backend in self.backends:
            value = await self.sdk_rpc("create", backend)
            ids[backend] = value["id"]
            value["record"] = self.record(value["id"])
            mount = command("findmnt", "-J", "-T", str(self.run / "runtime/state/bootstrap" / self.sha))
            if not ("erofs" in mount and "ro," in mount):
                raise AssertionError(mount)
            value["host_erofs"] = json.loads(mount)
            (self.run / ("create-" + backend + ".json")).write_text(
                json.dumps(value, indent=2, ensure_ascii=False)
            )
            self.results.append({"case": backend + "-create-exec-files-erofs", "status": "passed"})
            logging.info("%s %s %s", "PASS", backend, "create-exec-files-erofs")
        before = {backend: self.record(environment_id) for backend, environment_id in ids.items()}
        await self.restart_service("node1")
        await self.until(lambda: self.same_runtimes_ready(ids, before), "adxlet adopts original runtimes")
        await self.check_all("adxlet-restart", ids)
        await self.restart_service("coordinator")
        await self.until(
            lambda: self.same_runtimes_ready(ids, before), "coordinator catalog and runtime adoption"
        )
        await self.check_all("coordinator-restart", ids)
        await self.restart_service("api")
        await self.check_all("apiserver-restart", ids)
        await self.restart_service("ingress")
        await self.check_all("ingress-restart", ids)
        old = {backend: self.record(environment_id) for backend, environment_id in ids.items()}
        await self.stop(self.jw, signal.SIGKILL)
        await self.start_jw()
        await self.until(lambda: self.new_runtimes_ready(ids, old), "Jiuwenbox restart cold recovery", 240)
        await self.check_all("jiuwenbox-restart", ids)
        if "conch" in self.backends:
            old_conch = self.record(ids["conch"])
            await self.stop(self.conch, signal.SIGKILL)
            await self.start_conch()
            await self.until(
                lambda: self.new_runtimes_ready({"conch": ids["conch"]}, {"conch": old_conch}),
                "conchd restart cold recovery",
                240,
            )
            await self.check_all("conchd-restart", ids)
        for backend, environment_id in ids.items():
            await self.sdk_rpc("delete", backend)
            await self.until(lambda: self.record(environment_id) is None, "public deletion committed")
            self.results.append({"case": backend + "-delete", "status": "passed"})
        await self.until(
            self.managed_backends_absent,
            "runtime deletion converges",
        )
        await self.audit(self.runtime_config)
        self.results.append({"case": "native-resource-audit", "status": "passed"})

    async def run_all(self):
        status = "failed"
        ids = {}
        try:
            await self.prepare()
            await self.scenarios(ids)
            status = "passed"
        except Exception as error:
            self.error = f"{type(error).__name__}: {error}"
            traceback.print_exc()
            raise
        finally:
            if hasattr(self, "sdk"):
                try:
                    await self.sdk_rpc("close")
                    self.sdk.stdin.close()
                    await asyncio.wait_for(self.sdk.wait(), 30)
                except Exception:
                    traceback.print_exc()
                    status = "failed"
            if hasattr(self, "deployment") and self.deployment.poll() is None:
                try:
                    (self.run / "stop.log").write_text(
                        await asyncio.to_thread(
                            command,
                            str(self.package / "bin/adxctl"),
                            "--config",
                            str(self.config_path),
                            "stop",
                        )
                    )
                    await asyncio.to_thread(self.deployment.wait, 120)
                except Exception:
                    traceback.print_exc()
                    status = "failed"
            if hasattr(self, "runtime_config"):
                try:
                    await self.audit(self.runtime_config)
                except Exception:
                    traceback.print_exc()
                    status = "failed"
            for name in ("jw", "conch"):
                process = getattr(self, name, None)
                if process is not None:
                    await self.stop(process)
            host = self.host_audit()
            if host["leftovers"] or host["conch_bridge_ports"]:
                status = "failed"
            (self.run / "results.json").write_text(
                json.dumps(
                    {
                        "status": status,
                        "cases": self.results,
                        "ids": ids,
                        "error": getattr(self, "error", None),
                    },
                    indent=2,
                )
            )
            if status != "passed":
                raise RuntimeError("full SDK acceptance failed")

    def same_runtimes_ready(self, ids, before):
        for backend, environment_id in ids.items():
            record = self.record(environment_id)
            if (
                not record
                or record["result"]["state"] != "Running"
                or record["result"]["runtime"]["id"] != before[backend]["result"]["runtime"]["id"]
            ):
                return False
        return True

    def new_runtimes_ready(self, ids, before):
        for backend, environment_id in ids.items():
            record = self.record(environment_id)
            if (
                not record
                or record["result"]["state"] != "Running"
                or record["result"]["runtime"]["id"] == before[backend]["result"]["runtime"]["id"]
            ):
                return False
        return True


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run", required=True)
    parser.add_argument("--runtime", action="append", choices=("supervisor", "conch"))
    args = parser.parse_args()
    asyncio.run(FullSDK(args.root.resolve(), args.run, args.runtime).run_all())
