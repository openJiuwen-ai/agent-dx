#!/usr/bin/env python3
"""Prepare/check real OS reboot acceptance on a dedicated Linux VM.

The external orchestrator reboots the VM between phases. Persistent systemd
units restart the same ADX deployment and unchanged external backends.
"""

import argparse
import asyncio
import hashlib
import json
import logging
from pathlib import Path
import signal
import subprocess
import time

from full_sdk import FullSDK, command


class UnitProcess:
    def __init__(self, unit):
        self.unit = unit

    def poll(self):
        state = subprocess.run(
            ["/usr/bin/systemctl", "is-active", self.unit], capture_output=True, text=True
        ).stdout.strip()
        return None if state in ("active", "activating") else 0

    def send_signal(self, sig):
        if sig != signal.SIGTERM:
            command("systemctl", "kill", "--signal", str(sig), self.unit)
        else:
            command("systemctl", "disable", "--now", self.unit)

    def wait(self, timeout):
        end = time.monotonic() + timeout
        while self.poll() is None:
            if time.monotonic() >= end:
                raise TimeoutError(self.unit)
            time.sleep(0.2)
        return 0


class HostRecovery(FullSDK):
    def spawn(self, name, args, env=None):
        unit = "adx-jbx-" + self.name + "-" + name + ".service"

        def quote(value):
            return json.dumps(str(value)).replace("%", "%%").replace("$", "$$")

        environment = {
            key: value
            for key, value in (env or {}).items()
            if key == "PATH" or key.startswith(("JIUWENBOX_", "RUST_LOG", "ADX_LOG_FORMAT"))
        }
        lines = [
            "[Unit]",
            "Description=ADX Jiuwenbox dedicated reboot acceptance",
            "After=network-online.target",
            "[Service]",
            "Type=simple",
            "Restart=on-failure",
            "RestartSec=3",
        ]
        # An explicit deployment stop drains/deletes Environments. A host service
        # interruption must retain desired state for boot recovery instead.
        if name == "deployment":
            lines += ["KillSignal=SIGKILL", "KillMode=control-group"]
        lines += ["Environment=" + quote(key + "=" + value) for key, value in environment.items()]
        lines += [
            "ExecStart=" + " ".join(quote(value) for value in args),
            "StandardOutput=append:" + str(self.run / (name + ".log")),
            "StandardError=inherit",
            "[Install]",
            "WantedBy=multi-user.target",
            "",
        ]
        path = Path("/etc/systemd/system") / unit
        if not (not path.exists()):
            raise AssertionError(path)
        path.write_text("\n".join(lines))
        command("systemctl", "daemon-reload")
        command("systemctl", "enable", "--now", unit)
        process = UnitProcess(unit)
        self.children[name] = process
        return process

    @classmethod
    def load(cls, root, name):
        self = cls.__new__(cls)
        self.root, self.name = root, name
        self.run = root / "runs" / name
        state = json.loads((self.run / "host-state.json").read_text())
        self.package = root / "package-sdk"
        self.config_path = self.run / "deployment.yaml"
        self.runtime_config = self.run / "runtime/config.json"
        self.namespace = state["namespace"]
        self.tls_dir = self.run / "tls"
        self.jw_socket = self.run / "j.sock"
        self.conch_socket = Path("/var/run/conch/conchd.sock")
        self.sha = state["bootstrap_sha256"]
        self.children = {name: UnitProcess(unit) for name, unit in state["units"].items()}
        self.results = []
        self.expected = {}
        self.driver_configs = {}
        return self, state

    async def stage(self):
        await self.prepare()
        ids = {}
        for backend in ("supervisor", "conch"):
            value = await self.sdk_rpc("create", backend)
            ids[backend] = value["id"]
            value["record"] = self.record(value["id"])
            (self.run / ("before-" + backend + ".json")).write_text(json.dumps(value, indent=2))
        detached = await self.sdk_rpc("detach")
        if not (detached == ids):
            raise AssertionError("acceptance condition failed")
        self.sdk.stdin.close()
        await self.sdk.wait()
        state = {
            "ids": ids,
            "records": {backend: self.record(environment_id) for backend, environment_id in ids.items()},
            "namespace": self.namespace,
            "boot_id": Path("/proc/sys/kernel/random/boot_id").read_text().strip(),
            "units": {name: process.unit for name, process in self.children.items()},
            "bootstrap_sha256": self.sha,
            "workspace_sha256": {
                backend: hashlib.sha256(
                    (self.run / "workspace" / (backend + ".bin")).read_bytes()
                ).hexdigest()
                for backend in ids
            },
        }
        (self.run / "host-state.json").write_text(json.dumps(state, indent=2))
        command("sync")
        logging.info("%s", json.dumps({"status": "prepared", "boot_id": state["boot_id"], "ids": ids}))

    async def verify(self, state):
        current_boot = Path("/proc/sys/kernel/random/boot_id").read_text().strip()
        report = {"status": "failed", "before_boot_id": state["boot_id"], "after_boot_id": current_boot}
        try:
            if not (current_boot != state["boot_id"]):
                raise AssertionError("kernel boot ID did not change")
            await self.until(self.node_ready, "boot restores node admission", 240)
            records = {
                backend: self.record(environment_id) for backend, environment_id in state["ids"].items()
            }
            for record in records.values():
                if not (record["result"]["state"] != "Failed" or record["result"]["restart_pending"]):
                    raise AssertionError("boot lost configured restart intent")
            await self.until(
                lambda: self.new_runtimes_ready(state["ids"], state["records"]), "boot cold recovery", 180
            )
            await self.start_sdk()
            attached = await self.sdk_rpc("attach", ids=state["ids"])
            if not (set(attached) == set(state["ids"])):
                raise AssertionError("acceptance condition failed")
            await self.check_all("host-reboot", state["ids"])
            for backend, environment_id in state["ids"].items():
                record = self.record(environment_id)
                if not (record["assignment"] == state["records"][backend]["assignment"]):
                    raise AssertionError(record)
                if not (
                    record["result"]["restart_attempts"]
                    == state["records"][backend]["result"]["restart_attempts"] + 1
                ):
                    raise AssertionError(record)
                if not (
                    hashlib.sha256((self.run / "workspace" / (backend + ".bin")).read_bytes()).hexdigest()
                    == state["workspace_sha256"][backend]
                ):
                    raise AssertionError("acceptance condition failed")
                await self.sdk_rpc("delete", backend)
                await self.until(lambda: self.record(environment_id) is None, "delete commit after reboot")
            await self.until(
                self.managed_backends_absent,
                "reboot native cleanup",
            )
            await self.audit(self.runtime_config)
            await self.sdk_rpc("close")
            self.sdk.stdin.close()
            await self.sdk.wait()
            report["status"] = "passed"
            report["cases"] = self.results
        except Exception as error:
            report["error"] = f"{type(error).__name__}: {error}"
            report["records"] = {
                backend: self.record(environment_id) for backend, environment_id in state["ids"].items()
            }
            raise
        finally:
            (self.run / "host-result.json").write_text(json.dumps(report, indent=2))

    async def cleanup(self, state):
        # Explicit deletion works for both Failed and Running Environment records.
        if self.children["deployment"].poll() is None:
            await self.start_sdk()
            await self.sdk_rpc("delete_ids", ids=state["ids"])
            await self.sdk_rpc("close")
            self.sdk.stdin.close()
            await self.sdk.wait()
            command(str(self.package / "bin/adxctl"), "--config", str(self.config_path), "stop")
        for name in reversed(list(state["units"])):
            command("systemctl", "disable", "--now", state["units"][name])
        for name, unit in state["units"].items():
            Path("/etc/systemd/system", unit).unlink()
        command("systemctl", "daemon-reload")
        # conchd is stopped; perform the final host-level resource audit.
        host = self.host_audit()
        if not (not host["leftovers"] and not host["conch_bridge_ports"]):
            raise AssertionError(host)
        logging.info("%s", json.dumps({"cleanup": "passed", "host_audit": host}))


async def main(args):
    if args.phase == "prepare":
        await HostRecovery(args.root.resolve(), args.run).stage()
    else:
        driver, state = HostRecovery.load(args.root.resolve(), args.run)
        if args.phase == "check":
            await driver.verify(state)
        else:
            await driver.cleanup(state)


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    parser = argparse.ArgumentParser()
    parser.add_argument("phase", choices=["prepare", "check", "cleanup"])
    parser.add_argument("--root", type=Path, required=True)
    parser.add_argument("--run", required=True)
    asyncio.run(main(parser.parse_args()))
