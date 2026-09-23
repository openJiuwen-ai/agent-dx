#!/usr/bin/env python3
# Copyright (c) Huawei Technologies Co., Ltd. 2026. All rights reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import hashlib
import json
import os
import shlex
import statistics
import subprocess
import time
from collections import defaultdict
from dataclasses import dataclass
from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
REMOTE_ROOT = "/tmp/adx-data-plane-3vm"
COORDINATOR = "adx-coordinator"
WORKER = "adx-worker-1"
WORKER2 = "adx-worker-2"
TOKEN = "e30.eyJzdWIiOiJtb2NrLXRlbmFudCIsImV4cCI6MH0.signature"


@dataclass(frozen=True)
class RawCase:
    path: str
    direction: str
    size: int
    iterations: int
    concurrency: int
    resource: bool = False


@dataclass(frozen=True)
class HttpCase:
    variant: str
    path: str
    requests: int
    concurrency: int
    resource: bool = False


@dataclass(frozen=True)
class RequestCase:
    variant: str
    body_size: int
    requests: int
    concurrency: int
    targets: int
    resource: bool = False


def sha256_line(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return f"{digest.hexdigest()}  {path}\n"


class Runner:
    def __init__(self):
        self.limactl = os.environ.get("LIMACTL", "limactl")
        self.rounds = int(os.environ.get("ADX_DATA_PLANE_PERF_ROUNDS", "3"))
        self.request_count = int(os.environ.get("ADX_DATA_PLANE_REQUESTS", "0"))
        self.request_round_cooldown = float(
            os.environ.get("ADX_DATA_PLANE_REQUEST_ROUND_COOLDOWN", "0")
        )
        run_id = os.environ.get("ADX_DATA_PLANE_PERF_RUN_ID", time.strftime("%Y%m%d-%H%M%S"))
        default_root = REPO_ROOT / ".adx-cache" / "data-plane-gateway-perf" / run_id
        self.evidence = Path(os.environ.get("ADX_DATA_PLANE_PERF_EVIDENCE_DIR", default_root))
        self.evidence.mkdir(parents=True, exist_ok=True)
        (self.evidence / "raw").mkdir(exist_ok=True)
        self.commands = []
        self.rows = []
        self.coordinator_ip = self.remote(COORDINATOR, "hostname -I | awk '{print $1}'").strip()
        self.worker_ip = self.remote(WORKER, "hostname -I | awk '{print $1}'").strip()
        self.worker2_ip = self.remote(WORKER2, "hostname -I | awk '{print $1}'").strip()
        self.bypass = f"127.0.0.1,localhost,{self.coordinator_ip},{self.worker_ip},{self.worker2_ip}"

    def remote(self, node, command, check=True):
        bypass = getattr(self, "bypass", "127.0.0.1,localhost")
        shell = (
            f"export NO_PROXY={shlex.quote(bypass)} no_proxy={shlex.quote(bypass)}; " + command
        )
        result = subprocess.run(
            [self.limactl, "shell", node, "bash", "-lc", shell],
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
        )
        if check and result.returncode != 0:
            raise RuntimeError(f"{node}: {command}\n{result.stdout}")
        return result.stdout

    def run_remote(self, node, command) -> None:
        """Check setup commands and retain their output for failure diagnosis."""
        output = self.remote(node, command)
        with (self.evidence / "setup.log").open("a") as stream:
            stream.write(f"[{node}]\n{output}\n")

    def start_samplers(self, case_id):
        specs = {
            COORDINATOR: [("ingress", f"{REMOTE_ROOT}/ingress-frontend.pid")],
            WORKER: [
                ("node", f"{REMOTE_ROOT}/relay.pid"),
                ("direct_relay", f"{REMOTE_ROOT}/direct-relay-perf.pid"),
                ("sandbox_relay", f"{REMOTE_ROOT}/sandbox-relay-perf.pid"),
                ("direct_http", f"{REMOTE_ROOT}/direct-http.pid"),
                ("sandbox_http", f"{REMOTE_ROOT}/sandbox-http.pid"),
            ],
            WORKER2: [
                ("node", f"{REMOTE_ROOT}/relay.pid"),
                ("sandbox_http", f"{REMOTE_ROOT}/sandbox-http.pid"),
            ],
        }
        for node, processes in specs.items():
            prefix = f"{REMOTE_ROOT}/results/resources/{case_id}-{node}"
            args = " ".join(
                f"--process {shlex.quote(name + '=' + pid_file)}" for name, pid_file in processes
            )
            command = (
                f"mkdir -p {REMOTE_ROOT}/results/resources; "
                f"rm -f {prefix}.stop {prefix}.json; "
                f"nohup python3 {REMOTE_ROOT}/process_resource_sampler.py {args} "
                f"--output {prefix}.json --stop-file {prefix}.stop --interval 0.05 "
                f">{prefix}.log 2>&1 & echo $! >{prefix}.pid"
            )
            self.run_remote(node, command)
        time.sleep(0.12)

    def stop_samplers(self, case_id):
        summaries = {}
        for node in (COORDINATOR, WORKER, WORKER2):
            prefix = f"{REMOTE_ROOT}/results/resources/{case_id}-{node}"
            command = (
                f"touch {prefix}.stop; "
                f"for attempt in $(seq 1 100); do "
                f"test -s {prefix}.json && cat {prefix}.json && exit 0; sleep 0.05; done; exit 1"
            )
            summaries[node] = json.loads(self.remote(node, command).strip().splitlines()[-1])
        return summaries

    def execute_json(self, case_id, command, resource):
        self.commands.append(command)
        if resource:
            self.start_samplers(case_id)
        started = time.time_ns()
        result = self.remote(COORDINATOR, command, check=False)
        ended = time.time_ns()
        resources = self.stop_samplers(case_id) if resource else {}
        (self.evidence / "raw" / f"{case_id}.log").write_text(result)
        lines = [line for line in result.splitlines() if line.startswith("{")]
        if not lines:
            metrics = {"errors": 1, "failure": result[-2000:]}
        else:
            metrics = json.loads(lines[-1])
        metrics["process_exit_ok"] = not bool(metrics.get("failure")) and metrics.get("errors", 0) == 0
        return metrics, resources, started, ended

    def raw_command(self, case):
        tail = f"{case.direction} {case.size} {case.iterations} {case.concurrency}"
        binary = f"{REMOTE_ROOT}/bin/relay_perf"
        if case.path == "direct":
            return f"{binary} bench-direct {self.worker_ip}:19000 {tail}"
        if case.path == "node":
            return f"{binary} bench-node {self.worker_ip}:8443 10.88.1.2 19001 {tail}"
        if case.path == "ingress-plain":
            return f"{binary} bench-ingress 127.0.0.1:8080 vm-sandbox-1 19001 {tail}"
        if case.path == "ingress-tls":
            return (
                f"{binary} bench-ingress-tls 127.0.0.1:8443 vm-sandbox-1 19001 "
                f"{REMOTE_ROOT}/ca.crt adx-ingress.local {tail}"
            )
        raise ValueError(case.path)

    def run_raw_case(self, round_number, case):
        case_id = (
            f"r{round_number}-raw-{case.path}-{case.direction}-{case.size}-"
            f"c{case.concurrency}"
        )
        metrics, resources, started, ended = self.execute_json(
            case_id, self.raw_command(case), case.resource
        )
        self.rows.append(
            {
                "family": "raw",
                "round": round_number,
                "variant": case.path,
                "direction": case.direction,
                "bytes_per_stream": case.size,
                "iterations": case.iterations,
                "concurrency": case.concurrency,
                "started_ns": started,
                "ended_ns": ended,
                "metrics": metrics,
                "resources": resources,
            }
        )

    def run_http_case(self, round_number, case: HttpCase):
        case_id = f"r{round_number}-http-{case.variant}-{case.path}-c{case.concurrency}"
        if case.variant == "direct":
            url = f"http://{self.worker_ip}:18082/{case.path}"
            security = ""
        else:
            url = f"https://127.0.0.1:8443/direct/vm-sandbox-1/{case.path}"
            security = f"--ca {REMOTE_ROOT}/ca.crt --token {shlex.quote(TOKEN)}"
        warmup = 10 if case.path == "small.txt" else 0
        command = (
            f"python3 {REMOTE_ROOT}/http_keepalive_bench.py {url} "
            f"--requests {case.requests} --concurrency {case.concurrency} --warmup {warmup} {security}"
        )
        metrics, resources, started, ended = self.execute_json(case_id, command, case.resource)
        self.rows.append(
            {
                "family": "http",
                "round": round_number,
                "variant": case.variant,
                "path": case.path,
                "direction": "download",
                "bytes_per_stream": 0 if case.path == "small.txt" else 32 * 1024 * 1024,
                "iterations": case.requests,
                "concurrency": case.concurrency,
                "started_ns": started,
                "ended_ns": ended,
                "metrics": metrics,
                "resources": resources,
            }
        )

    def node_connect_total(self):
        total = 0
        for address in (self.worker_ip, self.worker2_ip):
            output = self.remote(
                COORDINATOR,
                f"curl -fsS http://{address}:18443/metrics | "
                "awk '/data_plane_relay_connect_total / {print $2}'",
            )
            total += int(output.strip())
        return total

    def run_request_case(self, round_number, case: RequestCase):
        case_id = (
            f"r{round_number}-request-{case.variant}-b{case.body_size}-c{case.concurrency}-t{case.targets}"
        )
        if case.variant == "direct":
            url = f"http://{self.worker_ip}:18082/bytes/{case.body_size}"
            routing = ""
        else:
            url = "https://127.0.0.1:8443/"
            routing = (
                f"--ca {REMOTE_ROOT}/ca.crt --token {shlex.quote(TOKEN)} "
                f"--targets {case.targets} "
                f"--path-template '/direct/perf-sandbox-{{target:04d}}/bytes/{case.body_size}'"
            )
        command = (
            f"python3 {REMOTE_ROOT}/http_keepalive_bench.py {url} "
            f"--requests {case.requests} --concurrency {case.concurrency} --warmup 2 {routing}"
        )
        connects_before = self.node_connect_total() if case.variant != "direct" else 0
        metrics, resources, started, ended = self.execute_json(case_id, command, case.resource)
        connects_after = self.node_connect_total() if case.variant != "direct" else 0
        self.rows.append(
            {
                "family": "request",
                "round": round_number,
                "variant": case.variant,
                "direction": "request-response",
                "body_size": case.body_size,
                "bytes_per_stream": case.body_size,
                "iterations": case.requests,
                "concurrency": case.concurrency,
                "logical_targets": case.targets,
                "backend_connect_delta": connects_after - connects_before,
                "started_ns": started,
                "ended_ns": ended,
                "metrics": metrics,
                "resources": resources,
            }
        )

    def prepare_request_targets(self, count=100):
        targets_per_node = (count + 1) // 2
        for node, address, sandbox_subnet in (
            (WORKER, self.worker_ip, "10.88.1"),
            (WORKER2, self.worker2_ip, "10.88.2"),
        ):
            alias_commands = "; ".join(
                f"ip address replace {sandbox_subnet}.{index + 2}/24 dev yrsb-net"
                for index in range(targets_per_node)
            )
            # Enter the namespace and sudo once per node. Repeating both for
            # every alias adds seconds of process/namespace setup latency on
            # Lima and used to make a 100-sandbox benchmark spend minutes in
            # setup before sending its first request.
            aliases = (
                "sudo ip netns exec adx-sandbox bash -lc "
                f"{shlex.quote(alias_commands)}"
            )
            self.run_remote(
                node,
                f"{aliases}; "
                f"kill $(cat {REMOTE_ROOT}/direct-http.pid) 2>/dev/null || true; "
                f"sudo kill $(cat {REMOTE_ROOT}/sandbox-http.pid) 2>/dev/null || true; "
                f"nohup {REMOTE_ROOT}/bin/relay_perf http-server {address}:18082 "
                f">{REMOTE_ROOT}/logs/direct-http-rust.log 2>&1 & "
                f"echo $! >{REMOTE_ROOT}/direct-http.pid; "
                f"sudo ip netns exec adx-sandbox bash -lc 'nohup {REMOTE_ROOT}/bin/relay_perf "
                f"http-server 0.0.0.0:18080 >{REMOTE_ROOT}/logs/sandbox-http-rust.log 2>&1 & "
                f"echo $! >{REMOTE_ROOT}/sandbox-http.pid'",
            )
        commands = []
        for index in range(count):
            use_first = index % 2 == 0
            address = self.worker_ip if use_first else self.worker2_ip
            subnet = "10.88.1" if use_first else "10.88.2"
            bridge = f"{subnet}.{index // 2 + 2}"
            instance = f"perf-sandbox-{index:04d}"
            route = json.dumps(
                {
                    "instanceID": instance,
                    "instanceStatus": {"code": 3},
                    "tenantID": "mock-tenant",
                    "sandboxID": instance,
                    "nodeProxyAddress": f"{address}:8443",
                    "sandboxIP": bridge,
                },
                separators=(",", ":"),
            )
            commands.append(
                f"{REMOTE_ROOT}/bin/etcdctl --endpoints=http://127.0.0.1:2379 put "
                f"/adx/route/business/adxk/{instance} {shlex.quote(route)} >/dev/null"
            )
        self.run_remote(COORDINATOR, "; ".join(commands))
        self.run_remote(
            COORDINATOR,
            "for attempt in $(seq 1 100); do "
            "entries=$(curl -fsS http://127.0.0.1:18080/metrics | "
            "awk '/route_cache_entries / {print $2}'); "
            f"test \"${{entries:-0}}\" -ge {count + 2} && exit 0; sleep 0.1; done; exit 1",
        )
        self.run_remote(
            COORDINATOR,
            f"curl -fsS http://{self.worker_ip}:18082/bytes/128 >/dev/null; "
            f"curl -fsS --cacert {REMOTE_ROOT}/ca.crt -H 'Authorization: Bearer {TOKEN}' "
            "https://127.0.0.1:8443/direct/perf-sandbox-0000/bytes/128 >/dev/null",
        )

    def preflight(self):
        checks = [
            f"test -x {REMOTE_ROOT}/bin/relay_perf",
            f"test -s {REMOTE_ROOT}/ca.crt",
            "curl -fsS http://127.0.0.1:18080/readyz >/dev/null",
            f"curl -fsS http://{self.worker_ip}:18443/readyz >/dev/null",
        ]
        self.run_remote(COORDINATOR, "; ".join(checks))
        topology = {
            "nodes": {"coordinator": COORDINATOR, "worker": WORKER},
            "addresses": {
                "coordinator": self.coordinator_ip,
                "worker": self.worker_ip,
                "worker2": self.worker2_ip,
            },
            "architecture": self.remote(COORDINATOR, "uname -m").strip(),
            "kernel": self.remote(COORDINATOR, "uname -r").strip(),
            "cpu_per_vm": self.remote(COORDINATOR, "nproc").strip(),
            "memory": self.remote(COORDINATOR, "awk '/MemTotal/ {print $2}' /proc/meminfo").strip(),
        }
        (self.evidence / "topology.json").write_text(json.dumps(topology, indent=2) + "\n")
        source = subprocess.run(
            ["git", "status", "--short"], cwd=REPO_ROOT, text=True, capture_output=True, check=True
        ).stdout
        head = subprocess.run(
            ["git", "rev-parse", "HEAD"], cwd=REPO_ROOT, text=True, capture_output=True, check=True
        ).stdout.strip()
        (self.evidence / "source-identity.txt").write_text(f"HEAD={head}\n{source}")
        binaries = (
            REPO_ROOT / "build/output/data_plane/bin/adx-ingress",
            REPO_ROOT / "build/output/data_plane/bin/adx-relay",
            REPO_ROOT / ".adx-cache/data-plane-gateway-perf/bin/relay_perf",
        )
        hashes = "".join(sha256_line(path) for path in binaries)
        (self.evidence / "release-sha256.txt").write_text(hashes)

    def collect_metrics(self, suffix):
        (self.evidence / f"ingress-metrics-{suffix}.txt").write_text(
            self.remote(COORDINATOR, "curl -fsS http://127.0.0.1:18080/metrics")
        )
        (self.evidence / f"node-metrics-{suffix}.txt").write_text(
            self.remote(COORDINATOR, f"curl -fsS http://{self.worker_ip}:18443/metrics")
        )

    def run(self):
        self.preflight()
        self.collect_metrics("before")
        families = {
            item.strip()
            for item in os.environ.get("ADX_DATA_PLANE_PERF_FAMILIES", "raw,http").split(",")
            if item.strip()
        }
        paths = ["direct", "node", "ingress-plain", "ingress-tls"]
        raw_cases = []
        for concurrency, iterations in ((1, 300), (8, 600)):
            raw_cases.extend(RawCase(path, "download", 0, iterations, concurrency) for path in paths)
        for size, counts in ((1024 * 1024, (16, 64)), (100 * 1024 * 1024, (3, 8))):
            for direction in ("download", "upload"):
                for concurrency, iterations in ((1, counts[0]), (8, counts[1])):
                    raw_cases.extend(
                        RawCase(
                            path,
                            direction,
                            size,
                            iterations,
                            concurrency,
                            resource=size >= 100 * 1024 * 1024 and direction == "download",
                        )
                        for path in paths
                    )
        for direction in ("download", "upload"):
            raw_cases.extend(
                RawCase(path, direction, 1024 * 1024 * 1024, 1, 1, resource=True)
                for path in paths
            )

        for round_number in range(1, self.rounds + 1):
            if "raw" in families:
                cases = raw_cases if round_number % 2 else list(reversed(raw_cases))
                for case in cases:
                    self.run_raw_case(round_number, case)
            if "http" in families:
                for concurrency, requests in ((1, 500), (8, 1000)):
                    for variant in ("direct", "ingress-tls"):
                        self.run_http_case(
                            round_number, HttpCase(variant, "small.txt", requests, concurrency)
                        )
                for concurrency, requests in ((1, 4), (8, 16)):
                    for variant in ("direct", "ingress-tls"):
                        self.run_http_case(
                            round_number, HttpCase(variant, "blob.bin", requests, concurrency, resource=True)
                        )
            if "request" in families:
                if round_number == 1:
                    self.prepare_request_targets()
                cases = []
                for body_size in (128, 4096):
                    for concurrency in (1, 8, 32, 64):
                        requests = self.request_count or max(2_000, concurrency * 200)
                        cases.append(
                            RequestCase("direct", body_size, requests, concurrency, 1, resource=concurrency >= 32)
                        )
                        for targets in (1, 10, 100):
                            cases.append(
                                RequestCase(
                                    "ingress-tls",
                                    body_size,
                                    requests,
                                    concurrency,
                                    targets,
                                    resource=concurrency >= 32,
                                )
                            )
                if round_number % 2 == 0:
                    cases.reverse()
                for case in cases:
                    self.run_request_case(round_number, case)
                if round_number != self.rounds and self.request_round_cooldown > 0:
                    time.sleep(self.request_round_cooldown)
            self.write_results()
        self.collect_metrics("after")
        self.write_results()

    def write_results(self):
        with (self.evidence / "results.jsonl").open("w") as stream:
            for row in self.rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
        (self.evidence / "command.txt").write_text("\n".join(self.commands) + "\n")
        grouped = defaultdict(list)
        for row in self.rows:
            key = (
                row["family"],
                row["variant"],
                row.get("path", ""),
                row["direction"],
                row["bytes_per_stream"],
                row["concurrency"],
                row.get("logical_targets", 1),
            )
            grouped[key].append(row)
        summary = []
        for key, rows in sorted(grouped.items(), key=lambda item: str(item[0])):
            metric_names = (
                "throughput_mib_s",
                "requests_per_second",
                "p50_ms",
                "p95_ms",
                "p99_ms",
            )
            metrics = {}
            for name in metric_names:
                values = [row["metrics"][name] for row in rows if name in row["metrics"]]
                if values:
                    metrics[name + "_median"] = statistics.median(values)
                    metrics[name + "_min"] = min(values)
                    metrics[name + "_max"] = max(values)
            metrics["errors_total"] = sum(row["metrics"].get("errors", 0) for row in rows)
            summary.append(
                {
                    "family": key[0],
                    "variant": key[1],
                    "path": key[2],
                    "direction": key[3],
                    "bytes_per_stream": key[4],
                    "concurrency": key[5],
                    "logical_targets": key[6],
                    "rounds": len(rows),
                    **metrics,
                }
            )
        (self.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        (self.evidence / "verdict.txt").write_text(
            "PASS\n" if all(row["metrics"].get("errors", 0) == 0 for row in self.rows) else "FAIL\n"
        )


if __name__ == "__main__":
    Runner().run()
