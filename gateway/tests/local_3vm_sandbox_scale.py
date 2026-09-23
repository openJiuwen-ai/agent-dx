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

import json
import os
import shlex
import statistics
import subprocess
import time
from collections import defaultdict
from pathlib import Path

from local_3vm_perf import COORDINATOR, REMOTE_ROOT, TOKEN, WORKER, Runner, sha256_line


REPO_ROOT = Path(__file__).resolve().parents[2]


class SandboxScaleRunner(Runner):
    def __init__(self):
        super().__init__()
        self.benchmark = Path(
            os.environ.get(
                "ADX_DATA_PLANE_HOST_BENCH",
                REPO_ROOT / "data-plane-gateway/target/release/examples/relay_perf",
            )
        )
        self.scale_rounds = int(os.environ.get("ADX_DATA_PLANE_SCALE_ROUNDS", "3"))
        self.request_floor = int(os.environ.get("ADX_DATA_PLANE_SCALE_REQUEST_FLOOR", "60000"))
        self.requests_per_worker = int(
            os.environ.get("ADX_DATA_PLANE_SCALE_REQUESTS_PER_WORKER", "100")
        )
        access_log = os.environ.get("ADX_DATA_PLANE_SCALE_ACCESS_LOG_ENABLED", "true")
        if access_log not in {"true", "false"}:
            raise ValueError("ADX_DATA_PLANE_SCALE_ACCESS_LOG_ENABLED must be true or false")
        self.access_log_enabled = access_log
        self.ca_path = self.evidence / "ca.crt"

    def prepare(self):
        self.preflight()
        if not self.benchmark.is_file():
            raise RuntimeError(f"host benchmark does not exist: {self.benchmark}")
        self.ca_path.write_text(self.remote(COORDINATOR, f"cat {REMOTE_ROOT}/ca.crt"))
        subprocess.run(
            [self.limactl, "copy", str(self.ca_path), f"{WORKER}:{REMOTE_ROOT}/ca.crt"],
            text=True,
            check=True,
        )
        self.restart_ingress_for_load()
        self.prepare_request_targets(100)
        self.collect_metrics("before-scale")
        hashes = sha256_line(self.benchmark)
        with (self.evidence / "release-sha256.txt").open("a") as stream:
            stream.write(hashes)

    def restart_ingress_for_load(self):
        # The functional E2E deliberately uses a 1 MiB rotation threshold to
        # prove rotation. A request ceiling test keeps audit logging enabled,
        # but uses a production-like threshold so rotation itself is not the
        # dominant CPU and I/O workload.
        command = f"""
set -e
kill $(cat {REMOTE_ROOT}/ingress-frontend.pid) 2>/dev/null || true
for attempt in $(seq 1 100); do
  if ! kill -0 $(cat {REMOTE_ROOT}/ingress-frontend.pid) 2>/dev/null; then
    break
  fi
  sleep 0.05
done
nohup env \\
  ADX_DATA_PLANE_INGRESS_ETCD_ENDPOINTS=http://{self.coordinator_ip}:2379 \\
  ADX_DATA_PLANE_INGRESS_TLS_BIND=0.0.0.0:8443 \\
  ADX_DATA_PLANE_INGRESS_PLAIN_BIND=0.0.0.0:8080 \\
  ADX_DATA_PLANE_INGRESS_HEALTH_BIND=0.0.0.0:18080 \\
  ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS=127.0.0.1:18888 \\
  ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ROUTES=exact:/control.txt \\
  ADX_DATA_PLANE_INGRESS_TLS_CERT={REMOTE_ROOT}/ingress.crt \\
  ADX_DATA_PLANE_INGRESS_TLS_KEY={REMOTE_ROOT}/ingress.key \\
  ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network \\
  ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS=127.0.0.0/8,192.168.104.0/24 \\
  ADX_DATA_PLANE_INGRESS_VALIDATE_IAM=false \\
  ADX_DATA_PLANE_INGRESS_DIRECT_PORT=18080 \\
  ADX_DATA_PLANE_LOG_DIR={REMOTE_ROOT}/logs \\
  ADX_DATA_PLANE_LOG_MAX_SIZE_MB=64 \\
  ADX_DATA_PLANE_LOG_MAX_FILES=4 \\
  ADX_DATA_PLANE_LOG_STDOUT=false \\
  ADX_DATA_PLANE_INGRESS_ACCESS_LOG_ENABLED={self.access_log_enabled} \\
  RUST_LOG=info \\
  {REMOTE_ROOT}/bin/adx-ingress >{REMOTE_ROOT}/logs/ingress-launcher.log 2>&1 &
echo $! >{REMOTE_ROOT}/ingress-frontend.pid
ready=0
for attempt in $(seq 1 600); do
  if curl -fsS http://127.0.0.1:18080/readyz >/dev/null; then
    ready=1
    break
  fi
  sleep 0.05
done
test "$ready" -eq 1
"""
        self.remote(COORDINATOR, command)

    def scale_cases(self):
        cases = set()
        for targets in (1, 2, 5, 10, 20, 50, 100):
            cases.add((targets, 8, "fixed-per-sandbox-8"))
        for per_sandbox in (1, 2, 4, 8, 16, 32, 64, 128, 256):
            cases.add((1, per_sandbox, "single-sandbox-ceiling"))
        for per_sandbox in (1, 2, 4, 8, 16, 32, 64, 72, 76, 77, 96):
            cases.add((10, per_sandbox, "ten-sandbox-ceiling"))
        for per_sandbox in (1, 2, 4, 8, 12, 16, 24, 32):
            cases.add((100, per_sandbox, "hundred-sandbox-ceiling"))
        return sorted(cases, key=lambda case: (case[0] * case[1], case[0], case[1], case[2]))

    def run_case(self, round_number, targets, per_sandbox, family):
        concurrency = targets * per_sandbox
        requests = max(self.request_floor, concurrency * self.requests_per_worker)
        case_id = f"r{round_number}-{family}-s{targets}-p{per_sandbox}-c{concurrency}"
        arguments = [
            f"{REMOTE_ROOT}/bin/relay_perf",
            "bench-http-tls",
            f"{self.coordinator_ip}:8443",
            f"{REMOTE_ROOT}/ca.crt",
            self.coordinator_ip,
            TOKEN,
            "/direct/perf-sandbox-{target}/bytes/128",
            str(targets),
            str(requests),
            str(concurrency),
            "2",
        ]
        display = f"{WORKER}: " + " ".join(shlex.quote(item) for item in arguments)
        self.commands.append(display)
        connects_before = self.node_connect_total()
        self.start_samplers(case_id)
        started = time.time_ns()
        command = " ".join(shlex.quote(item) for item in arguments)
        output = self.remote(
            WORKER,
            f"{command}; code=$?; echo __BENCH_EXIT__=$code",
            check=False,
        )
        ended = time.time_ns()
        resources = self.stop_samplers(case_id)
        connects_after = self.node_connect_total()
        (self.evidence / "raw" / f"{case_id}.log").write_text(output)
        payloads = [line for line in output.splitlines() if line.startswith("{")]
        exit_lines = [line for line in output.splitlines() if line.startswith("__BENCH_EXIT__=")]
        exit_code = int(exit_lines[-1].split("=", 1)[1]) if exit_lines else 1
        if payloads:
            metrics = json.loads(payloads[-1])
        else:
            metrics = {"errors": 1, "failure": output[-4000:]}
        metrics["process_exit_code"] = exit_code
        self.rows.append(
            {
                "family": family,
                "round": round_number,
                "logical_targets": targets,
                "per_sandbox_concurrency": per_sandbox,
                "concurrency": concurrency,
                "requests": requests,
                "backend_connect_delta": connects_after - connects_before,
                "started_ns": started,
                "ended_ns": ended,
                "metrics": metrics,
                "resources": resources,
            }
        )
        self.wait_for_idle()
        if self.request_round_cooldown > 0:
            time.sleep(self.request_round_cooldown)

    def wait_for_idle(self):
        command = f"""
idle=0
for attempt in $(seq 1 200); do
  ingress=$(curl -fsS http://127.0.0.1:18080/metrics | awk '/backend_http_idle_connections / {{print $2}}')
  node1=$(curl -fsS http://{self.worker_ip}:18443/metrics | awk '/relay_active_streams / {{print $2}}')
  node2=$(curl -fsS http://{self.worker2_ip}:18443/metrics | awk '/relay_active_streams / {{print $2}}')
  if test "${{ingress:-1}}" -eq 0 && test "${{node1:-1}}" -eq 0 && test "${{node2:-1}}" -eq 0; then
    idle=1
    break
  fi
  sleep 0.1
done
test "$idle" -eq 1
"""
        self.remote(COORDINATOR, command)

    def run(self):
        self.prepare()
        cases = self.scale_cases()
        for round_number in range(1, self.scale_rounds + 1):
            ordered = cases if round_number % 2 else list(reversed(cases))
            for targets, per_sandbox, family in ordered:
                self.run_case(round_number, targets, per_sandbox, family)
                self.write_scale_results()
        time.sleep(8)
        self.collect_metrics("after-idle")
        self.write_scale_results()

    def write_scale_results(self):
        with (self.evidence / "results.jsonl").open("w") as stream:
            for row in self.rows:
                stream.write(json.dumps(row, sort_keys=True) + "\n")
        (self.evidence / "command.txt").write_text("\n".join(self.commands) + "\n")
        grouped = defaultdict(list)
        for row in self.rows:
            grouped[
                (
                    row["family"],
                    row["logical_targets"],
                    row["per_sandbox_concurrency"],
                    row["concurrency"],
                )
            ].append(row)
        summary = []
        for key, rows in sorted(grouped.items(), key=lambda item: str(item[0])):
            current = {
                "family": key[0],
                "logical_targets": key[1],
                "per_sandbox_concurrency": key[2],
                "concurrency": key[3],
                "rounds": len(rows),
            }
            for metric in (
                "requests_per_second",
                "p50_ms",
                "p95_ms",
                "p99_ms",
                "generator_cpu_cores_avg",
                "generator_rss_peak_kib",
            ):
                values = [row["metrics"][metric] for row in rows if metric in row["metrics"]]
                if values:
                    current[f"{metric}_median"] = statistics.median(values)
                    current[f"{metric}_min"] = min(values)
                    current[f"{metric}_max"] = max(values)
            current["errors_total"] = sum(row["metrics"].get("errors", 0) for row in rows)
            current["backend_connect_delta_total"] = sum(
                row["backend_connect_delta"] for row in rows
            )
            summary.append(current)
        (self.evidence / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
        passed = all(
            row["metrics"].get("errors", 0) == 0
            and row["metrics"].get("process_exit_code", 1) == 0
            for row in self.rows
        )
        (self.evidence / "verdict.txt").write_text("PASS\n" if passed else "FAIL\n")


if __name__ == "__main__":
    SandboxScaleRunner().run()
