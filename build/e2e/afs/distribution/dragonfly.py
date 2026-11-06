#!/usr/bin/env python3
"""Start/reset only the task-owned standalone scheduler and four Dragonfly peers."""

import argparse
import concurrent.futures
import json
import pathlib
import subprocess

p = argparse.ArgumentParser(description=__doc__)
p.add_argument("--workspace", type=pathlib.Path, required=True)
p.add_argument("--adx-vm", required=True)
p.add_argument("action", choices=["start", "reset", "stop"])
a = p.parse_args()
session = json.loads((a.workspace / "environment/open.json").read_text())["session"]


def execute(node, command):
    r = subprocess.run(
        [a.adx_vm, "exec", "--session", session, "--node", node, "--", "bash", "-euc", command],
        capture_output=True,
        text=True,
        timeout=90,
    )
    if r.returncode:
        raise RuntimeError(f"{node}: {r.stdout}\n{r.stderr}")


def stop(node, name, binary):
    execute(
        node,
        f"""if test -f /workspace/dragonfly/{name}.pid; then
pid=$(cat /workspace/dragonfly/{name}.pid)
if kill -0 "$pid" 2>/dev/null; then
test "$(cat /proc/$pid/comm)" = {binary}
kill -TERM "$pid"
for i in $(seq 1 100); do if ! kill -0 "$pid" 2>/dev/null; then break; fi; sleep 0.1; done
! kill -0 "$pid" 2>/dev/null
fi
rm /workspace/dragonfly/{name}.pid
fi""",
    )


def peer(i):
    # Peer shutdown can leave TCP listeners in TIME_WAIT. Wait outside the
    # measured workload instead of racing an immediate bind on restart.
    execute(
        f"worker-{i}",
        """python3 - <<'PY'
import socket,time
deadline=time.monotonic()+70
while True:
 try:
  for port in range(4000,4007):
   with socket.socket() as probe: probe.bind(('0.0.0.0',port))
  break
 except OSError:
  if time.monotonic()>=deadline: raise
  time.sleep(0.2)
PY""",
    )
    execute(
        f"worker-{i}",
        f"""mkdir -p /workspace/dragonfly
test ! -f /workspace/dragonfly/peer.pid
if test -S /workspace/dragonfly/dfdaemon.sock; then rm /workspace/dragonfly/dfdaemon.sock; fi
nohup /artifacts/dragonfly/dfdaemon --config /artifacts/dragonfly/client-{i}.yaml \\
 --dynconfig /artifacts/dragonfly/client-dynconfig.yaml --console \\
 >>/output/logs/dragonfly-{i}.log 2>&1 </dev/null &
echo $! >/workspace/dragonfly/peer.pid
for n in $(seq 1 50); do if curl -sf http://127.0.0.1:4002/metrics >/dev/null; then exit 0; fi; sleep 0.1; done
exit 1""",
    )


if a.action in ["reset", "stop"]:
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        list(pool.map(lambda i: stop(f"worker-{i}", "peer", "dfdaemon"), range(1, 5)))
    stop("mgmt", "scheduler", "scheduler")
if a.action == "reset":
    for peer_number in range(1, 5):
        execute(
            f"worker-{peer_number}",
            "test ! -f /workspace/dragonfly/peer.pid; test ! -L /workspace/dragonfly/peer; "
            "rm -rf -- /workspace/dragonfly/peer /workspace/dragonfly/config-cache",
        )
if a.action in ["start", "reset"]:
    execute(
        "mgmt",
        """mkdir -p /workspace/dragonfly
test ! -f /workspace/dragonfly/scheduler.pid
nohup /artifacts/dragonfly/scheduler --config /artifacts/dragonfly/scheduler.yaml \\
 --dynconfig /artifacts/dragonfly/scheduler-dynconfig.yaml \\
 >>/output/logs/scheduler-console.log 2>&1 </dev/null &
echo $! >/workspace/dragonfly/scheduler.pid""",
    )
    with concurrent.futures.ThreadPoolExecutor(4) as pool:
        list(pool.map(peer, range(1, 5)))
print(json.dumps({"action": a.action, "status": "passed"}))
