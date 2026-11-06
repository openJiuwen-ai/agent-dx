#!/usr/bin/env python3
"""Ordinary-file cache WAL reconciliation across Meta outage and Node restart.

Requires the isolated four-worker R1 topology. Only this task's B->Meta state and
A availability are faulted; finally removes this script's narrow firewall rule.
"""

import importlib.util
import json
import pathlib
import time

spec = importlib.util.spec_from_file_location("lab", pathlib.Path(__file__).parents[1] / "nydus/run.py")
lab = importlib.util.module_from_spec(spec)
spec.loader.exec_module(lab)

CATALOG = pathlib.Path(__file__).with_name("catalog_probe.py").read_text()


def catalog(node):
    return json.loads(lab.execute(node, "python3", "-c", CATALOG))


def signal(node, sig):
    lab.execute(node, "bash", "-euc", f'kill -{sig} "$(cat /workspace/afs/run/pid)"')


def crash_b():
    signal("worker-2", "KILL")
    lab.execute("worker-2", "bash", "-euc", "umount /workspace/afs/mount || true; rm /workspace/afs/run/pid")


def wait_for(predicate, timeout=12):
    until = time.monotonic() + timeout
    while time.monotonic() < until:
        value = predicate()
        if value:
            return value
        time.sleep(0.1)
    raise AssertionError("condition did not converge")


RULE = [
    "iptables",
    "-A",
    "OUTPUT",
    "-d",
    "10.203.0.10",
    "-p",
    "tcp",
    "--dport",
    "7400",
    "-m",
    "comment",
    "--comment",
    "afs-recovery-cache",
    "-j",
    "REJECT",
]


def unblock():
    command = RULE.copy()
    command[1] = "-D"
    lab.execute("worker-2", *command)


def start_b_then_block():
    # The existing maintenance loop waits one second before its first tick.
    # Block only after startup has registered/mounted, before that first tick.
    command = """mkdir -p /workspace/afs/run /workspace/afs/mount /output/logs
test ! -f /workspace/afs/run/pid
nohup /artifacts/afs/afs-node --config /artifacts/afs-configs/node-b.toml >>/output/logs/node-b.log 2>&1 </dev/null &
echo $! >/workspace/afs/run/pid
for i in $(seq 1 1000); do
 if mountpoint -q /workspace/afs/mount; then
  iptables -A OUTPUT -d 10.203.0.10 -p tcp --dport 7400 -m comment --comment afs-recovery-cache -j REJECT
  date +%s.%N
  exit 0
 fi
 sleep 0.01
done
exit 1
"""
    return lab.execute("worker-2", "bash", "-euc", command).strip()


result = {"scope": "ordinary file, one VM/four node namespaces", "phases": []}
blocked = False
b_crashed = False
a_paused = False
try:
    before = set(catalog("worker-1")["records"])
    name = "cache-recovery-" + str(time.time_ns()) + ".bin"
    write = f"""import os,hashlib,json
p='/workspace/afs/mount/{name}';body=hashlib.shake_256(b'{name}').digest(1048576)
f=os.open(p,os.O_CREAT|os.O_EXCL|os.O_WRONLY,0o600);assert os.write(f,body)==len(body);os.fsync(f);os.close(f)
print(json.dumps({{'sha256':hashlib.sha256(body).hexdigest(),'size':len(body)}}))
"""
    result.update(json.loads(lab.execute("worker-1", "python3", "-c", write)))
    result["file"] = name
    new = set(catalog("worker-1")["records"]) - before
    assert len(new) == 1, new
    chunk = new.pop()
    result["chunk"] = chunk
    read = (
        f"import pathlib,hashlib; b=pathlib.Path('/workspace/afs/mount/{name}').read_bytes();"
        f"assert hashlib.sha256(b).hexdigest()=='{result['sha256']}'"
    )
    lab.execute("worker-2", "python3", "-c", read)
    intent = wait_for(lambda: catalog("worker-2")["intents"].get(chunk))
    result["original_intent"] = intent
    lab.stop("node-b")
    result["blocked_at"] = start_b_then_block()
    blocked = True
    # A changed session requires a new advertisement. The failed publication
    # remains in the same catalog even though no user has read this incarnation.
    old_epoch = intent["copy"]["location"]["Node"]["node_epoch"]
    wait_for(
        lambda: any(
            i["copy"]["location"]["Node"]["node_epoch"] > old_epoch for i in catalog("worker-2")["intents"].values()
        )
    )
    pending = catalog("worker-2")["intents"][chunk]
    result["pending_publication"] = pending
    time.sleep(2)
    crash_b()
    b_crashed = True
    unblock()
    blocked = False
    lab.start("node-b")
    b_crashed = False
    refreshed = wait_for(lambda: (i if (i := catalog("worker-2")["intents"].get(chunk)) and i != pending else None))
    result["restarted_intent"] = refreshed
    time.sleep(2)
    signal("worker-1", "STOP")
    a_paused = True
    lab.execute("worker-3", "python3", "-c", read)
    signal("worker-1", "CONT")
    a_paused = False
    result["phases"].append("publication retry after Meta outage and B restart; cold C reads without A")
    lab.execute("worker-2", *RULE)
    blocked = True
    record = catalog("worker-2")["records"][chunk]
    assert record["read_cache_epoch"] > 0, record
    lab.execute(
        "worker-2",
        "python3",
        "-c",
        "import pathlib;p=pathlib.Path('/workspace/afs/state-replicated/dfs/chunks')/"
        f"'{chunk}';assert p.is_file();p.unlink()",
    )
    time.sleep(2)
    assert chunk in catalog("worker-2")["intents"]
    crash_b()
    b_crashed = True
    unblock()
    blocked = False
    lab.start("node-b")
    b_crashed = False
    wait_for(lambda: chunk not in catalog("worker-2")["intents"])
    result["phases"].append("retirement retry after physical loss, Meta outage and B restart")
    result["passed"] = True
finally:
    if a_paused:
        signal("worker-1", "CONT")
    if blocked:
        unblock()
    if b_crashed:
        lab.start("node-b")
    (lab.ROOT / "evidence/cache-reconciliation.json").write_text(json.dumps(result, indent=2))
print(json.dumps(result, indent=2))
