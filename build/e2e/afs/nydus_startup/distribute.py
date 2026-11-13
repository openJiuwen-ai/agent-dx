#!/usr/bin/env python3
"""Run a configured wave plan using a caller's prewarm and readiness functions."""
from concurrent.futures import ThreadPoolExecutor


def distribute(plan, warm, await_source):
    nodes = set(plan["nodes"])
    ready = set(plan["seeds"])
    waves = plan["waves"]
    if not ready or not ready <= nodes or not waves:
        raise ValueError("the plan must identify known seeds and at least one wave")
    scheduled = ready.copy()
    for wave in waves:
        if not wave or any(target not in nodes or target in scheduled or source not in scheduled
                           for target, source in wave.items()):
            raise ValueError("each target must be new and depend on an earlier wave or seed")
        scheduled.update(wave)

    outcomes = {}
    for wave in waves:
        for source in set(wave.values()) - ready:
            await_source(source, outcomes[source])
            ready.add(source)
        with ThreadPoolExecutor(max_workers=len(wave)) as pool:
            pending = {target: pool.submit(warm, target, source) for target, source in wave.items()}
            for target, result in pending.items():
                outcomes[target] = result.result()
    return outcomes


def main():
    import argparse
    import json
    from pathlib import Path
    import subprocess
    import time

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("plan", type=Path)
    parser.add_argument("--timeout", type=float, default=120)
    args = parser.parse_args()
    plan = json.loads(args.plan.read_text())
    if args.timeout <= 0:
        parser.error("timeout must be positive")
    commands = plan["commands"]

    def call(action, node, source=None):
        command = commands[node][action]
        if not isinstance(command, list) or not command or not all(isinstance(x, str) for x in command):
            raise ValueError(f"{node}: {action} must be a nonempty command array")
        start = time.monotonic_ns()
        result = subprocess.run(command, check=False, timeout=args.timeout)
        record = {"node": node, "upstream": source, "action": action,
                  "started_ns": start, "finished_ns": time.monotonic_ns(),
                  "exit_code": result.returncode}
        print(json.dumps(record), flush=True)
        result.check_returncode()
        return record

    distribute(plan, lambda node, source: call("warm", node, source),
               lambda node, receipt: call("ready", node))


if __name__ == "__main__":
    main()
