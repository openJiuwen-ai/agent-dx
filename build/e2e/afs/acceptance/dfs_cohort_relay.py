#!/usr/bin/env python3
"""Four-role acceptance transport; Linux workers own all measured timestamps.

Keep failed protocol output and reap the whole cohort before product teardown.
This helper records transport/process closure, not data or performance acceptance.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import queue
import signal
import subprocess
import threading
import time

ROLES = ("ctl", "A", "B", "C")


def relay_commands(commands: dict[str, list[str]], output: Path, *, timeout: float = 95,
                   drain_timeout: float = 65, terminate_timeout: float = 2,
                   protocol: str = "multinode") -> dict[str, object]:
    if protocol not in ("multinode", "pair"):
        raise ValueError("protocol must be multinode or pair")
    roles = ROLES if protocol == "multinode" else ("ctl", "B", "C")
    workers = roles[1:]
    if set(commands) != set(roles) or any(not v or not all(isinstance(s, str) and s for s in v) for v in commands.values()):
        raise ValueError("exact protocol roles with nonempty argv required")
    if min(timeout, drain_timeout, terminate_timeout) <= 0:
        raise ValueError("positive timeouts required")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    events: list[dict[str, object]] = []
    errors: list[dict[str, str]] = []
    primary: dict[str, object] | None = None
    procs: dict[str, subprocess.Popen[str]] = {}
    handles: dict[str, tuple[object, object]] = {}
    threads: dict[str, threading.Thread] = {}
    messages: queue.Queue[tuple[str, str | None]] = queue.Queue()
    ended: set[str] = set()
    codes: dict[str, int | None] = {}
    failure_deadline: float | None = None

    def close_stdin(role: str) -> None:
        p = procs[role]
        try:
            if p.stdin and not p.stdin.closed:
                p.stdin.close()
        except (BrokenPipeError, OSError) as error:
            errors.append({"stage": "stdin_close", "role": role, "error": repr(error)})

    def fail(stage: str, error: object, role: str = "", *, authoritative: bool = False) -> None:
        nonlocal primary, failure_deadline
        record = {"stage": stage, "role": role, "error": str(error)}
        errors.append(record)
        # A coordinator's explicit rejection outranks a downstream pipe symptom.
        if primary is None or authoritative:
            primary = record
        if failure_deadline is None:
            failure_deadline = time.monotonic() + drain_timeout
            for member in procs:
                close_stdin(member)

    def pump(role: str, proc: subprocess.Popen[str]) -> None:
        try:
            assert proc.stdout is not None
            for line in proc.stdout:
                messages.put((role, line))
        finally:
            messages.put((role, None))

    def record_line(role: str, line: str) -> None:
        out = handles[role][0]
        out.write(line)
        out.flush()
        try:
            event = json.loads(line)
            if not isinstance(event, dict):
                raise ValueError("event must be an object")
        except (ValueError, TypeError) as error:
            fail("decode", error, role)
            return
        events.append({"source": role, "event": event})
        if role == "ctl" and event.get("event") == "SUMMARY" and event.get("status") != "DATA_RECORDED":
            fail("coordinator_rejected", event.get("error", event), role, authoritative=True)
            return
        if primary is not None:
            return  # Keep all tail output; a rejected coordinator receives no more events.
        kind = event.get("event")
        if role == "ctl":
            if kind in ("START", "ACK"):
                target = event.get("reader_id")
                if target not in workers:
                    fail("route", "invalid reader_id", role)
                    return
            elif kind in ("SUMMARY", "FINAL"):
                return
            else:
                fail("protocol", f"unexpected coordinator event {kind!r}", role)
                return
        else:
            allowed = ("READY", "C_DONE", "FINAL") if protocol == "multinode" else ("HELLO", "READY", "DONE", "FINAL")
            if kind not in allowed:
                fail("protocol", f"unexpected worker event {kind!r}", role)
                return
            target = "ctl"
        try:
            stream = procs[target].stdin
            if stream is None or stream.closed:
                raise BrokenPipeError("target stdin already closed")
            stream.write(line)
            stream.flush()
        except (BrokenPipeError, OSError) as error:
            fail("forward", error, role)

    def terminate_groups(sig: int) -> None:
        for role, proc in procs.items():
            # A child can retain stdout after its leader exits; signal that group too.
            if role not in ended or proc.poll() is None:
                try:
                    os.killpg(proc.pid, sig)
                    errors.append({"stage": "terminate", "role": role, "signal": str(sig)})
                except ProcessLookupError:
                    pass

    try:
        for role in roles:
            (output / (role + ".command.json")).write_text(json.dumps({"argv": commands[role],
                "timeout": timeout, "drain_timeout": drain_timeout, "timing_owner": "Linux worker/ctl only"}, indent=2) + "\n")
            out = (output / (role + ".stdout")).open("w")
            err = (output / (role + ".stderr")).open("w")
            handles[role] = (out, err)
            proc = subprocess.Popen(commands[role], stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                    stderr=err, text=True, bufsize=1, start_new_session=True)
            procs[role] = proc
            thread = threading.Thread(target=pump, args=(role, proc), daemon=True)
            threads[role] = thread
            thread.start()
        deadline = time.monotonic() + timeout
        while len(ended) != len(procs):
            limit = failure_deadline if failure_deadline is not None else deadline
            remaining = limit - time.monotonic()
            if remaining <= 0:
                fail("timeout", "cohort drain deadline" if failure_deadline else "cohort deadline")
                break
            try:
                role, line = messages.get(timeout=remaining)
            except queue.Empty:
                fail("timeout", "cohort drain deadline" if failure_deadline else "cohort deadline")
                break
            if line is None:
                ended.add(role)
            else:
                record_line(role, line)
    except Exception as error:
        fail("relay", repr(error))
    finally:
        for role in procs:
            close_stdin(role)
        if len(ended) != len(procs):
            terminate_groups(signal.SIGTERM)
        for role, proc in procs.items():
            try:
                codes[role] = proc.wait(timeout=terminate_timeout)
            except subprocess.TimeoutExpired:
                fail("wait_timeout", "cohort member did not exit", role)
                terminate_groups(signal.SIGKILL)
                codes[role] = proc.wait(timeout=terminate_timeout)
        for role, thread in threads.items():
            thread.join(timeout=terminate_timeout)
            if thread.is_alive():
                fail("drain_timeout", "stdout pump remains", role)
                terminate_groups(signal.SIGKILL)
                thread.join(timeout=terminate_timeout)
        while not messages.empty():
            role, line = messages.get_nowait()
            if line is None:
                ended.add(role)
            else:
                record_line(role, line)
        for role, proc in procs.items():
            if proc.stdout:
                proc.stdout.close()
            (output / (role + ".result.json")).write_text(json.dumps({"rc": codes.get(role),
                "reaped": proc.poll() is not None}, indent=2) + "\n")
        for out, err in handles.values():
            out.close()
            err.close()
    summaries = [v["event"] for v in events if v["source"] == "ctl" and v["event"].get("event") == "SUMMARY"]
    finals = {r: [v["event"] for v in events if v["source"] == r and v["event"].get("event") == "FINAL"] for r in workers}
    summary_complete = ((len(summaries) == 1 and summaries[0].get("status") == "DATA_RECORDED")
                        if protocol == "multinode" else not summaries)
    if primary is None and (not summary_complete
                           or any(len(rows) != 1 or rows[0].get("status") != "DATA_RECORDED" for rows in finals.values())):
        fail("missing_completion", "exact protocol completion and successful worker finals required")
    if primary is None and (set(codes) != set(roles) or any(rc != 0 for rc in codes.values())):
        fail("process_exit", codes)
    result = {"status": "PASS_TRANSPORT_CLOSURE_ONLY" if primary is None else "FAIL",
              "primary_error": primary, "errors": errors, "codes": codes,
              "all_started_processes_reaped": all(p.poll() is not None for p in procs.values()),
              "all_stdout_pumps_closed": all(not t.is_alive() for t in threads.values()),
              "started_roles": list(procs), "protocol": protocol, "no_data_or_performance_acceptance_claim": True}
    (output / "events.json").write_text(json.dumps(events, indent=2) + "\n")
    (output / "summary.json").write_text(json.dumps(result, indent=2) + "\n")
    return result


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--commands", type=Path, required=True, help="JSON object: ctl/A/B/C argv")
    parser.add_argument("--output", type=Path, required=True, help="fresh receipt directory")
    parser.add_argument("--timeout", type=float, default=95)
    parser.add_argument("--drain-timeout", type=float, default=65)
    parser.add_argument("--protocol", choices=("multinode", "pair"), default="multinode",
                        help="pair uses existing ctl/B/C HELLO/READY/DONE protocol; default unchanged")
    args = parser.parse_args()
    result = relay_commands(json.loads(args.commands.read_text()), args.output,
                            timeout=args.timeout, drain_timeout=args.drain_timeout, protocol=args.protocol)
    print(json.dumps(result, indent=2))
    return 0 if result["status"] == "PASS_TRANSPORT_CLOSURE_ONLY" else 1


if __name__ == "__main__":
    raise SystemExit(main())
