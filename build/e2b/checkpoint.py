#!/usr/bin/env python3
"""Real E2B KVM checkpoint acceptance driver.

The state file contains only run metadata and resource IDs.  Credentials and
per-sandbox envd/traffic tokens stay in process memory and are never written
to stdout, the state file, or the evidence JSON.
"""
import argparse
import json
import logging
import os
import posixpath
import re
import shlex
import ssl
import sys
import time
import uuid
from pathlib import Path

PHASES = ("pause", "create-snapshot", "create-clones", "check", "cleanup", "all")
TOKEN_RE = re.compile(r"(?i)(bearer\s+|x-api-key[=:]\s*|token[=:]\s*)[^\s,}\]]+")


class AcceptanceError(RuntimeError):
    pass


def redact(value):
    return TOKEN_RE.sub(r"\1[redacted]", str(value))[:500]


def observation(text):
    """Parse the deliberately small guest observation written by the loop."""
    fields = text.strip().split()
    if len(fields) != 4 or not fields[0].isdigit() or not fields[2].isdigit():
        raise AcceptanceError("invalid guest observation")
    try:
        epoch = float(fields[3])
    except ValueError as exc:
        raise AcceptanceError("invalid guest epoch") from exc
    return {"pid": fields[0], "nonce": fields[1], "count": int(fields[2]), "epoch": epoch}


def require_status(response, statuses, context):
    if response.status_code not in statuses:
        raise AcceptanceError(f"{context}: HTTP {response.status_code}: {redact(response.text)}")
    return response


def snapshot_entries(payload):
    """Normalize the checked-in E2B object shape and reject malformed lists."""
    if isinstance(payload, dict):
        payload = payload.get("snapshots", [])
    if not isinstance(payload, list) or not all(isinstance(item, dict) for item in payload):
        raise AcceptanceError("invalid snapshot list response")
    return payload


def snapshot_name(run_id):
    return "kvm-" + run_id[:12]


def single_snapshot_attempt(send, timeout_type, expect_disconnect):
    """Invoke the mutating request once; timeout handling never retries it."""
    try:
        return send(), False
    except timeout_type as exc:
        if not expect_disconnect:
            raise AcceptanceError("snapshot response timed out") from exc
        return None, True


def bounded_read(call, timeout_type, deadline, sleep=time.sleep, now=time.monotonic):
    """Retry only transient read failures; callers still validate final status."""
    transient = []
    while True:
        try:
            response = call()
            if response.status_code not in (502, 503):
                return response, transient
            transient.append(response.status_code)
        except timeout_type:
            transient.append("timeout")
        if now() >= deadline:
            raise AcceptanceError("transient read did not recover")
        sleep(1)


def load_state(path):
    if not path.exists():
        return {"run_id": str(uuid.uuid4()), "boxes": {}, "snapshots": []}
    value = json.loads(path.read_text())
    if set(value) - {"run_id", "boxes", "snapshots", "snapshot_observation", "template", "phase"}:
        raise AcceptanceError("state has unsupported fields")
    if not isinstance(value.get("run_id"), str) or not isinstance(value.get("boxes", {}), dict):
        raise AcceptanceError("invalid state")
    # Refuse a state accidentally contaminated with secrets.
    if TOKEN_RE.search(json.dumps(value)):
        raise AcceptanceError("state appears to contain a credential")
    value.setdefault("snapshots", [])
    return value


def save_state(path, state):
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(state, sort_keys=True, indent=2) + "\n")


def clock_sample(box):
    before = time.time()
    # BusyBox date is only required to implement the POSIX %s conversion.
    guest = float(box.commands.run("date +%s").stdout.strip())
    after = time.time()
    skew = abs(guest - ((before + after) / 2))
    # Whole-second guest observations have up to one second quantization error;
    # retain a small scheduling allowance plus the observed request duration.
    clocksource = box.commands.run(
        "cat /sys/devices/system/clocksource/clocksource0/current_clocksource"
    ).stdout.strip()
    return {
        "clocksource": clocksource,
        "guest_epoch": guest,
        "host_midpoint": (before + after) / 2,
        "request_seconds": after - before,
        "skew_seconds": skew,
    }


def require_clock(sample):
    if sample["skew_seconds"] > 3 + sample["request_seconds"]:
        raise AcceptanceError(f"guest/host epoch midpoint skew {sample['skew_seconds']:.3f}s")


def loop_command(nonce, workdir="/tmp"):
    """Return a detached bash loop; $$ is the long-lived bash child PID."""
    if not isinstance(workdir, str) or not workdir.startswith("/"):
        raise AcceptanceError("loop workdir must be absolute")
    next_file = posixpath.join(workdir, ".e2b-kvm-observe.next")
    observed = posixpath.join(workdir, "e2b-kvm-observe")
    log = posixpath.join(workdir, "e2b-kvm-loop.log")
    pid_file = posixpath.join(workdir, "e2b-kvm-pid")
    loop = (
        "trap '' HUP; n=0; while :; do n=$((n+1)); now=$(date +%s); "
        "printf '%s %s %s %s\n' \"$$\" '"
        + nonce
        + "' \"$n\" \"$now\" > "
        + shlex.quote(next_file)
        + "; mv "
        + shlex.quote(next_file)
        + " "
        + shlex.quote(observed)
        + "; "
        "sleep 1; done"
    )
    command = shlex.join(["/bin/bash", "-c", loop])
    return (
        f"{command} </dev/null >{shlex.quote(log)} 2>&1 & child=$!; "
        f"printf '%s\\n' \"$child\" > {shlex.quote(pid_file)}"
    )


def main():
    output_logger = logging.Logger(__name__, level=logging.INFO)
    output_logger.addHandler(logging.StreamHandler(sys.stdout))
    error_logger = logging.Logger(__name__ + ".error", level=logging.ERROR)
    error_logger.addHandler(logging.StreamHandler(sys.stderr))
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument("--api-url", required=True)
    p.add_argument("--sandbox-url")
    p.add_argument("--key-file", type=Path, required=True)
    p.add_argument("--other-key-file", type=Path, required=True)
    p.add_argument("--ca", type=Path, required=True)
    p.add_argument("--template", default="base")
    p.add_argument("--state", type=Path, required=True)
    p.add_argument("--output", type=Path, required=True)
    p.add_argument("--phase", choices=PHASES, default="all")
    p.add_argument("--pause-wait-seconds", type=float, default=0)
    p.add_argument("--snapshot-response-timeout-seconds", type=float, default=900)
    p.add_argument("--expect-snapshot-disconnect", action="store_true")
    args = p.parse_args()
    if not args.api_url.startswith("https://"):
        p.error("--api-url must be HTTPS")
    if args.pause_wait_seconds < 0:
        p.error("--pause-wait-seconds must be >= 0")
    if args.snapshot_response_timeout_seconds <= 0:
        p.error("--snapshot-response-timeout-seconds must be > 0")
    os.environ["SSL_CERT_FILE"] = str(args.ca.resolve())
    state = load_state(args.state)
    evidence = {"result": "failed", "checks": [], "run_id": None}
    evidence["run_id"] = state["run_id"]
    key = other_key = None
    boxes, snapshots = {}, []
    try:
        import httpx
        from importlib.metadata import version
        if version("e2b") != "2.52.0":
            raise AcceptanceError("requires installed e2b==2.52.0")
        from e2b import Sandbox, SandboxQuery
        key = args.key_file.read_text().strip()
        other_key = args.other_key_file.read_text().strip()
        opts = {
            "api_key": key,
            "api_url": args.api_url,
            "sandbox_url": args.sandbox_url or args.api_url,
            "debug": False,
            "retries": 0,
            "request_timeout": 900,
        }
        verify = ssl.create_default_context(cafile=str(args.ca))
        client = httpx.Client(base_url=args.api_url, verify=verify, http2=True, timeout=900)

        def api(method, path, credential=key, **kw):
            return client.request(method, path, headers={"X-API-Key": credential}, **kw)

        def remember(name, box):
            boxes[name] = box
            state["boxes"][name] = box.sandbox_id
            save_state(args.state, state)

        def reconnect(name):
            if name in boxes:
                return boxes[name]
            box = Sandbox.connect(state["boxes"][name], timeout=900, **opts)
            boxes[name] = box
            return box

        def read(box):
            return observation(box.commands.run("cat /tmp/e2b-kvm-observe").stdout)

        def require_live(box, sample):
            if box.commands.run("kill -0 " + sample["pid"]).exit_code != 0:
                raise AcceptanceError("observed guest PID is not live")

        def detail(box_id, credential=key):
            return require_status(
                api("GET", f"/sandboxes/{box_id}", credential),
                (200,),
                "sandbox detail",
            ).json()
        if args.phase in ("pause", "all"):
            if "source" not in state["boxes"]:
                source = Sandbox.create(args.template, timeout=900, metadata={"runID": state["run_id"]}, **opts)
                remember("source", source)
                nonce = uuid.uuid4().hex
                # The loop owns nonce/count in RAM; the file is observation only.
                source.commands.run(loop_command(nonce))
                time.sleep(2)
            source = reconnect("source")
            pre = read(source)
            require_live(source, pre)
            pre_clock = clock_sample(source)
            require_status(
                api("POST", f"/sandboxes/{source.sandbox_id}/pause", json={"memory": True}),
                (204,),
                "pause",
            )
            time.sleep(args.pause_wait_seconds)
            resumed = Sandbox.connect(source.sandbox_id, timeout=900, **opts)
            boxes["source"] = resumed
            time.sleep(2)
            post = read(resumed)
            require_live(resumed, post)
            post_clock = clock_sample(resumed)
            evidence["checks"].append(
                {
                    "clock": {
                        "source": "guest date +%s versus host wall-clock midpoint",
                        "pause_wait_seconds": args.pause_wait_seconds,
                        "pre": pre_clock,
                        "post": post_clock,
                    }
                }
            )
            evidence["checks"].append({"process_before": pre, "process_after": post})
            require_clock(pre_clock)
            require_clock(post_clock)
            if (pre["pid"], pre["nonce"]) != (post["pid"], post["nonce"]) or post["count"] <= pre["count"]:
                raise AcceptanceError("pause/connect did not preserve live process memory")
            evidence["checks"].append({"pause_connect": {"pid": post["pid"], "count": post["count"]}})

        if args.phase in ("create-snapshot", "all"):
            source = reconnect("source")
            before = read(source)
            state["snapshot_observation"] = {
                item_key: before[item_key] for item_key in ("pid", "nonce", "count")
            }
            save_state(args.state, state)
            name = snapshot_name(state["run_id"])
            snapshot_id, disconnected = None, False
            try:
                # Exactly one mutation.  A disconnect is resolved only by
                # subsequent list polling; this code never repeats POST.
                response, disconnected = single_snapshot_attempt(
                    lambda: api(
                        "POST",
                        f"/sandboxes/{source.sandbox_id}/snapshots",
                        json={"name": name, "memory": True},
                        timeout=args.snapshot_response_timeout_seconds,
                    ),
                    httpx.ReadTimeout,
                    args.expect_snapshot_disconnect,
                )
                if disconnected:
                    response = None
                if response is None:
                    raise httpx.ReadTimeout("expected snapshot disconnect")
                require_status(response, (201,), "snapshot")
                snapshot_id = response.json().get("snapshotID")
                if not isinstance(snapshot_id, str) or not snapshot_id:
                    raise AcceptanceError("snapshot response lacks snapshotID")
                if args.expect_snapshot_disconnect:
                    if snapshot_id not in state["snapshots"]:
                        state["snapshots"].append(snapshot_id)
                        save_state(args.state, state)
                    raise AcceptanceError("snapshot response completed; expected disconnect")
            except httpx.ReadTimeout as exc:
                if not args.expect_snapshot_disconnect:
                    raise AcceptanceError("snapshot response timed out") from exc
                disconnected = True
            if disconnected:
                disconnect_evidence = {"snapshot_disconnect": {"expected": True, "observed": True,
                                        "response_timeout_seconds": args.snapshot_response_timeout_seconds,
                                        "transient_statuses": []}}
                evidence["checks"].append(disconnect_evidence)
                deadline = time.monotonic() + 180
                while time.monotonic() < deadline:
                    response, transient = bounded_read(
                        lambda: api("GET", f"/snapshots?limit=100&sandboxID={source.sandbox_id}&name={name}"),
                        httpx.ReadTimeout, deadline)
                    disconnect_evidence["snapshot_disconnect"]["transient_statuses"].extend(transient)
                    listed = snapshot_entries(require_status(response, (200,), "snapshot poll").json())
                    ready = [
                        item.get("snapshotID")
                        for item in listed
                        if isinstance(item.get("snapshotID"), str)
                    ]
                    if ready:
                        snapshot_id = ready[0]
                        break
                    time.sleep(1)
                if not snapshot_id:
                    raise AcceptanceError("snapshot absent after caller disconnect")
            if snapshot_id not in state["snapshots"]:
                state["snapshots"].append(snapshot_id)
                save_state(args.state, state)
            # Ready snapshot publication can precede source restoration.  Wait
            # for the source state before testing its data-plane continuity.
            deadline = time.monotonic() + 120
            while True:
                source_response, transient = bounded_read(
                    lambda: api("GET", f"/sandboxes/{source.sandbox_id}"),
                    httpx.ReadTimeout,
                    deadline,
                )
                if transient:
                    evidence["checks"].append({"source_restore_transient_statuses": transient})
                if source_response.status_code == 200 and source_response.json().get("state") == "running":
                    break
                if time.monotonic() >= deadline:
                    raise AcceptanceError("snapshot source did not return to running")
                time.sleep(1)
            time.sleep(2)
            after = read(source)
            if (before["pid"], before["nonce"]) != (after["pid"], after["nonce"]) or after["count"] <= before["count"]:
                raise AcceptanceError("snapshot did not restore continuing source memory")
            evidence["checks"].append(
                {
                    "snapshot_source": {
                        "snapshot_id": snapshot_id,
                        "pid": after["pid"],
                        "count": after["count"],
                        "disconnect_expected": args.expect_snapshot_disconnect,
                        "disconnect_observed": disconnected,
                        "response_timeout_seconds": args.snapshot_response_timeout_seconds,
                    }
                }
            )

        if args.phase in ("create-clones", "all"):
            if not state["snapshots"]:
                raise AcceptanceError("create-snapshot phase is required")
            snapshot_id = state["snapshots"][-1]
            for name in ("clone1", "clone2"):
                if name not in state["boxes"]:
                    remember(
                        name,
                        Sandbox.create(
                            snapshot_id,
                            timeout=900,
                            metadata={"runID": state["run_id"]},
                            **opts,
                        ),
                    )
            source = reconnect("source")
            base = state.get("snapshot_observation")
            if not isinstance(base, dict):
                raise AcceptanceError("snapshot checkpoint observation is missing")
            clones = [reconnect("clone1"), reconnect("clone2")]
            for clone in clones:
                got = read(clone)
                require_live(clone, got)
                if (got["pid"], got["nonce"]) != (base["pid"], base["nonce"]) or got["count"] < base["count"]:
                    raise AcceptanceError("clone lacks checkpointed process state")
            samples = [read(clone) for clone in clones]
            time.sleep(2)
            runtime_checks = []
            for clone, before in zip(clones, samples):
                after = read(clone)
                require_live(clone, after)
                identity_changed = (before["pid"], before["nonce"]) != (
                    after["pid"],
                    after["nonce"],
                )
                if identity_changed or after["count"] <= before["count"]:
                    raise AcceptanceError("clone process did not continue independently")
                clock = clock_sample(clone)
                require_clock(clock)
                runtime_checks.append(
                    {
                        "sandbox_id": clone.sandbox_id,
                        "before": before,
                        "after": after,
                        "clock": clock,
                    }
                )
            evidence["checks"].append({"clone_runtime": runtime_checks})
            source.files.write("/home/user/e2b-isolation", "source")
            clones[0].files.write("/home/user/e2b-isolation", "clone1")
            clones[1].files.write("/home/user/e2b-isolation", "clone2")
            if [x.files.read("/home/user/e2b-isolation") for x in [source] + clones] != ["source", "clone1", "clone2"]:
                raise AcceptanceError("clone writable storage is not isolated")
            evidence["checks"].append({"clones": [c.sandbox_id for c in clones]})

        if args.phase in ("check", "all"):
            source = reconnect("source")
            clone = reconnect("clone1")
            if api("GET", f"/sandboxes/{source.sandbox_id}", other_key).status_code not in (403, 404):
                raise AcceptanceError("other tenant can read source detail")
            connect_response = api(
                "POST",
                f"/v2/sandboxes/{source.sandbox_id}/connect",
                other_key,
                json={"timeout": 60},
            )
            if connect_response.status_code not in (403, 404):
                raise AcceptanceError("other tenant can connect source")
            source_tokens = detail(source.sandbox_id)
            clone_tokens = detail(clone.sandbox_id)
            # Only retain these secret values until the requests finish.
            st, ct = source_tokens.get("envdAccessToken"), clone_tokens.get("envdAccessToken")
            straff, ctraff = source_tokens.get("trafficAccessToken"), clone_tokens.get("trafficAccessToken")
            token_types_valid = isinstance(st, str) and isinstance(ct, str)
            traffic_token_types_valid = isinstance(straff, str) and isinstance(ctraff, str)
            tokens_are_distinct = st != ct
            traffic_tokens_are_distinct = straff != ctraff
            if not all(
                (
                    token_types_valid,
                    traffic_token_types_valid,
                    tokens_are_distinct,
                    traffic_tokens_are_distinct,
                )
            ):
                raise AcceptanceError("missing distinct envd/traffic tokens")
            headers = {"E2b-Sandbox-Id": clone.sandbox_id, "E2b-Sandbox-Port": "49983", "X-Access-Token": st}
            if client.post("/process.Process/List", headers=headers, json={}).status_code not in (401, 403):
                raise AcceptanceError("source envd token accepted for clone")
            clone.commands.run("/bin/busybox httpd -p 18080 -h /home/user >/dev/null 2>&1 &")
            headers.update({"E2b-Sandbox-Port": "18080", "E2b-Traffic-Access-Token": straff})
            if client.get("/e2b-isolation", headers=headers).status_code not in (401, 403):
                raise AcceptanceError("source traffic token accepted for clone route")
            headers["E2b-Traffic-Access-Token"] = ctraff
            headers["X-Access-Token"] = ct
            if client.get("/e2b-isolation", headers=headers).status_code != 200:
                raise AcceptanceError("clone traffic token failed for clone route")
            # A normal clone SDK call proves its own token still works.
            if clone.commands.run("printf token-ok").stdout != "token-ok":
                raise AcceptanceError("clone SDK token failed")
            evidence["checks"].append("tenant and per-box token isolation")

        if args.phase in ("cleanup",):
            pass
        if args.phase == "all":
            args.phase = "cleanup"
        if args.phase == "cleanup":
            # Recover every resource discoverable by this run's metadata before
            # deletion, including a create which completed after a prior crash.
            items = Sandbox.list(
                query=SandboxQuery(metadata={"runID": state["run_id"]}),
                limit=100,
                **opts,
            ).next_items()
            for item in items:
                if item.sandbox_id not in state["boxes"].values():
                    state["boxes"]["recovered-" + item.sandbox_id] = item.sandbox_id
            save_state(args.state, state)
            source_is_owned_and_present = False
            for name, box_id in state["boxes"].items():
                response = api("GET", f"/sandboxes/{box_id}")
                if response.status_code == 404:
                    continue
                value = require_status(response, (200,), "cleanup ownership detail").json()
                metadata = value.get("metadata") if isinstance(value, dict) else None
                if not isinstance(metadata, dict) or metadata.get("runID") != state["run_id"]:
                    raise AcceptanceError(f"cleanup ownership mismatch for {name}")
                if name == "source":
                    source_is_owned_and_present = True
            source_id = state["boxes"].get("source")
            # A timed-out caller can leave a completed snapshot absent from
            # state. Discover only this source and this run-derived name.
            if source_id:
                late_response = api(
                    "GET",
                    f"/snapshots?limit=100&sandboxID={source_id}"
                    f"&name={snapshot_name(state['run_id'])}",
                )
                late = snapshot_entries(
                    require_status(late_response, (200,), "late snapshot list").json()
                )
                for item in late:
                    snapshot_id = item.get("snapshotID")
                    if isinstance(snapshot_id, str) and snapshot_id not in state["snapshots"]:
                        state["snapshots"].append(snapshot_id)
                save_state(args.state, state)
            if source_id and state["snapshots"] and source_is_owned_and_present:
                owned_response = api("GET", f"/snapshots?limit=100&sandboxID={source_id}")
                owned = require_status(
                    owned_response,
                    (200,),
                    "snapshot ownership list",
                ).json()
                owned_ids = {item.get("snapshotID") for item in snapshot_entries(owned)}
                if not set(state["snapshots"]).issubset(owned_ids):
                    raise AcceptanceError("cleanup snapshot ownership mismatch")
            # IDs are now only this run's persisted/discovered resources.
            # Continue all cleanup and fail if any deletion fails.
            cleanup_errors = []
            for name, box_id in list(state["boxes"].items()):
                r = api("DELETE", f"/sandboxes/{box_id}")
                if r.status_code not in (204, 404):
                    cleanup_errors.append(f"box {name}: {r.status_code}")
            for snapshot_id in list(state["snapshots"]):
                r = api("DELETE", f"/templates/{snapshot_id}")
                if r.status_code not in (204, 404):
                    cleanup_errors.append(f"snapshot: {r.status_code}")
            if cleanup_errors:
                raise AcceptanceError("cleanup failed: " + "; ".join(cleanup_errors))
            remaining = Sandbox.list(
                query=SandboxQuery(metadata={"runID": state["run_id"]}),
                limit=100,
                **opts,
            ).next_items()
            if remaining:
                raise AcceptanceError("owned sandbox remains after cleanup")
            saved = snapshot_entries(
                require_status(api("GET", "/snapshots?limit=100"), (200,), "snapshot list").json()
            )
            if any(item.get("snapshotID") in state["snapshots"] for item in saved):
                raise AcceptanceError("owned snapshot remains after cleanup")
            state["boxes"] = {}
            state["snapshots"] = []
            state.pop("snapshot_observation", None)
            save_state(args.state, state)
            evidence["checks"].append("owned resource cleanup accepted")
        evidence["result"] = "passed"
    except Exception as exc:
        evidence["error"] = redact(exc)
        error_logger.error(json.dumps({"result": "failed", "error": evidence["error"]}))
    finally:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(json.dumps(evidence, sort_keys=True, indent=2) + "\n")
        if 'client' in locals():
            client.close()
    output_logger.info(json.dumps(evidence, sort_keys=True))
    return 0 if evidence["result"] == "passed" else 1


if __name__ == "__main__":
    sys.exit(main())
