#!/usr/bin/env python3
"""Standalone OOM contract acceptance. Run only against an isolated fixture."""

import argparse
import dataclasses
import importlib.metadata
import json
import os
import subprocess
import sys
import time
import uuid
from pathlib import Path

parser = argparse.ArgumentParser(
    description="Real standalone OOM-to-SDK acceptance; Linux/cgroup v2, isolated AKernel container required"
)
parser.add_argument("--output", type=Path, required=True)
parser.add_argument("--container", required=True)
parser.add_argument("--key-file", type=Path, required=True)
parser.add_argument("--api", required=True)
parser.add_argument("--gateway", required=True)
parser.add_argument("--backend-root", default="/home/akernel/sandboxd/root/containers")
args = parser.parse_args()
ROOT = args.output.resolve()
ROOT.mkdir(parents=True, exist_ok=True)
NAME = args.container
os.environ.update(
    ADX_SERVER_ADDRESS=args.api,
    ADX_GATEWAY_ADDRESS=args.gateway,
    ADX_TOKEN=args.key_file.read_text().strip(),
    NO_PROXY="*",
    no_proxy="*",
)
from adx_sandbox import Sandbox


def docker(command_args, check=True):
    return subprocess.run(
        ["docker", "exec", NAME] + command_args, capture_output=True, text=True, check=check, timeout=30
    )


def emit(x):
    x["time_unix"] = time.time()
    with (ROOT / "samples.jsonl").open("a") as f:
        f.write(json.dumps(x, default=str) + "\n")
    print(json.dumps(x, default=str), flush=True)


def error(e):
    return {
        "type": type(e).__name__,
        "message": str(e),
        "fields": {
            k: getattr(e, k, None) for k in ["status_code", "code", "retry", "outcome", "request_id", "instance_id"]
        },
        "payload": getattr(e, "payload", None),
        "failure": getattr(e, "failure", None),
    }


def record(sid):
    raw = docker(
        ["/opt/adx/current/bin/redis-cli", "--raw", "HGET", "adx:{akernel}:control:v1", "environment:" + sid]
    ).stdout
    r = json.loads(raw) if raw.strip() else None
    if not r:
        return None
    return {k: r.get(k) for k in ["assignment", "result", "invalidated", "revision"]}


MAP_CODE = (
    "from pathlib import Path\n"
    "import json,sys\n"
    "sid=sys.argv[1];matches=[]\n"
    "for p in Path(sys.argv[2]).glob('*/config.json'):\n"
    " try:s=json.loads(p.read_text())\n"
    " except (ValueError,OSError):continue\n"
    " if s.get('annotations',{}).get('adx.environment_id')!=sid:continue\n"
    " cg=s['linux']['cgroupsPath'];path=(Path('/sys/fs/cgroup')/cg.lstrip('/')).resolve()\n"
    " if not (str(path).startswith('/sys/fs/cgroup/') and path.name not in ['akernel','system.slice']):raise "
    "RuntimeError('unsafe sandbox cgroup')\n"
    " d={'backend_id':p.parent.name,'cgroup':str(path),'cgroup_config':cg,'memory_limit':s['linux']"
    ".get('resources',{}).get('memory',{}),'runtime_id':s.get('annotations',{}).get('adx.runtime_id')}\n"
    " for n in ['memory.max','memory.current','memory.oom.group','memory.events']:\n"
    "  try:d[n]=(path/n).read_text().strip()\n"
    "  except OSError:d[n]=None\n"
    " matches.append(d)\n"
    "if len(matches)!=1:raise RuntimeError('ambiguous sandbox identity: '+str(len(matches)))\n"
    "print(json.dumps(matches[0]))"
)
MONITOR = (
    "from pathlib import Path\n"
    "import time,json,sys\n"
    "p=Path(sys.argv[1]);old=None\n"
    "for i in range(900):\n"
    " d={'unix':time.time(),'present':p.exists()}\n"
    " for n in ['memory.events','memory.oom.group','memory.max']:\n"
    "  try:d[n]=(p/n).read_text().strip()\n"
    "  except OSError:d[n]=None\n"
    " try:d['memory.current']=int((p/'memory.current').read_text())\n"
    " except OSError:d['memory.current']=None\n"
    " key=(d['present'],d['memory.events'],d['memory.oom.group'])\n"
    " if key!=old:print(json.dumps(d),flush=True);old=key\n"
    " if not d['present']:break\n"
    " time.sleep(.1)"
)
PRESSURE = (
    "import os,time\n"
    "blocks=[]\n"
    "for i in range(1024):\n"
    " b=bytearray(1024*1024)\n"
    " for j in range(0,len(b),4096):b[j]=1\n"
    " blocks.append(b)\n"
    " if i%16==0:print('allocated_mib='+str(i+1),flush=True)\n"
    " time.sleep(.002)\n"
    "print('UNEXPECTED_ALLOCATION_COMPLETED',flush=True)\n"
    "time.sleep(2)\n"
)


def main():
    if (ROOT / "samples.jsonl").exists():
        raise RuntimeError("Refuse to overwrite previous run")
    parent_before = json.loads(subprocess.check_output(["docker", "inspect", NAME], text=True))[0]
    before = docker(["sbox", "list"]).stdout
    if [line for line in before.splitlines()[1:] if line.strip()]:
        raise RuntimeError("Standalone has unrelated backends; defer OOM test")
    identity = {
        "parent_before": {k: parent_before["State"][k] for k in ["Pid", "StartedAt"]},
        "sdk": importlib.metadata.version("adx-sandbox"),
        "image": subprocess.check_output(["docker", "inspect", "-f", "{{.Image}}", NAME], text=True).strip(),
        "container": NAME,
        "release_commit": json.loads(docker(["cat", "/opt/adx/current/manifest.json"]).stdout).get("commit"),
        "tests": ["runc-process", "runsc-process", "firecracker-process", "runc-environment-group"],
    }
    (ROOT / "identity.json").write_text(json.dumps(identity, indent=2) + "\n")
    emit({"kind": "identity", **identity})
    results = []
    for case, runtime, group in [
        ("runc-process", "runc", False),
        ("runsc-process", "runsc", False),
        ("firecracker-process", "firecracker", False),
        ("runc-environment-group", "runc", True),
    ]:
        sb = None
        mon = None
        log = None
        case_data = {"case": case, "runtime": runtime, "memory_mib": 256, "oom_group_enabled_by_test": group}
        try:
            emit({"kind": "case-start", **case_data})
            requested_name = "oom-probe-" + uuid.uuid4().hex[:10]
            case_data["environment_id"] = "default-" + requested_name
            sb = Sandbox(
                image="swr.cn-north-4.myhuaweicloud.com/openyuanrong/e2e-oci:python312-curl-http-20260927",
                runtime=runtime,
                memory=256,
                mem_limit=256,
                cpu=1000,
                storage_mb=None if runtime == "runc" else 64,
                idle_timeout=300,
                failover=False,
                name=requested_name,
            )
            case_data["environment_id"] = sb.id
            proof = sb.commands.run(
                (
                    "echo OOM_PREFLIGHT_OK; cat /proc/meminfo | head -n 3; cat /proc/self/cgroup; grep cgroup "
                    "/proc/self/mountinfo || "
                    "true"
                ),
                timeout=10,
            )
            if proof.exit_code != 0 or "OOM_PREFLIGHT_OK" not in proof.stdout:
                raise RuntimeError("OOM preflight command failed: " + json.dumps(dataclasses.asdict(proof)))
            case_data["preflight_command"] = dataclasses.asdict(proof)
            mapping = json.loads(docker(["python3", "-c", MAP_CODE, sb.id, args.backend_root]).stdout)
            case_data["backend"] = mapping
            cap = mapping["memory.max"]
            if runtime != "firecracker":
                if not cap or cap == "max" or not 0 < int(cap) <= 512 * 1048576:
                    raise RuntimeError("unsafe sandbox memory limit: " + json.dumps(mapping))
            else:
                memtotal = (
                    int(next((line.split()[1] for line in proof.stdout.splitlines() if line.startswith("MemTotal:"))))
                    * 1024
                )
                if not 0 < memtotal <= 512 * 1048576:
                    raise RuntimeError("unsafe guest memory limit: " + str(memtotal))
            if group:
                docker(
                    [
                        "python3",
                        "-c",
                        (
                            "from pathlib import Path\nimport sys\np=Path(sys.argv[1])\nif not p.is_dir():raise "
                            'RuntimeError("sandbox cgroup is absent")\n'
                            '(p/"memory.oom.group").write_text("1")'
                        ),
                        mapping["cgroup"],
                    ]
                )
            log = (ROOT / (case + "-cgroup.jsonl")).open("w")
            mon = subprocess.Popen(
                ["docker", "exec", NAME, "python3", "-u", "-c", MONITOR, mapping["cgroup"]],
                stdout=log,
                stderr=subprocess.STDOUT,
            )
            sb.files.write("/tmp/oom_pressure.py", PRESSURE)
            case_data["before_record"] = record(sb.id)
            t = time.monotonic()
            try:
                command = sb.commands.run("exec python3 /tmp/oom_pressure.py", timeout=20)
                case_data["command_result"] = dataclasses.asdict(command)
            except Exception as e:
                case_data["command_error"] = error(e)
            case_data["command_elapsed_seconds"] = time.monotonic() - t
            views = []
            for i in range(12):
                entry = {"elapsed": time.monotonic() - t}
                try:
                    entry["sdk_get_info"] = dataclasses.asdict(sb.get_info())
                except Exception as e:
                    entry["sdk_get_info_error"] = error(e)
                try:
                    entry["api_info"] = sb._client.instance_info(sb.id)
                except Exception as e:
                    entry["api_info_error"] = error(e)
                views.append(entry)
                if entry.get("sdk_get_info", {}).get("state") == "failed":
                    break
                time.sleep(1)
            case_data["views"] = views
            case_data["after_record"] = record(sb.id)
            try:
                case_data["subsequent_command"] = dataclasses.asdict(sb.commands.run("echo AFTER_OOM_ALIVE", timeout=5))
            except Exception as e:
                case_data["subsequent_command_error"] = error(e)
            if runtime == "firecracker":
                try:
                    dm = sb.commands.run("dmesg | tail -n 100", timeout=5)
                    (ROOT / (case + "-guest-kernel.log")).write_text(dm.stdout + "\n" + dm.stderr)
                except Exception as e:
                    case_data["guest_dmesg_error"] = error(e)
            if runtime == "runsc" or group:
                view = views[-1].get("sdk_get_info", {})
                if view.get("state") != "failed" or view.get("failure", {}).get("reason") != "OomKilled":
                    raise AssertionError("backend OOM reason did not reach SDK get_info: " + json.dumps(view))
                after = case_data.get("subsequent_command_error", {})
                if (
                    after.get("fields", {}).get("code") != "SANDBOX_OOM"
                    or after.get("fields", {}).get("retry") != "never"
                ):
                    raise AssertionError("backend OOM must be a terminal SDK error: " + json.dumps(after))
                if case_data["after_record"]["result"]["resources_held"]:
                    raise AssertionError("backend cleanup did not release resources")
            else:
                result = case_data.get("command_result", {})
                if (
                    result.get("exit_signal") != 9
                    or result.get("status") != "FAILED"
                    or result.get("error_code") is not None
                ):
                    raise AssertionError(
                        "user-process termination signal did not reach SDK CommandResult: " + json.dumps(result)
                    )
                if (
                    views[-1].get("sdk_get_info", {}).get("state") != "running"
                    or case_data.get("subsequent_command", {}).get("exit_code") != 0
                ):
                    raise AssertionError("user process OOM must leave this fixture running")
                handle = sb.commands.run("exec python3 /tmp/oom_pressure.py", background=True, timeout=20)
                background_result = dataclasses.asdict(handle.wait(timeout=25))
                case_data["background_oom_result"] = background_result
                if background_result.get("exit_signal") != 9 or background_result.get("error_code") is not None:
                    raise AssertionError(
                        "background termination signal was not retained: " + json.dumps(background_result)
                    )
                controls = []
                for command, timeout in [('exec sh -c "kill -9 $$"', 5), ("sleep 3", 1), ("exit 7", 5)]:
                    result = dataclasses.asdict(sb.commands.run(command, timeout=timeout))
                    controls.append({"command": command, "result": result})
                    if result.get("error_code") is not None:
                        raise AssertionError("ordinary termination unexpectedly received an error classification")
                case_data["negative_controls"] = controls
            case_data["harness_status"] = "completed"
        except Exception as e:
            case_data["harness_status"] = "failed"
            case_data["harness_error"] = error(e)
        finally:
            if sb:
                try:
                    sb.kill()
                    case_data["delete"] = "success"
                except Exception as e:
                    case_data["delete_error"] = error(e)
            if not sb and case_data.get("environment_id"):
                try:
                    Sandbox.delete(case_data["environment_id"])
                    case_data["failed_create_delete"] = "success"
                except Exception as e:
                    case_data["failed_create_delete_error"] = error(e)
            if mon:
                try:
                    mon.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    mon.terminate()
                    mon.wait(timeout=5)
            if log:
                log.close()
            case_data["cgroup_samples"] = (
                [
                    json.loads(line)
                    for line in (ROOT / (case + "-cgroup.jsonl")).read_text().splitlines()
                    if line.startswith("{")
                ]
                if (ROOT / (case + "-cgroup.jsonl")).exists()
                else []
            )
            results.append(case_data)
            emit({"kind": "case-result", **case_data})
            (ROOT / "results.json").write_text(json.dumps(results, indent=2, default=str) + "\n")
    inventory = docker(["sbox", "list"]).stdout
    (ROOT / "backend-final.txt").write_text(inventory)
    environments = docker(["/opt/adx/current/bin/redis-cli", "--raw", "HKEYS", "adx:{akernel}:control:v1"]).stdout
    own = {r.get("environment_id") for r in results}
    environment_prefix = "environment:"
    prefix_length = len(environment_prefix)
    residual = [
        line
        for line in environments.splitlines()
        if line.startswith(environment_prefix) and line[prefix_length:] in own
    ]
    (ROOT / "redis-final.json").write_text(json.dumps({"test_environment_fields": residual}) + "\n")
    parent = json.loads(subprocess.check_output(["docker", "inspect", "-f", "{{json .State}}", NAME], text=True))
    clean = (
        not residual
        and (not [line for line in inventory.splitlines()[1:] if line.strip()])
        and parent["Running"]
        and (parent["Pid"] == parent_before["State"]["Pid"])
        and (parent["StartedAt"] == parent_before["State"]["StartedAt"])
    )
    summary = {
        "harness_completed_cases": len([r for r in results if r["harness_status"] == "completed"]),
        "planned_cases": 4,
        "clean": clean,
        "parent_running": parent["Running"],
        "parent_oom_killed": parent["OOMKilled"],
        "residual_test_environments": residual,
    }
    (ROOT / "summary.json").write_text(json.dumps(summary, indent=2) + "\n")
    emit({"kind": "summary", **summary})
    if summary["harness_completed_cases"] != 4 or not clean:
        sys.exit(1)
    print("OOM_SDK_ACCEPTANCE_PASSED", flush=True)


if __name__ == "__main__":
    main()
