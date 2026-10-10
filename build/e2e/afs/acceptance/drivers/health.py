#!/usr/bin/env python3
"""OPS-01 health readiness acceptance driver.

The runner owns case/profile/matrix/run-dir selection. This driver consumes a
per-run binding file that names live Node /health endpoints, exact target
identity, and expected healthy/degraded scenarios. It is fail-closed: no binding,
loose identity, arbitrary HTTP responses, missing raw artifacts, or incomplete
component coverage cannot produce PASS.
"""
from __future__ import annotations

import datetime as dt
import hashlib
import json
import os
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path
from typing import Any

STATUS_PASS = "PASS"
STATUS_FAIL = "FAIL"
STATUS_BLOCKED = "BLOCKED"
VALID_COMPONENTS = {"Meta", "Node", "device", "mount", "RDMA"}
VALID_SCOPES = {"foundation", "configured_fs"}
COMPONENT_CHECKS = {
    "Meta": ("meta_persistence",),
    "Node": ("node_registration",),
    "device": ("data_device",),
    "mount": ("mounts",),
    "RDMA": ("rdma",),
}
BINDING_KEYS = {"case_id", "profile", "matrix", "target", "scenarios"}
TARGET_KEYS = {"role", "id", "scope", "backend", "process"}
PROCESS_KEYS = {"pid", "executable_name", "exe_sha256", "boot_id", "config_sha256"}
SCENARIO_KEYS = {"name", "component", "url", "expected_ready", "expected_http_status", "timeout_seconds", "expected_scope"}


class BindingError(RuntimeError):
    pass


def utc() -> str:
    return dt.datetime.now(dt.timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(ch in "0123456789abcdef" for ch in value)


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def write_json(path: Path, value: Any, run_dir: Path) -> dict[str, Any]:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    resolved = path.resolve()
    relative = resolved.relative_to(run_dir.resolve())
    return {"artifact": str(relative), "artifact_bytes": resolved.stat().st_size, "artifact_sha256": sha256_file(resolved)}


def matrix_from_env(env: dict[str, str]) -> dict[str, str]:
    raw = env.get("AFS_ACCEPTANCE_MATRIX", "{}")
    try:
        parsed = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise BindingError(f"AFS_ACCEPTANCE_MATRIX is not valid JSON: {exc}") from exc
    if not isinstance(parsed, dict):
        raise BindingError("AFS_ACCEPTANCE_MATRIX must decode to an object")
    return {str(key): str(value) for key, value in parsed.items()}


def required_context(env: dict[str, str]) -> tuple[str, str, dict[str, str], Path]:
    case_id = env.get("AFS_ACCEPTANCE_CASE_ID")
    profile = env.get("AFS_ACCEPTANCE_PROFILE")
    run_dir = env.get("AFS_ACCEPTANCE_RUN_DIR")
    if case_id != "OPS-01":
        raise BindingError(f"health driver only supports OPS-01, got {case_id!r}")
    if profile not in {"smoke", "full"}:
        raise BindingError(f"unsupported profile {profile!r}")
    if not run_dir:
        raise BindingError("AFS_ACCEPTANCE_RUN_DIR is missing")
    return case_id, profile, matrix_from_env(env), Path(run_dir).resolve()


def binding_matches(binding: dict[str, Any], case_id: str, profile: str, matrix: dict[str, str]) -> bool:
    if binding.get("case_id") != case_id:
        return False
    if binding.get("profile") not in {None, profile}:
        return False
    binding_matrix = binding.get("matrix")
    if not isinstance(binding_matrix, dict):
        raise BindingError("binding matrix must be an object")
    return {str(k): str(v) for k, v in binding_matrix.items()} == matrix


def select_binding(path: Path, case_id: str, profile: str, matrix: dict[str, str]) -> dict[str, Any]:
    document = load_json(path)
    if not isinstance(document, dict) or document.get("schema_version") != 1:
        raise BindingError("health binding file schema_version must be 1")
    bindings = document.get("bindings")
    if not isinstance(bindings, list):
        raise BindingError("health binding file must contain a bindings list")
    matches = [item for item in bindings if isinstance(item, dict) and binding_matches(item, case_id, profile, matrix)]
    if not matches:
        raise BindingError(f"no health binding for {case_id} with exact matrix {json.dumps(matrix, sort_keys=True)}")
    if len(matches) > 1:
        raise BindingError(f"ambiguous health bindings for {case_id} with exact matrix {json.dumps(matrix, sort_keys=True)}")
    return matches[0]


def validate_url(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.startswith(("http://", "https://")):
        raise BindingError(f"{name} must be an http(s) URL")
    if any(ch.isspace() for ch in value):
        raise BindingError(f"{name} must not contain whitespace")
    return value


def validate_target(binding: dict[str, Any], matrix: dict[str, str]) -> dict[str, Any]:
    unknown = sorted(set(binding) - BINDING_KEYS)
    if unknown:
        raise BindingError(f"unsupported binding field(s): {', '.join(unknown)}")
    target = binding.get("target")
    if not isinstance(target, dict):
        raise BindingError("binding target must be an object")
    unknown_target = sorted(set(target) - TARGET_KEYS)
    if unknown_target:
        raise BindingError(f"unsupported target field(s): {', '.join(unknown_target)}")
    if target.get("role") != "node":
        raise BindingError("target.role must be node")
    for key in ("id", "scope"):
        if not isinstance(target.get(key), str) or not target[key]:
            raise BindingError(f"target.{key} is required")
    expected_backend = matrix.get("backend")
    if target.get("backend") != expected_backend:
        raise BindingError(f"target.backend {target.get('backend')!r} does not match matrix backend {expected_backend!r}")
    process = target.get("process")
    if not isinstance(process, dict):
        raise BindingError("target.process must be an object with exact local process identity")
    unknown_process = sorted(set(process) - PROCESS_KEYS)
    if unknown_process:
        raise BindingError(f"unsupported target.process field(s): {', '.join(unknown_process)}")
    pid = process.get("pid")
    if isinstance(pid, str) and pid.isdecimal():
        pid = int(pid, 10)
    if not isinstance(pid, int) or pid <= 0:
        raise BindingError("target.process.pid must be a positive integer")
    process["pid"] = pid
    if not isinstance(process.get("executable_name"), str) or not process["executable_name"]:
        raise BindingError("target.process.executable_name is required")
    if not is_sha256(process.get("exe_sha256")):
        raise BindingError("target.process.exe_sha256 must be 64 lowercase hex characters")
    if process.get("boot_id") is not None and not isinstance(process.get("boot_id"), str):
        raise BindingError("target.process.boot_id must be a string when present")
    if process.get("config_sha256") is not None and not is_sha256(process.get("config_sha256")):
        raise BindingError("target.process.config_sha256 must be 64 lowercase hex characters when present")
    return target


def validate_binding(binding: dict[str, Any], profile: str, matrix: dict[str, str]) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    target = validate_target(binding, matrix)
    scenarios = binding.get("scenarios")
    if not isinstance(scenarios, list) or not scenarios:
        raise BindingError("binding scenarios must be a non-empty list")
    normalized: list[dict[str, Any]] = []
    degraded_components: set[str] = set()
    names: set[str] = set()
    for index, scenario in enumerate(scenarios):
        if not isinstance(scenario, dict):
            raise BindingError(f"scenario {index} must be an object")
        unknown = sorted(set(scenario) - SCENARIO_KEYS)
        if unknown:
            raise BindingError(f"scenario {index} has unsupported field(s): {', '.join(unknown)}")
        name = scenario.get("name")
        if not isinstance(name, str) or not name.strip():
            raise BindingError(f"scenario {index} name is required")
        if name in names:
            raise BindingError(f"duplicate scenario name {name!r}")
        names.add(name)
        component = scenario.get("component")
        if component not in VALID_COMPONENTS and component != "all":
            raise BindingError(f"scenario {name!r} component must be one of {sorted(VALID_COMPONENTS)} or 'all'")
        expected_ready = scenario.get("expected_ready")
        if not isinstance(expected_ready, bool):
            raise BindingError(f"scenario {name!r} expected_ready must be boolean")
        expected_status = scenario.get("expected_http_status", 200 if expected_ready else 503)
        if expected_status not in {200, 503}:
            raise BindingError(f"scenario {name!r} expected_http_status must be 200 or 503")
        timeout = scenario.get("timeout_seconds", 5)
        if not isinstance(timeout, (int, float)) or timeout <= 0 or timeout > 30:
            raise BindingError(f"scenario {name!r} timeout_seconds must be in (0, 30]")
        expected_scope = scenario.get("expected_scope", target["scope"])
        if expected_scope not in VALID_SCOPES:
            raise BindingError(f"scenario {name!r} expected_scope must be one of {sorted(VALID_SCOPES)}")
        if component != "all" and expected_ready is False:
            degraded_components.add(str(component))
        normalized.append({
            "name": name,
            "component": component,
            "url": validate_url(scenario.get("url"), f"scenario {name!r} url"),
            "expected_ready": expected_ready,
            "expected_status": int(expected_status),
            "timeout_seconds": float(timeout),
            "expected_scope": expected_scope,
        })
    if profile == "full" and degraded_components != VALID_COMPONENTS:
        missing = sorted(VALID_COMPONENTS - degraded_components)
        raise BindingError(f"full OPS-01 binding must include degraded scenarios for every component; missing {missing}")
    if profile == "smoke" and not degraded_components:
        raise BindingError("smoke OPS-01 binding must include at least one degraded component scenario")
    return target, normalized


def cmdline_parts(pid: int) -> list[str]:
    try:
        raw = Path(f"/proc/{pid}/cmdline").read_bytes()
    except OSError:
        return []
    return [part.decode("utf-8", errors="replace") for part in raw.split(b"\0") if part]


def config_path_from_cmdline(parts: list[str]) -> Path | None:
    for index, part in enumerate(parts):
        if part == "--config" and index + 1 < len(parts):
            return Path(parts[index + 1])
        if part.startswith("--config="):
            return Path(part.split("=", 1)[1])
    return None


def observe_process(process: dict[str, Any]) -> dict[str, Any]:
    pid = int(process["pid"])
    proc = Path(f"/proc/{pid}")
    observed: dict[str, Any] = {"pid": pid, "exists": proc.exists()}
    try:
        exe_path = Path(os.readlink(proc / "exe"))
        observed["exe_path"] = str(exe_path)
        observed["executable_name"] = exe_path.name
        observed["exe_sha256"] = sha256_file(exe_path)
    except OSError as exc:
        observed["exe_error"] = str(exc)
    try:
        observed["boot_id"] = Path("/proc/sys/kernel/random/boot_id").read_text(encoding="utf-8").strip()
    except OSError as exc:
        observed["boot_id_error"] = str(exc)
    parts = cmdline_parts(pid)
    observed["cmdline_present"] = bool(parts)
    cfg = config_path_from_cmdline(parts)
    if cfg is not None:
        observed["config_path"] = str(cfg)
        try:
            observed["config_sha256"] = sha256_file(cfg)
        except OSError as exc:
            observed["config_error"] = str(exc)
    return observed


def target_identity_check(target: dict[str, Any], run_dir: Path) -> dict[str, Any]:
    process = target["process"]
    observed = observe_process(process)
    errors: list[str] = []
    if observed.get("exists") is not True:
        errors.append("target process does not exist")
    for key in ("executable_name", "exe_sha256"):
        if observed.get(key) != process.get(key):
            errors.append(f"target process {key} mismatch")
    if process.get("boot_id") is not None and observed.get("boot_id") != process.get("boot_id"):
        errors.append("target process boot_id mismatch")
    if process.get("config_sha256") is not None and observed.get("config_sha256") != process.get("config_sha256"):
        errors.append("target process config_sha256 mismatch")
    artifact_meta = write_json(run_dir / "health" / "00-target-identity.json", {"expected": target, "observed": observed, "errors": errors}, run_dir)
    return {
        "name": "target-identity",
        "status": STATUS_PASS if not errors else STATUS_FAIL,
        "evidence": {**artifact_meta, "errors": errors, "pid": process["pid"]},
        **artifact_meta,
    }


def fetch_json(url: str, timeout: float) -> dict[str, Any]:
    request = urllib.request.Request(url, headers={"Accept": "application/json"})
    started = time.time()
    status = None
    body = b""
    error = None
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:  # noqa: S310 - operator supplied acceptance URL
            status = response.getcode()
            body = response.read(1024 * 1024)
    except urllib.error.HTTPError as exc:
        status = exc.code
        body = exc.read(1024 * 1024)
    except Exception as exc:  # noqa: BLE001 - exact error is acceptance evidence
        error = f"{type(exc).__name__}: {exc}"
    record: dict[str, Any] = {
        "url": url,
        "http_status": status,
        "duration_seconds": round(time.time() - started, 3),
        "response_bytes": len(body),
        "response_sha256": hashlib.sha256(body).hexdigest() if body else None,
    }
    if error is not None:
        record["error"] = error
        return record
    try:
        record["json"] = json.loads(body.decode("utf-8"))
    except Exception as exc:  # noqa: BLE001
        record["decode_error"] = f"{type(exc).__name__}: {exc}"
        record["body_prefix"] = body[:4096].decode("utf-8", errors="replace")
    return record


def check_ready(value: Any) -> bool | None:
    if isinstance(value, dict) and isinstance(value.get("ready"), bool):
        return value["ready"]
    return None


def component_ready(document: dict[str, Any], component: str) -> tuple[bool | None, Any]:
    checks = document.get("checks")
    if not isinstance(checks, dict):
        return None, "health JSON missing checks object"
    names = COMPONENT_CHECKS[component]
    observed = {name: checks.get(name) for name in names}
    for name in names:
        ready = check_ready(checks.get(name))
        if ready is not None:
            return ready, observed
    return None, observed


def backend_configured(document: dict[str, Any], backend: str) -> bool:
    key = {"OwnerFs": "ownerfs", "DFS": "dfs"}.get(backend)
    if key is None:
        return False
    record = document.get(key)
    if isinstance(record, dict) and record.get("configured") is True:
        return True
    mounts = ((document.get("checks") or {}).get("mounts") or {}) if isinstance(document.get("checks"), dict) else {}
    if isinstance(mounts, dict):
        nested = mounts.get(key)
        if isinstance(nested, dict) and nested.get("configured") is True:
            return True
    return False


def endpoint_identity_errors(document: dict[str, Any], target: dict[str, Any], expected_scope: str) -> list[str]:
    errors: list[str] = []
    for key in ("role", "id"):
        if document.get(key) != target.get(key):
            errors.append(f"health {key} {document.get(key)!r} does not match target {target.get(key)!r}")
    if document.get("scope") != expected_scope:
        errors.append(f"health scope {document.get('scope')!r} does not match expected scope {expected_scope!r}")
    if not backend_configured(document, target["backend"]):
        errors.append(f"health response does not prove configured backend {target['backend']}")
    return errors


def validate_scenario(scenario: dict[str, Any], target: dict[str, Any], artifact: Path, run_dir: Path) -> dict[str, Any]:
    record = fetch_json(scenario["url"], scenario["timeout_seconds"])
    artifact_meta = write_json(artifact, {"scenario": scenario, "target": target, "observation": record}, run_dir)
    evidence: dict[str, Any] = {
        **artifact_meta,
        "http_status": record.get("http_status"),
        "url": scenario["url"],
        "response_bytes": record.get("response_bytes"),
        "response_sha256": record.get("response_sha256"),
    }
    if record.get("error"):
        return {"name": scenario["name"], "status": STATUS_BLOCKED, "evidence": {**evidence, "error": record["error"]}, **artifact_meta}
    document = record.get("json")
    if not isinstance(document, dict):
        return {"name": scenario["name"], "status": STATUS_FAIL, "evidence": {**evidence, "decode_error": record.get("decode_error")}, **artifact_meta}
    identity_errors = endpoint_identity_errors(document, target, scenario["expected_scope"])
    if identity_errors:
        return {"name": scenario["name"], "status": STATUS_FAIL, "evidence": {**evidence, "identity_errors": identity_errors}, **artifact_meta}
    expected_status = scenario["expected_status"]
    if record.get("http_status") != expected_status:
        return {"name": scenario["name"], "status": STATUS_FAIL, "evidence": {**evidence, "expected_http_status": expected_status}, **artifact_meta}
    top_ready = document.get("status") in {"ready", "foundation"}
    if top_ready != scenario["expected_ready"]:
        return {"name": scenario["name"], "status": STATUS_FAIL, "evidence": {**evidence, "observed_status": document.get("status"), "expected_ready": scenario["expected_ready"]}, **artifact_meta}
    component = scenario["component"]
    if component == "all":
        missing: list[str] = []
        not_ready: list[str] = []
        observed: dict[str, Any] = {}
        for item in sorted(VALID_COMPONENTS):
            ready, detail = component_ready(document, item)
            observed[item] = detail
            if ready is None:
                missing.append(item)
            elif ready is not True:
                not_ready.append(item)
        if missing or (scenario["expected_ready"] and not_ready):
            return {"name": scenario["name"], "status": STATUS_FAIL, "evidence": {**evidence, "missing": missing, "not_ready": not_ready, "observed": observed}, **artifact_meta}
        return {"name": scenario["name"], "status": STATUS_PASS, "evidence": {**evidence, "components": sorted(VALID_COMPONENTS)}, **artifact_meta}
    ready, detail = component_ready(document, component)
    expected_component_ready = scenario["expected_ready"]
    if ready is None or ready != expected_component_ready:
        return {"name": scenario["name"], "status": STATUS_FAIL, "evidence": {**evidence, "component": component, "component_ready": ready, "expected_component_ready": expected_component_ready, "detail": detail}, **artifact_meta}
    return {"name": scenario["name"], "status": STATUS_PASS, "evidence": {**evidence, "component": component, "component_ready": ready}, **artifact_meta}


def coverage(profile: str, checks: list[dict[str, Any]], scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    refs: dict[str, str] = {}
    pass_names = {check["name"] for check in checks if check.get("status") == STATUS_PASS}
    healthy = next((scenario for scenario in scenarios if scenario["component"] == "all" and scenario["expected_ready"]), None)
    for scenario in scenarios:
        if scenario["component"] in VALID_COMPONENTS and scenario["name"] in pass_names:
            refs[scenario["component"]] = scenario["name"]
    if healthy and healthy["name"] in pass_names:
        for component in VALID_COMPONENTS:
            refs.setdefault(component, healthy["name"])
    return {
        "profile": profile,
        "axes": {"components": {"values": sorted(VALID_COMPONENTS), "checks": {component: refs.get(component, "") for component in sorted(VALID_COMPONENTS)}}},
    }


def proof(case_id: str, profile: str, matrix: dict[str, str], checks: list[dict[str, Any]], scenarios: list[dict[str, Any]]) -> dict[str, Any]:
    statuses = {str(check.get("status")) for check in checks}
    status = STATUS_PASS if statuses == {STATUS_PASS} else STATUS_BLOCKED if STATUS_BLOCKED in statuses else STATUS_FAIL
    return {"case_id": case_id, "profile": profile, "matrix": matrix, "status": status, "reason": "", "checks": checks, "coverage": coverage(profile, checks, scenarios), "generated_at": utc()}


def blocked(case_id: str, profile: str, matrix: dict[str, str], reason: str) -> dict[str, Any]:
    return {"case_id": case_id, "profile": profile, "matrix": matrix, "status": STATUS_BLOCKED, "reason": reason, "checks": [{"name": "health-driver-preflight", "status": STATUS_BLOCKED, "evidence": reason}], "generated_at": utc()}


def main() -> int:
    try:
        case_id, profile, matrix, run_dir = required_context(os.environ)
        binding_path = os.environ.get("AFS_ACCEPTANCE_HEALTH_BINDINGS")
        if not binding_path:
            result = blocked(case_id, profile, matrix, "AFS_ACCEPTANCE_HEALTH_BINDINGS is not set")
        else:
            binding = select_binding(Path(binding_path).resolve(), case_id, profile, matrix)
            target, scenarios = validate_binding(binding, profile, matrix)
            checks = [target_identity_check(target, run_dir)]
            checks.extend(validate_scenario(scenario, target, run_dir / "health" / f"{index + 1:02d}-{scenario['name']}.json", run_dir) for index, scenario in enumerate(scenarios))
            result = proof(case_id, profile, matrix, checks, scenarios)
    except BindingError as exc:
        case_id = os.environ.get("AFS_ACCEPTANCE_CASE_ID") or "OPS-01"
        profile = os.environ.get("AFS_ACCEPTANCE_PROFILE") or "smoke"
        try:
            matrix = matrix_from_env(os.environ)
        except BindingError:
            matrix = {}
        result = blocked(case_id, profile, matrix, str(exc))
    except Exception as exc:  # noqa: BLE001 - acceptance proof preserves exact driver failure
        case_id = os.environ.get("AFS_ACCEPTANCE_CASE_ID") or "OPS-01"
        profile = os.environ.get("AFS_ACCEPTANCE_PROFILE") or "smoke"
        try:
            matrix = matrix_from_env(os.environ)
        except BindingError:
            matrix = {}
        result = {"case_id": case_id, "profile": profile, "matrix": matrix, "status": STATUS_FAIL, "reason": f"health driver crashed: {type(exc).__name__}: {exc}", "checks": [{"name": "health-driver-exception", "status": STATUS_FAIL, "evidence": repr(exc)}], "generated_at": utc()}
    print(json.dumps(result, sort_keys=True))
    return 0 if result.get("status") == STATUS_PASS else 1


if __name__ == "__main__":
    raise SystemExit(main())
