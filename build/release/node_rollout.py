"""Journaled Pod/process publishing and explicit ready promotion. Standard library only."""
import argparse
import hashlib
import http.client
from collections import namedtuple
from functools import partial
from itertools import chain
import json
import os
from pathlib import Path, PurePosixPath
import re
import shutil
import stat
import subprocess
import sys
import threading
import tempfile
import time
from urllib.parse import quote, unquote, urlsplit

LIMIT = 8 * 1024 * 1024
TIMEOUT = 15
PodClients = namedtuple("PodClients", ("kube", "get", "post", "mutate"))
CAPABILITIES = ("planned_migration", "paused_transfer", "quiesce",
                "session_isolation", "recovery_compatibility", "runtime_rootfs_identity")


class Refusal(Exception):
    pass


def require(condition, message):
    if not condition:
        raise Refusal(message)


def text(value):
    return isinstance(value, str) and bool(value.strip()) and value == value.strip()


def decode(data):
    require(len(data) <= LIMIT, "response exceeds size limit")

    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result, "duplicate JSON key")
            result[key] = value
        return result

    def invalid_constant(value):
        raise Refusal("invalid JSON constant")

    result = json.loads(data.decode("utf-8"), object_pairs_hook=pairs,
                        parse_constant=invalid_constant)
    require(isinstance(result, dict), "expected JSON object")
    return result


def origin(value):
    parsed = urlsplit(value)
    require(parsed.scheme == "http" and parsed.hostname and not parsed.username
            and not parsed.password and parsed.path in ("", "/")
            and not parsed.query and not parsed.fragment
            and not any(c.isspace() for c in value),
            "origin must be a trusted plain HTTP origin without credentials or path")
    return parsed.hostname, parsed.port or 80


def http_json(base, path, body=None):
    host, port = origin(base)
    connection = http.client.HTTPConnection(host, port, timeout=TIMEOUT)
    deadline = time.monotonic() + TIMEOUT
    try:
        # http.client uses neither environment proxies nor redirect handling.
        payload = None if body is None else canonical(body).encode("utf-8")
        connection.request("GET" if body is None else "POST", path, body=payload,
                           headers={"Accept": "application/json", "Content-Type": "application/json"})
        response = connection.getresponse()
        status_error = "management request returned HTTP " + str(response.status)
        is_json = response.getheader("Content-Type", "").split(";")[0].strip() == "application/json"
        require(response.status == 200 or is_json, status_error)
        require(is_json, "management response is not JSON")
        data = bytearray()
        while len(data) <= LIMIT:
            remaining = deadline - time.monotonic()
            require(remaining > 0, "management request deadline exceeded")
            if connection.sock is not None:
                connection.sock.settimeout(remaining)
            try:
                chunk = response.read1(min(65536, LIMIT + 1 - len(data)))
            except (OSError, http.client.HTTPException):
                require(response.status == 200, status_error)
                raise
            if not chunk:
                if response.status != 200:
                    try:
                        error = decode(data).get("error")
                    except (Refusal, ValueError, RecursionError):
                        error = None
                    detail = (": " + error["code"] + ": " + error["message"]
                              if isinstance(error, dict) and text(error.get("code"))
                              and text(error.get("message")) else "")
                    raise Refusal(status_error + detail)
                return decode(data)
            data.extend(chunk)
        raise Refusal("management response exceeds size limit")
    finally:
        connection.close()


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(",", ":"), allow_nan=False)


def kubectl_json(args, body=None):
    executable = shutil.which("kubectl")
    require(executable is not None, "kubectl not found")
    command = [str(Path(executable).resolve()), "--request-timeout=10s", *args]
    payload = None if body is None else canonical(body).encode("utf-8")
    require(payload is None or len(payload) <= LIMIT, "kubectl input exceeds size limit")
    with subprocess.Popen(command, stdin=subprocess.PIPE if payload is not None else subprocess.DEVNULL,
                          stdout=subprocess.PIPE, stderr=subprocess.DEVNULL) as process:
        timer = threading.Timer(TIMEOUT, process.kill)
        timer.start()
        try:
            if payload is not None:
                process.stdin.write(payload)
                process.stdin.close()
            data = process.stdout.read(LIMIT + 1)
            if len(data) > LIMIT:
                process.kill()
                raise Refusal("kubectl response exceeds size limit")
            code = process.wait(timeout=TIMEOUT)
            require(code == 0, "kubectl failed or timed out")
            return decode(data)
        finally:
            timer.cancel()
            if process.poll() is None:
                process.kill()
            process.wait()


def kubectl_get(args):
    return kubectl_json(args if "--raw" in args else [*args, "-o", "json"])


def identity(obj, kind, namespace):
    require(obj["kind"] == kind, "unexpected Kubernetes kind")
    metadata = obj["metadata"]
    require(metadata["namespace"] == namespace and not metadata.get("deletionTimestamp"),
            "wrong namespace or terminating object")
    require(all(text(metadata[key]) for key in ("name", "uid", "resourceVersion")),
            "missing Kubernetes identity")
    return {"name": metadata["name"], "namespace": namespace, "uid": metadata["uid"],
            "resource_version": metadata["resourceVersion"]}


def image(obj, container):
    matches = [c["image"] for c in obj["spec"]["containers"] if c["name"] == container]
    require(len(matches) == 1 and text(matches[0]), "target container missing or duplicated")
    return matches[0]


def template_hash(template):
    return hashlib.sha256(canonical(template).encode("utf-8")).hexdigest()


def workload_kind(workload):
    """Legacy DaemonSet plans omit kind; only StatefulSet plans carry it explicitly."""
    kind = workload.get("kind", "DaemonSet")
    require(kind in ("DaemonSet", "StatefulSet"), "invalid workload kind")
    return kind


def workload_resource(workload):
    return "statefulset" if workload_kind(workload) == "StatefulSet" else "daemonset"


def workload_owns(workload, pod):
    """True when the Pod has exactly one controller owner that is this workload."""
    kind = workload_kind(workload)
    owners = [entry for entry in pod["metadata"].get("ownerReferences", []) if entry.get("controller") is True]
    return (len(owners) == 1 and owners[0].get("uid") == workload["uid"]
            and owners[0].get("kind") == kind and owners[0].get("apiVersion") == "apps/v1"
            and owners[0].get("name") == workload["name"])


def stable_statefulset_name(workload_name, name):
    """True when name is a canonical StatefulSet ordinal name (workers-0, workers-7).

    The controller only recreates Pods named <workload>-<ordinal> with a canonical
    decimal ordinal; suffixes such as workers-00 or workers-debug never match an
    owned Pod and are refused instead of silently accepted.
    """
    return re.fullmatch(re.escape(workload_name) + r"-(0|[1-9][0-9]*)", name) is not None


def pod_replacement(plan, pod, candidates):
    """Return the unique replacement (candidate, evidence, host) for this Pod.

    DaemonSet replacements keep the original host and require a new Pod UID;
    StatefulSet replacements keep the stable Pod name and may change host.
    """
    workload = plan["workload"]
    kind = workload_kind(workload)
    namespace = plan["begin"]["namespace"]
    matches = []
    for candidate in candidates:
        if kind == "DaemonSet" and candidate["spec"].get("nodeName") != pod["kubernetes_node_name"]:
            continue
        if kind == "StatefulSet" and candidate["metadata"].get("name") != pod["name"]:
            continue
        owners = [entry for entry in candidate["metadata"].get("ownerReferences", [])
                  if entry.get("controller") is True]
        if not any(entry.get("uid") == workload["uid"] for entry in owners):
            continue
        require(workload_owns(workload, candidate), "replacement owner differs")
        matches.append(candidate)
    require(len(matches) == 1, "unique replacement unavailable")
    candidate = matches[0]
    evidence = identity(candidate, "Pod", namespace)
    require(evidence["uid"] != pod["uid"] and image(candidate, plan["container"]) == plan["begin"]["target_image"],
            "replacement image or UID differs")
    host = candidate["spec"].get("nodeName")
    require(text(host), "unscheduled replacement Pod")
    return candidate, evidence, host


def replacement_host(workload, source_host, state):
    """Reconcile the recorded replacement host with a legacy journal.

    DaemonSet replacements keep the source host, so a pre-replacement_host
    journal may fall back to it. StatefulSet replacements may change host and
    must keep their recorded host instead.
    """
    recorded = state.get("replacement_host")
    if recorded is None and workload_kind(workload) == "DaemonSet":
        recorded = source_host
    require(text(recorded), "replacement host unavailable")
    return recorded


def plan_pod(args, kube, get, local):
    kind = "StatefulSet" if args.statefulset else "DaemonSet"
    resource = "statefulset" if args.statefulset else "daemonset"
    workload_name = args.statefulset if args.statefulset else args.daemonset
    require(text(args.context) and not args.context.startswith("-"), "explicit Kubernetes context required")
    require(text(args.config) and PurePosixPath(args.config).is_absolute()
            and PurePosixPath(args.config).suffix in (".yaml", ".yml"), "absolute deployment YAML required")
    require((args.declaration_candidate is None) == (args.configmap is None), "candidate and ConfigMap must be paired")
    require(args.declaration_candidate is not None or args.declaration_key == "target.yaml", "key requires candidate")
    source = None
    if args.declaration_candidate is not None:
        require(text(args.configmap) and not args.configmap.startswith("-")
                and re.fullmatch(r"[A-Za-z0-9._-]+", args.declaration_key), "invalid ConfigMap selection")
        candidate_text = read_json_file(args.declaration_candidate, raw=True)
        desired = declaration_candidate(decode(candidate_text.encode("utf-8")), candidate_text)
        require(not desired.get("upgrade_components", []), "Pod candidate cannot upgrade processes")
        cm, old = declaration_configmap(kube(["get", "configmap", args.configmap, "--namespace", args.namespace]),
                                       args.namespace, args.configmap, args.declaration_key)
        require(old != candidate_text, "candidate equals original declaration")
        source = dict(name=args.configmap, namespace=args.namespace, uid=cm["uid"],
                      resource_version=cm["resource_version"], key=args.declaration_key,
                      original_text=old, candidate_text=candidate_text)
    origin(args.origin)
    require(type(args.batch_size) is int and args.batch_size > 0, "batch size must be positive")
    require(text(args.task_id) and "/" not in args.task_id and "%" not in args.task_id
            and len(args.task_id.encode("utf-8")) <= 128, "invalid task ID")
    fields = (args.namespace, workload_name, args.container, args.task_id)
    require(all(text(v) and not v.startswith("-") for v in fields), "invalid identity")
    require(type(args.epoch) is int and 0 < args.epoch <= 2**64 - 1, "epoch must be positive u64")
    require(re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", args.target_image),
            "target image must be fixed by SHA-256 digest")
    mappings = {}
    for entry in args.node:
        pod, separator, node = entry.partition("=")
        require(separator and text(pod) and text(node) and "=" not in node
                and pod not in mappings and node not in mappings.values(), "invalid or duplicate mapping")
        mappings[pod] = node

    def inventory():
        obj = kube(["get", resource, workload_name, "--namespace", args.namespace])
        workload = identity(obj, kind, args.namespace)
        require(obj["apiVersion"] == "apps/v1" and workload["name"] == workload_name,
                "unexpected " + kind + " identity")
        downward_uid(obj["spec"]["template"])
        if source:
            declaration_mount(obj["spec"]["template"], args.container, source["name"], source["key"])
        workload["kind"] = kind
        workload["original_image"] = image(obj["spec"]["template"], args.container)
        template = obj["spec"]["template"]
        workload["original_template_hash"] = template_hash(template)
        for container in template["spec"]["containers"]:
            if container["name"] == args.container:
                container["image"] = args.target_image
        workload["target_template_hash"] = template_hash(template)
        owned = []
        for pod in pod_inventory(args.namespace, kube):
            owners = [entry for entry in pod["metadata"].get("ownerReferences", []) if entry.get("controller") is True]
            if not any(entry.get("uid") == workload["uid"] for entry in owners):
                continue
            require(workload_owns(workload, pod), "invalid controller owner")
            require(pod["apiVersion"] == "v1", "unexpected Pod API version")
            downward_uid(pod)
            record = identity(pod, "Pod", args.namespace)
            record.update(original_image=image(pod, args.container),
                          target_image=args.target_image, owner_uid=workload["uid"],
                          kubernetes_node_name=pod["spec"].get("nodeName"))
            require(text(record["kubernetes_node_name"]), "unscheduled source Pod")
            owned.append((pod, record))
        require(owned and len({p["name"] for _, p in owned}) == len(owned)
                and len({p["uid"] for _, p in owned}) == len(owned), "empty or duplicate source inventory")
        require({p["name"] for _, p in owned} == set(mappings),
                "mapping must cover exactly current owned Pods")
        if kind == "StatefulSet":
            require(all(stable_statefulset_name(workload_name, p["name"]) for _, p in owned),
                    "StatefulSet Pods must use canonical ordinal names")
        else:
            require(len({p["kubernetes_node_name"] for _, p in owned}) == len(owned),
                    "one source Pod per host required")
        pods = []
        for pod, record in owned:
            record["actual"] = pod_actual(pod, args.namespace, args.container,
                                          mappings.get(record["name"]), local(record["name"]))
            if source:
                path = declaration_mount(pod, args.container, source["name"], source["key"])
                record["declaration_path"] = path
                if source["original_text"] is not None:
                    observed = local(record["name"], path).get("external_declaration")
                    require(isinstance(observed, dict) and observed.get("path") == path
                            and observed.get("raw_sha256") == hashlib.sha256(old.encode("utf-8")).hexdigest()
                            and isinstance(observed.get("desired"), dict), "old declaration projection differs")
            pods.append(record)
        return workload, sorted(pods, key=lambda p: p["name"])

    def downward_uid(obj):
        containers = [c for c in obj["spec"]["containers"] if c.get("name") == args.container]
        require(len(containers) == 1, "target container missing or duplicated")
        entries = [e for e in containers[0].get("env", []) if e.get("name") == "ADX_POD_UID"]
        require(len(entries) == 1 and "value" not in entries[0], "unique Downward ADX_POD_UID required")
        source = entries[0].get("valueFrom", {})
        require(isinstance(source, dict), "invalid ADX_POD_UID valueFrom")
        field = source.get("fieldRef", {})
        require(set(source) == {"fieldRef"} and isinstance(field, dict) and field.get("fieldPath") == "metadata.uid"
                and field.get("apiVersion", "v1") == "v1", "ADX_POD_UID must use Downward metadata.uid")

    def inspect(node, uid):
        view = get(f"/v1/node-upgrades/nodes/{quote(node, safe='')}/inspect")
        require(view.get("node_id") == node and text(view.get("node_session_id"))
                and type(view.get("protocol_version")) is int and view["protocol_version"] == 3
                and view.get("unknown") is False, "unknown or incompatible node identity/session")
        require(view.get("observed_pod_uid") == uid, "observed Pod UID differs from Kubernetes")
        require(isinstance(view.get("runtime_rootfs_sha256"), str)
                and re.fullmatch(r"[0-9a-f]{64}", view["runtime_rootfs_sha256"])
                and text(view.get("runtime_rootfs_path")), "missing runtime rootfs evidence")
        return view

    workload, pods = inventory()
    capabilities = get("/v1/node-upgrades/capabilities")
    require(type(capabilities.get("version")) is int and capabilities["version"] == 2
            and type(capabilities.get("node_protocol_version")) is int
            and capabilities["node_protocol_version"] == 3
            and all(capabilities.get(c) is True for c in CAPABILITIES), "required capabilities unavailable")
    views = {mappings[pod["name"]]: inspect(mappings[pod["name"]], pod["uid"]) for pod in pods}
    require((workload, pods) == inventory(), "Kubernetes objects changed during planning")
    for node, view in views.items():
        current = inspect(node, view["observed_pod_uid"])
        fields = ("node_session_id", "runtime_rootfs_sha256", "runtime_rootfs_path", "observed_pod_uid")
        require(all(current.get(k) == view.get(k) for k in fields),
                "node session or runtime changed during planning")
    bindings = []
    for pod in pods:
        node = mappings[pod["name"]]
        pod.update(node_id=node, inspection=views[node])
        require(pod["actual"]["node_session"] == views[node]["node_session_id"],
                "actual session differs from Coordinator")
        bindings.append(dict(node_id=node, pod_uid=pod["uid"],
                              node_session_id=views[node]["node_session_id"]))
    ids = [pod["node_id"] for pod in pods]
    batches = [ids[:1]] + [ids[i:i + args.batch_size] for i in range(1, len(ids), args.batch_size)]
    result = {"schema_version": 1, "mode": "pod", "batches": batches, "config": args.config,
            "context": args.context, "origin": args.origin, "container": args.container,
            "workload": workload, "pods": pods, "capabilities": capabilities,
            "begin": dict(task_id=args.task_id, epoch=args.epoch, namespace=args.namespace,
                           daemonset_uid=workload["uid"], target_image=args.target_image, nodes=bindings)}
    if source:
        fresh, current = declaration_configmap(kube([
            "get", "configmap", source["name"], "--namespace", args.namespace]),
                                              args.namespace, source["name"], source["key"])
        require(fresh["uid"] == source["uid"] and current == old, "ConfigMap source changed")
        source["resource_version"] = fresh["resource_version"]
        result["declaration"] = source
    validate_pod_plan(result)
    require(len(canonical(result).encode("utf-8")) <= LIMIT, "plan exceeds size limit")
    return result


def process_pod(pod, namespace, pod_name, container):
    evidence = identity(pod, "Pod", namespace)
    require(pod.get("apiVersion") == "v1" and evidence["name"] == pod_name, "unexpected Pod identity")
    evidence["kubernetes_node_name"] = pod["spec"].get("nodeName")
    require(text(evidence["kubernetes_node_name"]), "unscheduled Pod")
    containers = [item for item in pod["spec"]["containers"] if item.get("name") == container]
    require(len(containers) == 1, "selected container missing or duplicated")
    statuses = [item for item in pod["status"]["containerStatuses"] if item.get("name") == container]
    require(len(statuses) == 1, "selected container status missing or duplicated")
    status = statuses[0]
    require(text(status.get("containerID")), "missing container ID")
    restarts = status.get("restartCount")
    require(isinstance(restarts, int) and not isinstance(restarts, bool) and restarts >= 0, "invalid restart count")
    state = status.get("state")
    require(isinstance(state, dict) and set(state) == {"running"}, "selected container is not running")
    require(isinstance(state["running"], dict), "invalid running state")
    evidence.update(container=container, container_id=status["containerID"],
                    restart_count=status["restartCount"], running=state["running"])
    return evidence


def pod_actual(pod, namespace, container, node, response, plan=None, session=None):
    evidence = process_pod(pod, namespace, pod["metadata"]["name"], container)
    statuses = [item for item in pod["status"]["containerStatuses"] if item.get("name") == container]
    require(text(statuses[0].get("imageID")), "missing opaque image content identity")
    owner, boot = process_component(response, "adxlet", node, evidence["uid"], eligible=False,
                                    modes=("standalone", "embedded"))
    runtime = owner["runtime"]
    require(session is None or runtime["node_session"] == session, "actual session differs")
    components = response["node_info"]["components"]
    roles = ("adxlet", "relay") if owner["mode"] == "standalone" else ("adxlet",)
    require(len(components) == len(roles) and {item.get("role") for item in components} == set(roles),
            "incomplete owned components")
    for role in roles:
        component, _ = process_component(response, role, node, evidence["uid"], eligible=False,
                                          modes=(owner["mode"],))
        require(all(component["runtime"][key] == runtime[key] for key in ("node_session", "relay_session")),
                "owned component sessions differ")
    if plan is not None:
        if "declaration" not in plan:
            require("external_declaration" in response and response["external_declaration"] is None
                    and "external_startup" in response and response["external_startup"] is None,
                    "replacement is not default image startup")
            require(all("sources" in item and item["sources"] == {} for item in components),
                    "default image provenance unknown or externally changed")
        else:
            source = plan["declaration"]
            desired = declaration_candidate(decode(source["candidate_text"].encode("utf-8")), source["candidate_text"])
            path = declaration_mount(pod, container, source["name"], source["key"])
            require(response.get("external_declaration") == dict(
                path=path, raw_sha256=hashlib.sha256(source["candidate_text"].encode("utf-8")).hexdigest(),
                desired=desired), "replacement default initialization declaration differs")
            startup = response.get("external_startup")
            require(isinstance(startup, dict) and startup.get("selection") == "target"
                    and startup.get("mode") == desired["mode"] == owner["mode"], "target startup unconfirmed")
            initial = startup.get("components")
            require(isinstance(initial, list) and len(initial) == 2
                    and {item.get("role") for item in initial} == {"adxlet", "relay"}, "startup roles incomplete")
            for component in components:
                expected_roles = (component["role"],) if owner["mode"] == "standalone" else ("adxlet", "relay")
                expected_sources = {role: dict(version=desired["components"][role]["version"],
                    archive_sha256=desired["components"][role]["artifact"]["sha256"],
                    raw_config_sha256=desired["components"][role]["config"]["sha256"]) for role in expected_roles}
                require(component.get("sources") == expected_sources, "actual target provenance differs")
                for role in expected_roles:
                    selected = next(item for item in initial if item["role"] == role)
                    target = desired["components"][role]
                    require(selected.get("version") == target["version"]
                            and selected.get("release_id") == target["release_id"]
                            and selected.get("raw_config_sha256") == target["config"]["sha256"]
                            and selected.get("binary_sha256") == component["runtime"]["executable_sha256"]
                            and selected.get("effective_config_sha256") == component["runtime"]["configuration_sha256"],
                            "actual binary/config differs from selected target")
    return dict(container_id=evidence["container_id"], restart_count=evidence["restart_count"],
                running=evidence["running"], image_id=statuses[0]["imageID"], boot_nonce=boot,
                node_session=runtime["node_session"])


def process_component(response, role, node, pod_uid, eligible=True, complete=True, modes=("standalone",)):
    require(response.get("ok") is True, "node-info failed")
    view = response["node_info"]
    version = view.get("schema_version")
    require(isinstance(version, int) and not isinstance(version, bool) and version == 1, "incompatible node-info")
    require(view.get("pod_uid") == pod_uid, "node-info Pod UID differs from Kubernetes")
    require(text(view.get("boot_nonce")) and len(view["boot_nonce"].encode("utf-8")) <= 128, "missing boot nonce")
    matches = [item for item in view["components"] if item.get("role") == role and item.get("node_id") == node]
    require(len(matches) == 1, "no unique component for explicit role and ADX node ID")
    component = matches[0]
    require(component.get("mode") in modes, "unsupported component mode")
    if eligible:
        require(component.get("can_upgrade") is True, "component cannot upgrade")
    service = component.get("service_id")
    require(isinstance(service, str) and re.fullmatch(r"[A-Za-z0-9_-]{1,64}", service), "invalid service ID")
    runtime = component.get("runtime")
    if runtime is None and not complete:
        return component, view["boot_nonce"]
    require(isinstance(runtime, dict), "missing runtime")
    require(set(runtime) == {"boot_nonce", "pod_uid", "pid", "executable_sha256",
                             "configuration_sha256", "node_session", "relay_session"},
            "incomplete or unsupported runtime")
    require(runtime["pod_uid"] == pod_uid and runtime["boot_nonce"] == view["boot_nonce"],
            "runtime Pod UID or boot nonce differs from node-info")
    pid = runtime["pid"]
    require(isinstance(pid, int) and not isinstance(pid, bool) and 0 < pid <= 2**32 - 1, "invalid runtime PID")
    fields = ("pod_uid", "node_session", "relay_session")
    require(all(text(runtime[key]) and len(runtime[key].encode("utf-8")) <= 128 for key in fields),
            "missing runtime identity/session")
    digest = runtime["executable_sha256"]
    require(isinstance(runtime["configuration_sha256"], str)
            and re.fullmatch(r"[0-9a-f]{64}", runtime["configuration_sha256"]), "invalid configuration SHA-256")
    require(isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest), "invalid executable SHA-256")
    return component, view["boot_nonce"]


def require_process_steps(response):
    """Refuse an adxctl whose Submit would run the whole upgrade by itself.

    Only `node_info.step_protocol` strictly equal to the int 2 marks a local
    adxctl whose Submit merely prepares files and exposes exit/start/rollback
    as publisher-driven Steps; rollback starts the original locally. Missing, null, bool or any other value is refused.
    adxlet process planning and every pre-side-effect observation call this;
    Pod and Relay paths do not require the marker.
    """
    view = response.get("node_info") if isinstance(response, dict) else None
    require(isinstance(view, dict), "node-info unavailable for step protocol check")
    marker = view.get("step_protocol")
    require(type(marker) is int and marker == 2,
            "adxlet process rollout requires step_protocol 2; incompatible local step semantics")


def remote_archive(value):
    require(isinstance(value, dict) and set(value) == {"url", "archive_sha256"}, "invalid remote archive")
    require(text(value["url"]) and not any(character.isspace() for character in value["url"]),
            "invalid archive URL")
    try:
        parsed = urlsplit(value["url"])
        host, port = parsed.hostname, parsed.port
    except ValueError as error:
        raise Refusal("invalid archive URL") from error
    require(host and not any(ord(character) <= 32 or ord(character) == 127 for character in value["url"])
            and not re.search(r"%(?![0-9a-fA-F]{2})", value["url"]), "invalid archive URL")
    decoded_host = unquote(host)
    require(not any(character in decoded_host for character in "#%/<>?@[\\]^|")
            and not any(ord(character) <= 32 or ord(character) == 127 for character in decoded_host)
            and (":" not in decoded_host or parsed.netloc.startswith("[")), "invalid archive host")
    require(value["url"].startswith("https://") and parsed.scheme == "https" and host
            and (port is None or 0 < port <= 65535) and "\\" not in value["url"] and parsed.username is None
            and parsed.password is None and "#" not in value["url"], "archive URL must be anonymous HTTPS")
    require(isinstance(value["archive_sha256"], str)
            and re.fullmatch(r"[0-9a-f]{64}", value["archive_sha256"]), "invalid archive SHA-256")
    return value


def component_config(value):
    require(isinstance(value, dict) and set(value) == {"mount_path", "sha256"},
            "invalid component configuration descriptor")
    require(isinstance(value["mount_path"], str) and PurePosixPath(value["mount_path"]).is_absolute()
            and isinstance(value["sha256"], str) and re.fullmatch(r"[0-9a-f]{64}", value["sha256"]),
            "invalid component configuration descriptor")
    return value


def declaration_candidate(value, raw_text=None):
    if raw_text is not None:
        require(len(raw_text.encode("utf-8")) <= 4 * 1024 * 1024, "declaration exceeds 4 MiB")
    require(isinstance(value, dict) and set(value) <= {
        "schema", "mode", "components", "ready", "upgrade_components"}, "invalid declaration fields")
    require(type(value.get("schema")) is int and value["schema"] == 1
            and value.get("mode") in ("standalone", "embedded"), "candidate requires schema 1 node mode")
    roles = value.get("upgrade_components", [])
    require(isinstance(roles, list) and len(roles) <= 1 and all(role in ("adxlet", "relay") for role in roles)
            and (value["mode"] != "embedded" or not roles), "invalid upgrade components")
    ready = value.get("ready")
    require(isinstance(ready, dict) and set(ready) == {"components"}, "invalid ready group")
    for group in (value.get("components"), ready["components"]):
        require(isinstance(group, dict) and set(group) == {"adxlet", "relay"}, "complete roles required")
        for component in group.values():
            require(isinstance(component, dict)
                    and set(component) == {"version", "release_id", "artifact", "config"},
                    "invalid declaration component")
            require(isinstance(component["version"], str)
                    and re.fullmatch(r"[0-9a-f]{40}", component["version"])
                    and isinstance(component["release_id"], str) and component["release_id"].strip(),
                    "invalid declaration version or release")
            artifact = component["artifact"]
            require(isinstance(artifact, dict) and set(artifact) == {"url", "sha256"},
                    "invalid official artifact")
            remote_archive(dict(url=artifact["url"], archive_sha256=artifact["sha256"]))
            component_config(component["config"])
        if value["mode"] == "embedded":
            require(all(group["adxlet"][key] == group["relay"][key] for key in ("version", "release_id", "artifact")),
                    "embedded requires same package")
    normalized = dict(value, upgrade_components=roles)
    require(len(canonical(normalized).encode("utf-8")) <= 64 * 1024, "desired declaration exceeds 64 KiB")
    return normalized


def declaration_configmap(obj, namespace, name, key):
    evidence = identity(obj, "ConfigMap", namespace)
    require(obj.get("apiVersion") == "v1" and evidence["name"] == name
            and obj.get("immutable", False) is False, "invalid or immutable ConfigMap")
    data = obj.get("data", {})
    require(isinstance(data, dict) and isinstance(obj.get("binaryData", {}), dict)
            and key not in obj.get("binaryData", {}), "invalid declaration data")
    require(key not in data or (isinstance(data[key], str) and bool(data[key].strip())),
            "invalid existing declaration text")
    return evidence, data.get(key)


def declaration_mount(pod, container, name, key):
    volumes = pod["spec"].get("volumes", [])
    names = [volume.get("name") for volume in volumes]
    require(all(text(name) for name in names) and len(set(names)) == len(names), "ambiguous volumes")
    containers = [item for item in pod["spec"]["containers"] if item.get("name") == container]
    require(len(containers) == 1, "ambiguous container")
    mounts = containers[0].get("volumeMounts", [])
    candidates = []
    for volume in volumes:
        source = volume.get("configMap")
        if not isinstance(source, dict) or source.get("name") != name:
            continue
        relative = key
        if "items" in source:
            items = source["items"]
            require(isinstance(items, list), "invalid ConfigMap items")
            selected = [item for item in items if item.get("key") == key]
            require(len(selected) == 1, "declaration key not uniquely mounted")
            relative = selected[0].get("path")
        require(isinstance(relative, str) and relative and not PurePosixPath(relative).is_absolute()
                and all(part not in ("", ".", "..") for part in relative.split("/")),
                "invalid declaration relative path")
        for mount in mounts:
            if mount.get("name") != volume.get("name"):
                continue
            require("subPath" not in mount and "subPathExpr" not in mount, "subPath unsupported")
            root = mount.get("mountPath")
            require(isinstance(root, str) and PurePosixPath(root).is_absolute()
                    and ".." not in PurePosixPath(root).parts, "invalid mount path")
            path = PurePosixPath(root) / relative
            for other in mounts:
                if other is mount:
                    continue
                other_root = other.get("mountPath")
                require(isinstance(other_root, str) and PurePosixPath(other_root).is_absolute(),
                        "invalid shadow mount")
                require(not (PurePosixPath(other_root).is_relative_to(PurePosixPath(root))
                             and path.is_relative_to(PurePosixPath(other_root))),
                        "shadowed declaration mount")
            candidates.append(str(path))
    require(len(candidates) == 1, "declaration requires one direct ConfigMap mount")
    return candidates[0]


def plan_process(args, kube, local):
    """Freeze two observations; injected readers perform GET and node-info only."""
    source = None
    candidate_path = args.declaration_candidate
    require((candidate_path is None) == (args.configmap is None), "candidate and ConfigMap must be paired")
    require(candidate_path is not None or args.declaration_key == "target.yaml", "key requires candidate")
    if candidate_path is not None:
        fields = (
            "role", "commit", "sha256", "archive_url", "archive_sha256",
            "component_config_path", "component_config_sha256")
        require(all(getattr(args, field) is None for field in fields), "candidate conflicts with manual target")
        candidate_text = read_json_file(candidate_path, raw=True)
        desired = declaration_candidate(decode(candidate_text.encode("utf-8")), candidate_text)
        require(desired["mode"] == "standalone" and len(desired.get("upgrade_components", [])) == 1,
                "process candidate requires standalone and exactly one upgrade component")
        args.role = desired["upgrade_components"][0]
        target = desired["components"][args.role]
        args.commit = target["version"]
        args.archive_url, args.archive_sha256 = target["artifact"]["url"], target["artifact"]["sha256"]
        args.component_config_path = target["config"]["mount_path"]
        args.component_config_sha256 = target["config"]["sha256"]
    require((args.component_config_path is None) == (args.component_config_sha256 is None),
            "component configuration flags must be paired")
    configuration = None if args.component_config_path is None else component_config(
        dict(mount_path=args.component_config_path, sha256=args.component_config_sha256))
    require(all(text(value) and not value.startswith("-") for value in (args.context, args.namespace, args.container)),
            "invalid Kubernetes selection")
    require(text(args.config) and PurePosixPath(args.config).is_absolute(),
            "config must be an absolute container path")
    require(PurePosixPath(args.config).suffix in (".yaml", ".yml"), "config must use .yaml or .yml")
    require(args.role in ("adxlet", "relay"), "invalid process role")
    require(text(args.task_id) and len(args.task_id.encode("utf-8")) <= 128, "invalid rollout ID")
    require(isinstance(args.epoch, int) and not isinstance(args.epoch, bool)
            and 0 < args.epoch <= 2**64 - 1, "epoch must be positive u64")
    require(isinstance(args.batch_size, int) and not isinstance(args.batch_size, bool)
            and args.batch_size > 0, "batch size must be positive")
    require(isinstance(args.commit, str) and re.fullmatch(r"[0-9a-f]{40}", args.commit), "invalid commit")
    require((args.sha256 is None and args.archive_url is not None)
            or (isinstance(args.sha256, str) and re.fullmatch(r"[0-9a-f]{64}", args.sha256)),
            "binary SHA-256 required without archive, or invalid expected SHA-256")
    require((args.archive_url is None) == (args.archive_sha256 is None), "archive flags must be paired")
    archive = None if args.archive_url is None else remote_archive(
        dict(url=args.archive_url, archive_sha256=args.archive_sha256))
    if args.role == "adxlet":
        require(text(args.origin), "adxlet requires origin")
        origin(args.origin)
    else:
        require(args.origin is None, "relay does not use origin")
    mappings = {}
    for entry in args.node:
        pod_name, separator, node = entry.partition("=")
        require(separator and text(pod_name) and not pod_name.startswith("-"), "invalid Pod mapping")
        require(text(node) and len(node.encode("utf-8")) <= 128 and "=" not in node, "invalid ADX node ID")
        require(pod_name not in mappings and node not in mappings.values(), "duplicate Pod or ADX node mapping")
        mappings[pod_name] = node
    require(mappings, "at least one explicit Pod mapping is required")
    if candidate_path is not None:
        require(text(args.configmap) and not args.configmap.startswith("-")
                and isinstance(args.declaration_key, str)
                and re.fullmatch(r"[A-Za-z0-9._-]+", args.declaration_key), "invalid ConfigMap selection")
        cm, original_text = declaration_configmap(kube([
            "get", "configmap", args.configmap, "--namespace", args.namespace]),
            args.namespace, args.configmap, args.declaration_key)
        require(original_text != candidate_text, "candidate equals original declaration")
        source = dict(name=args.configmap, namespace=args.namespace, uid=cm["uid"],
                      resource_version=cm["resource_version"], key=args.declaration_key,
                      original_text=original_text, candidate_text=candidate_text)

    def observe(pod_name, node):
        pod = kube(["get", "pod", pod_name, "--namespace", args.namespace])
        evidence = process_pod(pod, args.namespace, pod_name, args.container)
        path = declaration_mount(pod, args.container, source["name"], source["key"]) if source else None
        response = local(pod_name, path) if source else local(pod_name)
        if args.role == "adxlet":
            require_process_steps(response)
        component, _ = process_component(response, args.role, node, evidence["uid"])
        if source and original_text is not None:
            observation = response.get("external_declaration")
            require(isinstance(observation, dict) and set(observation) == {"path", "raw_sha256", "desired"}
                    and observation["path"] == path
                    and observation["raw_sha256"] == hashlib.sha256(original_text.encode("utf-8")).hexdigest()
                    and isinstance(observation["desired"], dict), "declaration source differs")
        record = {"node_id": node, "service_id": component["service_id"], "kube": evidence,
                  "runtime": component["runtime"].copy()}
        if source:
            record["declaration_path"] = path
        return record

    ordered = sorted(mappings.items(), key=lambda item: item[1])
    nodes = [observe(pod_name, node) for pod_name, node in ordered]
    for record, (pod_name, node) in zip(nodes, ordered):
        current = observe(pod_name, node)
        original_rv = record["kube"].pop("resource_version")
        current_rv = current["kube"].pop("resource_version")
        require(record == current, "Pod/container incarnation or process identity changed during planning")
        record["kube"].update(resource_version=original_rv, rechecked_resource_version=current_rv)
        # No batch index or mutable observation participates in the local task ID.
        key = dict(context=args.context, namespace=args.namespace, container=args.container, config=args.config,
                   role=args.role, pod=pod_name, node_id=node, service_id=record["service_id"],
                   rollout_id=args.task_id, epoch=args.epoch)
        local_id = hashlib.sha256(json.dumps(key, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()
        runtime = record.pop("runtime")
        submission = dict(selection={"mode": "publisher", "expected": runtime.copy()}, task_id=local_id,
                           role=args.role, service_id=record.pop("service_id"), commit=args.commit, sha256=args.sha256)
        if args.sha256 is None:
            del submission["sha256"]
        if configuration is not None:
            submission["component_config"] = configuration.copy()
        if archive is not None:
            submission["remote_archive"] = archive.copy()
        if args.role == "adxlet":
            submission["process"] = {"node_id": node, "identity": {
                "rollout_id": args.task_id, "epoch": args.epoch, "old_node_session": runtime["node_session"],
                "old_relay_session": runtime["relay_session"], "original_sha256": runtime["executable_sha256"]}}
        record["submission"] = submission
    node_ids = [record["node_id"] for record in nodes]
    batches = [node_ids[:1]]
    batches.extend(node_ids[index:index + args.batch_size] for index in range(1, len(nodes), args.batch_size))
    result = dict(schema_version=1, mode="process", context=args.context, namespace=args.namespace,
                  container=args.container, config=args.config, role=args.role, commit=args.commit,
                  sha256=args.sha256, rollout_id=args.task_id, epoch=args.epoch, nodes=nodes, batches=batches)
    if args.role == "adxlet":
        result["origin"] = args.origin
    if source:
        fresh, fresh_text = declaration_configmap(kube([
            "get", "configmap", source["name"], "--namespace", args.namespace]),
            args.namespace, source["name"], source["key"])
        require(fresh["uid"] == source["uid"] and fresh_text == original_text, "ConfigMap source changed")
        source["resource_version"] = fresh["resource_version"]
        result["declaration"] = source
    validate_process_plan(result)
    require(len(canonical(result).encode("utf-8")) <= LIMIT, "plan exceeds size limit")
    return result


def read_json_file(path, raw=False):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, mode=0o600)
    with os.fdopen(descriptor, "rb") as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "JSON input must be a regular file")
        data = stream.read(LIMIT + 1)
        value = decode(data)
        return data.decode("utf-8") if raw else value


def write_journal(path, journal):
    if path.is_symlink():
        raise Refusal("journal cannot be a symlink")
    if path.exists():
        require(stat.S_ISREG(path.stat().st_mode), "journal must be regular")
    payload = canonical(journal).encode("utf-8")
    require(len(payload) <= LIMIT, "journal exceeds size limit")
    descriptor, temporary = tempfile.mkstemp(prefix=".publisher-", dir=path.parent)
    try:
        with os.fdopen(descriptor, "wb") as stream:
            os.fchmod(stream.fileno(), 0o600)
            stream.write(payload)
            stream.flush()
            os.fsync(stream.fileno())
        os.replace(temporary, path)
        directory = os.open(path.parent, os.O_RDONLY | os.O_DIRECTORY, mode=0o700)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def validate_process_plan(plan):
    require(plan.get("schema_version") == 1 and plan.get("mode") == "process", "expected schema 1 process plan")
    require(not isinstance(plan["schema_version"], bool), "invalid schema version")
    source = plan.get("declaration")
    target = None
    if source is not None:
        require(isinstance(source, dict) and set(source) == {
            "name", "namespace", "uid", "resource_version", "key", "original_text", "candidate_text"},
            "invalid declaration source")
        require(all(text(source[key]) for key in ("name", "namespace", "uid", "resource_version", "key"))
                and not source["name"].startswith("-") and source["namespace"] == plan["namespace"]
                and re.fullmatch(r"[A-Za-z0-9._-]+", source["key"]), "invalid declaration metadata")
        require((source["original_text"] is None or isinstance(source["original_text"], str))
                and isinstance(source["candidate_text"], str)
                and source["original_text"] != source["candidate_text"], "invalid declaration texts")
        require(source["original_text"] is None or bool(source["original_text"].strip()),
                "invalid original declaration text")
        desired = declaration_candidate(decode(source["candidate_text"].encode("utf-8")), source["candidate_text"])
        require(desired["mode"] == "standalone" and desired.get("upgrade_components") == [plan["role"]]
                and plan["sha256"] is None,
                "candidate selection differs")
        target = desired["components"][plan["role"]]
        require(plan["commit"] == target["version"], "candidate version differs")
    require(plan["role"] in ("adxlet", "relay"), "invalid role")
    require(all(text(plan[key]) and not plan[key].startswith("-") for key in ("context", "namespace", "container")),
            "invalid Kubernetes selection")
    config = plan["config"]
    require(text(config) and PurePosixPath(config).is_absolute(), "config must be absolute")
    require(PurePosixPath(config).suffix in (".yaml", ".yml"), "config must be YAML")
    require(text(plan["rollout_id"]) and len(plan["rollout_id"].encode("utf-8")) <= 128, "invalid rollout ID")
    epoch = plan["epoch"]
    require(isinstance(epoch, int) and not isinstance(epoch, bool) and 0 < epoch <= 2**64 - 1, "invalid epoch")
    for key, size in (("commit", 40), ("sha256", 64)):
        if key == "sha256" and plan[key] is None:
            continue
        require(isinstance(plan[key], str) and re.fullmatch("[0-9a-f]{" + str(size) + "}", plan[key]),
                "invalid target " + key)
    if plan["role"] == "adxlet":
        origin(plan["origin"])
    else:
        require("origin" not in plan, "relay must not use Coordinator")
    nodes = plan["nodes"]
    require(isinstance(nodes, list) and bool(nodes), "empty process plan")
    ids, pods, tasks = [], set(), set()
    archive_source = nodes[0]["submission"].get("remote_archive")
    for node in nodes:
        fields = {"node_id", "kube", "submission"}
        if source is not None:
            fields.add("declaration_path")
            path = node.get("declaration_path")
            require(text(path) and PurePosixPath(path).is_absolute()
                    and ".." not in PurePosixPath(path).parts, "invalid declaration mount path")
        require(set(node) == fields, "invalid node fields")
        node_id, evidence, submission = node["node_id"], node["kube"], node["submission"]
        if target is not None:
            require(submission.get("component_config") == target["config"]
                    and submission.get("remote_archive") == dict(
                        url=target["artifact"]["url"], archive_sha256=target["artifact"]["sha256"])
                    and "sha256" not in submission, "Submission differs from candidate")
        require(submission.get("remote_archive") == archive_source, "archive source differs between nodes")
        require(text(node_id) and len(node_id.encode("utf-8")) <= 128, "invalid node ID")
        require(evidence["namespace"] == plan["namespace"] and evidence["container"] == plan["container"],
                "Kubernetes selection differs from plan")
        fields = ("name", "uid", "kubernetes_node_name", "container_id",
                  "resource_version", "rechecked_resource_version")
        require(all(text(evidence[key]) for key in fields),
                "missing evidence")
        require(not evidence["name"].startswith("-"), "invalid Pod name")
        restarts = evidence["restart_count"]
        require(isinstance(restarts, int) and not isinstance(restarts, bool) and restarts >= 0, "invalid restart count")
        require(isinstance(evidence["running"], dict), "missing running evidence")
        require(submission["selection"].get("mode") == "publisher", "publisher selection required")
        require(set(submission["selection"]) == {"mode", "expected"}, "invalid selection")
        expected = submission["selection"]["expected"]
        component = dict(role=plan["role"], node_id=node_id, service_id=submission["service_id"],
                         mode="standalone", can_upgrade=True, runtime=expected)
        process_component({"ok": True, "node_info": dict(schema_version=1, boot_nonce=expected["boot_nonce"],
                           pod_uid=evidence["uid"], components=[component])}, plan["role"], node_id, evidence["uid"])
        require(all(submission[key] == plan[key] for key in ("role", "commit")), "target differs")
        require(submission.get("sha256") == plan["sha256"], "expected digest differs")
        fields = {"selection", "task_id", "role", "service_id", "commit"}
        if "component_config" in submission:
            component_config(submission["component_config"])
            fields.add("component_config")
        if plan["sha256"] is not None:
            fields.add("sha256")
        else:
            require("remote_archive" in submission, "archive required without binary SHA-256")
        if "remote_archive" in submission:
            remote_archive(submission["remote_archive"])
            fields.add("remote_archive")
        if plan["role"] == "adxlet":
            fields.add("process")
            require(submission["process"] == {"node_id": node_id, "identity": {
                "rollout_id": plan["rollout_id"], "epoch": epoch, "old_node_session": expected["node_session"],
                "old_relay_session": expected["relay_session"], "original_sha256": expected["executable_sha256"]}},
                    "maintenance identity differs")
        require(set(submission) == fields, "invalid Submission fields")
        key = dict(context=plan["context"], namespace=plan["namespace"], container=plan["container"], config=config,
                   role=plan["role"], pod=evidence["name"], node_id=node_id, service_id=submission["service_id"],
                   rollout_id=plan["rollout_id"], epoch=epoch)
        task_id = hashlib.sha256(canonical(key).encode("utf-8")).hexdigest()
        require(submission["task_id"] == task_id, "local task ID differs from canonical identity")
        require(evidence["name"] not in pods and task_id not in tasks, "duplicate Pod/task")
        ids.append(node_id)
        pods.add(evidence["name"])
        tasks.add(task_id)
    require(ids == sorted(set(ids)), "nodes must be unique and sorted")
    batches = plan["batches"]
    require(isinstance(batches, list) and bool(batches), "missing batches")
    require(all(isinstance(batch, list) and bool(batch) for batch in batches), "invalid batch")
    require(batches[0] == ids[:1] and list(chain.from_iterable(batches)) == ids,
            "invalid canary or batch order")


def maintenance_owner(plan, node):
    return dict(node_id=node["node_id"], rollout_id=plan["rollout_id"],
                local_task_id=node["submission"]["task_id"], epoch=plan["epoch"])


def submit_arguments(submission):
    arguments = ["submit"]
    for key in ("task_id", "role", "service_id", "commit"):
        arguments.extend(["--" + key.replace("_", "-"), submission[key]])
    if "sha256" in submission:
        arguments.extend(["--sha256", submission["sha256"]])
    if "component_config" in submission:
        descriptor = submission["component_config"]
        arguments.extend(["--component-config-path", descriptor["mount_path"],
                          "--component-config-sha256", descriptor["sha256"]])
    arguments.extend(["--expected-runtime", canonical(submission["selection"]["expected"])])
    if "remote_archive" in submission:
        archive = remote_archive(submission["remote_archive"])
        arguments.extend(["--archive-url", archive["url"], "--archive-sha256", archive["archive_sha256"]])
    if "process" in submission:
        arguments.extend(["--process-identity", canonical(submission["process"])])
    return arguments


def check_process(plan, node, kube, local, original=False, complete=False):
    runtime, _ = observe_process(plan, node, kube, local, original, complete)
    return runtime


def observe_process(plan, node, kube, local, original=False, complete=False):
    evidence = node["kube"]
    current = process_pod(kube(["get", "pod", evidence["name"], "--namespace", plan["namespace"]]),
                          plan["namespace"], evidence["name"], plan["container"])
    for key in ("name", "namespace", "uid", "kubernetes_node_name", "container", "container_id", "restart_count"):
        require(current[key] == evidence[key], "Pod/container incarnation changed")
    response = local(evidence["name"])
    if plan["role"] == "adxlet":
        # Re-checked before every Submit/maintenance side effect: an old adxctl must
        # never receive a Submit that it would turn into a self-driven upgrade.
        require_process_steps(response)
    component, boot = process_component(response, plan["role"], node["node_id"],
                                        evidence["uid"], eligible=original, complete=original or complete)
    submission = node["submission"]
    require(component["service_id"] == submission["service_id"], "service changed")
    require(boot == submission["selection"]["expected"]["boot_nonce"], "supervisor boot changed")
    if original:
        require(component["runtime"] == submission["selection"]["expected"], "original runtime changed")
    return component.get("runtime"), response


def finish_process(plan, node, task, runtime):
    """Return proven batch outcome/session, or None when evidence is inconclusive."""
    phase = task.get("phase")
    expected = node["submission"]["selection"]["expected"]
    prepared = task.get("prepared")
    digest = prepared.get("binary_sha256") if isinstance(prepared, dict) else None
    prepared_valid = (isinstance(digest, str) and re.fullmatch(r"[0-9a-f]{64}", digest)
                      and (plan["sha256"] is None or plan["sha256"] == digest))
    configuration = prepared.get("configuration_sha256") if isinstance(prepared, dict) else None
    prepared_valid = (prepared_valid and isinstance(configuration, str)
                      and re.fullmatch(r"[0-9a-f]{64}", configuration))
    if plan["role"] == "relay":
        execution = task.get("execution")
        if phase == "failed" and execution is None:
            return "failed", None
        if not isinstance(execution, dict):
            return None
        stage = execution.get("phase")
        if phase == "failed" and stage == "cancelled_before_begin":
            return "failed", None
        success = phase == "completed" and stage == "completed"
        restored = phase == "failed" and stage == "rolled_back"
        retained = phase == "failed" and stage == "failed_retained"
        if retained and execution.get("loss_observed") is not True:
            return None
        if not any((success, restored, retained)):
            return None
        if not restored and not prepared_valid:
            return None
        digest = expected["executable_sha256"] if restored else digest
        expected_configuration = expected["configuration_sha256"] if restored else configuration
        if runtime["configuration_sha256"] != expected_configuration:
            return None
        if runtime["executable_sha256"] != digest or runtime["node_session"] != expected["node_session"]:
            return None
        session = execution.get("candidate_session")
        if not text(session) or runtime["relay_session"] != session:
            return None
        return ("completed" if success else "failed"), None
    execution = task.get("adxlet_execution")
    if phase == "failed" and execution is None and task.get("execution") is None:
        return ("failed", expected["node_session"]) if runtime == expected else None
    if not isinstance(execution, dict):
        return None
    success = phase == "completed" and execution.get("stage") == "completed"
    restored = phase == "failed" and execution.get("stage") == "original_selected"
    if not (success or restored):
        return None
    if not text(execution.get("attempt")) or not text(execution.get("node_session")):
        return None
    candidate = execution.get("candidate_pid")
    if isinstance(candidate, bool) or not isinstance(candidate, int):
        return None
    if success and not prepared_valid:
        return None
    digest = digest if success else expected["executable_sha256"]
    if runtime["configuration_sha256"] != (configuration if success else expected["configuration_sha256"]):
        return None
    if runtime["pid"] != candidate or runtime["executable_sha256"] != digest:
        return None
    if runtime["node_session"] != execution["node_session"]:
        return None
    if runtime["relay_session"] != expected["relay_session"]:
        return None
    return ("completed" if success else "failed"), runtime["node_session"]


def process_upgrade_status(post, owner):
    """Read one owner-scoped HTTP snapshot; transport failures never mean absent."""
    result = post("/v1/adxlet-process-upgrades/query", dict(owner=owner))
    require(isinstance(result, dict) and result.get("owner") == owner
            and isinstance(result.get("owner"), dict) and type(result["owner"].get("epoch")) is int,
            "maintenance query owner differs")
    status = result.get("status")
    fields = {"absent": {"status", "owner"},
              "active": {"status", "owner", "origin_session", "current_session", "yielded"},
              "released": {"status", "owner", "node_session"}}
    require(isinstance(status, str) and status in fields and set(result) == fields.get(status),
            "invalid maintenance query status or fields")
    sessions = ("origin_session", "current_session") if status == "active" else (
        ("node_session",) if status == "released" else ())
    require(all(text(result.get(key)) and len(result[key].encode("utf-8")) <= 128 for key in sessions),
            "invalid maintenance query session")
    require(status != "active" or isinstance(result.get("yielded"), bool), "invalid maintenance yield state")
    return result


def process_release_receipt(post, owner, session):
    result = process_upgrade_status(post, owner)
    if result["status"] == "released":
        require(result["node_session"] == session, "release receipt session differs")
        return dict(owner=owner, node_session=session)
    if result["status"] == "active":
        require(result["current_session"] == session, "release maintenance session differs")
    return None


def advance_adxlet_process(plan, node, state, save, kube, local, cli, post, sleep):
    """Drive local Steps under HTTP maintenance; new processes register themselves."""
    submission = node["submission"]
    expected = submission["selection"]["expected"]
    owner = maintenance_owner(plan, node)
    task_id = submission["task_id"]
    base = "/v1/adxlet-process-upgrades"
    resuming_release = state["stage"] == "release_intent"

    def task_response(response):
        require(response.get("ok") is True, "task response unavailable")
        task = response["task"]
        require(task.get("submission") == submission, "returned Submission differs")
        return task

    def query_task():
        check_process(plan, node, kube, local)
        return task_response(cli(node, ["query", "--task-id", task_id]))

    def marker_state(marker):
        require(isinstance(marker, dict) and marker.get("owner") == owner
                and marker.get("origin_session") == expected["node_session"],
                "maintenance owner/origin differs")
        return marker

    def query_marker():
        result = process_upgrade_status(post, owner)
        if result["status"] == "absent":
            return None
        require(result["status"] == "active", "maintenance already released")
        return marker_state(result)

    def owned_marker():
        return marker_state(query_marker())

    def prepared_task(task):
        require(task.get("phase") in ("preparing", "running")
                and isinstance(task.get("prepared"), dict), "Task is terminal or files are not prepared")

    def exited(task):
        execution = task.get("adxlet_execution")
        if not isinstance(execution, dict) or execution.get("stage") != "exited":
            return None
        require(type(execution.get("old_pid")) is int and execution["old_pid"] == expected["pid"],
                "exited old PID differs")
        fields = ("attempt", "node_session")
        require(all(text(execution.get(key)) and len(execution[key].encode("utf-8")) <= 128 for key in fields),
                "exited identity unavailable")
        require(execution["node_session"] != expected["node_session"], "new session equals old")
        require(type(execution.get("deadline_unix_ms")) is int and execution["deadline_unix_ms"] > 0,
                "local execution deadline unavailable")
        return execution

    def same_execution(task):
        execution = task.get("adxlet_execution")
        require(isinstance(execution, dict) and execution.get("attempt") == state["attempt"]
                and execution.get("node_session") == state["node_session"], "execution identity differs")
        return execution

    def budget_open(execution):
        deadline = execution.get("deadline_unix_ms")
        require(type(deadline) is int and deadline > 0, "local execution deadline unavailable")
        return int(time.time() * 1000) < deadline

    def finish_terminal(task):
        execution = task.get("adxlet_execution")
        retained = (task.get("phase") == "failed" and execution is None and task.get("execution") is None
                    and state["stage"] in ("prepare_intent", "exit_intent") and "attempt" not in state)
        selected = (isinstance(execution, dict) and
                    (task.get("phase"), execution.get("stage")) in
                    (("completed", "completed"), ("failed", "original_selected")))
        if not (retained or selected):
            return False
        if selected:
            same_execution(task)
            if task["phase"] == "failed":
                require(execution.get("rollback_from") == state.get("rollback_from")
                        and isinstance(execution.get("rollback_from"), dict), "original selection lacks A proof")
        runtime = check_process(plan, node, kube, local, complete=True)
        outcome = finish_process(plan, node, task, runtime)
        if outcome is None:
            return False
        marker = owned_marker()
        require(marker.get("current_session") == outcome[1]
                and marker.get("yielded") is (not retained), "terminal maintenance session differs")
        state.update(stage="release_intent", outcome=outcome[0], release_session=outcome[1])
        save()
        return True

    def begin_rollback(task):
        execution = same_execution(task)
        require(execution.get("rollback_from") is None, "original attempt cannot roll back again")
        require(execution.get("stage") in ("exited", "start_intent", "start_unknown"),
                "target outcome must be reconciled before rollback")
        require(type(execution.get("old_pid")) is int and execution["old_pid"] == expected["pid"],
                "rollback old PID differs")
        marker = owned_marker()
        require(marker.get("yielded") is True
                and marker.get("current_session") in (expected["node_session"], state["node_session"]),
                "rollback requires yielded original and known current session")
        state.update(stage="rollback_intent",
                     rollback_from=dict(attempt=state["attempt"], node_session=state["node_session"]))
        save()

    def adopt_original(task):
        execution = task.get("adxlet_execution")
        if not isinstance(execution, dict) or execution.get("rollback_from") is None:
            return False
        require(execution["rollback_from"] == state["rollback_from"], "rollback B names a different A")
        fields = ("attempt", "node_session")
        for key in fields:
            require(text(execution.get(key))
                        and len(execution[key].encode("utf-8")) <= 128
                        and execution[key] != state["rollback_from"][key],
                    "rollback B identity unavailable or reuses A")
        require(execution["node_session"] != expected["node_session"], "rollback B reuses origin session")
        require(execution.get("stage") in ("rollback_ready", "original_start_intent", "original_start_unknown",
                                           "original_commit_unknown", "original_selected"),
                "rollback B is not ready")
        budget_open(execution)  # validate the local, already fixed 600-second deadline
        state.update(stage="original_start_intent", attempt=execution["attempt"],
                     node_session=execution["node_session"])
        save()
        return True

    if state["stage"] == "new":
        if "declaration" in plan:
            declaration_gate(plan, node, kube, local, sleep)
        check_process(plan, node, kube, local, original=True)
        # A valid owner-scoped HTTP query is the read-only preflight before any effect.
        marker = query_marker()
        if marker is not None:
            require(marker["current_session"] == expected["node_session"] and marker["yielded"] is False,
                    "preflight maintenance session differs")
        state.update(stage="submit_intent", first_submit_unix_ms=int(time.time() * 1000))
        save()
        task = task_response(cli(node, submit_arguments(submission)))
    elif state["stage"] == "submit_intent":
        check_process(plan, node, kube, local)
        try:
            response = cli(node, ["query", "--task-id", task_id])
            require(response.get("ok") is True, "query result unavailable")
        except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                subprocess.SubprocessError, http.client.HTTPException):
            # Unknown Query is not non-execution proof. Only the exact original
            # identity permits replay through existing same-Submission idempotency.
            require(check_process(plan, node, kube, local) == expected, "original runtime changed")
            marker = query_marker()
            if marker is not None:
                require(marker["current_session"] == expected["node_session"] and marker["yielded"] is False,
                        "submission recovery maintenance session differs")
            save()
            response = cli(node, submit_arguments(submission))
        task = task_response(response)
    else:
        task = None

    if state["stage"] == "submit_intent":
        if task.get("phase") == "failed":
            runtime = check_process(plan, node, kube, local, complete=True)
            if (task.get("adxlet_execution") is None and task.get("execution") is None
                    and runtime == expected):
                state["stage"] = "failed"  # no maintenance has been acquired
                save()
                return "failed"
            return "pending"
        if task.get("phase") not in ("preparing", "running") or not isinstance(task.get("prepared"), dict):
            return "pending"
        state["stage"] = "prepared"
        save()
    if state["stage"] == "prepared":
        prepared_task(query_task())
        if "declaration" in plan:
            declaration_gate(plan, node, kube, local, sleep)
        require(check_process(plan, node, kube, local) == expected, "original runtime changed")
        state["stage"] = "prepare_intent"
        save()
    if state["stage"] == "prepare_intent":
        task = query_task()
        if not finish_terminal(task):
            prepared_task(task)
            require(check_process(plan, node, kube, local) == expected, "original runtime changed")
            marker = query_marker()  # also preflight before maintenance on recovery
            if marker is None:
                save()
                marker = post(base, dict(action="prepare", owner=owner, node_session=expected["node_session"]))
                require(marker.get("owner") == owner
                        and marker.get("origin_session") == expected["node_session"], "prepare receipt differs")
            # Exit re-queries this marker before sending the Step.
            require(marker.get("current_session") == expected["node_session"]
                    and marker.get("yielded") is False, "maintenance is not at the old active session")
            state["stage"] = "exit_intent"
            save()
    if state["stage"] == "exit_intent":
        task = query_task()
        if not finish_terminal(task):
            execution = exited(task)
            if execution is None:
                current = task.get("adxlet_execution")
                if current is None:
                    prepared_task(task)
                    require(check_process(plan, node, kube, local) == expected, "old runtime changed before Exit")
                elif current.get("stage") not in ("exit_intent", "exit_unknown"):
                    return "pending"
                marker = owned_marker()
                require(marker.get("current_session") == expected["node_session"], "Exit origin session differs")
                if current is None:
                    require(marker.get("yielded") is False, "first Exit requires unyielded original")
                save()
                # exit_unknown replays the original empty input, including expiry-only observation.
                cli(node, ["step", "--task-id", task_id, "--step", "exit",
                           "--attempt", "", "--node-session", ""])
                task = query_task()
                execution = exited(task)
            if execution is None:
                return "pending"
            marker = owned_marker()
            if marker.get("yielded") is not True or marker.get("current_session") != expected["node_session"]:
                return "pending"
            state.update(stage="start_intent", attempt=execution["attempt"], node_session=execution["node_session"])
            save()
    if state["stage"] == "start_intent":
        task = query_task()
        if not finish_terminal(task):
            execution = same_execution(task)
            marker = owned_marker()
            stage = execution.get("stage")
            allowed_sessions = (expected["node_session"],) if stage == "exited" else (
                expected["node_session"], state["node_session"])
            require(marker.get("yielded") is True and marker.get("current_session") in allowed_sessions,
                    "Start requires yielded origin and known current session")
            if stage not in ("exited", "start_intent", "start_unknown", "target_commit_unknown"):
                return "pending"
            if task.get("phase") == "completed":
                return "pending"  # contradictory success without complete proof cannot authorize rollback
            if stage != "target_commit_unknown" and (not budget_open(execution) or task.get("phase") == "failed"):
                begin_rollback(task)
            else:
                # Replaying a local Start intent/unknown is idempotent. A plain exited
                # Task with an unexplained live process is not permission to spawn.
                if stage == "exited" and check_process(plan, node, kube, local) is not None:
                    return "pending"
                save()
                step_error = None
                try:
                    cli(node, ["step", "--task-id", task_id, "--step", "start",
                               "--attempt", state["attempt"], "--node-session", state["node_session"]])
                except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                        subprocess.SubprocessError, http.client.HTTPException) as error:
                    step_error = error
                task = query_task()
                if step_error is not None and not (
                        stage == "start_unknown" and task.get("phase") in ("unknown", "failed")
                        and task.get("adxlet_execution", {}).get("stage") == "start_unknown"):
                    # A CLI failure is never rollback proof. Preserve the unknown
                    # request unless fresh Query confirms the same local failure stage.
                    raise step_error
                if not finish_terminal(task):
                    execution = same_execution(task)
                    # Commit ambiguity uses only same-A Receipt, even after expiry.
                    if execution.get("stage") == "target_commit_unknown":
                        return "pending"
                    if execution.get("stage") != "start_unknown" or task.get("phase") not in ("unknown", "failed"):
                        return "pending"
                    if stage != "start_unknown" and budget_open(execution):
                        return "pending"
                    begin_rollback(task)
    if state["stage"] in ("rollback_intent", "original_start_intent"):
        task = query_task()
        # B can already be terminal when the accepted local rollback returns.
        # Validate its source and adopt its identity before terminal comparison.
        adopted = adopt_original(task)
        if not finish_terminal(task):
            execution = same_execution(task)
            if adopted:
                require(execution.get("rollback_from") == state["rollback_from"], "original attempt lacks A proof")
            else:
                require(state["rollback_from"] == dict(
                        attempt=execution["attempt"], node_session=execution["node_session"]),
                        "Rollback must replay the original A")
                require(execution.get("stage") in ("exited", "start_intent", "start_unknown",
                                                   "rollback_intent", "rollback_unknown"),
                        "rollback target outcome unavailable")
                # The first request still requires this owner's yielded marker.
                # Once B exists the accepted local action needs no new approval.
                marker = owned_marker()
                require(marker.get("yielded") is True and marker.get("current_session") in (
                        expected["node_session"], state["rollback_from"]["node_session"]),
                        "Rollback requires yielded origin and known current session")
            save()
            # Even after B exists, always replay the frozen source A. Only the
            # local rollback worker may start/resume B; never issue Start(B).
            cli(node, ["step", "--task-id", task_id, "--step", "rollback",
                       "--attempt", state["rollback_from"]["attempt"],
                       "--node-session", state["rollback_from"]["node_session"]])
            task = query_task()
            adopt_original(task)
            if not finish_terminal(task):
                return "pending"  # B failure retains maintenance, without recursive rollback
    if state["stage"] == "release_intent":
        body = dict(owner=owner, node_session=state["release_session"])
        receipt = process_release_receipt(post, owner, body["node_session"]) if resuming_release else None
        task = query_task()
        if receipt is not None:
            require(receipt == body, "release receipt differs")
            require(task.get("phase") == state["outcome"], "local task outcome differs from release intent")
            if "attempt" in state:
                execution = same_execution(task)
                terminal_stage = "completed" if state["outcome"] == "completed" else "original_selected"
                require(execution.get("stage") == terminal_stage, "local task has no terminal selection")
                require(execution["node_session"] == body["node_session"], "local release session differs")
                require(execution.get("rollback_from") == state.get("rollback_from"), "local rollback source differs")
            else:
                require(state["outcome"] == "failed" and task.get("adxlet_execution") is None
                        and task.get("execution") is None, "local task did not retain original process")
                require(body["node_session"] == expected["node_session"], "local original session differs")
        else:
            if "rollback_from" in state and state["outcome"] == "failed":
                execution = same_execution(task)
                require(execution.get("rollback_from") == state["rollback_from"], "release B names a different A")
            runtime = check_process(plan, node, kube, local, complete=True)
            require(finish_process(plan, node, task, runtime) == (state["outcome"], body["node_session"]),
                    "release requires current terminal candidate and frozen session")
            save()
            try:
                receipt = post(base, dict(action="release", **body))
            except (Refusal, OSError, ValueError, http.client.HTTPException):
                receipt = None
            if receipt != body:
                receipt = process_release_receipt(post, owner, body["node_session"])
            require(receipt == body, "release receipt unavailable or differs")
        state["stage"] = state["outcome"]
        save()
    return state["stage"] if state["stage"] in ("completed", "failed") else "pending"


def advance_process(plan, node, state, save, kube, local, cli, post, sleep):
    if plan["role"] == "adxlet":
        return advance_adxlet_process(plan, node, state, save, kube, local, cli, post, sleep)
    submission = node["submission"]
    deadline_queried = False
    if state["stage"] == "new":
        if "declaration" in plan:
            declaration_gate(plan, node, kube, local, sleep)
        check_process(plan, node, kube, local, original=True)
        state["stage"] = "prepared"
        save()
    if state["stage"] == "prepared":
        if "declaration" in plan:
            declaration_gate(plan, node, kube, local, sleep)
        check_process(plan, node, kube, local, original=True)
        state.update(stage="submit_intent", first_submit_unix_ms=int(time.time() * 1000))
        save()
        response = cli(node, submit_arguments(submission))
    elif state["stage"] == "submit_intent":
        check_process(plan, node, kube, local)
        try:
            response = cli(node, ["query", "--task-id", submission["task_id"]])
            require(response.get("ok") is True, "query result unavailable")
        except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                subprocess.SubprocessError, http.client.HTTPException):
            if int(time.time() * 1000) >= state["first_submit_unix_ms"] + 2400 * 1000:
                return "pending"
            # Uncertain Query is not non-execution proof. Exact original identity
            # only permits the server's existing same-Submission idempotent replay.
            check_process(plan, node, kube, local, original=True)
            response = cli(node, submit_arguments(submission))
    else:
        response = None
    while state["stage"] == "submit_intent":
        require(response.get("ok") is True, "task response unavailable")
        task = response["task"]
        require(task.get("submission") == submission, "returned Submission differs")
        phase = task.get("phase")
        if phase in ("completed", "failed"):
            runtime = check_process(plan, node, kube, local, complete=True)
            outcome = finish_process(plan, node, task, runtime)
            if outcome is None:
                return "pending"
            state["stage"] = outcome[0]
            save()
            break
        if phase not in ("preparing", "running"):
            return "pending"
        if int(time.time() * 1000) >= state["first_submit_unix_ms"] + 2400 * 1000:
            if deadline_queried:
                return "pending"
            # One final query this invocation, not a new server execution budget.
            deadline_queried = True
        else:
            sleep(2)
        check_process(plan, node, kube, local)
        response = cli(node, ["query", "--task-id", submission["task_id"]])
    return state["stage"] if state["stage"] in ("completed", "failed") else "pending"


def declaration_patch(source, resource_version, before, after, data_present=True, fence=None):
    key = source["key"].replace("~", "~0").replace("/", "~1")
    body = [dict(op="test", path="/metadata/uid", value=source["uid"]),
            dict(op="test", path="/metadata/resourceVersion", value=resource_version)]
    if before is not None:
        body.append(dict(op="test", path=f'/data/{key}', value=before))
    if before == after:
        pass
    elif after is None:
        body.append(dict(op="remove", path=f'/data/{key}'))
    elif before is None:
        body.append(dict(op="add", path=f'/data/{key}' if data_present else "/data",
                         value=after if data_present else {source["key"]: after}))
    else:
        body.append(dict(op="replace", path=f'/data/{key}', value=after))
    if fence is not None:
        body.append(dict(op="add", path="/metadata/annotations", value=fence))
    return body


def declaration_write(plan, journal, save, kube, mutate, restore=False):
    source = plan["declaration"]
    obj = kube(["get", "configmap", source["name"], "--namespace", source["namespace"]])
    evidence, current = declaration_configmap(obj, source["namespace"], source["name"], source["key"])
    require(evidence["uid"] == source["uid"], "ConfigMap UID changed")
    resource_version = evidence["resource_version"]
    marker_key = "adx.dev/rollout-input-fence"
    nonce = template_hash(plan)
    applied, restored = nonce + ":applied", nonce + ":restored"
    annotations = obj["metadata"].get("annotations", {})
    require(isinstance(annotations, dict), "invalid CM annotations")
    marker = annotations.get(marker_key)
    owned = journal.get("declaration_write")
    for name, intent in (("declaration_write", owned), ("declaration_restore", journal.get("declaration_restore"))):
        if intent is None:
            continue
        require(isinstance(intent, dict) and set(intent) == {"rv", "data_present", "fence", "body", "before"}
                and text(intent["rv"]) and isinstance(intent.get("data_present"), bool), "invalid CM intent")
        reverse = name == "declaration_restore"
        before = intent["before"]
        require(before in (source["original_text"], source["candidate_text"])
                and (reverse or before == source["original_text"]), "invalid CM intent input")
        after = source["original_text"] if reverse else source["candidate_text"]
        fence = intent["fence"]
        require(isinstance(fence, dict) and fence.get(marker_key) == (restored if reverse else applied),
                "invalid CM fence")
        require(reverse or intent["rv"] == source["resource_version"], "forward RV differs from frozen source")
        require(intent["body"] == declaration_patch(
            source, intent["rv"], before, after, intent["data_present"], fence), "CM intent body differs")
    require(current in (source["original_text"], source["candidate_text"]), "foreign ConfigMap text")
    require(marker != applied or current == source["candidate_text"], "applied marker/data conflict")
    require(marker != restored or current == source["original_text"], "restored marker/data conflict")
    if restore:
        require(owned is not None, "no owned ConfigMap write")
        if marker == restored:
            require(current == source["original_text"], "restored marker/data conflict")
            return
        require((marker == applied and current == source["candidate_text"])
                or (resource_version == source["resource_version"] and current == source["original_text"]),
                "CM ownership unknown or superseded")
        name = "declaration_restore"
        before, after = current, source["original_text"]
        desired_marker = restored
    else:
        require("declaration_restore" not in journal, "input restoration already started")
        require(marker != restored, "input restoration already closed this rollout")
        if marker == applied:
            require(owned is not None, "foreign candidate without owned intent")
            require(current == source["candidate_text"], "applied marker/data conflict")
            return
        require(resource_version == source["resource_version"] and current == source["original_text"],
                "CM ownership unknown or superseded; replan required")
        name, before, after = "declaration_write", source["original_text"], source["candidate_text"]
        desired_marker = applied
    fence = dict(annotations)
    fence[marker_key] = desired_marker
    intent = journal.get(name)
    if intent is None:
        intent = dict(rv=resource_version, data_present="data" in obj, fence=fence, before=before,
                      body=declaration_patch(source, resource_version, before, after, "data" in obj, fence))
        journal[name] = intent
        save()
    require(intent["rv"] == resource_version and intent["before"] == current, "fixed CM intent no longer applicable")
    require(mutate is not None, "ConfigMap mutation transport unavailable")
    if intent["rv"] == resource_version:
        require(intent["data_present"] == ("data" in obj), "CM intent data presence differs")
    require(intent["fence"] == fence, "fence annotations differ from fresh object")
    mutate(["patch", "configmap", source["name"], "--namespace", source["namespace"],
            "--type=json", "--patch-file=/dev/stdin", "-o", "json"], intent["body"])
    fresh = kube(["get", "configmap", source["name"], "--namespace", source["namespace"]])
    identity_now, text_now = declaration_configmap(fresh, source["namespace"], source["name"], source["key"])
    require(identity_now["uid"] == source["uid"] and text_now == after
            and fresh["metadata"].get("annotations", {}).get(marker_key) == desired_marker
            and identity_now["resource_version"] != resource_version, "ConfigMap effect unconfirmed")


def declaration_gate(plan, node, kube, local, sleep, override=False, expected_text=None, marker="applied"):
    source = plan["declaration"]
    expected_text = source["candidate_text"] if expected_text is None else expected_text
    desired = declaration_candidate(decode(expected_text.encode("utf-8")))
    deadline = time.monotonic() + 30
    for _ in range(16):
        obj = kube(["get", "configmap", source["name"], "--namespace", source["namespace"]])
        evidence, current = declaration_configmap(obj, source["namespace"], source["name"], source["key"])
        require(evidence["uid"] == source["uid"] and current == expected_text
                and obj["metadata"].get("annotations", {}).get("adx.dev/rollout-input-fence")
                == template_hash(plan) + ":" + marker, "CM gate changed")
        pod = kube(["get", "pod", node["kube"]["name"], "--namespace", source["namespace"]])
        require(declaration_mount(pod, plan["container"], source["name"], source["key"])
                == node["declaration_path"], "declaration mount changed")
        response = local(node["kube"]["name"], node["declaration_path"]) if override else local(node["kube"]["name"])
        observation = response.get("external_declaration")
        if observation == dict(path=node["declaration_path"], desired=desired,
                               raw_sha256=hashlib.sha256(expected_text.encode("utf-8")).hexdigest()):
            require(time.monotonic() < deadline, "candidate projection observation deadline exceeded")
            return
        if time.monotonic() >= deadline:
            break
        sleep(2)
    raise Refusal("candidate projection not yet observed")


def process_declaration_restore(plan, node, state, journal, save, kube, local, cli, post, mutate):
    response = cli(node, ["query", "--task-id", node["submission"]["task_id"]])
    require(response.get("ok") is True and response["task"].get("submission") == node["submission"],
            "restoration requires fresh exact task")
    task = response["task"]
    runtime = check_process(plan, node, kube, local, complete=True)
    expected = node["submission"]["selection"]["expected"]
    execution = task.get("adxlet_execution") if plan["role"] == "adxlet" else task.get("execution")
    restored = (isinstance(execution, dict) and
                execution.get("stage" if plan["role"] == "adxlet" else "phase") ==
                ("original_selected" if plan["role"] == "adxlet" else "rolled_back"))
    never = (execution is None or (plan["role"] == "relay" and isinstance(execution, dict)
                                   and execution.get("phase") == "cancelled_before_begin"))
    require(task.get("phase") == "failed" and ((restored and finish_process(plan, node, task, runtime)
            == ("failed", runtime["node_session"] if plan["role"] == "adxlet" else None))
            or (never and runtime == expected)), "original runtime not proven")
    if plan["role"] == "adxlet" and "release_session" in state:
        body = dict(owner=maintenance_owner(plan, node), node_session=runtime["node_session"])
        require(state.get("release_session") == runtime["node_session"]
                and process_release_receipt(post, body["owner"], body["node_session"]) == body,
                "fresh released original receipt required")
    declaration_write(plan, journal, save, kube, mutate, restore=True)


def completed_scope(plan, node, state, kube, local):
    if plan["mode"] == "process":
        runtime, response = observe_process(plan, node, kube, local, complete=True)
        namespace = plan["namespace"]
        pod = kube(["get", "pod", node["kube"]["name"], "--namespace", namespace])
        actual = pod_actual(pod, namespace, plan["container"], node["node_id"], response)
        require(all(actual[key] == node["kube"][key] for key in ("container_id", "restart_count"))
                and actual["boot_nonce"] == node["submission"]["selection"]["expected"]["boot_nonce"],
                "process container or parent changed")
    else:
        namespace = plan["begin"]["namespace"]
        matches = [pod for pod in pod_inventory(namespace, kube) if pod["metadata"]["uid"] == state["replacement_uid"]]
        require(len(matches) == 1, "replacement UID changed")
        pod = matches[0]
        response = local(pod["metadata"]["name"])
        actual = pod_actual(pod, namespace, plan["container"], node["node_id"], response)
        require(all(actual[key] == state["replacement_actual"][key] for key in actual if key != "node_session"),
                "replacement container or parent changed")
        require(workload_owns(plan["workload"], pod)
                and pod["spec"].get("nodeName") == replacement_host(
                    plan["workload"], node["kubernetes_node_name"], state)
                and image(pod, plan["container"]) == plan["begin"]["target_image"], "replacement scope changed")
        runtime = None
    return pod, runtime, response


def promote_ready(plan, journal, save, kube, local, mutate, sleep, cli=None, post=None, get=None):
    require("declaration" in plan and "declaration_restore" not in journal and "cleanup" not in journal,
            "promotion requires an unrestored declaration rollout")
    source = plan["declaration"]
    desired = declaration_candidate(decode(source["candidate_text"].encode("utf-8")), source["candidate_text"])
    promoted = dict(desired, ready=dict(components=desired["components"]))
    after = canonical(promoted)
    declaration_candidate(promoted, after)
    marker_key = "adx.dev/rollout-input-fence"
    nonce = template_hash(plan)
    obj = kube(["get", "configmap", source["name"], "--namespace", source["namespace"]])
    evidence, current = declaration_configmap(obj, source["namespace"], source["name"], source["key"])
    require(evidence["uid"] == source["uid"], "promotion ConfigMap UID changed")
    marker = obj["metadata"].get("annotations", {}).get(marker_key)
    intent = journal.get("ready_intent")
    if intent is not None:
        require(set(intent) == {"rv", "data_present", "fence", "body"}
                and intent["fence"].get(marker_key) == nonce + ":promoted"
                and intent["body"] == declaration_patch(source, intent["rv"], source["candidate_text"], after,
                                                       intent["data_present"], intent["fence"]),
                "promotion intent differs")
    confirmed = marker == nonce + ":promoted" and current == after
    require(confirmed or (marker == nonce + ":applied" and current == source["candidate_text"]),
            "promotion ownership unknown or superseded")
    require(not confirmed or intent is not None, "promoted marker without durable intent")
    if plan["mode"] == "process":
        require(all(journal["nodes"].get(node["node_id"], {}).get("stage") == "completed" for node in plan["nodes"]),
                "all process batches must complete before promotion")
        nodes = plan["nodes"]
    else:
        require(journal["stage"] == "completed", "all Pod batches must complete before promotion")
        task = pod_task(plan, get)
        require(task["phase"] == "Completed" and all(n["phase"] == "verified" for n in task["nodes"].values())
                and all(op["phase"] == "Completed" for op in task["operations"].values()),
                "Pod completion proof unavailable")
        nodes = plan["pods"]
    gates = []
    for node in nodes:
        state = journal["nodes"][node["node_id"]]
        if plan["mode"] == "process":
            result = cli(node, ["query", "--task-id", node["submission"]["task_id"]])
            task = result.get("task", {})
            require(result.get("ok") is True and task.get("submission") == node["submission"]
                    and task.get("phase") == "completed", "exact completed process task required")
            prepared = task.get("prepared", {})
            require(all(isinstance(prepared.get(key), str) and re.fullmatch(r"[0-9a-f]{64}", prepared[key])
                        for key in ("binary_sha256", "configuration_sha256")), "prepared identity unknown")
            if plan["role"] == "adxlet":
                body = dict(owner=maintenance_owner(plan, node), node_session=state["release_session"])
                require(process_release_receipt(post, body["owner"], body["node_session"]) == body,
                        "exact historical released receipt required")
        else:
            require(state.get("stage") == "verified", "Pod node not historically verified")
        pod, runtime, response = completed_scope(plan, node, state, kube, local)
        pod_name = pod["metadata"]["name"]
        if plan["mode"] == "process":
            if not confirmed:
                require(runtime["executable_sha256"] == prepared["binary_sha256"]
                        and runtime["configuration_sha256"] == prepared["configuration_sha256"],
                        "current selected component differs from prepared content")
        require(declaration_mount(pod, plan["container"], source["name"], source["key"])
                == node["declaration_path"], "promotion projection mount changed")
        if not confirmed:
            components = response["node_info"]["components"]
            require(components[0]["mode"] == desired["mode"], "current node mode differs")
            for component in components:
                roles = (component["role"],) if desired["mode"] == "standalone" else ("adxlet", "relay")
                sources = {role: dict(version=desired["components"][role]["version"],
                    archive_sha256=desired["components"][role]["artifact"]["sha256"],
                    raw_config_sha256=desired["components"][role]["config"]["sha256"]) for role in roles}
                require(component.get("sources") == sources, "complete current group provenance required")
        gates.append(dict(kube={"name": pod_name}, declaration_path=node["declaration_path"]))
    if not confirmed:
        fence = dict(obj["metadata"].get("annotations", {}))
        fence[marker_key] = nonce + ":promoted"
        if intent is None or intent["rv"] != evidence["resource_version"]:
            intent = dict(rv=evidence["resource_version"], data_present="data" in obj, fence=fence)
            intent["body"] = declaration_patch(source, intent["rv"], source["candidate_text"], after,
                                                intent["data_present"], fence)
            journal["ready_intent"] = intent
            save()
        require(intent["rv"] == evidence["resource_version"] and intent["fence"] == fence,
                "fixed promotion intent superseded")
        mutate(["patch", "configmap", source["name"], "--namespace", source["namespace"],
                "--type=json", "--patch-file=/dev/stdin", "-o", "json"], intent["body"])
    for node in gates:
        declaration_gate(plan, node, kube, local, sleep, override=True, expected_text=after, marker="promoted")
    return dict(status="completed", ready_promoted=True)


def apply_process(plan, journal_path, batch_index, kube, local, cli, post, sleep=time.sleep, mutate=None,
                  promote=False):
    validate_process_plan(plan)
    # Linux publisher Job only. Keep this lock across all I/O and atomic journal replacements.
    require(sys.platform == "linux", "apply-process requires a Linux publisher")
    import fcntl

    require((promote and batch_index is None)
            or (not promote and type(batch_index) is int
                and 0 <= batch_index < len(plan["batches"])), "select batch index or promotion")
    path = Path(journal_path).absolute()
    lock = os.open(path.with_name(path.name + ".lock"), os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(lock, "a+b") as lock_stream:
        require(stat.S_ISREG(os.fstat(lock_stream.fileno()).st_mode), "lock must be regular")
        try:
            fcntl.flock(lock_stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refusal("another publisher holds journal lock") from error
        digest = template_hash(plan)
        journal_schema = 2 if plan["role"] == "adxlet" else 1
        if path.exists() or path.is_symlink():
            journal = read_json_file(path)
            require(journal.get("plan_hash") == digest, "journal plan hash differs")
            require(journal.get("schema_version") == journal_schema, "invalid journal schema")
        else:
            require(not promote, "post-release actions require existing journal")
            journal = dict(schema_version=journal_schema, plan_hash=digest, nodes={})
            write_journal(path, journal)
        states = journal["nodes"]
        require(isinstance(states, dict), "invalid journal nodes")
        by_id = {node["node_id"]: node for node in plan["nodes"]}
        require(set(states).issubset(by_id), "journal contains unknown nodes")
        for node_id, state in states.items():
            require(isinstance(state, dict), "invalid node journal")
            stage = state.get("stage")
            stages = {"new", "prepared", "submit_intent", "completed", "failed"}
            if plan["role"] == "adxlet":
                stages.update(("prepare_intent", "exit_intent", "start_intent", "release_intent",
                               "rollback_intent", "original_start_intent"))
            require(stage in stages, "invalid journal stage")
            if stage in ("submit_intent", "release_intent", "completed", "failed") or (
                    plan["role"] == "adxlet" and stage != "new"):
                first = state.get("first_submit_unix_ms")
                require(isinstance(first, int) and not isinstance(first, bool) and first > 0,
                        "missing first submit timestamp")
            if "release_session" in state:
                session = state["release_session"]
                require(plan["role"] == "adxlet", "invalid release journal")
                require(text(session) and len(session.encode("utf-8")) <= 128,
                        "invalid release session")
                require(state.get("outcome") in ("completed", "failed"), "invalid release outcome")
            if "attempt" in state or "node_session" in state or stage in (
                    "start_intent", "rollback_intent", "original_start_intent"):
                require(plan["role"] == "adxlet" and all(text(state.get(key))
                        and len(state[key].encode("utf-8")) <= 128 for key in ("attempt", "node_session")),
                        "invalid durable Step identity")
                require(state["node_session"] != by_id[node_id]["submission"]["selection"]["expected"]["node_session"],
                        "new session equals old")
            if "rollback_from" in state or stage in (
                    "rollback_intent", "original_start_intent"):
                source = state.get("rollback_from")
                require(plan["role"] == "adxlet" and isinstance(source, dict)
                        and set(source) == {"attempt", "node_session"}
                        and all(text(source[key]) and len(source[key].encode("utf-8")) <= 128 for key in source),
                        "invalid frozen rollback A")
                require(source["node_session"] != by_id[node_id]["submission"]["selection"]["expected"]["node_session"],
                        "rollback A reuses origin session")
                if stage == "rollback_intent":
                    require(all(state[key] == source[key] for key in source), "Rollback identity must remain A")
                if stage == "original_start_intent":
                    require(all(state[key] != source[key] for key in source), "original identity must be B")
            if plan["role"] == "adxlet" and stage in ("release_intent", "completed"):
                require("release_session" in state, "missing durable release session")
            if plan["role"] == "adxlet" and stage == "failed" and "release_session" not in state:
                require("attempt" not in state and "outcome" not in state,
                        "failed execution requires durable release session")
        for previous in (plan["batches"][:batch_index] if not promote else []):
            require(all(states.get(node_id, {}).get("stage") == "completed" for node_id in previous),
                    "prior batch is not successfully completed and released")

        def save():
            write_journal(path, journal)

        if promote:
            return promote_ready(plan, journal, save, kube, local, mutate, sleep, cli=cli, post=post)
        selected = plan["batches"][batch_index]
        declaration_stages = ("new", "prepare_intent", "prepared")
        if plan["role"] == "adxlet":
            declaration_stages += ("submit_intent",)
        for node_id in selected:
            state = states.setdefault(node_id, {"stage": "new"})
            if "declaration" in plan and state["stage"] in declaration_stages:
                try:
                    if plan["role"] == "adxlet":
                        owner = maintenance_owner(plan, by_id[node_id])
                        old_session = by_id[node_id]["submission"]["selection"]["expected"]["node_session"]
                        marker = process_upgrade_status(post, owner)
                        require(marker["status"] == "absent" or (marker["status"] == "active"
                                and marker["origin_session"] == old_session
                                and marker["current_session"] == old_session and marker["yielded"] is False),
                                "maintenance owner/origin differs")
                    if "declaration_write" not in journal:
                        for candidate in plan["nodes"]:
                            if states.get(candidate["node_id"], {}).get("stage", "new") in declaration_stages:
                                check_process(plan, candidate, kube, local, original=True)
                    declaration_write(plan, journal, save, kube, mutate)
                except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                        subprocess.SubprocessError, http.client.HTTPException) as error:
                    return dict(status="pending", batch_index=batch_index, node_id=node_id, detail=str(error))
            if state["stage"] in ("completed", "failed"):
                result = state["stage"]
            else:
                try:
                    result = advance_process(plan, by_id[node_id], state, save, kube, local, cli, post, sleep)
                except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                        subprocess.SubprocessError, http.client.HTTPException) as error:
                    return dict(status="pending", batch_index=batch_index, node_id=node_id, detail=str(error))
            if result != "completed":
                if result == "failed" and "declaration" in plan:
                    try:
                        process_declaration_restore(plan, by_id[node_id], state, journal, save,
                                                    kube, local, cli, post, mutate)
                    except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                            subprocess.SubprocessError, http.client.HTTPException) as error:
                        return dict(status="failed", batch_index=batch_index, node_id=node_id,
                                    input_restore="pending", detail=str(error))
                return dict(status=result, batch_index=batch_index, node_id=node_id)
        return dict(status="completed", batch_index=batch_index)


def validate_pod_plan(plan):
    require(type(plan.get("schema_version")) is int and plan["schema_version"] == 1
            and plan.get("mode") == "pod", "expected schema 1 Pod plan")
    origin(plan["origin"])
    require(text(plan["config"]) and PurePosixPath(plan["config"]).is_absolute()
            and PurePosixPath(plan["config"]).suffix in (".yaml", ".yml"), "absolute deployment YAML required")
    require(all(text(plan[k]) and not plan[k].startswith("-") for k in ("context", "container")),
            "invalid Pod selection")
    begin, workload = plan["begin"], plan["workload"]
    kind = workload_kind(workload)
    if "declaration" in plan:
        source = plan["declaration"]
        require(set(source) == {
            "name", "namespace", "uid", "resource_version", "key", "original_text", "candidate_text"}
                and all(text(source[key]) for key in ("name", "namespace", "uid", "resource_version", "key"))
                and source["namespace"] == begin["namespace"]
                and not source["name"].startswith("-") and re.fullmatch(r"[A-Za-z0-9._-]+", source["key"])
                and (source["original_text"] is None or text(source["original_text"]))
                and source["original_text"] != source["candidate_text"], "invalid declaration source")
        desired = declaration_candidate(decode(source["candidate_text"].encode("utf-8")), source["candidate_text"])
        require(not desired.get("upgrade_components", []), "Pod candidate cannot upgrade processes")
    require(text(begin["task_id"]) and len(begin["task_id"].encode("utf-8")) <= 128
            and "/" not in begin["task_id"] and "%" not in begin["task_id"], "invalid task ID")
    require(type(begin["epoch"]) is int and 0 < begin["epoch"] <= 2**64 - 1, "invalid epoch")
    require(begin["namespace"] == workload["namespace"] and begin["daemonset_uid"] == workload["uid"],
            "workload binding differs")
    require(re.fullmatch(r"[^\s@]+@sha256:[0-9a-f]{64}", begin["target_image"]), "immutable image required")
    require(all(text(workload[k]) for k in ("name", "uid", "namespace", "original_image")), "invalid workload")
    for key in ("original_template_hash", "target_template_hash"):
        require(re.fullmatch("[0-9a-f]{64}", workload[key]), "invalid template fingerprint")
    pods = plan["pods"]
    require(isinstance(pods, list) and bool(pods), "empty Pod plan")
    ids, names, hosts, uids = [], [], [], []
    bindings = []
    for pod in pods:
        require(all(text(pod[k]) for k in ("name", "uid", "node_id", "kubernetes_node_name")), "invalid Pod")
        require(pod["namespace"] == begin["namespace"] and pod["owner_uid"] == workload["uid"]
                and pod["target_image"] == begin["target_image"], "Pod plan binding differs")
        inspection = pod["inspection"]
        actual = pod["actual"]
        require(all(text(actual[key]) for key in ("container_id", "image_id", "boot_nonce", "node_session"))
                and actual["node_session"] == inspection["node_session_id"], "invalid actual source identity")
        if "declaration" in plan:
            require(text(pod["declaration_path"]) and PurePosixPath(pod["declaration_path"]).is_absolute(),
                    "invalid declaration mount")
        require(inspection["observed_pod_uid"] == pod["uid"] and inspection["node_id"] == pod["node_id"]
                and text(inspection["node_session_id"]), "Pod inspection binding differs")
        bindings.append(dict(node_id=pod["node_id"], pod_uid=pod["uid"],
                             node_session_id=inspection["node_session_id"]))
        ids.append(pod["node_id"])
        names.append(pod["name"])
        hosts.append(pod["kubernetes_node_name"])
        uids.append(pod["uid"])
    require(names == sorted(names) and len(set(names)) == len(names)
            and len(set(ids)) == len(ids) and len(set(uids)) == len(uids)
            and (kind == "StatefulSet" or len(set(hosts)) == len(hosts)),
            "duplicate or unordered Pod bindings")
    if kind == "StatefulSet":
        require(all(stable_statefulset_name(workload["name"], name) for name in names),
                "StatefulSet Pod names must be canonical ordinals")
    require(begin["nodes"] == bindings, "begin bindings differ")
    batches = plan["batches"]
    require(isinstance(batches, list) and batches and all(isinstance(b, list) and b for b in batches)
            and batches[0] == ids[:1] and list(chain.from_iterable(batches)) == ids, "invalid Pod batches")


def pod_task(plan, get, cleanup=False):
    begin = plan["begin"]
    task = get(f"/v1/node-upgrades/tasks/{quote(begin['task_id'], safe='')}")
    require(task.get("protocol_version") == 3 and task.get("kind") == "pod", "incompatible Pod task")
    require(all(task.get(k) == begin[k] for k in ("task_id", "epoch", "namespace", "daemonset_uid", "target_image")),
            "task owner differs")
    require(set(task["nodes"]) == {n["node_id"] for n in begin["nodes"]}, "task node set differs")
    for binding in begin["nodes"]:
        node = task["nodes"][binding["node_id"]]
        require(node["node_id"] == binding["node_id"] and node["original_pod_uid"] == binding["pod_uid"]
                and node["original_session_id"] == binding["node_session_id"], "original binding differs")
    phases = ("Active", "Aborting", "Aborted", "Completed") if cleanup else ("Active", "Completed")
    require(task["phase"] in phases, "task stopped or aborted; normal apply refused")
    return task


def pod_instances(node_id, get):
    records, stamp, page = {}, None, 1
    while True:
        value = get(f"/instance-manager/query-tenant-instances?node_id={quote(node_id, safe='')}"
                    f"&page={page}&page_size=1000")
        current = (value["epoch"], value["revision"], value["count"])
        require(all(type(n) is int and n >= 0 for n in current), "invalid inventory counters")
        require(stamp is None or stamp == current, "inventory changed; reread from page one")
        stamp = current
        admission = value["node"]
        require(admission["node_id"] == node_id and admission["scheduling_paused"] is True
                and admission["accepting_allocations"] is False, "source scheduling not fenced")
        items = value["instanceInfos"]
        require(isinstance(items, list) and len(items) == min(1000, max(0, stamp[2] - (page - 1) * 1000)),
                "incomplete inventory page")
        for item in items:
            key = item["instanceID"]
            require(text(key) and key not in records and item["node_id"] == node_id, "invalid inventory binding")
            require(all(type(item[k]) is int and 0 <= item[k] <= 2**64 - 1 for k in ("generation", "revision")),
                    "invalid Environment revision")
            fields = ("invalidated", "restart_pending", "resume_pending", "recovery_pending")
            require(item["status"] in ("running", "paused") and isinstance(item.get("resources_held"), bool)
                    and all(item.get(k) is False for k in fields),
                    "Environment not ready for planned migration")
            records[key] = item
        if len(records) == stamp[2]:
            return records
        page += 1


def pod_inventory(namespace, kube):
    listing = kube(["get", "--raw", f"/api/v1/namespaces/{quote(namespace, safe='')}/pods"])
    require(listing["kind"] == "PodList" and listing["apiVersion"] == "v1"
            and text(listing.get("metadata", {}).get("resourceVersion"))
            and not listing.get("metadata", {}).get("continue"), "incomplete authoritative Pod list")
    require(isinstance(listing["items"], list), "invalid Pod inventory")
    uids = set()
    for pod in listing["items"]:
        require(pod.get("apiVersion", "v1") == "v1" and pod.get("kind", "Pod") == "Pod",
                "unexpected inventory item kind/version")
        # The typed v1 PodList supplies item TypeMeta when the API omits it.
        pod.setdefault("apiVersion", "v1")
        pod.setdefault("kind", "Pod")
        metadata = pod["metadata"]
        require(metadata["namespace"] == namespace
                and all(text(metadata[key]) for key in ("name", "uid", "resourceVersion")),
                "invalid inventory Pod identity")
        require(metadata["uid"] not in uids, "duplicate Pod UID")
        uids.add(metadata["uid"])
    return listing["items"]


def pod_inspection(plan, pod, uid, session, get, phase=None, permit=None, revision=None):
    view = get(f"/v1/node-upgrades/nodes/{quote(pod['node_id'], safe='')}/inspect")
    require(view.get("protocol_version") == 3 and view.get("node_id") == pod["node_id"]
            and view.get("observed_pod_uid") == uid and view.get("node_session_id") == session
            and view.get("unknown") is False, "live Pod identity/session unknown or changed")
    if phase is not None:
        fields = ("controllers", "holds", "backends", "authority_tasks")
        require(view.get("task_id") == plan["begin"]["task_id"] and view.get("epoch") == plan["begin"]["epoch"]
                and view.get("phase") == phase
                and (phase == "isolated" or view.get("strict_empty") is True)
                and all(type(view.get(k)) is int and view[k] == 0 for k in fields), "strict empty proof unavailable")
        if permit is not None:
            require(view.get("permit_id") == permit and view.get("revision") == revision,
                    "permit evidence differs")
    return view


def advance_pod(plan, journal, save, batch_index, clients, cleanup=False, cleanup_node=None,
                local=None, sleep=time.sleep):
    """Advance with durable intent recorded before each external mutation."""
    kube, get, post, mutate = clients
    begin, workload = plan["begin"], plan["workload"]
    kind = workload_kind(workload)
    resource = workload_resource(workload)
    base = f"/v1/node-upgrades/tasks/{quote(begin['task_id'], safe='')}"
    stage = journal["stage"]
    if stage == "new":
        current_workload = kube(["get", resource, workload["name"], "--namespace", begin["namespace"]])
        require(identity(current_workload, kind, begin["namespace"])["uid"] == workload["uid"]
                and template_hash(current_workload["spec"]["template"]) == workload["original_template_hash"],
                kind + " changed before begin")
        inventory = pod_inventory(begin["namespace"], kube)
        for pod in plan["pods"]:
            matches = [p for p in inventory if p["metadata"]["uid"] == pod["uid"]]
            require(len(matches) == 1 and identity(matches[0], "Pod", begin["namespace"])["name"] == pod["name"]
                    and image(matches[0], plan["container"]) == pod["original_image"],
                    "source Pod changed before begin")
            pod_inspection(plan, pod, pod["uid"], pod["inspection"]["node_session_id"], get)
            require(local is not None and pod_actual(matches[0], begin["namespace"], plan["container"],
                    pod["node_id"], local(pod["name"])) == pod["actual"], "source actual identity changed")
        journal["stage"] = "begin_intent"
        save()
        post("/v1/node-upgrades/tasks", begin)
        journal["stage"] = "template"
        save()
        return False
    if stage == "begin_intent":
        # Begin is same-owner idempotent, including an ambiguous first reply.
        try:
            pod_task(plan, get)
        except (OSError, Refusal, ValueError, KeyError, TypeError, http.client.HTTPException):
            post("/v1/node-upgrades/tasks", begin)
        journal["stage"] = "template"
        save()
        return False
    task = pod_task(plan, get, cleanup)
    selected = [cleanup_node] if cleanup else plan["batches"][batch_index]
    prior_batches = [] if cleanup else plan["batches"][:batch_index]
    for prior in chain.from_iterable(prior_batches):
        require(journal["nodes"].get(prior, {}).get("stage") == "verified"
                and task["nodes"][prior]["phase"] == "verified", "prior batch is not verified")
    if task["phase"] == "Completed":
        require(all(n["phase"] == "verified" for n in task["nodes"].values())
                and all(entry["phase"] == "Completed" for entry in task["operations"].values()), "invalid completion")
        journal["stage"] = "completed"
        save()
        return True
    current_workload = kube(["get", resource, workload["name"], "--namespace", begin["namespace"]])
    evidence = identity(current_workload, kind, begin["namespace"])
    require(evidence["uid"] == workload["uid"], kind + " owner changed")
    fingerprint = template_hash(current_workload["spec"]["template"])
    if stage in ("template", "patch_intent"):
        if fingerprint == workload["target_template_hash"]:
            require(stage == "patch_intent" or workload["original_template_hash"] == fingerprint,
                    "template changed without publisher intent")
            journal["stage"] = "nodes"
            save()
            return False
        require(fingerprint == workload["original_template_hash"], "foreign template change")
        containers = current_workload["spec"]["template"]["spec"]["containers"]
        indexes = [i for i, c in enumerate(containers) if c["name"] == plan["container"]]
        require(len(indexes) == 1, "selected template container changed")
        pointer = f'/spec/template/spec/containers/{str(indexes[0])}/image'
        patch = [{"op": "test", "path": "/metadata/uid", "value": workload["uid"]},
                 {"op": "test", "path": "/metadata/resourceVersion", "value": evidence["resource_version"]},
                 {"op": "test", "path": pointer, "value": workload["original_image"]},
                 {"op": "replace", "path": pointer, "value": begin["target_image"]}]
        if stage == "template":
            journal.update(stage="patch_intent", patch=patch)
            save()
        if journal["patch"] != patch:
            previous = journal["patch"]
            require(len(previous) == len(patch) and previous[:1] == patch[:1] and previous[2:] == patch[2:]
                    and previous[1].get("op") == "test"
                    and previous[1].get("path") == "/metadata/resourceVersion", "PATCH intent differs")
            # Exact original template and fresh UID checks above fence
            # the same effect; the old RV can no longer authorize a late PATCH.
            journal["patch"] = patch
            save()
        mutate(["patch", resource, workload["name"], "--namespace", begin["namespace"],
                "--type=json", "--patch", canonical(patch), "-o", "json"], None)
        return False
    allowed = (workload["original_template_hash"], workload["target_template_hash"]) if cleanup else (
        workload["target_template_hash"],)
    require(fingerprint in allowed, "target template changed")
    if cleanup and fingerprint != workload["original_template_hash"]:
        require("patch" in journal, "cleanup target template is not publisher-owned")
    require(stage in ("nodes", "complete_intent"), "invalid Pod journal stage")
    by_id = {pod["node_id"]: pod for pod in plan["pods"]}
    for node_id in selected:
        pod = by_id[node_id]
        state = journal["nodes"].setdefault(node_id, {"stage": "new"})
        node = task["nodes"][node_id]
        local_stage = state["stage"]
        original_session = pod["inspection"]["node_session_id"]
        binding = dict(node_id=node_id, pod_uid=pod["uid"], node_session_id=original_session)

        def action(name, extra=None, node_id=node_id, *, intent_key='intent', binding, state):
            body = dict(task_id=begin["task_id"], epoch=begin["epoch"], **binding)
            if cleanup:
                body["rollback"] = True
            body.update(extra or {})
            body["operation_id"] = hashlib.sha256(canonical([begin, node_id, name, body]).encode("utf-8")).hexdigest()
            if intent_key not in state:
                state[intent_key] = dict(action=name, body=body)
                save()
            require(state[intent_key] == dict(action=name, body=body), "action intent differs")
            return post(f'{base}/{name}', body)
        action = partial(action, binding=binding, state=state)

        if local_stage == "verified":
            require(node["phase"] == "verified" and node["pod_uid"] == state["replacement_uid"]
                    and node["node_session_id"] == state["replacement_session"], "verified binding changed")
            continue
        if local_stage not in ("delete", "replacement", "replacement_quiesce", "verify"):
            require(node["pod_uid"] == binding["pod_uid"] and node["node_session_id"] == original_session,
                    "original task binding changed")
            originals = [p for p in pod_inventory(begin["namespace"], kube) if p["metadata"]["uid"] == pod["uid"]]
            require(len(originals) == 1 and identity(originals[0], "Pod", begin["namespace"])["name"] == pod["name"]
                    and image(originals[0], plan["container"]) == pod["original_image"], "original Pod changed")
            require(workload_owns(workload, originals[0])
                    and originals[0]["spec"].get("nodeName") == pod["kubernetes_node_name"],
                    "source owner/host changed")
            pod_inspection(plan, pod, pod["uid"], original_session, get)
        if local_stage in ("new", "prepare"):
            state["stage"] = "prepare"
            action("prepare-node")
            state.update(stage="evacuate")
            state.pop("intent", None)
            save()
            return False
        if local_stage == "evacuate":
            def migrate(item, *, intent_key="intent", send=action):
                return send("migrate", dict(environment_id=item["instanceID"],
                                              expected_source_generation=item["generation"],
                                              expected_source_revision=item["revision"]),
                              intent_key=intent_key)

            intent_key = "paused_intent" if "paused_intent" in state else "intent"
            if intent_key in state:
                body = state[intent_key]["body"]
                operation = task["operations"].get(body["operation_id"])
                if operation is not None:
                    expected_operation = {
                        "environment_id": body["environment_id"], "source_node_id": node_id,
                        "source_session_id": original_session, "source_generation": body["expected_source_generation"],
                        "source_revision": body["expected_source_revision"]}
                    require(all(operation.get(k) == v for k, v in expected_operation.items()),
                            "migration operation differs")
                    require(operation["phase"] in ("Prepared", "Assigned", "Completed"), "migration not successful")
                    if operation["phase"] == "Completed":
                        state.pop(intent_key)
                        save()
                        return False
                    waiting_for_idle = operation.get("was_running") and operation.get("source_stopped_revision") is None
                    if intent_key == "intent" and operation["phase"] == "Prepared" and waiting_for_idle:
                        # A separate paused Environment can transfer while the
                        # original operation waits for an idle window. Preserve
                        # both bodies until their authoritative receipts settle.
                        try:
                            instances = pod_instances(node_id, get)
                        except (Refusal, OSError):
                            # A failed priority scan must not gate the tracked retry.
                            instances = {}
                        original = instances.get(body["environment_id"])
                        if original and original["status"] == "running":
                            pending = {op["environment_id"] for op in task["operations"].values()
                                       if op["phase"] != "Completed"}
                            paused = next((instance for key, instance in sorted(instances.items())
                                           if key not in pending and instance["status"] == "paused"), None)
                            if paused is not None:
                                migrate(paused, intent_key="paused_intent")
                                return False
                post(f'{base}/migrate', body)
                return False
            require(all(entry["phase"] == "Completed" for entry in task["operations"].values()
                        if node_id in (entry["source_node_id"], entry["target_node_id"])), "unresolved migration")
            instances = pod_instances(node_id, get)
            if instances:
                item = min(instances.values(), key=lambda item: (item["status"] != "paused", item["instanceID"]))
                migrate(item)
                return False
            state.update(stage="quiesce", baseline_revision=node["control_revision"])
            save()
            action("quiesce")
            return False
        if local_stage in ("quiesce", "replacement_quiesce"):
            replacement = local_stage == "replacement_quiesce"
            uid = state["replacement_uid"] if replacement else pod["uid"]
            session = state["replacement_session"] if replacement else original_session
            if node["pod_uid"] != uid or node["node_session_id"] != session or not text(node.get("permit_id")):
                if node["control_revision"] == state["baseline_revision"]:
                    binding.update(pod_uid=uid, node_session_id=session)
                    action("quiesce")
                    return False
                same_incarnation = node["pod_uid"] == uid and node["node_session_id"] == session
                no_delete_permit = node.get("permit_id") is None and node.get("delete_resource_version") is None
                if same_incarnation and node["phase"] == "quiescing" and no_delete_permit:
                    view = pod_inspection(plan, pod, uid, session, get)
                    require(view.get("task_id") == begin["task_id"] and view.get("epoch") == begin["epoch"],
                            "partial quiesce observation owner differs")
                    # Control intent can precede Node RPC or permit persistence.
                    # Same-body replay is serialized by the server, not proof
                    # that the earlier call did not execute.
                    binding.update(pod_uid=uid, node_session_id=session)
                    action("quiesce")
                    return False
                raise Refusal("quiesce receipt pending; keep isolation")
            require(node["phase"] == "quiescing" and type(node["control_revision"]) is int
                    and node["control_revision"] > state["baseline_revision"], "quiesce revision missing")
            pod_inspection(plan, pod, uid, session, get, "Quiescing", node["permit_id"], node["control_revision"])
            state.update(stage="verify" if replacement else "delete_intent",
                         permit_id=node["permit_id"], control_revision=node["control_revision"])
            state.pop("intent", None)
            save()
            return False
        if local_stage == "delete_intent":
            require(node["permit_id"] == state["permit_id"] and node["control_revision"] == state["control_revision"],
                    "source permit changed")
            if node.get("delete_resource_version") is None and "intent" not in state:
                state["delete_rv"] = originals[0]["metadata"]["resourceVersion"]
                save()
            if node.get("delete_resource_version") != state["delete_rv"]:
                require(node.get("delete_resource_version") is None, "foreign delete intent")
                pod_inspection(plan, pod, pod["uid"], original_session, get,
                               "Quiescing", state["permit_id"], state["control_revision"])
                action("quiesce", dict(delete_intent=True, permit_id=state["permit_id"],
                                       pod_resource_version=state["delete_rv"]))
                return False
            state.update(stage="delete")
            state.pop("intent", None)
            save()
            return False
        if local_stage == "delete":
            require(node["pod_uid"] == pod["uid"] and node["node_session_id"] == original_session
                    and node["control_revision"] == state["control_revision"], "DELETE binding changed")
            authorized = (node["permit_id"] == state["permit_id"]
                          and node["delete_resource_version"] == state["delete_rv"])
            revoking = state.get("intent")
            require(authorized or (revoking and node["permit_id"] is None
                                   and node["delete_resource_version"] is None), "DELETE authorization changed")
            originals = [p for p in pod_inventory(begin["namespace"], kube) if p["metadata"]["uid"] == pod["uid"]]
            if not originals:
                require(authorized and not revoking, "original disappeared during DELETE revocation")
                state["stage"] = "replacement"
                save()
                return False
            current = originals[0]
            if current["metadata"].get("deletionTimestamp"):
                raise Refusal("original UID is terminating; wait for authoritative disappearance")
            current_rv = current["metadata"]["resourceVersion"]
            if current_rv != state["delete_rv"]:
                require(identity(current, "Pod", begin["namespace"])["name"] == pod["name"]
                        and image(current, plan["container"]) == pod["original_image"]
                        and workload_owns(workload, current)
                        and current["spec"].get("nodeName") == pod["kubernetes_node_name"], "original Pod changed")
                pod_inspection(plan, pod, pod["uid"], original_session, get,
                               "Quiescing", state["permit_id"] if authorized else None, state["control_revision"])
                # The old UID/RV DELETE can no longer apply. Persist the revocation
                # before requesting another strict-empty check and permit.
                fenced_rv = revoking["body"]["pod_resource_version"] if revoking else current_rv
                response = action("revoke-delete", dict(expected_delete_resource_version=state["delete_rv"],
                                                        pod_resource_version=fenced_rv))
                cleared = response["nodes"][node_id]
                require(response["task_id"] == begin["task_id"] and response["epoch"] == begin["epoch"]
                        and cleared["pod_uid"] == pod["uid"] and cleared["node_session_id"] == original_session
                        and cleared["control_revision"] == state["control_revision"]
                        and cleared["permit_id"] is None and cleared["delete_resource_version"] is None,
                        "DELETE revocation receipt differs")
                state.update(stage="quiesce", baseline_revision=state["control_revision"])
                for key in ("intent", "delete_rv", "permit_id", "control_revision"):
                    state.pop(key)
                save()
                return False
            require(not revoking, "DELETE revocation still pending")
            body = dict(apiVersion="v1", kind="DeleteOptions",
                         preconditions=dict(uid=pod["uid"], resourceVersion=state["delete_rv"]))
            if "declaration" in plan:
                declaration_write(plan, journal, save, kube, mutate)
                gate_node = dict(kube={"name": pod["name"]}, declaration_path=pod["declaration_path"])
                declaration_gate(plan, gate_node, kube, local, sleep, override=True)
            save()
            url = f"/api/v1/namespaces/{quote(begin['namespace'], safe='')}/pods/{quote(pod['name'], safe='')}"
            mutate(["delete", "--raw", url, "-f", "-"], body)
            return False
        if local_stage == "replacement":
            inventory = pod_inventory(begin["namespace"], kube)
            require(not any(p["metadata"]["uid"] == pod["uid"] for p in inventory), "original UID still present")
            candidate, evidence, new_host = pod_replacement(plan, pod, inventory)
            containers = [c for c in candidate["spec"]["containers"] if c["name"] == plan["container"]]
            entries = [e for e in containers[0].get("env", []) if e.get("name") == "ADX_POD_UID"]
            require(len(entries) == 1 and "value" not in entries[0], "replacement Downward UID missing")
            field = entries[0].get("valueFrom", {}).get("fieldRef", {})
            require(field.get("fieldPath") == "metadata.uid" and field.get("apiVersion", "v1") == "v1",
                    "replacement Downward UID differs")
            view = get(f"/v1/node-upgrades/nodes/{quote(node_id, safe='')}/inspect")
            session = view.get("node_session_id")
            require(text(session) and session != original_session, "replacement session unavailable")
            pod_inspection(plan, pod, evidence["uid"], session, get, "isolated")
            state.update(stage="replacement_quiesce", replacement_uid=evidence["uid"],
                         replacement_session=session, replacement_host=new_host,
                         baseline_revision=node["control_revision"])
            require(local is not None, "NodeInfo transport unavailable")
            state["replacement_actual"] = pod_actual(candidate, begin["namespace"], plan["container"], node_id,
                                                     local(evidence["name"]), None if cleanup else plan, session)
            binding.update(pod_uid=evidence["uid"], node_session_id=session)
            save()
            action("quiesce")
            return False
        if local_stage == "verify":
            binding.update(pod_uid=state["replacement_uid"], node_session_id=state["replacement_session"])
            require(node["pod_uid"] == binding["pod_uid"] and node["node_session_id"] == binding["node_session_id"],
                    "replacement binding changed")
            candidates = [p for p in pod_inventory(begin["namespace"], kube)
                          if p["metadata"]["uid"] == binding["pod_uid"]]
            require(len(candidates) == 1, "replacement disappeared")
            current = candidates[0]
            identity(current, "Pod", begin["namespace"])
            require(workload_owns(workload, current)
                    and current["spec"].get("nodeName") == replacement_host(
                        workload, pod["kubernetes_node_name"], state)
                    and image(current, plan["container"]) == begin["target_image"], "replacement changed before verify")
            pod_inspection(plan, pod, binding["pod_uid"], binding["node_session_id"], get)
            require(local is not None, "NodeInfo transport unavailable")
            actual = pod_actual(current, begin["namespace"], plan["container"], node_id,
                                local(current["metadata"]["name"]), None if cleanup else plan,
                                binding["node_session_id"])
            require(state.get("replacement_actual") == actual, "replacement container/parent identity changed")
            if node["phase"] != "verified":
                require(node["permit_id"] == state["permit_id"], "replacement permit changed")
                pod_inspection(plan, pod, binding["pod_uid"], binding["node_session_id"], get,
                               "Quiescing", state["permit_id"], state["control_revision"])
            response = action("verify-node", dict(permit_id=state["permit_id"]))
            fields = ("task_id", "epoch", "namespace", "daemonset_uid", "target_image")
            require(all(response[k] == begin[k] for k in fields)
                    and response["protocol_version"] == 3 and response["kind"] == "pod"
                    and response["phase"] == ("Aborting" if cleanup else "Active"), "verify task receipt differs")
            verified = response["nodes"][node_id]
            require(verified["node_id"] == node_id and verified["pod_uid"] == binding["pod_uid"]
                    and verified["node_session_id"] == binding["node_session_id"]
                    and verified["phase"] == "verified", "verify binding receipt differs")
            state["stage"] = "verified"
            state.pop("intent", None)
            save()
            return False
        raise Refusal("invalid Pod node stage")
    if not cleanup and batch_index == len(plan["batches"]) - 1:
        require(all(n["phase"] == "verified" for n in task["nodes"].values())
                and all(entry["phase"] == "Completed" for entry in task["operations"].values()), "task not complete")
        journal["stage"] = "complete_intent"
        save()
        operation_id = hashlib.sha256(canonical([begin, "complete"]).encode("utf-8")).hexdigest()
        post(f'{base}/complete', dict(task_id=begin["task_id"], epoch=begin["epoch"], operation_id=operation_id))
        return False
    return True


def cleanup_inventory(plan, task, get, kube, local):
    """Prove a complete current workload, without treating saved outcomes as live facts."""
    inventory = pod_inventory(plan["begin"]["namespace"], kube)
    workload, begin = plan["workload"], plan["begin"]
    kind = workload_kind(workload)
    owned = []
    for item in inventory:
        owners = item["metadata"].get("ownerReferences", [])
        if any(entry.get("controller") is True and entry.get("uid") == workload["uid"] for entry in owners):
            owned.append(item)
    require(len(owned) == len(plan["pods"]), "extra or missing owned Pods; cleanup inventory unknown")
    result = {}
    for pod in plan["pods"]:
        if kind == "StatefulSet":
            matches = [p for p in owned if p["metadata"].get("name") == pod["name"]]
            require(len(matches) == 1, "cleanup requires exactly one StatefulSet Pod per stable name")
        else:
            matches = [p for p in owned if p["spec"].get("nodeName") == pod["kubernetes_node_name"]]
            require(len(matches) == 1, "cleanup requires exactly one owned Pod per planned host")
        current = matches[0]
        evidence = identity(current, "Pod", begin["namespace"])
        require(workload_owns(workload, current), "cleanup controller differs")
        node = task["nodes"][pod["node_id"]]
        require(evidence["uid"] == node["pod_uid"] and node["delete_resource_version"] is None,
                "cleanup binding or DELETE unresolved")
        original = evidence["uid"] == pod["uid"]
        expected_image = pod["original_image"] if original else begin["target_image"]
        require(image(current, plan["container"]) == expected_image, "cleanup current image differs")
        if original:
            require(node["node_session_id"] == pod["inspection"]["node_session_id"]
                    and node["phase"] in ("pending", "RestoredOriginal", "verified"), "original not restored")
        else:
            require(node["phase"] == "verified", "replacement not verified")
        view = pod_inspection(plan, pod, evidence["uid"], node["node_session_id"], get)
        if node["phase"] != "pending":
            require(view.get("task_id") == begin["task_id"] and view.get("epoch") == begin["epoch"]
                    and view.get("revision") == node["control_revision"]
                    and view.get("phase") in (("released",) if task["phase"] in ("Aborted", "Completed")
                                             or original else ("verified",)), "cleanup actual control unknown")
        result[pod["node_id"]] = dict(uid=evidence["uid"], session=node["node_session_id"], image=expected_image,
                                      outcome="original" if original else "retained_target")
        report = result[pod["node_id"]]
        report["components"] = {"unknown": True}
        try:
            require(local is not None, "NodeInfo transport unavailable")
            response = local(evidence["name"])
            pod_actual(current, begin["namespace"], plan["container"], pod["node_id"], response,
                       session=node["node_session_id"])
            report["components"] = [dict(role=item["role"], sources=item.get("sources"),
                executable_sha256=item["runtime"]["executable_sha256"],
                configuration_sha256=item["runtime"]["configuration_sha256"])
                for item in response["node_info"]["components"]]
        except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                subprocess.SubprocessError, http.client.HTTPException):
            pass
    require(all(entry["phase"] in ("Completed", "Cancelled") for entry in task["operations"].values()),
            "cleanup migration unresolved")
    return result


def advance_pod_cleanup(plan, journal, save, kube, get, post, mutate, stop=False, local=None):
    begin, workload = plan["begin"], plan["workload"]
    kind = workload_kind(workload)
    resource = workload_resource(workload)
    base = f"/v1/node-upgrades/tasks/{quote(begin['task_id'], safe='')}"
    cleanup = journal.setdefault("cleanup", {"nodes": {}})

    def action(name, extra):
        body = dict(task_id=begin["task_id"], epoch=begin["epoch"], **extra)
        body["operation_id"] = template_hash([begin, name, extra])
        cleanup["intent"] = dict(action=name, body=body)
        save()
        return post(f'{base}/{name}', body)

    if journal["stage"] == "new":
        if not stop and "declaration_write" in journal:
            declaration_write(plan, journal, save, kube, mutate, restore=True)
        cleanup["local_terminal"] = "stopped" if stop else "aborted"
        save()
        return dict(status=cleanup["local_terminal"], remote_task=False, effects=False)
    if journal["stage"] == "begin_intent":
        # A failed GET cannot prove Begin was never executed. Close the exact
        # Begin request before attempting freeze, without changing its identity.
        save()
        response = post("/v1/node-upgrades/tasks", begin)
        require(response["task_id"] == begin["task_id"] and response["epoch"] == begin["epoch"],
                "begin recovery receipt differs")
        journal["stage"] = "template"
        save()
        return None
    task = pod_task(plan, get, cleanup=True)
    if task["phase"] in ("Aborted", "Completed"):
        if task["phase"] == "Aborted":
            current_workload = kube(["get", resource, workload["name"], "--namespace", begin["namespace"]])
            require(identity(current_workload, kind, begin["namespace"])["uid"] == workload["uid"]
                    and template_hash(current_workload["spec"]["template"]) == workload["original_template_hash"],
                    "aborted task current template unknown")
            if "declaration_write" in journal:
                declaration_write(plan, journal, save, kube, mutate, restore=True)
        current = cleanup_inventory(plan, task, get, kube, local)
        return dict(status="partial", task_phase=task["phase"], nodes=current,
                    mixed_images=len({value["image"] for value in current.values()}) > 1,
                    current_images={key: value["image"] for key, value in current.items()})
    if not cleanup.get("frozen"):
        response = action("abort", dict(stage="freeze"))
        require(all(response[k] == begin[k] for k in ("task_id", "epoch", "namespace", "daemonset_uid", "target_image"))
                and response["phase"] == "Aborting" and response["protocol_version"] == 3
                and response["kind"] == "pod" and set(response["nodes"]) == set(task["nodes"])
                and isinstance(response["operations"], dict), "freeze success receipt differs")
        cleanup["frozen"] = True
        save()
        return None
    if stop:
        return dict(status="stopped", task_phase="Aborting")
    if "declaration_write" in journal:
        declaration_write(plan, journal, save, kube, mutate, restore=True)
    if any(entry["phase"] not in ("Completed", "Cancelled") for entry in task["operations"].values()):
        action("abort", dict(stage="freeze"))
        raise Refusal("migration outcomes unresolved; keep maintenance and retry cleanup")

    inventory = pod_inventory(begin["namespace"], kube)
    for pod in plan["pods"]:
        node_id = pod["node_id"]
        node = task["nodes"][node_id]
        state = cleanup["nodes"].setdefault(node_id, {})
        originals = [p for p in inventory if p["metadata"]["uid"] == pod["uid"]]
        if originals:
            current = originals[0]
            require(not current["metadata"].get("deletionTimestamp"), "original terminating; wait for disappearance")
            evidence = identity(current, "Pod", begin["namespace"])
            require(evidence["name"] == pod["name"] and workload_owns(workload, current)
                    and current["spec"].get("nodeName") == pod["kubernetes_node_name"]
                    and image(current, plan["container"]) == pod["original_image"], "original identity changed")
            session = pod["inspection"]["node_session_id"]
            view = pod_inspection(plan, pod, pod["uid"], session, get)
            require(node["pod_uid"] == pod["uid"] and node["node_session_id"] == session,
                    "original task binding changed")
            binding = dict(node_id=node_id, pod_uid=pod["uid"], node_session_id=session)
            normal = journal["nodes"].get(node_id, {})
            delete_rv = node.get("delete_resource_version") or normal.get("delete_rv")
            if delete_rv and not state.get("revoked"):
                marker = template_hash([begin, node_id, "delete-fence"])
                annotation = "adx.dev/rollout-delete-fence"
                metadata = current["metadata"]
                if metadata.get("annotations", {}).get(annotation) != marker:
                    patch = [{"op": "test", "path": "/metadata/uid", "value": pod["uid"]},
                             {"op": "test", "path": "/metadata/resourceVersion",
                              "value": evidence["resource_version"]}]
                    if "annotations" not in metadata:
                        patch.append(dict(op="add", path="/metadata/annotations", value={}))
                    patch.append(dict(op="add", path="/metadata/annotations/adx.dev~1rollout-delete-fence",
                                      value=marker))
                    state["fence_intent"] = patch
                    save()
                    mutate(["patch", "pod", pod["name"], "--namespace", begin["namespace"], "--type=json",
                            "--patch", canonical(patch), "-o", "json"], None)
                    return None
                require(evidence["resource_version"] != delete_rv, "DELETE fence has not advanced RV")
                # This fresh list also proves the fenced original is not terminating.
                if "revoke_body" not in state:
                    state["revoke_body"] = dict(binding, expected_delete_resource_version=delete_rv,
                                                pod_resource_version=evidence["resource_version"])
                    save()
                response = action("revoke-delete", state["revoke_body"])
                require(response["task_id"] == begin["task_id"] and response["epoch"] == begin["epoch"]
                        and response["nodes"][node_id]["delete_resource_version"] is None,
                        "delete revocation receipt differs")
                state["revoked"] = True
                save()
                return None
            require(node.get("delete_resource_version") is None, "delete authorization remains unresolved")
            if node["phase"] == "pending":
                state.update(outcome="untouched", uid=pod["uid"], session=session, image=pod["original_image"])
                save()
                continue
            if state.get("outcome") == "restored":
                require(view.get("task_id") == begin["task_id"] and view.get("epoch") == begin["epoch"]
                        and view.get("phase") == "released" and view.get("revision") == node["control_revision"],
                        "restored original control unknown")
                page = get(f"/instance-manager/query-tenant-instances?node_id={quote(node_id, safe='')}"
                           "&page=1&page_size=1000")
                require(page["node"]["node_id"] == node_id, "original admission node differs")
                state["admission"] = page["node"]
                save()
                continue
            response = action("abort", dict(binding, stage="restore-original"))
            require(response.get("restored_original") is True
                    and all(response[k] == binding[k] for k in binding), "restore receipt differs")
            state.update(outcome="restored", uid=pod["uid"], session=session, image=pod["original_image"])
            save()
            return None
        # Keep replacements at the current target version; never migrate work back.
        normal = journal["nodes"].setdefault(node_id, {"stage": "replacement"})
        require(node.get("delete_resource_version") is not None or node["phase"] == "verified"
                or normal["stage"] in ("replacement_quiesce", "verify", "verified"),
                "original disappeared without a known delete authorization")
        if node["phase"] == "verified":
            matches = [p for p in inventory if p["metadata"]["uid"] == node["pod_uid"]]
            require(len(matches) == 1, "verified replacement missing")
            current = matches[0]
            identity(current, "Pod", begin["namespace"])
            require(workload_owns(workload, current)
                    and current["spec"].get("nodeName") == replacement_host(
                        workload, pod["kubernetes_node_name"], normal)
                    and image(current, plan["container"]) == begin["target_image"], "replacement changed")
            pod_inspection(plan, pod, node["pod_uid"], node["node_session_id"], get)
            if state.get("outcome") != "retained_target":
                intent = normal.get("intent")
                if intent and intent["action"] == "verify-node" and intent["body"].get("rollback") is True:
                    body = intent["body"]
                    require(body["node_id"] == node_id and body["pod_uid"] == node["pod_uid"]
                            and body["node_session_id"] == node["node_session_id"], "verify intent binding differs")
                    save()
                    response = post(f'{base}/verify-node', body)
                else:
                    response = action("verify-node", dict(node_id=node_id, pod_uid=node["pod_uid"],
                                                          node_session_id=node["node_session_id"], rollback=True,
                                                          permit_id=normal.get("permit_id") or ""))
                require(response["phase"] == "Aborting" and response["task_id"] == begin["task_id"]
                        and response["epoch"] == begin["epoch"]
                        and response["nodes"][node_id]["phase"] == "verified"
                        and response["nodes"][node_id]["pod_uid"] == node["pod_uid"]
                        and response["nodes"][node_id]["node_session_id"] == node["node_session_id"],
                        "verify cleanup receipt differs")
                state.update(outcome="retained_target", uid=node["pod_uid"], session=node["node_session_id"],
                             image=begin["target_image"])
                save()
                return None
            continue
        if normal["stage"] not in ("replacement", "replacement_quiesce", "verify"):
            normal["stage"] = "replacement"
            normal.pop("intent", None)
            save()
        # Reuse only the normal replacement leg, never its migration/source leg.
        if "intent" in normal and not normal["intent"]["body"].get("rollback"):
            normal.pop("intent")
        journal["stage"] = "nodes"
        save()
        batch = next(i for i, b in enumerate(plan["batches"]) if node_id in b)

        def replacement_post(path, body, selected_node=node_id):
            require(body.get("node_id") == selected_node and path.rsplit("/", 1)[-1] in ("quiesce", "verify-node"),
                    "abort cleanup cannot start source work")
            return post(path, body)
        advance_pod(plan, journal, save, batch, PodClients(kube, get, replacement_post, mutate),
                    cleanup=True, cleanup_node=node_id, local=local)
        return None

    current_workload = kube(["get", resource, workload["name"], "--namespace", begin["namespace"]])
    evidence = identity(current_workload, kind, begin["namespace"])
    require(evidence["uid"] == workload["uid"],
            "cleanup workload identity changed")
    fingerprint = template_hash(current_workload["spec"]["template"])
    marker = template_hash([begin, "template-fence"])
    patch = [{"op": "test", "path": "/metadata/uid", "value": workload["uid"]},
             {"op": "test", "path": "/metadata/resourceVersion", "value": evidence["resource_version"]}]
    if fingerprint == workload["target_template_hash"] and fingerprint != workload["original_template_hash"]:
        require("patch" in journal, "target template is not publisher-owned")
        containers = current_workload["spec"]["template"]["spec"]["containers"]
        indexes = [i for i, c in enumerate(containers) if c["name"] == plan["container"]]
        require(len(indexes) == 1, "template container changed")
        pointer = f'/spec/template/spec/containers/{str(indexes[0])}/image'
        patch.extend([dict(op="test", path=pointer, value=begin["target_image"]),
                      dict(op="replace", path=pointer, value=workload["original_image"])])
    else:
        require(fingerprint == workload["original_template_hash"], "foreign template; refuse restoration")
        old_patch = journal.get("patch")
        old_rv = old_patch[1]["value"] if old_patch else None
        if old_rv == evidence["resource_version"]:
            if "annotations" not in current_workload["metadata"]:
                patch.append(dict(op="add", path="/metadata/annotations", value={}))
            patch.append(dict(op="add", path="/metadata/annotations/adx.dev~1rollout-template-fence", value=marker))
        else:
            require(all(n.get("outcome") in ("untouched", "restored", "retained_target")
                        for n in cleanup["nodes"].values()), "unsettled cleanup node")
            cleanup_inventory(plan, task, get, kube, local)
            if "declaration_write" in journal:
                declaration_write(plan, journal, save, kube, mutate, restore=True)
            action("abort", dict(stage="finalize", template_restored=True))
            return None
    cleanup["template_intent"] = patch
    save()
    mutate(["patch", resource, workload["name"], "--namespace", begin["namespace"], "--type=json",
            "--patch", canonical(patch), "-o", "json"], None)
    return None


def apply_pod(plan, journal_path, batch_index, clients, sleep=time.sleep, mode="apply", local=None,
              promote=False):
    kube, get, post, mutate = clients
    require(sys.platform == "linux", "apply-pod requires a Linux publisher")
    import fcntl

    validate_pod_plan(plan)
    require((promote and batch_index is None and mode == "apply")
            or (not promote and type(batch_index) is int
                and 0 <= batch_index < len(plan["batches"])), "select batch index or promotion")
    path = Path(journal_path).absolute()
    descriptor = os.open(path.with_name(path.name + ".lock"),
                         os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(descriptor, "a+b") as lock:
        require(stat.S_ISREG(os.fstat(lock.fileno()).st_mode), "lock must be regular")
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refusal("another publisher holds journal lock") from error
        digest = template_hash(plan)
        if path.exists() or path.is_symlink():
            journal = read_json_file(path)
            require(journal.get("schema_version") == 1 and journal.get("plan_hash") == digest,
                    "Pod journal plan hash differs")
        else:
            require(not promote, "post-release actions require existing journal")
            require(batch_index == 0, "first invocation must select canary")
            journal = dict(schema_version=1, plan_hash=digest, stage="new", nodes={})
            write_journal(path, journal)
        require(journal.get("stage") in ("new", "begin_intent", "template", "patch_intent", "nodes",
                                         "complete_intent", "completed"), "invalid Pod journal stage")
        require(mode != "apply" or "cleanup" not in journal, "Pod rollout was manually stopped; use abort-pod")
        if mode != "apply":
            journal.setdefault("cleanup", {"nodes": {}})
            write_journal(path, journal)
        require(isinstance(journal.get("nodes"), dict)
                and set(journal["nodes"]).issubset({p["node_id"] for p in plan["pods"]}), "invalid journal nodes")
        require(all(isinstance(s, dict) and s.get("stage") in
                    ("new", "prepare", "evacuate", "quiesce", "delete_intent", "delete", "replacement",
                     "replacement_quiesce", "verify", "verified") for s in journal["nodes"].values()),
                "invalid Pod node stage")

        def save():
            write_journal(path, journal)

        if promote:
            return promote_ready(plan, journal, save, kube, local, mutate, sleep, get=get)
        deadline = time.monotonic() + 2400
        try:
            while time.monotonic() < deadline:
                if mode != "apply":
                    outcome = advance_pod_cleanup(plan, journal, save, kube, get, post, mutate,
                                                  stop=mode == "stop", local=local)
                    if outcome is not None:
                        return outcome
                    sleep(2)
                    continue
                if advance_pod(plan, journal, save, batch_index, clients, local=local, sleep=sleep):
                    return dict(status="completed", batch_index=batch_index)
                sleep(2)
            return dict(status="pending", batch_index=batch_index, detail="publisher observation budget expired")
        except (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                subprocess.SubprocessError, http.client.HTTPException) as error:
            return dict(status="pending", batch_index=batch_index, detail=str(error))


def parser():
    result = argparse.ArgumentParser(description=__doc__)
    commands = result.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan-pod", help="GET-only plan; emits a JSON snapshot to stdout")
    for name in ("origin", "namespace", "container", "config", "target-image", "task-id"):
        plan.add_argument("--" + name, required=True)
    workload_choice = plan.add_mutually_exclusive_group(required=True)
    workload_choice.add_argument("--daemonset")
    workload_choice.add_argument("--statefulset")
    plan.add_argument("--epoch", type=int, required=True)
    plan.add_argument("--node", action="append", default=[], metavar="POD=NODE")
    plan.add_argument("--context", required=True)
    plan.add_argument("--batch-size", type=int, default=1)
    plan.add_argument("--declaration-candidate")
    plan.add_argument("--configmap")
    plan.add_argument("--declaration-key", default="target.yaml")
    process = commands.add_parser("plan-process", help="GET-only frozen process plan")
    process.add_argument("--archive-url")
    process.add_argument("--archive-sha256")
    process.add_argument("--sha256")
    process.add_argument("--component-config-path")
    process.add_argument("--component-config-sha256")
    process.add_argument("--declaration-candidate")
    process.add_argument("--configmap")
    process.add_argument("--declaration-key", default="target.yaml")
    process.add_argument("--commit")
    for name in ("context", "namespace", "container", "config", "task-id"):
        process.add_argument("--" + name, required=True)
    process.add_argument("--role", choices=("adxlet", "relay"))
    process.add_argument("--origin")
    process.add_argument("--epoch", type=int, required=True)
    process.add_argument("--node", action="append", required=True, metavar="POD=NODE")
    process.add_argument("--batch-size", type=int, default=1)
    apply = commands.add_parser("apply-process", help="select one batch or ready promotion")
    apply.add_argument("--plan", required=True)
    apply.add_argument("--journal", required=True)
    selection = apply.add_mutually_exclusive_group(required=True)
    selection.add_argument("--batch-index", type=int)
    selection.add_argument("--promote-ready", action="store_true")
    pod_apply = commands.add_parser("apply-pod", help="select one Pod batch or ready promotion")
    pod_apply.add_argument("--plan", required=True)
    pod_apply.add_argument("--journal", required=True)
    selection = pod_apply.add_mutually_exclusive_group(required=True)
    selection.add_argument("--batch-index", type=int)
    selection.add_argument("--promote-ready", action="store_true")
    for command in ("stop-pod", "abort-pod"):
        cleanup = commands.add_parser(command, help="freeze or clean up the same Pod rollout")
        cleanup.add_argument("--plan", required=True)
        cleanup.add_argument("--journal", required=True)
    return result


def emit_cli(message, *, error=False):
    output = sys.stderr if error else sys.stdout
    output.write(message + "\n")


def main(argv=None):
    try:
        args = parser().parse_args(argv)
        adxctl = os.environ.get("ADX_ROLLOUT_ADXCTL", "adxctl")
        require(adxctl == "adxctl" or PurePosixPath(adxctl).is_absolute(),
                "ADX_ROLLOUT_ADXCTL must be an absolute container path")
        if args.command in ("apply-pod", "stop-pod", "abort-pod"):
            require(sys.platform == "linux", "apply-pod requires a Linux publisher")
            snapshot = read_json_file(args.plan)

            def pod_kube(arguments):
                return kubectl_get(["--context", snapshot["context"], *arguments])

            def pod_mutate(arguments, body):
                return kubectl_json(["--context", snapshot["context"], *arguments], body)

            def pod_cli(pod, arguments):
                return kubectl_json(["--context", snapshot["context"], "--namespace", snapshot["begin"]["namespace"],
                                     "exec", pod, "--container", snapshot["container"], "--", adxctl, "--config",
                                     snapshot["config"], "rollout", *arguments])

            def pod_local(pod, declaration_path=None):
                observation = ["--declaration-path", declaration_path] if declaration_path is not None else []
                return pod_cli(pod, ["node-info", *observation])

            outcome = apply_pod(snapshot, args.journal, getattr(args, "batch_index", 0), PodClients(pod_kube,
                                lambda path: http_json(snapshot["origin"], path),
                                lambda path, body: http_json(snapshot["origin"], path, body), pod_mutate),
                                mode={"apply-pod": "apply", "stop-pod": "stop", "abort-pod": "abort"}[args.command],
                                local=pod_local, promote=getattr(args, "promote_ready", False))
            emit_cli(canonical(outcome))
            return 0 if outcome["status"] in ("completed", "stopped", "aborted", "partial") else 1
        if args.command == "apply-process":
            require(sys.platform == "linux", "apply-process requires a Linux publisher")
            snapshot = read_json_file(args.plan)

            def apply_kube(arguments):
                return kubectl_get(["--context", snapshot["context"], *arguments])

            def apply_cli(node, arguments):
                return kubectl_json(["--context", snapshot["context"], "--namespace", snapshot["namespace"],
                                     "exec", node["kube"]["name"], "--container", snapshot["container"], "--",
                                     adxctl, "--config", snapshot["config"], "rollout", *arguments])

            def apply_local(pod_name, declaration_path=None):
                node = next(item for item in snapshot["nodes"] if item["kube"]["name"] == pod_name)
                arguments = ["node-info"]
                if declaration_path is not None or "declaration_path" in node:
                    arguments.extend(["--declaration-path", declaration_path or node["declaration_path"]])
                return apply_cli(node, arguments)

            def apply_mutate(arguments, body):
                return kubectl_json(["--context", snapshot["context"], *arguments], body)

            def apply_post(path, body):
                return http_json(snapshot["origin"], path, body)

            outcome = apply_process(snapshot, args.journal, args.batch_index,
                                    apply_kube, apply_local, apply_cli, apply_post, mutate=apply_mutate,
                                    promote=args.promote_ready)
            emit_cli(canonical(outcome))
            return 0 if outcome["status"] == "completed" else 1

        def kube(arguments):
            context = ["--context", args.context] if args.context else []
            return kubectl_get([*context, *arguments])

        def local(pod, declaration_path=None):
            observation = ["--declaration-path", declaration_path] if declaration_path is not None else []
            return kubectl_json(["--context", args.context, "--namespace", args.namespace,
                                 "exec", pod, "--container", args.container, "--",
                                 adxctl, "--config", args.config, "rollout", "node-info", *observation])

        if args.command == "plan-process":
            snapshot = plan_process(args, kube, local)
        else:
            snapshot = plan_pod(args, kube, lambda path: http_json(args.origin, path), local)
        if args.command == "plan-process":
            emit_cli(json.dumps(snapshot, sort_keys=True, separators=(",", ":")))
        else:
            emit_cli(json.dumps(snapshot, indent=2, sort_keys=True))
        return 0
    except (Refusal, ValueError, KeyError, TypeError, AttributeError, OSError,
            subprocess.SubprocessError, http.client.HTTPException) as error:
        emit_cli(json.dumps({"error": {"code": "PlanRefused",
                                      "message": str(error)}}), error=True)
        return 1


if __name__ == "__main__":
    sys.exit(main())
