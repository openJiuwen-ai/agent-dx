"""Journaled Pod/process publishing. Standard library only."""
import argparse
import hashlib
import http.client
from collections import namedtuple
from contextlib import contextmanager
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
from urllib.parse import quote, urlsplit

LIMIT = 8 * 1024 * 1024
TIMEOUT = 15
PodClients = namedtuple("PodClients", ("kube", "get", "post", "mutate"))
CAPABILITIES = ("planned_migration", "paused_transfer", "quiesce",
                "session_isolation", "recovery_compatibility", "runtime_rootfs_identity")


class Refusal(Exception):
    pass


class ManagementError(Refusal):
    def __init__(self, status, code, message):
        super().__init__(f"management request returned HTTP {status}: {code}: {message}")
        self.status = status
        self.code = code


RECOVERABLE_ERRORS = (Refusal, OSError, ValueError, KeyError, TypeError, AttributeError,
                      subprocess.SubprocessError, http.client.HTTPException)


@contextmanager
def locked_journal(journal_path):
    """Hold the publisher lock across journal replacements and all external I/O."""
    import fcntl

    path = Path(journal_path).absolute()
    descriptor = os.open(path.with_name(path.name + ".lock"),
                         os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(descriptor, "a+b") as lock:
        require(stat.S_ISREG(os.fstat(lock.fileno()).st_mode), "lock must be regular")
        try:
            fcntl.flock(lock.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise Refusal("another publisher holds journal lock") from error
        yield path


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
                    if isinstance(error, dict) and text(error.get("code")) and text(error.get("message")):
                        raise ManagementError(response.status, error["code"], error["message"])
                    raise Refusal(status_error)
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
    kind = workload.get("kind")
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


def pod_task_identity(task_id, epoch):
    require(isinstance(task_id, str) and re.fullmatch(r"[A-Za-z0-9._~-]{1,128}", task_id)
            and task_id not in (".", ".."), "invalid task ID")
    require(isinstance(epoch, int) and not isinstance(epoch, bool) and 0 < epoch <= 2**64 - 1,
            "epoch must be positive u64")


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


def replacement_host(state):
    recorded = state.get("replacement_host")
    require(text(recorded), "replacement host unavailable")
    return recorded


def plan_pod(args, kube, get, local):
    kind = "StatefulSet" if args.statefulset else "DaemonSet"
    resource = "statefulset" if args.statefulset else "daemonset"
    workload_name = args.statefulset if args.statefulset else args.daemonset
    require(text(args.context) and not args.context.startswith("-"), "explicit Kubernetes context required")
    require(text(args.config) and PurePosixPath(args.config).is_absolute()
            and PurePosixPath(args.config).suffix in (".yaml", ".yml"), "absolute deployment YAML required")
    origin(args.origin)
    require(type(args.batch_size) is int and args.batch_size > 0, "batch size must be positive")
    pod_task_identity(args.task_id, args.epoch)
    fields = (args.namespace, workload_name, args.container)
    require(all(text(v) and not v.startswith("-") for v in fields), "invalid identity")
    require(text(args.target_image), "target image required")
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
        require(text(view.get("runtime_rootfs_sha256")) and text(view.get("runtime_rootfs_path")),
                "missing runtime rootfs evidence")
        return view

    workload, pods = inventory()
    capabilities = get("/v1/node-upgrades/capabilities")
    require(type(capabilities.get("version")) is int and capabilities["version"] == 2
            and type(capabilities.get("node_protocol_version")) is int
            and capabilities["node_protocol_version"] == 3
            and all(capabilities.get(c) is True for c in CAPABILITIES), "required capabilities unavailable")
    views = {mappings[pod["name"]]: inspect(mappings[pod["name"]], pod["uid"]) for pod in pods}
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


def pod_actual(pod, namespace, container, node, response, session=None):
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
    return dict(container_id=evidence["container_id"], restart_count=evidence["restart_count"],
                running=evidence["running"], image_id=statuses[0]["imageID"], boot_nonce=boot,
                node_session=runtime["node_session"])


def process_component(response, role, node, pod_uid, eligible=True, complete=True, modes=("standalone",)):
    require(response.get("ok") is True, "node-info failed")
    view = response["node_info"]
    require(isinstance(view.get("schema_version"), int) and not isinstance(view["schema_version"], bool)
            and view["schema_version"] == 1, "unsupported node-info schema")
    require(view.get("pod_uid") == pod_uid, "node-info Pod UID differs from Kubernetes")
    require(text(view.get("boot_nonce")), "missing boot nonce")
    matches = [item for item in view["components"] if item.get("role") == role and item.get("node_id") == node]
    require(len(matches) == 1, "no unique component for explicit role and ADX node ID")
    component = matches[0]
    require(component.get("mode") in modes, "unsupported component mode")
    if eligible:
        require(component.get("can_upgrade") is True, "component cannot upgrade")
    require(text(component.get("service_id")), "missing service ID")
    runtime = component.get("runtime")
    if runtime is None and not complete:
        return component, view["boot_nonce"]
    require(isinstance(runtime, dict), "missing runtime")
    require(runtime["pod_uid"] == pod_uid and runtime["boot_nonce"] == view["boot_nonce"],
            "runtime Pod UID or boot nonce differs from node-info")
    require(text(runtime.get("node_session")), "missing node session")
    if component["mode"] == "standalone" or runtime.get("relay_session") != "":
        require(text(runtime.get("relay_session")), "missing relay session")
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


def plan_process(args, kube, local):
    """Freeze one observation; execution authority rechecks the selected identity."""
    configuration = None if args.component_config_path is None else dict(mount_path=args.component_config_path)
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
    require(text(args.commit), "commit required")
    require(args.sha256 is not None or args.archive_url is not None, "binary SHA-256 required without archive")
    require((args.archive_url is None) == (args.archive_sha256 is None), "archive flags must be paired")
    archive = None if args.archive_url is None else dict(url=args.archive_url, archive_sha256=args.archive_sha256)
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

    def observe(pod_name, node):
        pod = kube(["get", "pod", pod_name, "--namespace", args.namespace])
        evidence = process_pod(pod, args.namespace, pod_name, args.container)
        response = local(pod_name)
        if args.role == "adxlet":
            require_process_steps(response)
        component, _ = process_component(response, args.role, node, evidence["uid"])
        record = {"node_id": node, "service_id": component["service_id"], "kube": evidence,
                  "runtime": component["runtime"].copy()}
        return record

    ordered = sorted(mappings.items(), key=lambda item: item[1])
    nodes = [observe(pod_name, node) for pod_name, node in ordered]
    for record in nodes:
        pod_name, node = record["kube"]["name"], record["node_id"]
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
    require(len(canonical(result).encode("utf-8")) <= LIMIT, "plan exceeds size limit")
    return result


def read_json_file(path):
    descriptor = os.open(path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, mode=0o600)
    with os.fdopen(descriptor, "rb") as stream:
        require(stat.S_ISREG(os.fstat(stream.fileno()).st_mode), "JSON input must be a regular file")
        data = stream.read(LIMIT + 1)
        return decode(data)


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
    """Check execution scope and lossless CLI adaptation; adxctl owns execution fencing."""
    require(type(plan.get("schema_version")) is int and plan["schema_version"] == 1
            and plan.get("mode") == "process", "expected schema 1 process plan")
    require(plan["role"] in ("adxlet", "relay"), "invalid role")
    require(all(text(plan[key]) and not plan[key].startswith("-") for key in ("context", "namespace", "container")),
            "invalid Kubernetes selection")
    config = plan["config"]
    require(text(config) and PurePosixPath(config).is_absolute()
            and PurePosixPath(config).suffix in (".yaml", ".yml"), "absolute deployment YAML required")
    if plan["role"] == "adxlet":
        origin(plan["origin"])
    nodes = plan["nodes"]
    require(isinstance(nodes, list) and bool(nodes), "empty process plan")
    ids, pods, tasks = [], set(), set()
    for node in nodes:
        node_id, evidence, submission = node["node_id"], node["kube"], node["submission"]
        submit_arguments(submission)
        require(submission.get("remote_archive") == nodes[0]["submission"].get("remote_archive"),
                "archive differs across process batches")
        require(submission.get("component_config") == nodes[0]["submission"].get("component_config"),
                "configuration descriptor differs across process batches")
        require(text(evidence["name"]) and not evidence["name"].startswith("-"), "invalid Pod name")
        require(evidence["namespace"] == plan["namespace"] and evidence["container"] == plan["container"],
                "Kubernetes selection differs from plan")
        require(submission["selection"].get("mode") == "publisher", "publisher selection required")
        expected = submission["selection"]["expected"]
        require(expected["pod_uid"] == evidence["uid"], "runtime Pod UID differs from Kubernetes")
        require(all(submission.get(key) == plan[key] for key in ("role", "commit", "sha256")), "target differs")
        if plan["role"] == "adxlet":
            require(submission["process"] == {"node_id": node_id, "identity": {
                "rollout_id": plan["rollout_id"], "epoch": plan["epoch"], "old_node_session": expected["node_session"],
                "old_relay_session": expected["relay_session"], "original_sha256": expected["executable_sha256"]}},
                    "maintenance identity differs")
        key = dict(context=plan["context"], namespace=plan["namespace"], container=plan["container"], config=config,
                   role=plan["role"], pod=evidence["name"], node_id=node_id, service_id=submission["service_id"],
                   rollout_id=plan["rollout_id"], epoch=plan["epoch"])
        task_id = hashlib.sha256(canonical(key).encode("utf-8")).hexdigest()
        require(submission["task_id"] == task_id, "local task ID differs from canonical identity")
        require(evidence["name"] not in pods and task_id not in tasks, "duplicate Pod/task")
        ids.append(node_id)
        pods.add(evidence["name"])
        tasks.add(task_id)
    require(ids == sorted(set(ids)), "nodes must be unique and sorted")
    batches = plan["batches"]
    require(isinstance(batches, list) and batches and all(isinstance(batch, list) and batch for batch in batches),
            "invalid process batches")
    require(batches[0] == ids[:1] and list(chain.from_iterable(batches)) == ids, "invalid canary or batch order")


def maintenance_owner(plan, node):
    return dict(node_id=node["node_id"], rollout_id=plan["rollout_id"],
                local_task_id=node["submission"]["task_id"], epoch=plan["epoch"])


def submit_arguments(submission):
    def fields(value, required, optional=()):
        require(isinstance(value, dict), "expected Submission object")
        require(set(required) <= set(value) <= set(required) | set(optional), "unsupported Submission fields")

    fields(submission, ("selection", "task_id", "role", "service_id", "commit"),
           ("sha256", "component_config", "remote_archive", "process"))
    selection = submission["selection"]
    fields(selection, ("mode", "expected"))
    require(selection["mode"] == "publisher", "publisher selection required")
    fields(selection["expected"], ("configuration_sha256", "boot_nonce", "pod_uid", "pid",
                                   "executable_sha256", "node_session", "relay_session"))
    for name, keys in (("remote_archive", ("url", "archive_sha256")),
                       ("component_config", ("mount_path",))):
        if name in submission:
            fields(submission[name], keys)
    if "process" in submission:
        fields(submission["process"], ("node_id", "identity"))
        fields(submission["process"]["identity"], ("rollout_id", "epoch", "old_node_session",
                                                  "old_relay_session", "original_sha256"))
    arguments = ["submit"]
    for key in ("task_id", "role", "service_id", "commit"):
        arguments.extend(["--" + key.replace("_", "-"), submission[key]])
    if "sha256" in submission:
        arguments.extend(["--sha256", submission["sha256"]])
    if "component_config" in submission:
        descriptor = submission["component_config"]
        arguments.extend(["--component-config-path", descriptor["mount_path"]])
    arguments.extend(["--expected-runtime", canonical(submission["selection"]["expected"])])
    if "remote_archive" in submission:
        archive = submission["remote_archive"]
        arguments.extend(["--archive-url", archive["url"], "--archive-sha256", archive["archive_sha256"]])
    if "process" in submission:
        arguments.extend(["--process-identity", canonical(submission["process"])])
    require(all(isinstance(value, str) and "\x00" not in value for value in arguments),
            "Submission CLI values must be strings without NUL")
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
    prepared_valid = text(digest) and (plan["sha256"] is None or plan["sha256"] == digest)
    configuration = prepared.get("configuration_sha256") if isinstance(prepared, dict) else None
    prepared_valid = prepared_valid and text(configuration)
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
    leg = execution.get("leg")
    if not isinstance(leg, dict):
        return None
    success = phase == "completed" and leg.get("kind") == "target"
    restored = phase == "failed" and leg.get("kind") == "rollback"
    if execution.get("stage") != "selected" or not (success or restored):
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
    require(status in ("absent", "active", "released"), "invalid maintenance query status")
    sessions = ("origin_session", "current_session") if status == "active" else (
        ("node_session",) if status == "released" else ())
    require(all(text(result.get(key)) for key in sessions),
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


def advance_adxlet_process(plan, node, state, save, kube, local, cli, post):
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
        if (not isinstance(execution, dict) or execution.get("stage") != "exited"
                or execution.get("leg") != dict(kind="target")):
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
        require(isinstance(execution, dict), "execution identity unavailable")
        source = dict(attempt=state["attempt"], node_session=state["node_session"])
        if execution["leg"] == dict(kind="target"):
            require(all(execution.get(key) == source[key] for key in source), "execution identity differs")
        else:
            require(state["stage"] in ("rollback_intent", "release_intent", "failed")
                    and execution["leg"] == {"kind": "rollback", "from": source}, "rollback B names a different A")
            require(all(text(execution.get(key)) and len(execution[key].encode("utf-8")) <= 128
                        and execution[key] != source[key] for key in source),
                    "rollback B identity unavailable or reuses A")
            require(execution["node_session"] != expected["node_session"], "rollback B reuses origin session")
            budget_open(execution)  # validate the local, already fixed 600-second deadline
        return execution

    def budget_open(execution):
        deadline = execution.get("deadline_unix_ms")
        require(type(deadline) is int and deadline > 0, "local execution deadline unavailable")
        return int(time.time() * 1000) < deadline

    def finish_terminal(task):
        execution = task.get("adxlet_execution")
        retained = (task.get("phase") == "failed" and execution is None and task.get("execution") is None
                    and state["stage"] in ("prepare_intent", "exit_intent") and "attempt" not in state)
        selected = (isinstance(execution, dict) and execution.get("stage") == "selected"
                    and ((task.get("phase") == "completed" and execution.get("leg") == dict(kind="target"))
                         or (task.get("phase") == "failed" and execution.get("leg", {}).get("kind") == "rollback")))
        if not (retained or selected):
            return False
        if selected:
            same_execution(task)
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
        require(execution.get("leg") == dict(kind="target"), "original attempt cannot roll back again")
        require(execution.get("stage") in ("exited", "starting"),
                "target outcome must be reconciled before rollback")
        require(type(execution.get("old_pid")) is int and execution["old_pid"] == expected["pid"],
                "rollback old PID differs")
        marker = owned_marker()
        require(marker.get("yielded") is True
                and marker.get("current_session") in (expected["node_session"], state["node_session"]),
                "rollback requires yielded original and known current session")
        state["stage"] = "rollback_intent"
        save()

    if state["stage"] == "new":
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
        except RECOVERABLE_ERRORS:
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
                elif current.get("stage") != "exiting":
                    return "pending"
                marker = owned_marker()
                require(marker.get("current_session") == expected["node_session"], "Exit origin session differs")
                if current is None:
                    require(marker.get("yielded") is False, "first Exit requires unyielded original")
                save()
                # Exiting replays the original empty input, including expiry-only observation.
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
            stage = execution.get("stage")
            if stage not in ("exited", "starting", "committing") or task.get("phase") == "completed":
                return "pending"
            if stage == "committing" and not (
                    type(execution.get("candidate_pid")) is int and execution["candidate_pid"] > 0):
                return "pending"
            require(execution.get("leg") == dict(kind="target"), "Start requires the target leg")
            marker = owned_marker()
            allowed_sessions = (expected["node_session"],) if stage == "exited" else (
                expected["node_session"], state["node_session"])
            require(marker.get("yielded") is True and marker.get("current_session") in allowed_sessions,
                    "Start requires yielded origin and known current session")
            failed_start = stage == "starting" and text(execution.get("error"))
            if stage != "committing" and (not budget_open(execution) or task.get("phase") == "failed"):
                begin_rollback(task)
            else:
                # Replaying a local Starting attempt is idempotent. A plain exited
                # Task with an unexplained live process is not permission to spawn.
                if stage == "exited" and check_process(plan, node, kube, local) is not None:
                    return "pending"
                save()
                step_error = None
                try:
                    cli(node, ["step", "--task-id", task_id, "--step", "start",
                               "--attempt", state["attempt"], "--node-session", state["node_session"]])
                except RECOVERABLE_ERRORS as error:
                    step_error = error
                task = query_task()
                if step_error is not None and not (
                        failed_start and task.get("phase") in ("unknown", "failed")
                        and task.get("adxlet_execution", {}).get("stage") == "starting"
                        and text(task.get("adxlet_execution", {}).get("error"))):
                    # A CLI failure is never rollback proof. Preserve the unknown
                    # request unless fresh Query confirms the same local failure stage.
                    raise step_error
                if not finish_terminal(task):
                    execution = same_execution(task)
                    # Commit ambiguity uses only same-A Receipt, even after expiry.
                    if execution.get("stage") == "committing":
                        return "pending"
                    if (execution.get("stage") != "starting" or not text(execution.get("error"))
                            or task.get("phase") not in ("unknown", "failed")):
                        return "pending"
                    if not failed_start and budget_open(execution):
                        return "pending"
                    begin_rollback(task)
    if state["stage"] == "rollback_intent":
        task = query_task()
        if not finish_terminal(task):
            execution = same_execution(task)
            if execution["leg"] == dict(kind="target"):
                require(execution.get("stage") in ("exited", "starting", "stopping_for_rollback"),
                        "rollback target outcome unavailable")
                # The first request still requires this owner's yielded marker.
                # Once B exists the accepted local action needs no new approval.
                marker = owned_marker()
                require(marker.get("yielded") is True and marker.get("current_session") in (
                        expected["node_session"], state["node_session"]),
                        "Rollback requires yielded origin and known current session")
            elif execution.get("stage") not in ("exited", "starting", "committing"):
                return "pending"
            elif execution["stage"] == "committing" and not (
                    type(execution.get("candidate_pid")) is int and execution["candidate_pid"] > 0):
                return "pending"
            save()
            # Even after B exists, always replay the frozen source A. Only the
            # local rollback worker may start/resume B; never issue Start(B).
            cli(node, ["step", "--task-id", task_id, "--step", "rollback",
                       "--attempt", state["attempt"], "--node-session", state["node_session"]])
            task = query_task()
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
                require(execution.get("stage") == "selected", "local task has no terminal selection")
                require(execution["node_session"] == body["node_session"], "local release session differs")
                require(execution["leg"]["kind"] == ("target" if state["outcome"] == "completed" else "rollback"),
                        "local selected leg differs")
            else:
                require(state["outcome"] == "failed" and task.get("adxlet_execution") is None
                        and task.get("execution") is None, "local task did not retain original process")
                require(body["node_session"] == expected["node_session"], "local original session differs")
        else:
            if "attempt" in state:
                same_execution(task)
            else:
                require(task.get("adxlet_execution") is None and task.get("execution") is None,
                        "release without an attempt requires retained original")
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
        return advance_adxlet_process(plan, node, state, save, kube, local, cli, post)
    submission = node["submission"]
    deadline_queried = False
    if state["stage"] == "new":
        check_process(plan, node, kube, local, original=True)
        state["stage"] = "prepared"
        save()
    if state["stage"] == "prepared":
        check_process(plan, node, kube, local, original=True)
        state.update(stage="submit_intent", first_submit_unix_ms=int(time.time() * 1000))
        save()
        response = cli(node, submit_arguments(submission))
    elif state["stage"] == "submit_intent":
        check_process(plan, node, kube, local)
        try:
            response = cli(node, ["query", "--task-id", submission["task_id"]])
            require(response.get("ok") is True, "query result unavailable")
        except RECOVERABLE_ERRORS:
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


def apply_process(plan, journal_path, batch_index, kube, local, cli, post, sleep=time.sleep):
    validate_process_plan(plan)
    # Linux publisher Job only. Keep this lock across all I/O and atomic journal replacements.
    require(sys.platform == "linux", "apply-process requires a Linux publisher")

    require(type(batch_index) is int and 0 <= batch_index < len(plan["batches"]), "select batch index")
    with locked_journal(journal_path) as path:
        digest = template_hash(plan)
        journal_schema = 2 if plan["role"] == "adxlet" else 1
        if path.exists() or path.is_symlink():
            journal = read_json_file(path)
            require(journal.get("plan_hash") == digest, "journal plan hash differs")
            require(journal.get("schema_version") == journal_schema, "invalid journal schema")
        else:
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
                               "rollback_intent"))
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
                    "start_intent", "rollback_intent"):
                require(plan["role"] == "adxlet" and all(text(state.get(key))
                        and len(state[key].encode("utf-8")) <= 128 for key in ("attempt", "node_session")),
                        "invalid durable Step identity")
                require(state["node_session"] != by_id[node_id]["submission"]["selection"]["expected"]["node_session"],
                        "new session equals old")
            if plan["role"] == "adxlet" and stage in ("release_intent", "completed"):
                require("release_session" in state, "missing durable release session")
            if plan["role"] == "adxlet" and stage == "failed" and "release_session" not in state:
                require("attempt" not in state and "outcome" not in state,
                        "failed execution requires durable release session")
        for previous in plan["batches"][:batch_index]:
            require(all(states.get(node_id, {}).get("stage") == "completed" for node_id in previous),
                    "prior batch is not successfully completed and released")

        def save():
            write_journal(path, journal)

        selected = plan["batches"][batch_index]
        for node_id in selected:
            state = states.setdefault(node_id, {"stage": "new"})
            if state["stage"] in ("completed", "failed"):
                result = state["stage"]
            else:
                try:
                    result = advance_process(plan, by_id[node_id], state, save, kube, local, cli, post, sleep)
                except RECOVERABLE_ERRORS as error:
                    return dict(status="pending", batch_index=batch_index, node_id=node_id, detail=str(error))
            if result != "completed":
                return dict(status=result, batch_index=batch_index, node_id=node_id)
        return dict(status="completed", batch_index=batch_index)


def validate_pod_plan(plan):
    """Bind local Pod selection to the Coordinator owner before any Kubernetes effect."""
    require(type(plan.get("schema_version")) is int and plan["schema_version"] == 1
            and plan.get("mode") == "pod", "expected schema 1 Pod plan")
    origin(plan["origin"])
    require(text(plan["config"]) and PurePosixPath(plan["config"]).is_absolute()
            and PurePosixPath(plan["config"]).suffix in (".yaml", ".yml"), "absolute deployment YAML required")
    begin, workload = plan["begin"], plan["workload"]
    pod_task_identity(begin["task_id"], begin["epoch"])
    kind = workload_kind(workload)
    selectors = (plan["context"], plan["container"], begin["namespace"], workload["name"])
    require(all(text(value) and not value.startswith("-") for value in selectors), "invalid Pod selection")
    require(begin["namespace"] == workload["namespace"] and begin["daemonset_uid"] == workload["uid"],
            "workload binding differs")
    require(all(isinstance(workload.get(key), str) and re.fullmatch(r"[0-9a-f]{64}", workload[key])
                for key in ("original_template_hash", "target_template_hash")), "invalid template fingerprint")
    require(text(workload.get("original_image")), "missing original template image")
    pods = plan["pods"]
    require(isinstance(pods, list) and bool(pods), "empty Pod plan")
    ids, names, hosts, uids, bindings = [], [], [], [], []
    for pod in pods:
        require(text(pod["name"]) and not pod["name"].startswith("-"), "invalid Pod name")
        if kind == "StatefulSet":
            require(stable_statefulset_name(workload["name"], pod["name"]),
                    "StatefulSet Pods must use canonical ordinal names")
        require(text(pod.get("kubernetes_node_name")) and text(pod.get("original_image")),
                "missing source host or image")
        require(pod["namespace"] == begin["namespace"] and pod["owner_uid"] == workload["uid"]
                and pod["target_image"] == begin["target_image"], "Pod plan binding differs")
        inspection = pod["inspection"]
        require(pod["actual"]["node_session"] == inspection["node_session_id"], "actual source session differs")
        require(inspection["observed_pod_uid"] == pod["uid"] and inspection["node_id"] == pod["node_id"],
                "Pod inspection binding differs")
        bindings.append(dict(node_id=pod["node_id"], pod_uid=pod["uid"],
                             node_session_id=inspection["node_session_id"]))
        ids.append(pod["node_id"])
        names.append(pod["name"])
        hosts.append(pod["kubernetes_node_name"])
        uids.append(pod["uid"])
    require(names == sorted(set(names)) and len(set(ids)) == len(ids)
            and len(set(uids)) == len(uids), "duplicate or unordered Pod bindings")
    require(kind == "StatefulSet" or len(set(hosts)) == len(hosts), "duplicate source host")
    require(begin["nodes"] == bindings, "begin bindings differ")
    batches = plan["batches"]
    require(isinstance(batches, list) and batches and all(isinstance(batch, list) and batch for batch in batches),
            "invalid Pod batches")
    require(batches[0] == ids[:1] and list(chain.from_iterable(batches)) == ids, "invalid Pod batches")


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
                local=None):
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
        try:
            post("/v1/node-upgrades/tasks", begin)
        except ManagementError as error:
            # Only a confirmed rejection of this first attempt proves it did not
            # execute. Replays after an unknown response never create this proof.
            if error.status == 400 and error.code == "InvalidArgument":
                journal["begin_rejected"] = True
                save()
            raise
        journal["stage"] = "template"
        save()
        return False
    if stage == "begin_intent":
        require(journal.get("begin_rejected") is not True, "Begin was rejected; use stop-pod or abort-pod")
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
                                                     local(evidence["name"]), session)
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
                    and current["spec"].get("nodeName") == replacement_host(state)
                    and image(current, plan["container"]) == begin["target_image"], "replacement changed before verify")
            pod_inspection(plan, pod, binding["pod_uid"], binding["node_session_id"], get)
            require(local is not None, "NodeInfo transport unavailable")
            actual = pod_actual(current, begin["namespace"], plan["container"], node_id,
                                local(current["metadata"]["name"]), binding["node_session_id"])
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
        except RECOVERABLE_ERRORS:
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

    if journal.get("begin_rejected") is True:
        # A previous task with the same ID may exist. Only an authoritative
        # NotFound permits local cleanup; transport errors remain unknown.
        try:
            pod_task(plan, get, cleanup=True)
        except ManagementError as error:
            if error.status != 404 or error.code != "NotFound":
                raise
        else:
            journal.pop("begin_rejected")
            journal["stage"] = "template"
            save()
            return None
        journal["stage"] = "new"
        journal.pop("begin_rejected")
        save()
    if journal["stage"] == "new":
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
                    and current["spec"].get("nodeName") == replacement_host(normal)
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
            action("abort", dict(stage="finalize", template_restored=True))
            return None
    cleanup["template_intent"] = patch
    save()
    mutate(["patch", resource, workload["name"], "--namespace", begin["namespace"], "--type=json",
            "--patch", canonical(patch), "-o", "json"], None)
    return None


def apply_pod(plan, journal_path, batch_index, clients, sleep=time.sleep, mode="apply", local=None):
    kube, get, post, mutate = clients
    require(sys.platform == "linux", "apply-pod requires a Linux publisher")

    validate_pod_plan(plan)
    require(type(batch_index) is int and 0 <= batch_index < len(plan["batches"]), "select batch index")
    with locked_journal(journal_path) as path:
        digest = template_hash(plan)
        if path.exists() or path.is_symlink():
            journal = read_json_file(path)
            require(journal.get("schema_version") == 1 and journal.get("plan_hash") == digest,
                    "Pod journal plan hash differs")
        else:
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
                if advance_pod(plan, journal, save, batch_index, clients, local=local):
                    return dict(status="completed", batch_index=batch_index)
                sleep(2)
            return dict(status="pending", batch_index=batch_index, detail="publisher observation budget expired")
        except RECOVERABLE_ERRORS as error:
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
    process = commands.add_parser("plan-process", help="GET-only frozen process plan")
    process.add_argument("--archive-url")
    process.add_argument("--archive-sha256")
    process.add_argument("--sha256")
    process.add_argument("--component-config-path")
    process.add_argument("--commit", required=True)
    for name in ("context", "namespace", "container", "config", "task-id"):
        process.add_argument("--" + name, required=True)
    process.add_argument("--role", choices=("adxlet", "relay"), required=True)
    process.add_argument("--origin")
    process.add_argument("--epoch", type=int, required=True)
    process.add_argument("--node", action="append", required=True, metavar="POD=NODE")
    process.add_argument("--batch-size", type=int, default=1)
    apply = commands.add_parser("apply-process", help="select one process batch")
    apply.add_argument("--plan", required=True)
    apply.add_argument("--journal", required=True)
    apply.add_argument("--batch-index", type=int, required=True)
    pod_apply = commands.add_parser("apply-pod", help="select one Pod batch")
    pod_apply.add_argument("--plan", required=True)
    pod_apply.add_argument("--journal", required=True)
    pod_apply.add_argument("--batch-index", type=int, required=True)
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

            def pod_local(pod):
                return pod_cli(pod, ["node-info"])

            outcome = apply_pod(snapshot, args.journal, getattr(args, "batch_index", 0), PodClients(pod_kube,
                                lambda path: http_json(snapshot["origin"], path),
                                lambda path, body: http_json(snapshot["origin"], path, body), pod_mutate),
                                mode={"apply-pod": "apply", "stop-pod": "stop", "abort-pod": "abort"}[args.command],
                                local=pod_local)
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

            def apply_local(pod_name):
                node = next(item for item in snapshot["nodes"] if item["kube"]["name"] == pod_name)
                return apply_cli(node, ["node-info"])

            def apply_post(path, body):
                return http_json(snapshot["origin"], path, body)

            outcome = apply_process(snapshot, args.journal, args.batch_index,
                                    apply_kube, apply_local, apply_cli, apply_post)
            emit_cli(canonical(outcome))
            return 0 if outcome["status"] == "completed" else 1

        def kube(arguments):
            context = ["--context", args.context] if args.context else []
            return kubectl_get([*context, *arguments])

        def local(pod):
            return kubectl_json(["--context", args.context, "--namespace", args.namespace,
                                 "exec", pod, "--container", args.container, "--",
                                 adxctl, "--config", args.config, "rollout", "node-info"])

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
