#!/usr/bin/env python3
"""Publish RAFS images and prepare DFS seed copies and worker caches."""
import argparse
from concurrent.futures import ThreadPoolExecutor
import hashlib
import json
import os
from pathlib import Path
import platform
import shlex
import signal
import subprocess
import sys
import time
from urllib.error import HTTPError, URLError
from urllib.parse import quote, urlsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener
from uuid import uuid4

from prewarm import validate_plan
from sample_ranges import analyze, make_plan

MANIFEST_ACCEPT = ", ".join((
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
))
MAX_RESPONSE = 8 * 1024 * 1024
STARTUP_PREFIX = "/v1/admin/nydus/"
BOOTSTRAP = "containerd.io/snapshot/nydus-bootstrap"


class NoRedirect(HTTPRedirectHandler):
    """Keep management credentials and job affinity on the selected endpoint."""

    def redirect_request(self, req, fp, code, msg, headers, newurl):
        return None


def read_json(path):
    return json.loads(Path(path).read_bytes())


def write_json(path, value):
    with path.open("x", encoding="utf-8") as output:
        json.dump(value, output, indent=2)
        output.write("\n")


def secret(config, name):
    path = config.get(name)
    return Path(path).read_text().strip() if path else None


def request(endpoint, path, *, token=None, payload=None, accept=None):
    url = urlsplit(endpoint)
    if (url.scheme not in {"http", "https"} or url.hostname is None
            or url.username is not None or url.path not in {"", "/"} or url.query or url.fragment):
        raise ValueError("endpoint must be an HTTP(S) origin")
    headers = {"Accept": accept or "application/json"}
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    req = Request(endpoint.rstrip("/") + path, data=body, headers=headers)
    if token:
        req.add_unredirected_header("Authorization", "Bearer " + token)
    with build_opener(NoRedirect()).open(req, timeout=10) as response:
        raw = response.read(MAX_RESPONSE + 1)
        if len(raw) > MAX_RESPONSE:
            raise ValueError("HTTP response exceeds the tool limit")
        return raw, response.headers


def admin(endpoint, path, token, payload=None):
    if not token:
        raise ValueError("a management credential is required")
    return json.loads(request(endpoint, path, token=token, payload=payload)[0])


def discover(config):
    return admin(config["meta_admin_url"], "/v1/admin/nodes", secret(config, "meta_admin_token_file"))


def choose_targets(snapshot, deployment, purpose, explicit=None):
    online = [node for node in snapshot["nodes"] if node["online"]]
    if purpose == "seed":
        candidates = [node for node in online if node["has_dfs_storage"]]
    elif purpose == "prewarm":
        candidates = [
            node for node in online
            if node["deployment"] == deployment and node["role"] == "worker"
            and "nydus" in node["connectors"]
        ]
    else:
        raise ValueError("unknown target selection purpose")
    indexed = {node["node_id"]: node for node in candidates}
    if explicit:
        missing = set(explicit) - indexed.keys()
        if missing:
            raise ValueError(f"nodes are not eligible for {purpose}: {', '.join(sorted(missing))}")
        candidates = [indexed[node] for node in dict.fromkeys(explicit)]
    if not candidates:
        raise ValueError(f"no eligible nodes for {purpose}")
    return sorted(candidates, key=lambda node: node["node_id"])


def seed_coordinator(snapshot, deployment):
    candidates = [
        node for node in snapshot["nodes"]
        if node["online"] and node["deployment"] == deployment
        and "nydus" in node["connectors"] and node["management_rest_addr"]
    ]
    if not candidates:
        raise ValueError("no Nydus job endpoint in the selected deployment")
    return min(candidates, key=lambda node: (node["role"] != "seed", node["node_id"]))


def warm_payload(image, plan, request_id):
    if plan["manifest"] != image["manifest_digest"]:
        raise ValueError("plan does not match the image manifest")
    return {
        "source": image["source"], "manifest_digest": image["manifest_digest"],
        "request_id": request_id,
        "ranges": [
            {"blob_digest": span["blob"], "offset": span["offset"], "length": span["length"]}
            for span in plan["ranges"]
        ],
    }


def run_job(node, kind, payload, config):
    endpoint = node["management_rest_addr"]
    if not endpoint:
        raise ValueError(f"{node['node_id']} lacks a management address")
    token = secret(config, "node_admin_token_file")
    path = STARTUP_PREFIX + kind + "-jobs"
    deadline = time.monotonic() + float(config.get("job_wait_seconds", 1200))

    def call(path, payload=None):
        while time.monotonic() < deadline:
            try:
                return admin(endpoint, path, token, payload)
            except HTTPError as error:
                if error.code not in {429, 502, 503, 504}:
                    raise
            except (URLError, TimeoutError):
                pass
            time.sleep(0.3)
        raise TimeoutError(f"{kind} on {node['node_id']} exceeded its wait budget")

    status = call(path, payload)
    resubmitted = False
    while status["state"] not in {"ready", "failed"}:
        time.sleep(0.3)
        try:
            status = call(path + "/" + quote(status["job_id"], safe=""))
        except HTTPError as error:
            if error.code != 404 or resubmitted:
                raise
            status = call(path, payload)
            resubmitted = True
    return dict(status, endpoint=endpoint, node_id=node["node_id"], request_id=payload["request_id"])


def seed(config, snapshot, image, explicit):
    targets = choose_targets(snapshot, config["deployment"], "seed",
                             explicit or config.get("seed_node_ids"))
    receiver = seed_coordinator(snapshot, config["deployment"])
    payload = {
        "source": image["source"], "manifest_digest": image["manifest_digest"],
        "node_ids": [node["node_id"] for node in targets], "request_id": str(uuid4()),
    }
    return dict(run_job(receiver, "seed", payload, config), snapshot_revision=snapshot["snapshot_revision"])


def prewarm(config, snapshot, image, manifest_path, plan):
    validate_plan(manifest_path, plan, plan["workload"], plan["command"])
    targets = choose_targets(snapshot, config["deployment"], "prewarm")

    def run(node):
        payload = warm_payload(image, plan, str(uuid4()))
        try:
            return run_job(node, "prewarm", payload, config)
        except (ValueError, OSError) as error:
            return {"node_id": node["node_id"], "request_id": payload["request_id"],
                    "state": "failed", "error": str(error)}

    with ThreadPoolExecutor(max_workers=min(8, len(targets))) as pool:
        results = list(pool.map(run, targets))
    complete = all(status["state"] == "ready" for status in results)
    return {"snapshot_revision": snapshot["snapshot_revision"], "jobs": results,
            "state": "ready" if complete else "failed"}


def registry_manifest(endpoint, repository, reference, token):
    path = "/v2/" + quote(repository, safe="/") + "/manifests/" + quote(reference, safe=":")
    raw, headers = request(endpoint, path, token=token, accept=MANIFEST_ACCEPT)
    digest = "sha256:" + hashlib.sha256(raw).hexdigest()
    declared = headers.get("Docker-Content-Digest")
    if (declared and digest != declared) or (reference.startswith("sha256:") and reference != digest):
        raise ValueError("Registry manifest digest mismatch")
    return raw, digest


def resolve_manifest(endpoint, repository, tag, target_platform, token):
    def is_nydus(raw):
        document = json.loads(raw)
        return any(layer.get("annotations", {}).get(BOOTSTRAP) == "true"
                   for layer in document.get("layers", []))

    raw, digest = registry_manifest(endpoint, repository, tag, token)
    index = json.loads(raw).get("manifests")
    if index is None:
        if not is_nydus(raw):
            raise ValueError("published image has no Nydus bootstrap")
        return raw, digest
    system, arch, *variant = target_platform.split("/")
    matches = []
    for descriptor in index:
        descriptor_platform = descriptor.get("platform", {})
        if (descriptor_platform.get("os") != system or descriptor_platform.get("architecture") != arch
                or (variant and descriptor_platform.get("variant") != variant[0])):
            continue
        candidate = registry_manifest(endpoint, repository, descriptor["digest"], token)
        if is_nydus(candidate[0]):
            matches.append(candidate)
    if len(matches) != 1:
        raise ValueError(f"published index needs exactly one RAFS manifest for {target_platform}")
    return matches[0]


def convert(config, input_image, target, output):
    target = target or config["publish_target"]
    authority, _, path = target.partition("/")
    repository, colon, tag = path.rpartition(":")
    if not authority or not repository or not colon or "/" in tag or "@" in path:
        raise ValueError("publish target must be a Registry repository with an explicit tag")
    if "publish_target" in config:
        configured = config["publish_target"].rsplit(":", 1)[0]
        if target.rsplit(":", 1)[0] != configured:
            raise ValueError("publish target repository differs from the configured AFS source")
    output.mkdir(mode=0o700, parents=True, exist_ok=False)
    command = [
        config.get("nydusify", "nydusify"), "convert", "--fs-version", "5",
        "--source", input_image, "--target", target, "--work-dir", str(output / "conversion"),
    ]
    if config.get("nydus_image"):
        command.extend(["--nydus-image", config["nydus_image"]])
    if config.get("plain_http"):
        command.extend(["--plain-http", "--source-insecure", "--target-insecure"])
    with (output / "convert.log").open("xb") as log:
        subprocess.run(command, stdout=log, stderr=subprocess.STDOUT, check=True)
    scheme = "http" if config.get("plain_http") else "https"
    endpoint = config.get("manifest_registry_url", f"{scheme}://{authority}")
    native_arch = {"x86_64": "amd64", "aarch64": "arm64", "arm64": "arm64"}.get(platform.machine())
    target_platform = config.get("platform") or f"linux/{native_arch}"
    raw, digest = resolve_manifest(endpoint, repository, tag, target_platform, secret(config, "registry_token_file"))
    with (output / "manifest.json").open("xb") as manifest:
        manifest.write(raw)
    image = {"image": f"{urlsplit(endpoint).netloc}/{repository}@{digest}",
             "manifest_digest": digest, "repository": repository, "source": config["source"],
             "platform": target_platform, "publish_target": target}
    write_json(output / "image.json", image)
    return image


def sample(config, snapshot, image, output, worker, workload, command, expected):
    """Run on the isolated sampling worker, in its sandboxd/AFS network namespace."""
    from adx_environment import CommandStatus, ConnectionConfig, Environment

    [target] = choose_targets(snapshot, config["deployment"], "prewarm", [worker])
    command = shlex.join(command)
    capture = output / "startup.pcap"
    log = output / "tcpdump.log"
    connection = ConnectionConfig.from_env(
        server_address=config.get("adx_server_address"), token=secret(config, "adx_token_file"),
        verify_tls=config.get("adx_verify_tls", True),
    )
    with log.open("xb") as stderr:
        sniffer = subprocess.Popen(
            ["tcpdump", "-i", "lo", "-s", "0", "-U", "-w", str(capture),
             *(["-Z", "root"] if os.geteuid() == 0 else []),
             f"ip and tcp dst port {int(config.get('sampling_proxy_port', 7401))}"],
            stdout=subprocess.DEVNULL, stderr=stderr, env=dict(os.environ, LC_ALL="C"),
        )
        try:
            end = time.monotonic() + 10
            while "listening on " not in log.read_text():
                if sniffer.poll() is not None or time.monotonic() >= end:
                    raise RuntimeError(f"cannot start capture, see {log}")
                time.sleep(0.05)
            data = {"submit_wall_ns": time.time_ns(), "submit_ns": time.monotonic_ns()}
            with Environment(
                connection=connection, image=image["image"], runtime="runsc",
                node_id=target["adx_worker_id"], cpu=500, memory=512,
            ) as environment:
                result = environment.commands.run(command, timeout=60)
                data.update(command_ns=time.monotonic_ns(), command_wall_ns=time.time_ns(),
                            exit_code=result.exit_code, stdout=result.stdout,
                            ok=(result.status == CommandStatus.SUCCEEDED and result.exit_code == 0
                                and not result.stdout_truncated and result.stdout.strip() == expected.strip()))
        finally:
            if sniffer.poll() is None:
                sniffer.send_signal(signal.SIGINT)
                sniffer.wait(timeout=10)
    if "0 packets dropped by kernel" not in log.read_text().splitlines():
        raise RuntimeError(f"capture has unreadable loss counters, see {log}")
    evidence = output / "startup.json"
    receipt = {"digest": image["manifest_digest"], "samples": [data]}
    write_json(evidence, receipt)
    if not data["ok"]:
        raise RuntimeError(f"sampling command failed validation, see {evidence}")
    digest, observed = analyze(output / "manifest.json", capture, receipt)
    write_json(output / "events.json", observed)
    plan = make_plan(digest, workload, command, evidence.stem, observed)
    write_json(output / "startup-plan.json", plan)
    return plan


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--config", type=Path, required=True)
    actions = parser.add_subparsers(dest="action", required=True)
    actions.add_parser("discover")
    for action in ("convert", "prepare"):
        sub = actions.add_parser(action)
        sub.add_argument("--image", required=True)
        sub.add_argument("--target")
        sub.add_argument("--output", type=Path, required=True)
        if action == "prepare":
            sub.add_argument("--sample-worker", required=True, help="AFS Node ID of this cold sampling worker")
            sub.add_argument("--workload", default="startup")
            sub.add_argument("--expect-stdout", required=True)
            sub.add_argument("command", nargs=argparse.REMAINDER)
    seed_arguments = actions.add_parser("seed")
    seed_arguments.add_argument("--image", type=Path, required=True)
    seed_arguments.add_argument("--node-id", action="append")
    warm = actions.add_parser("prewarm")
    warm.add_argument("--image", type=Path, required=True)
    warm.add_argument("--plan", type=Path, required=True)

    args = parser.parse_args()
    try:
        config = read_json(args.config)
        if args.action == "discover":
            result = discover(config)
        elif args.action in {"convert", "prepare"}:
            if args.action == "prepare":
                command = args.command[1:] if args.command[:1] == ["--"] else args.command
                if not command:
                    raise ValueError("prepare requires a sandbox command after --")
            result = convert(config, args.image, args.target, args.output)
            if args.action == "prepare":
                snapshot = discover(config)
                seeded = seed(config, snapshot, result, None)
                write_json(args.output / "seed.json", seeded)
                if seeded["state"] != "ready":
                    raise RuntimeError("complete image seed failed; see seed.json")
                plan = sample(config, snapshot, result, args.output, args.sample_worker, args.workload,
                              command, args.expect_stdout)
                warmed = prewarm(config, snapshot, result, args.output / "manifest.json", plan)
                write_json(args.output / "prewarm.json", warmed)
                result = dict(result, state=warmed["state"], seed=seeded, prewarm=warmed,
                              plan=str(args.output / "startup-plan.json"))
        else:
            image = read_json(args.image)
            if image["source"] != config["source"]:
                raise ValueError("image source key differs from deployment configuration")
            snapshot = discover(config)
            result = (seed(config, snapshot, image, args.node_id) if args.action == "seed"
                      else prewarm(config, snapshot, image, args.image.with_name("manifest.json"), read_json(args.plan)))
        print(json.dumps(result, indent=2))
        return int(result.get("state") == "failed")
    except (AssertionError, KeyError, ValueError, OSError, RuntimeError, subprocess.SubprocessError) as error:
        print(f"nydus-startup: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
