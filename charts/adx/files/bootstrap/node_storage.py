"""Node storage configuration and startup checks; never handles Sandbox user identity."""

import json
import re
import shutil
import subprocess
import uuid
from pathlib import Path
from urllib.parse import urlsplit

OBS_KEYS = ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY")


def require_executable(name):
    executable = shutil.which(name)
    if executable is None:
        raise RuntimeError("Required executable is missing: " + name)
    return str(Path(executable).resolve())


def absolute_path(value):
    return (
        isinstance(value, str)
        and value.startswith("/")
        and (
            value == "/"
            or all(re.fullmatch(r"[A-Za-z0-9_.-]+", part) and part not in (".", "..") for part in value[1:].split("/"))
        )
    )


def validate_storage(node):
    storage = node["checkpoint_storage"]
    if set(storage) != {
        "kind",
        "alias",
        "bucket",
        "region",
        "endpoint",
        "allow_http",
        "prefix",
        "root",
        "cache_budget_bytes",
    }:
        raise ValueError("Invalid checkpoint storage fields; credentials must come from a Secret")
    endpoint = urlsplit(storage["endpoint"])
    if (
        storage["kind"] != "s3"
        or storage["allow_http"] is not False
        or endpoint.scheme != "https"
        or not endpoint.hostname
        or endpoint.username
        or endpoint.password
        or endpoint.path not in ("", "/")
        or endpoint.query
        or endpoint.fragment
    ):
        raise ValueError("Checkpoint storage requires an HTTPS S3 endpoint")
    if not re.fullmatch(r"[a-zA-Z0-9-]+", storage["alias"]) or storage["alias"] == "local":
        raise ValueError("Invalid checkpoint storage alias")
    if not re.fullmatch(r"[a-z0-9][a-z0-9.-]{1,61}[a-z0-9]", storage["bucket"]) or not re.fullmatch(
        r"[a-z0-9-]+", storage["region"]
    ):
        raise ValueError("Invalid checkpoint bucket or region")
    # This CCE deployment uses OBS, which requires virtual-hosted bucket access.
    # object_store expects a custom endpoint to include the bucket in this mode.
    expected_host = f"{storage['bucket']}.obs.{storage['region']}.myhuaweicloud.com"
    if endpoint.netloc != expected_host:
        raise ValueError("OBS endpoint must contain the configured bucket and region")
    if not isinstance(storage["prefix"], str) or not storage["prefix"] or not absolute_path("/" + storage["prefix"]):
        raise ValueError("Checkpoint prefix must be a nonempty relative path")
    if not absolute_path(storage["root"]) or not storage["root"].startswith("/work/"):
        raise ValueError("Checkpoint staging/cache must stay on the private /work volume")
    if type(storage["cache_budget_bytes"]) is not int or storage["cache_budget_bytes"] <= 0:
        raise ValueError("Checkpoint cache budget must be positive")
    sfs = node["sfs"]
    if set(sfs) != {
        "share_id",
        "server",
        "export_path",
        "mount_path",
        "claim_size",
        "mount_options",
    }:
        raise ValueError("Invalid SFS configuration fields")
    if not isinstance(sfs["claim_size"], str) or not re.fullmatch(r"[1-9][0-9]*(?:[KMGTP]i?)?", sfs["claim_size"]):
        raise ValueError("SFS claim size must be a positive storage quantity")
    options = sfs["mount_options"]
    required = {"vers=3", "hard", "proto=tcp", "lookupcache=pos"}
    if (
        not isinstance(options, list)
        or any(not isinstance(v, str) for v in options)
        or len(options) != len(set(options))
        or not required.issubset(options)
        or not set(options).issubset(required | {"noresvport"})
    ):
        raise ValueError("SFS requires NFSv3 hard TCP and lookupcache=pos mount options")
    try:
        uuid.UUID(sfs["share_id"])
    except (ValueError, TypeError, AttributeError):
        raise ValueError("Invalid SFS share ID") from None
    if not isinstance(sfs["server"], str) or not re.fullmatch(
        r"[a-zA-Z0-9](?:[a-zA-Z0-9.-]*[a-zA-Z0-9])?", sfs["server"]
    ):
        raise ValueError("SFS server must be a hostname or IPv4 address without a path")
    if not absolute_path(sfs["export_path"]):
        raise ValueError("SFS export path must be absolute without traversal")
    if not absolute_path(sfs["mount_path"]) or not sfs["mount_path"].startswith("/mnt/"):
        raise ValueError("SFS mount must be a dedicated path under /mnt")


def verify_sfs_mount(sfs):
    """Fail before sandboxd starts if Kubernetes did not provide the intended rw NFS mount."""
    target = sfs["mount_path"]
    if str(Path(target).resolve()) != target:
        raise RuntimeError("SFS mount target must not traverse a symlink")
    result = subprocess.run(
        [
            require_executable("findmnt"),
            "--json",
            "--mountpoint",
            target,
            "--output",
            "TARGET,FSTYPE,SOURCE,OPTIONS",
        ],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
    )
    if result.returncode:
        raise RuntimeError("SFS mount is missing; refusing a local-directory fallback")
    try:
        rows = json.loads(result.stdout)["filesystems"]
        mount = rows[0] if len(rows) == 1 else {}
        options = set(mount.get("options", "").split(","))
        expected = sfs["server"] + ":" + sfs["export_path"]
        valid = (
            mount.get("target") == target
            and mount.get("fstype") in ("nfs", "nfs4")
            and mount.get("source") == expected
            and "rw" in options
            and {"hard", "vers=3", "proto=tcp"}.issubset(options)
            and bool(options.intersection({"lookupcache=pos", "lookupcache=positive"}))
            and not options.intersection(
                {
                    "ro",
                    "soft",
                    "softerr",
                    "softreval",
                    "lookupcache=all",
                    "lookupcache=none",
                }
            )
        )
    except (ValueError, KeyError, TypeError):
        valid = False
    if not valid:
        raise RuntimeError("SFS mount requires configured source and rw/hard NFSv3 TCP with lookupcache=pos")


def checkpoint_environment(directory):
    """Read only at startup; values are passed to the supervisor, not written into its config."""
    # AmazonS3Builder::from_env reads this; only the supervisor/adxlet receives it.
    # validate_storage enforces the matching OBS bucket endpoint.
    result = {"AWS_VIRTUAL_HOSTED_STYLE_REQUEST": "true"}
    for key in OBS_KEYS:
        value = (Path(directory) / key).read_text().strip()
        if not value or any(ord(c) < 32 or ord(c) == 127 for c in value):
            raise ValueError("Invalid OBS credential file: " + key)
        result[key] = value
    return result


def runtime_configuration(node, pod_ip):
    """Configure explicit stream/volume capabilities after verifying the SFS identity."""
    import ipaddress

    result = {}
    if "runtime_control_port" in node:
        port = node["runtime_control_port"]
        if type(port) is not int or not 1 <= port <= 65535 or port in (17001, 17091, 18081, 18443, 19443, 16061):
            raise ValueError("Invalid or conflicting runtime control port")
        if ipaddress.ip_address(pod_ip).version != 4:
            raise ValueError("Runtime control requires a Pod IPv4 address")
        result["runtime_control"] = {
            "listen": "0.0.0.0:" + str(port),
            "advertised_address": "http://" + pod_ip + ":" + str(port),
            "key_file": "/work/runtime-control.key",
        }
    mounts = node.get("storage_mounts")
    if mounts is not None:
        if not isinstance(mounts, dict) or not mounts or "runtime_control_port" not in node:
            raise ValueError("Managed storage requires runtime control and a nonempty mount map")
        expected = {
            "backend_id": node["sfs"]["server"] + ":" + node["sfs"]["export_path"],
            "root": node["sfs"]["mount_path"],
        }
        for domain, mount in mounts.items():
            if not isinstance(domain, str) or not re.fullmatch(r"[A-Za-z0-9_-]+", domain) or mount != expected:
                raise ValueError("Managed storage must match the verified SFS backend and root")
        result["storage_mounts"] = mounts
    return result


def validate_domains(config):
    """Both Coordinator and Node must describe the same backend and tenant boundary."""
    node = config["node"]
    runtime_configuration(node, "127.0.0.1")
    mounts = node.get("storage_mounts", {})
    domains = config.get("storage_domains", {})
    if not isinstance(domains, dict) or set(domains) != set(mounts):
        raise ValueError("Storage domains and mounted domains must agree")
    for name, domain in domains.items():
        if (
            set(domain) != {"backend_id", "dynamic_nodes", "node_roots", "tenant_paths"}
            or domain["backend_id"] != mounts[name]["backend_id"]
            or domain["dynamic_nodes"] is not True
            or domain["node_roots"] != {}
            or domain["tenant_paths"] != {config["tenant"]: config["namespace"] + "/workspaces"}
        ):
            raise ValueError("Storage domain must use verified dynamic nodes and the environment tenant prefix")
