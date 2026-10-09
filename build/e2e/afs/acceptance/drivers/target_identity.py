"""Fail-closed suite target checks; labels cannot substitute for observed identity."""
from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any
from urllib.parse import urlparse


def mount_record(command: dict[str, Any]) -> dict[str, Any]:
    if command.get("returncode") != 0:
        return {}
    try:
        records = json.loads(command.get("stdout", "")).get("filesystems", [])
        return records[0] if len(records) == 1 else {}
    except (ValueError, TypeError, AttributeError):
        return {}


def _is_sha256(value: Any) -> bool:
    return isinstance(value, str) and len(value) == 64 and all(char in "0123456789abcdef" for char in value)


def process_matches(identity: dict[str, Any] | None, executable: str) -> bool:
    if not identity or identity.get("exists") is not True:
        return False
    digest = identity.get("exe_sha256")
    return (
        Path(str(identity.get("exe", ""))).name == executable
        and _is_sha256(digest)
        and bool(identity.get("cmdline"))
    )


def target_checks(
    system: str,
    backend: str | None,
    mount: dict[str, Any],
    base_mount: dict[str, Any],
    node: dict[str, Any] | None,
    meta: dict[str, Any] | None,
) -> dict[str, bool]:
    record = mount_record(mount)
    base = mount_record(base_mount)
    label = str(backend or "").lower()
    expected = {"ownerfs": "afs-ownerfs", "dfs": "afs-dfs"}.get(label)
    reference = label in {"reference", "ext4"}
    target_ok = (
        record.get("fstype") == "ext4"
        if reference
        else expected is not None
        and str(record.get("fstype", "")).startswith("fuse")
        and record.get("source") == expected
    )
    return {
        "linux-runtime": system == "Linux",
        "observed-target-backend": bool(record) and bool(target_ok),
        "same-fixture-filesystem": bool(base) and all(
            base.get(key) == record.get(key) for key in ("target", "source", "fstype")
        ),
        "product-process-identity": reference
        or (
            process_matches(node, "afs-node")
            and (
                process_matches(meta, "afs-meta")
                or os.environ.get("AFS_ACCEPTANCE_REMOTE_HOST_QUALIFIED") == "1"
            )
        ),
    }


def _expected_source(backend: str | None) -> str | None:
    return {"ownerfs": "afs-ownerfs", "dfs": "afs-dfs"}.get(str(backend or "").lower())


def _endpoint_port(endpoint: str | None) -> int | None:
    if not endpoint:
        return None
    parsed = urlparse(endpoint)
    if parsed.scheme and parsed.port is not None:
        return parsed.port
    # Accept bare host:port in config summaries if a future template emits it.
    text = endpoint.rsplit(":", 1)
    if len(text) == 2 and text[1].isdigit():
        return int(text[1])
    return None


def strict_process_identity(identity: dict[str, Any] | None, role: str, executable: str, expected_sha256: str) -> bool:
    if not identity or identity.get("exists") is not True:
        return False
    cfg = identity.get("config") or {}
    return (
        identity.get("role") == role
        and Path(str(identity.get("exe_path", ""))).name == executable
        and identity.get("sha256") == expected_sha256
        and identity.get("expected_sha256") == expected_sha256
        and identity.get("sha256_ok") is True
        and isinstance(identity.get("pid"), int)
        and isinstance(identity.get("start_ticks"), int)
        and bool(identity.get("boot_id"))
        and bool(identity.get("machine_id"))
        and isinstance(identity.get("exe_dev"), int)
        and isinstance(identity.get("exe_inode"), int)
        and _is_sha256(cfg.get("sha256"))
    )


def process_stable(before: dict[str, Any] | None, after: dict[str, Any] | None) -> bool:
    if not before or not after:
        return False
    keys = ("role", "pid", "boot_id", "machine_id", "start_ticks", "exe_dev", "exe_inode", "sha256", "expected_sha256")
    if any(before.get(key) != after.get(key) for key in keys):
        return False
    return (before.get("config") or {}).get("sha256") == (after.get("config") or {}).get("sha256")


def _endpoint_host(endpoint: str | None) -> str | None:
    if not endpoint:
        return None
    parsed = urlparse(endpoint)
    if parsed.scheme:
        return parsed.hostname
    if endpoint.count(":") == 1:
        return endpoint.rsplit(":", 1)[0]
    return None


def _is_wildcard_or_loopback(host: str | None) -> bool:
    return host in {None, "", "0.0.0.0", "::", "localhost", "127.0.0.1", "::1"}


def _digest_map_stable(before_items: dict[str, Any], after_items: dict[str, Any]) -> bool:
    if set(before_items) != set(after_items):
        return False
    for name, before_entry in before_items.items():
        after_entry = after_items.get(name) or {}
        before_entry = before_entry or {}
        if before_entry.get("exists") is not True or after_entry.get("exists") is not True:
            return False
        if not _is_sha256(before_entry.get("sha256")) or before_entry.get("sha256") != after_entry.get("sha256"):
            return False
        if not before_entry.get("path") or before_entry.get("path") != after_entry.get("path"):
            return False
    return True


def _configured_tls_digests_stable(before_cfg: dict[str, Any], after_cfg: dict[str, Any]) -> bool:
    required_names = {"tls_ca_certificate", "tls_identity_certificate", "tls_identity_private_key"}
    before_tls = before_cfg.get("tls") or {}
    after_tls = after_cfg.get("tls") or {}
    before_trusted = before_cfg.get("trusted_node_certs") or {}
    after_trusted = after_cfg.get("trusted_node_certs") or {}
    tls_required = bool(before_cfg.get("tls_required") or after_cfg.get("tls_required") or before_tls or after_tls or before_trusted or after_trusted)
    if tls_required:
        if set(before_cfg.get("tls_missing") or []) or set(after_cfg.get("tls_missing") or []):
            return False
        if not required_names.issubset(set(before_tls)) or not required_names.issubset(set(after_tls)):
            return False
    if not _digest_map_stable(before_tls, after_tls):
        return False
    if not _digest_map_stable(before_trusted, after_trusted):
        return False
    return True


def _meta_endpoint_binding(meta_identity: dict[str, Any] | None, expected_endpoint: str, require_cross_worker: bool) -> bool:
    if not meta_identity:
        return False
    host = _endpoint_host(expected_endpoint)
    port = _endpoint_port(expected_endpoint)
    if host is None or port is None:
        return False
    if require_cross_worker and _is_wildcard_or_loopback(host):
        return False
    network = meta_identity.get("network") or {}
    interface_ips = set(network.get("interface_ips") or [])
    if host not in interface_ips:
        return False
    cfg = meta_identity.get("config") or {}
    listen = cfg.get("grpc_listen")
    listen_host = _endpoint_host(listen)
    listen_port = _endpoint_port(listen)
    if listen_port != port:
        return False
    if listen_host and not _is_wildcard_or_loopback(listen_host) and listen_host != host:
        return False
    for sock in network.get("listen_sockets") or []:
        if sock.get("port") != port:
            continue
        sock_host = sock.get("ip")
        if sock_host == host or _is_wildcard_or_loopback(sock_host):
            return True
    return False


def remote_target_checks(
    system: str,
    backend: str | None,
    mount: dict[str, Any],
    base_mount: dict[str, Any],
    node_before: dict[str, Any] | None,
    node_after: dict[str, Any] | None,
    meta_before: dict[str, Any] | None,
    meta_after: dict[str, Any] | None,
    expected_node_sha256: str,
    expected_meta_sha256: str,
    expected_meta_endpoint: str,
    require_cross_worker: bool = True,
) -> dict[str, bool]:
    record = mount_record(mount)
    base = mount_record(base_mount)
    expected = _expected_source(backend)
    node_cfg = (node_before or {}).get("config") or {}
    meta_cfg = (meta_before or {}).get("config") or {}
    node_after_cfg = (node_after or {}).get("config") or {}
    meta_after_cfg = (meta_after or {}).get("config") or {}
    node_endpoint = node_cfg.get("meta_endpoint")
    node_boot = (node_before or {}).get("boot_id")
    meta_boot = (meta_before or {}).get("boot_id")
    node_machine = (node_before or {}).get("machine_id")
    meta_machine = (meta_before or {}).get("machine_id")
    node_pre_ok = strict_process_identity(node_before, "node", "afs-node", expected_node_sha256)
    node_post_ok = strict_process_identity(node_after, "node", "afs-node", expected_node_sha256)
    meta_pre_ok = strict_process_identity(meta_before, "meta", "afs-meta", expected_meta_sha256)
    meta_post_ok = strict_process_identity(meta_after, "meta", "afs-meta", expected_meta_sha256)
    return {
        "linux-runtime": system == "Linux",
        "observed-target-backend": expected is not None and bool(record) and str(record.get("fstype", "")).startswith("fuse") and record.get("source") == expected,
        "same-fixture-filesystem": bool(base) and all(base.get(key) == record.get(key) for key in ("target", "source", "fstype")),
        "node-process-identity": node_pre_ok and node_post_ok,
        "meta-process-identity": meta_pre_ok and meta_post_ok,
        "node-process-stable": process_stable(node_before, node_after),
        "meta-process-stable": process_stable(meta_before, meta_after),
        "node-meta-endpoint-bound": bool(expected_meta_endpoint) and node_endpoint == expected_meta_endpoint,
        "meta-listen-port-bound": _meta_endpoint_binding(meta_before, expected_meta_endpoint, require_cross_worker) and _meta_endpoint_binding(meta_after, expected_meta_endpoint, require_cross_worker),
        "tls-digests-recorded": _configured_tls_digests_stable(node_cfg, node_after_cfg) and _configured_tls_digests_stable(meta_cfg, meta_after_cfg),
        "cross-worker-identity": (bool(node_boot) and bool(meta_boot) and bool(node_machine) and bool(meta_machine) and (not require_cross_worker or (node_boot != meta_boot and node_machine != meta_machine))),
    }
