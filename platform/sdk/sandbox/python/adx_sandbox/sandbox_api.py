"""Sandbox API for adx, backed by frontend sandbox v1 and RRT.

Sandbox lifecycle is server-side and reached through the frontend HTTP control
plane. Commands, filesystem operations, shell sessions, direct file transfer,
and reverse tunnel helpers are exposed as Python objects on ``Sandbox``.
"""

import logging
import os
import random
import re
from collections.abc import Mapping
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlparse

from ._transport import SandboxClient, SandboxError
from .commands import Commands
from .scheduling import encode_affinities, instance_labels
from .filesystem import Filesystem
from .pty import Pty
from .shell import Shells
from .types import (
    ADX_GET_DEFAULT_TIMEOUT,
    ConnectionConfig,
    DataPlaneSecurityPolicy,
    Mount,
    NetworkPolicy,
    PauseResult,
    PortForwarding,
    ResumeResult,
    RestartPolicy,
    S3Config,
    SandboxInfo,
    SnapshotInfo,
)

logger = logging.getLogger(__name__)

DEFAULT_SCHEDULE_TIMEOUT = 30
_INIT_CALL_TIMEOUT = 30
CREATE_TIMEOUT_BUFFER = 30
_CREATE_TIMEOUT_RESERVE = _INIT_CALL_TIMEOUT + CREATE_TIMEOUT_BUFFER
_SUPPORTED_XPU_TYPES = frozenset({"gpu", "npu"})
_SNAPSHOT_RESOURCE_FIELDS = frozenset(
    {"cpu", "memory", "cpu_limit", "mem_limit"}
)
_ENTRYPOINT_POLL_INTERVAL = 10.0


def _get_create_timeout(timeout: Optional[int]) -> int:
    if timeout is not None:
        value = timeout
    else:
        raw = os.environ["ADX_SANDBOX_CREATE_TIMEOUT"].strip()
        try:
            value = int(raw)
        except ValueError as exc:
            raise ValueError(
                "ADX_SANDBOX_CREATE_TIMEOUT must be an integer number of seconds"
            ) from exc
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError("create_timeout must be a positive integer")
    return value


def _resolve_create_timeouts(
    create_timeout: Optional[int], schedule_timeout: Optional[int]
) -> tuple[int, int]:
    if schedule_timeout is not None and (
        isinstance(schedule_timeout, bool)
        or not isinstance(schedule_timeout, int)
        or schedule_timeout <= 0
    ):
        raise ValueError("schedule_timeout must be a positive integer")

    resolved_schedule = (
        DEFAULT_SCHEDULE_TIMEOUT
        if schedule_timeout is None
        else schedule_timeout
    )
    if create_timeout is None and "ADX_SANDBOX_CREATE_TIMEOUT" not in os.environ:
        resolved_create = resolved_schedule + _CREATE_TIMEOUT_RESERVE
    else:
        resolved_create = _get_create_timeout(create_timeout)

    if resolved_schedule > resolved_create:
        raise ValueError(
            "schedule_timeout must be less than or equal to create_timeout"
        )
    remaining_create_budget = resolved_create - resolved_schedule
    if remaining_create_budget < CREATE_TIMEOUT_BUFFER:
        raise ValueError(
            "create_timeout - schedule_timeout must be at least "
            f"{CREATE_TIMEOUT_BUFFER}"
        )
    if remaining_create_budget < _CREATE_TIMEOUT_RESERVE:
        resolved_create = resolved_schedule + _CREATE_TIMEOUT_RESERVE
    return resolved_create, resolved_schedule


def _get_tunnel_connect_timeout(timeout: Optional[float]) -> float:
    if timeout is not None:
        value = float(timeout)
    else:
        raw = os.environ.get("ADX_TUNNEL_CONNECT_TIMEOUT", "60")
        try:
            value = float(raw)
        except ValueError as e:
            raise ValueError(
                "ADX_TUNNEL_CONNECT_TIMEOUT must be a number of seconds"
            ) from e
    if value <= 0:
        raise ValueError("tunnel_connect_timeout must be greater than 0")
    return value


def _compose_gateway_url(*, gateway: str, scheme: str, path: str) -> str:
    """Compose a gateway URL from a frontend-returned path or URL.

    Frontend normally returns a path-only tunnel URL so deployments can choose
    the external gateway address locally. If the frontend returns a full URL,
    keep only its path; the SDK still owns the public gateway host and
    ws/wss scheme selection via ADX_GATEWAY_ADDRESS/ADX_GATEWAY_TLS.
    """
    if not gateway:
        raise ValueError("ADX_GATEWAY_ADDRESS or ADX_SERVER_ADDRESS must be set")
    parsed = urlparse(path)
    route = parsed.path or path
    if parsed.query:
        route = f"{route}?{parsed.query}"
    if not route.startswith("/"):
        route = f"/{route}"
    return f"{scheme}://{gateway}{route}"


def _gateway_address(connection: Optional[ConnectionConfig]) -> str:
    if connection is not None:
        return connection.gateway_address or connection.server_address
    gateway = os.environ.get("ADX_GATEWAY_ADDRESS", "").strip()
    if not gateway:
        gateway = os.environ.get("ADX_SERVER_ADDRESS", "").strip()
    if not gateway:
        raise ValueError("ADX_GATEWAY_ADDRESS or ADX_SERVER_ADDRESS must be set")
    return gateway


def _gateway_uses_tls(connection: Optional[ConnectionConfig]) -> bool:
    if connection is not None:
        return (
            connection.gateway_use_tls
            if connection.gateway_address is not None
            else connection.use_tls
        )
    gateway = os.environ.get("ADX_GATEWAY_ADDRESS", "").strip()
    flag = "ADX_GATEWAY_TLS" if gateway else "ADX_TLS"
    default = "0" if gateway else "1"
    return os.environ.get(flag, default).strip().lower() in (
        "1",
        "true",
        "yes",
    )


def _validate_xpu(xpu: Optional[str]) -> None:
    if xpu is None:
        return
    if not isinstance(xpu, str):
        raise TypeError("xpu must be a string or None")

    fields = xpu.split(":")
    if len(fields) != 3 or not fields[0] or not fields[2]:
        raise ValueError("xpu must have exactly three fields: type:model:count")
    if any(field != field.strip() for field in fields):
        raise ValueError("xpu fields must not contain surrounding whitespace")

    xpu_type, _, count_text = fields
    xpu_type = xpu_type.lower()
    if xpu_type not in _SUPPORTED_XPU_TYPES:
        raise ValueError(f"unsupported xpu type: {xpu_type}")
    if re.fullmatch(r"[0-9]+", count_text) is None:
        raise ValueError("xpu count must be a positive integer")
    count = int(count_text)
    if count <= 0:
        raise ValueError("xpu count must be a positive integer")


class Sandbox:
    """High-level sandbox API for adx sandboxes.

    Usage::

        with Sandbox(image="python:3.12-slim", cpu=2000, memory=4096) as sb:
            sb.files.write("/tmp/hello.txt", "hello world")
            result = sb.commands.run("cat /tmp/hello.txt")
            print(result.stdout)

            sh = await sb.shells.create(cwd="/tmp")
            await sh.run("export FOO=bar")
            result = await sh.run("echo $FOO")  # → bar
    """

    @classmethod
    def create(
        cls,
        snapshot_id: Union[str, SnapshotInfo],
        **kwargs: Any,
    ) -> "Sandbox":
        """Create a new sandbox by restoring a READY reusable Snapshot.

        Resource fields omitted from ``kwargs`` are inherited from the
        Snapshot. Explicit resource fields must be compatible with its runtime;
        the current Firecracker backend requires the original resource geometry.
        """
        value = (
            snapshot_id.snapshot_id
            if isinstance(snapshot_id, SnapshotInfo)
            else snapshot_id
        )
        explicit_resource_fields = frozenset(kwargs).intersection(
            _SNAPSHOT_RESOURCE_FIELDS
        )
        return cls(
            snapshot_id=value,
            _snapshot_resource_fields=explicit_resource_fields,
            **kwargs,
        )

    @staticmethod
    def _snapshot_info(payload: Mapping[str, Any]) -> SnapshotInfo:
        snapshot_id = str(payload.get("snapshotId") or "")
        names = payload.get("names") or []
        if (
            not snapshot_id
            or not isinstance(names, list)
            or not all(isinstance(name, str) for name in names)
        ):
            raise SandboxError("invalid reusable Snapshot response")
        return SnapshotInfo(snapshot_id=snapshot_id, names=tuple(names))

    @staticmethod
    def _get_snapshot(client: Any, snapshot_id: str) -> SnapshotInfo:
        return Sandbox._snapshot_info(client.get_snapshot(snapshot_id))

    @staticmethod
    def _list_snapshots(
        client: Any,
        *,
        name: Optional[str] = None,
        page_token: Optional[str] = None,
        page_size: Optional[int] = None,
    ) -> Tuple[List[SnapshotInfo], str]:
        payload = client.list_snapshots(
            name=name,
            page_token=page_token,
            page_size=page_size,
        )
        snapshots = payload.get("items") or []
        if not isinstance(snapshots, list):
            raise SandboxError("invalid reusable Snapshot list response")
        return (
            [Sandbox._snapshot_info(item) for item in snapshots],
            str(payload.get("nextPageToken") or ""),
        )

    @staticmethod
    def _delete_snapshot(client: Any, snapshot_id: str) -> None:
        client.delete_snapshot(snapshot_id)

    @classmethod
    def get_snapshot(cls, snapshot_id: str, *, connection: Optional[ConnectionConfig] = None) -> SnapshotInfo:
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            raise ValueError("snapshot_id must be a non-empty string")
        client = SandboxClient(connection=connection) if connection is not None else SandboxClient()
        try:
            return cls._get_snapshot(client, snapshot_id.strip())
        finally:
            client.close()

    @classmethod
    def list_snapshots(
        cls,
        *,
        name: Optional[str] = None,
        page_token: Optional[str] = None,
        page_size: Optional[int] = None,
        connection: Optional[ConnectionConfig] = None,
    ) -> Tuple[List[SnapshotInfo], str]:
        if name is not None and (
            not isinstance(name, str) or not name.strip()
        ):
            raise ValueError("name must be a non-empty string or None")
        if page_token is not None and not isinstance(page_token, str):
            raise TypeError("page_token must be a string or None")
        if page_size is not None:
            if isinstance(page_size, bool) or not isinstance(page_size, int):
                raise TypeError("page_size must be an integer or None")
            if page_size <= 0:
                raise ValueError("page_size must be greater than 0")
        client = SandboxClient(connection=connection) if connection is not None else SandboxClient()
        try:
            return cls._list_snapshots(
                client,
                name=name.strip() if name is not None else None,
                page_token=page_token,
                page_size=page_size,
            )
        finally:
            client.close()

    @classmethod
    def delete_snapshot(cls, snapshot_id: str, *, connection: Optional[ConnectionConfig] = None) -> None:
        if not isinstance(snapshot_id, str) or not snapshot_id.strip():
            raise ValueError("snapshot_id must be a non-empty string")
        client = SandboxClient(connection=connection) if connection is not None else SandboxClient()
        try:
            cls._delete_snapshot(client, snapshot_id.strip())
        finally:
            client.close()

    def __init__(
        self,
        image: Optional[str] = None,
        rootfs: Optional[S3Config] = None,
        runtime: Optional[str] = None,
        cpu: int = 1000,
        memory: int = 4096,
        cpu_limit: int = 0,
        mem_limit: int = 0,
        idle_timeout: int = 300,
        schedule_timeout: int = 30,
        env: Optional[Dict[str, str]] = None,
        name: Optional[str] = None,
        cwd: Optional[str] = None,
        port_forwardings: Optional[List[Union[int, PortForwarding]]] = None,
        mounts: Optional[List[Mount]] = None,
        upstream: Optional[str] = None,
        proxy_port: int = 8766,
        tunnel_connect_timeout: Optional[float] = None,
        detached: bool = False,
        node_id: Optional[str] = None,
        *,
        rootfs_readonly: Optional[bool] = None,
        labels: Optional[Dict[str, str]] = None,
        schedule_affinities: Optional[List[Dict[str, Any]]] = None,
        snapshot_id: Optional[str] = None,
        restart_policy: Optional[RestartPolicy] = None,
        failover: bool = False,
        inherit_entrypoint: bool = False,
        xpu: Optional[str] = None,
        storage_mb: Optional[int] = None,
        storage_limit_mb: int = 0,
        network: Optional[NetworkPolicy] = None,
        data_plane_security: Optional[DataPlaneSecurityPolicy] = None,
        create_timeout: Optional[int] = None,
        connection: Optional[ConnectionConfig] = None,
        extra_config: Optional[Dict[str, Any]] = None,
        _snapshot_resource_fields: Optional[frozenset[str]] = None,
    ):
        """Create a new sandbox.

        Args:
            image: Container image to use (e.g. ``"python:3.12-slim"``).
            rootfs: S3-compatible EROFS root filesystem configuration.
            rootfs_readonly: Optional override for the deployment rootfs
                ``readonly`` default. Omitting it inherits the deployment
                setting, including when replacing the source with *image* or
                *rootfs*.
            runtime: Sandbox isolation runtime identifier. Defaults to
                ``runsc`` and is validated by the runtime layer.
            cpu: CPU scheduling request in milli-cores (default 1000).
            memory: Memory scheduling request in MB (default 4096).
            cpu_limit: CPU cgroup limit in milli-cores (0 = same as *cpu*).
            mem_limit: Memory cgroup limit in MB (0 = same as *memory*).
            idle_timeout: Seconds before idle sandbox is reclaimed (default 300).
            restart_policy: Optional bounded node-local restart after an unexpected exit.
            create_timeout: Logical create budget in seconds. By default it is
                derived as ``schedule_timeout + 60``. An
                ``ADX_SANDBOX_CREATE_TIMEOUT`` value or a legacy explicit pair
                that leaves the former 30-second response buffer remains
                accepted; the SDK expands its effective create budget to also
                cover runtime initialization.
            schedule_timeout: Scheduling budget in seconds (default 30).
            env: Environment variables to set in the sandbox.
            name: Logical name for the sandbox instance.
            cwd: Working directory inside the sandbox.
            port_forwardings: Ports to forward from the sandbox. Each entry is
                a port number (defaults to TCP) or a ``PortForwarding`` object.
            mounts: Custom mount specifications for the sandbox.
            upstream: ``host:port`` or HTTP(S) URL of an SDK-side service to
                expose inside the sandbox.
            proxy_port: Sandbox-side HTTP proxy port for ``upstream``. The
                frontend derives the WebSocket tunnel port as
                ``proxy_port - 1``. Defaults to 8766.
            tunnel_connect_timeout: Seconds to wait for the tunnel WebSocket.
            detached: If True, ``kill()`` / context-manager exit skips teardown.
            xpu: Optional whole-device XPU request in ``type:model:count``
                format. Leave ``model`` empty to accept any model. The first
                version supports one ``gpu`` or ``npu`` request.
            storage_mb: Temporary writable root filesystem capacity in MiB.
                ``None`` uses the cluster default.
            storage_limit_mb: Writable root filesystem hard limit in MiB. ``0``
                uses *storage_mb* (or the cluster default when *storage_mb* is
                omitted).
            failover: Restore this sandbox on the same node from its latest
                local anonymous checkpoint after a sandbox failure.
            inherit_entrypoint: Start the effective ``Entrypoint`` and ``Cmd``
                configured by an image and manage it as the sandbox workload.
                This option is valid only for image-backed fresh creates.
            network: Optional creation-time network policy. Omitting it allows
                unrestricted network access.
            connection: Explicit frontend and gateway connection settings.
                Omitting it reads the existing ``ADX_*`` environment variables.
            data_plane_security: Optional per-sandbox ``tls`` or ``tls-token``
                policy for tunnel and port-forwarding. Unset fields inherit
                server defaults.
            extra_config: Extra sandbox-side configuration forwarded to sandboxd.
        """
        if image is not None and (
            not isinstance(image, str) or not image.strip()
        ):
            raise ValueError("image must be a non-empty string")
        if rootfs is not None and not isinstance(rootfs, S3Config):
            raise TypeError("rootfs must be an S3Config")
        if image is not None and rootfs is not None:
            raise ValueError("image and rootfs are mutually exclusive")
        if rootfs_readonly is not None and not isinstance(rootfs_readonly, bool):
            raise TypeError("rootfs_readonly must be a boolean or None")
        if snapshot_id is not None and (
            not isinstance(snapshot_id, str) or not snapshot_id.strip()
        ):
            raise ValueError("snapshot_id must be a non-empty string")
        if not isinstance(failover, bool):
            raise TypeError("failover must be a boolean")
        if not isinstance(inherit_entrypoint, bool):
            raise TypeError("inherit_entrypoint must be a boolean")
        if inherit_entrypoint and image is None:
            raise ValueError("inherit_entrypoint requires an image rootfs")
        if inherit_entrypoint and snapshot_id is not None:
            raise ValueError("inherit_entrypoint cannot be combined with snapshot_id")
        if snapshot_id is not None and rootfs_readonly is not None:
            raise ValueError("rootfs_readonly cannot be combined with snapshot_id")
        if env is not None and (
            not isinstance(env, Mapping)
            or not all(
                isinstance(key, str) and isinstance(value, str)
                for key, value in env.items()
            )
        ):
            raise TypeError("env must map strings to strings")
        if name is not None and (
            not isinstance(name, str) or not name.strip()
        ):
            raise ValueError("name must be a non-empty string")
        if cwd is not None:
            if not isinstance(cwd, str):
                raise TypeError("cwd must be a string")
            if not cwd.startswith("/"):
                raise ValueError("cwd must be an absolute POSIX path")
        if not isinstance(detached, bool):
            raise TypeError("detached must be a boolean")
        if node_id is not None:
            if not isinstance(node_id, str):
                raise TypeError("node_id must be a string")
            if not node_id:
                raise ValueError("node_id cannot be empty string")
        _validate_xpu(xpu)
        if storage_mb is not None:
            if isinstance(storage_mb, bool) or not isinstance(storage_mb, int):
                raise TypeError("storage_mb must be an integer or None")
            if storage_mb <= 0:
                raise ValueError("storage_mb must be greater than 0")
        if isinstance(storage_limit_mb, bool) or not isinstance(
            storage_limit_mb, int
        ):
            raise TypeError("storage_limit_mb must be an integer")
        if storage_limit_mb < 0:
            raise ValueError("storage_limit_mb must be 0 or greater")
        if storage_mb is not None and 0 < storage_limit_mb < storage_mb:
            raise ValueError(
                "storage_limit_mb must be greater than or equal to storage_mb"
            )
        if network is not None and not isinstance(network, NetworkPolicy):
            raise TypeError("network must be a NetworkPolicy or None")
        if connection is not None and not isinstance(connection, ConnectionConfig):
            raise TypeError("connection must be a ConnectionConfig or None")
        if data_plane_security is not None and not isinstance(
            data_plane_security, DataPlaneSecurityPolicy
        ):
            raise TypeError("data_plane_security must be a DataPlaneSecurityPolicy or None")
        if mounts is None:
            mount_list: List[Mount] = []
        else:
            if isinstance(mounts, (str, bytes)):
                raise TypeError("mounts must be a sequence of Mount objects")
            mount_list = list(mounts)
            if not all(isinstance(mount, Mount) for mount in mount_list):
                raise TypeError("mounts must contain only Mount objects")
        if upstream is not None and (
            not isinstance(upstream, str) or not upstream.strip()
        ):
            raise ValueError("upstream must be a non-empty address")
        if isinstance(proxy_port, bool) or not isinstance(proxy_port, int):
            raise TypeError("proxy_port must be an integer")
        if not 2 <= proxy_port <= 65535:
            raise ValueError("proxy_port must be between 2 and 65535")

        # ── port_forwardings ──────────────────────────────────────────────
        self._forwarded_ports: set = set()
        pf_ports: List[str] = []
        if port_forwardings:
            if isinstance(port_forwardings, (str, bytes)):
                raise TypeError(
                    "port_forwardings must contain integers or PortForwarding objects"
                )
            ports: List[int] = []
            for forwarding in port_forwardings:
                if isinstance(forwarding, PortForwarding):
                    port = forwarding.port
                else:
                    port = forwarding
                if isinstance(port, bool) or not isinstance(port, int):
                    raise TypeError(
                        "forwarded port must be an integer or PortForwarding object"
                    )
                if not 1 <= port <= 65535:
                    raise ValueError("forwarded port must be between 1 and 65535")
                ports.append(port)
            if len(set(ports)) != len(ports):
                raise ValueError("port_forwardings must not contain duplicate ports")
            self._forwarded_ports.update(ports)
            pf_ports.extend(str(port) for port in ports)
        if upstream is not None:
            conflicts = self._forwarded_ports.intersection(
                {proxy_port - 1, proxy_port}
            )
            if conflicts:
                rendered = ", ".join(str(port) for port in sorted(conflicts))
                raise ValueError(
                    "reverse tunnel ports conflict with port_forwardings: "
                    f"{rendered}"
                )

        # ── reverse tunnel ────────────────────────────────────────────────
        self._tunnel_client = None
        self._tunnel_url = f"http://127.0.0.1:{proxy_port}"
        self._closed = False
        self._upstream = upstream

        # ── build create body ─────────────────────────────────────────────
        resolved_create_timeout, resolved_schedule_timeout = _resolve_create_timeouts(
            create_timeout, schedule_timeout
        )
        body: Dict[str, Any] = {
            "namespace": "default",
            "snapshotId": snapshot_id.strip() if snapshot_id is not None else None,
            "failover": failover,
            "idleTimeoutSeconds": idle_timeout,
            "createTimeoutSeconds": resolved_create_timeout,
            "scheduleTimeoutSeconds": resolved_schedule_timeout,
            "initCallTimeoutSeconds": _INIT_CALL_TIMEOUT,
            "rootfs": ({"runtime": runtime} if runtime is not None else {}),
        }
        if body["snapshotId"] is None:
            del body["snapshotId"]
        if restart_policy is not None:
            if not isinstance(restart_policy, RestartPolicy):
                raise TypeError("restart_policy must be a RestartPolicy")
            body["restartPolicy"] = restart_policy.to_dict()
        if inherit_entrypoint:
            body["inheritEntrypoint"] = True
        if image:
            body["rootfs"].update(
                {
                    "type": "image",
                    "imageurl": image,
                }
            )
        elif rootfs:
            body["rootfs"].update(
                {
                    "type": "s3",
                    "storageInfo": rootfs.to_dict(),
                }
            )
        if rootfs_readonly is not None:
            body["rootfs"]["readonly"] = rootfs_readonly
        if name:
            body["name"] = name
        resource_values = {
            "cpu": cpu,
            "memory": memory,
            "cpu_limit": cpu_limit,
            "mem_limit": mem_limit,
        }
        serialized_resource_fields = (
            _SNAPSHOT_RESOURCE_FIELDS
            if snapshot_id is None or _snapshot_resource_fields is None
            else _snapshot_resource_fields
        )
        for field in ("cpu", "memory", "cpu_limit", "mem_limit"):
            if field in serialized_resource_fields:
                body[field] = resource_values[field]
        if xpu is not None:
            body["xpu"] = xpu
        if storage_mb is not None:
            body["storageMb"] = storage_mb
        body["storage_limit_mb"] = storage_limit_mb
        if env:
            body["env"] = dict(env)
        placement = encode_affinities(schedule_affinities, node_id)
        if placement:
            body["scheduleAffinities"] = placement
        if labels is not None:
            body["labels"] = instance_labels(labels)
        if mount_list:
            body["mounts"] = [mount.to_dict() for mount in mount_list]
        if network is not None and not network.is_empty:
            body["network"] = network.to_dict()
        if data_plane_security is not None and data_plane_security.to_dict():
            body["dataPlane"] = data_plane_security.to_dict()
        if extra_config:
            body["extra_config"] = dict(extra_config)
        if detached:
            body["lifecycle"] = "detached"
        if upstream is not None:
            # Frontend derives the WebSocket port as proxyPort - 1, owns both
            # forwarded ports and RRT_TUNNEL_* env injection, then returns a
            # stable /tunnel/{safeID} URL path.
            body["tunnel"] = {"enabled": True, "proxyPort": proxy_port}

        self._detached = detached
        self._image = image
        self._cpu = cpu
        self._memory = memory
        self._cwd = cwd
        self._inherit_entrypoint = inherit_entrypoint
        self._entrypoint_exit_info: Optional[Dict[str, Any]] = None

        # ── ports: user port_forwardings only ─────────────────────────────
        # Frontend owns RRT_HTTP_PORT=50090 and its sandbox network mapping for
        # /direct. SDK callers should not expose that internal control port.
        if connection is None:
            self._client = SandboxClient(connection=connection) if connection is not None else SandboxClient()
        else:
            self._client = SandboxClient(connection=connection)
        self._connection = connection
        if pf_ports:
            body["ports"] = pf_ports

        self._sid = ""
        try:
            create_info = self._create(body)
            sandbox_id = create_info.get("sandboxId") or create_info.get("instanceId")
            if not isinstance(sandbox_id, str) or not sandbox_id:
                raise RuntimeError(
                    f"create response missing sandbox id: {create_info}"
                )
            self._sid = sandbox_id

            # ── reverse tunnel: connect after sandbox is running ──────────
            if upstream is not None:
                # Build the tunnel WebSocket URL via the sandbox gateway.
                # The route owns the internal tunnel control-port mapping.
                gateway = _gateway_address(self._connection)

                tunnel_info = create_info.get("tunnel") or {}
                if not isinstance(tunnel_info, dict):
                    tunnel_info = {}
                self._tunnel_url = (
                    tunnel_info.get("proxyUrl")
                    or f"http://127.0.0.1:{proxy_port}"
                )
                tunnel_url = tunnel_info.get("url") or tunnel_info.get("path")
                safe_id = self._client._safe_id(self._sid)
                tls = _gateway_uses_tls(self._connection)
                ws_scheme = "wss" if tls else "ws"
                tunnel_ws_url = _compose_gateway_url(
                    gateway=gateway,
                    scheme=ws_scheme,
                    path=tunnel_url or f"/tunnel/{safe_id}",
                )
                connect_timeout = _get_tunnel_connect_timeout(
                    tunnel_connect_timeout
                )

                from .tunnel_client import TunnelClient

                # Only carry the JWT over a TLS tunnel. Plaintext mode is for
                # auth-disabled local/dev frontends.
                tunnel_token = self._client.token if tls else None
                self._tunnel_client = TunnelClient(upstream, token=tunnel_token)
                logger.info(
                    "Starting TunnelClient: sandbox_id=%s name=%s url=%s "
                    "timeout=%.1fs",
                    safe_id,
                    name or "",
                    tunnel_ws_url,
                    connect_timeout,
                )
                if self._tunnel_client.start(
                    tunnel_ws_url, timeout=connect_timeout
                ):
                    logger.info(
                        "TunnelClient connected: sandbox_id=%s name=%s",
                        safe_id,
                        name or "",
                    )
                else:
                    self._tunnel_client.stop()
                    self._tunnel_client = None
                    raise RuntimeError(
                        "TunnelClient connection timeout after "
                        f"{connect_timeout:.1f}s: sandbox_id={safe_id} "
                        f"name={name or ''} url={tunnel_ws_url}. "
                        "The tunnel route may be missing or not ready."
                    )

            self._files = Filesystem(self._client, self._sid)
            self._commands = Commands(self._client, self._sid, default_cwd=self._cwd)
            self._shells = Shells(self._client, self._sid, default_cwd=self._cwd)
            self._pty = Pty(self._sid, connection=self._connection)
        except Exception:
            if self._tunnel_client is not None:
                try:
                    self._tunnel_client.stop()
                except Exception as cleanup_error:
                    logger.warning(
                        "tunnel rollback failed: sandbox_id=%s error=%s",
                        self._sid,
                        cleanup_error,
                    )
                self._tunnel_client = None
            try:
                if self._sid:
                    self._client.delete(self._sid)
            except Exception as cleanup_error:
                logger.warning(
                    "sandbox rollback failed: sandbox_id=%s error=%s",
                    self._sid,
                    cleanup_error,
                )
            try:
                self._client.close()
            except Exception as cleanup_error:
                logger.warning(
                    "client rollback failed: sandbox_id=%s error=%s",
                    self._sid,
                    cleanup_error,
                )
            self._closed = True
            raise

    @classmethod
    def from_id(
        cls,
        sandbox_id: str,
        *,
        connection: Optional[ConnectionConfig] = None,
    ) -> "Sandbox":
        """Reconstruct a local handle for an existing sandbox by ID.

        The returned object owns only its local clients. ``close()`` never
        deletes the remote sandbox; use :meth:`delete` for explicit teardown.
        """
        if not isinstance(sandbox_id, str) or not sandbox_id.strip():
            raise ValueError("sandbox_id must be a non-empty string")
        if connection is not None and not isinstance(connection, ConnectionConfig):
            raise TypeError("connection must be a ConnectionConfig or None")

        client = SandboxClient() if connection is None else SandboxClient(connection=connection)
        try:
            info = client.instance_info(sandbox_id)
            if str(info.get("status", "")).lower() != "running":
                raise RuntimeError(
                    f"sandbox {sandbox_id} is not running: {info.get('status', 'unknown')}"
                )
            sandbox = cls.__new__(cls)
            sandbox._sid = sandbox_id
            sandbox._client = client
            sandbox._connection = connection
            sandbox._detached = True
            sandbox._closed = False
            sandbox._image = info.get("image")
            sandbox._cpu = info.get("required_cpu")
            sandbox._memory = info.get("required_mem")
            sandbox._cwd = None
            sandbox._forwarded_ports = set()
            sandbox._tunnel_client = None
            sandbox._tunnel_url = ""
            sandbox._upstream = None
            sandbox._files = Filesystem(client, sandbox_id)
            sandbox._commands = Commands(client, sandbox_id)
            sandbox._shells = Shells(client, sandbox_id)
            sandbox._pty = Pty(sandbox_id, connection=connection)
            return sandbox
        except Exception:
            client.close()
            raise

    # ── sub-resources ──────────────────────────────────────────────────

    @property
    def files(self):
        return self._files

    @property
    def commands(self):
        return self._commands

    @property
    def shells(self):
        return self._shells

    @property
    def pty(self):
        return self._pty

    @property
    def id(self) -> str:
        """Sandbox ID assigned by the frontend."""
        return self._sid

    @property
    def sandbox_id(self) -> str:
        return self._sid

    # ── port forwarding ─────────────────────────────────────────────────

    def get_port_url(self, port: int) -> str:
        """Return the external URL to reach a forwarded port.

        URL format: ``http(s)://{gateway}/{sandbox_id}/{port}``. TLS is
        selected from the sandbox's resolved gateway connection settings.
        """
        if port not in self._forwarded_ports:
            raise ValueError(
                f"Port {port} is not in forwarded ports: {self._forwarded_ports}"
            )
        connection = getattr(self, "_connection", None)
        gateway = _gateway_address(connection)
        safe_id = self._client._safe_id(self._sid)
        if connection is not None:
            tls = _gateway_uses_tls(connection)
        elif os.environ.get("ADX_GATEWAY_ADDRESS", "").strip():
            tls = os.environ.get("ADX_GATEWAY_TLS", "0").strip().lower() in (
                "1",
                "true",
                "yes",
            )
        else:
            tls = os.environ.get("ADX_TLS", "1").strip().lower() in (
                "1",
                "true",
                "yes",
            )
        scheme = "https" if tls else "http"
        return f"{scheme}://{gateway}/{safe_id}/{port}"

    def get_port_auth_headers(self) -> Dict[str, str]:
        """Return gateway authentication headers compatible with Frontend and Edge.

        Deployments with anonymous port-forwarding can omit this header. When
        the gateway policy requires authentication, or when callers want tenant
        binding in optional mode, pass the returned mapping to the HTTP client.
        """
        token = self._client.token
        return {"Authorization": f"Bearer {token}", "X-Auth": token}

    # ── reverse tunnel ──────────────────────────────────────────────────

    def get_tunnel_url(self) -> str:
        """Return the internal HTTP proxy URL for sandbox code.

        Returns:
            str: e.g. "http://127.0.0.1:8766"
        Raises:
            RuntimeError: if no reverse tunnel was configured.
        """
        if self._upstream is None:
            raise RuntimeError(
                "No upstream configured. Pass upstream= to Sandbox()."
            )
        return self._tunnel_url

    def _create(self, body: Dict[str, Any]) -> Dict[str, Any]:
        create_info = getattr(self._client, "create_info", None)
        if callable(create_info):
            return create_info(body)
        sid = self._client.create(body)
        return getattr(self._client, "last_create", None) or {"sandboxId": sid}

    def wait_entrypoint(self) -> int:
        """Wait for the inherited image startup process to exit.

        Each request is a bounded long poll so frontend and gateway request
        deadlines are never used as an unbounded wait. Signal exits use the
        conventional ``128 + signal`` code. Full terminal details remain
        available through :attr:`entrypoint_exit_info`.
        """
        if not self._inherit_entrypoint:
            raise RuntimeError("inherit_entrypoint was not enabled for this sandbox")
        if self._closed:
            raise RuntimeError("sandbox is closed")

        while True:
            poll_wait = _ENTRYPOINT_POLL_INTERVAL * (0.7 + random.random() * 0.6)
            result = self._client.invoke(
                self._sid,
                "entrypoint.poll",
                {"wait_timeout": poll_wait},
                timeout=max(1, int(poll_wait + 1)),
            )
            status = result.get("status")
            if status == "running":
                continue
            if status == "exited":
                self._entrypoint_exit_info = dict(result)
                exit_code = result.get("shell_exit_code", result.get("exit_code"))
                if isinstance(exit_code, bool) or not isinstance(exit_code, int):
                    raise SandboxError(
                        f"entrypoint exit response missing exit code: {result}"
                    )
                return exit_code
            if status == "error":
                raise SandboxError(
                    str(result.get("message") or "entrypoint polling failed")
                )
            raise SandboxError(f"invalid entrypoint poll response: {result}")

    @property
    def entrypoint_exit_info(self) -> Optional[Dict[str, Any]]:
        """Structured terminal status cached by :meth:`wait_entrypoint`."""
        if self._entrypoint_exit_info is None:
            return None
        return dict(self._entrypoint_exit_info)

    # ── lifecycle ──────────────────────────────────────────────────────

    def is_running(self) -> bool:
        if self._closed:
            return False
        try:
            info = self._client.instance_info(self._sid)
            return info.get("status") == "running"
        except Exception:
            return False

    def get_info(self) -> SandboxInfo:
        info = self._client.instance_info(self._sid)
        return SandboxInfo(
            id=str(info.get("id") or self._sid),
            state=str(info.get("status") or "stopped"),
            cpu=info.get("required_cpu", self._cpu),
            memory=info.get("required_mem", self._memory),
            image=info.get("image", self._image),
        )

    def create_snapshot(
        self,
        *,
        name: Optional[str] = None,
        timeout_seconds: int = ADX_GET_DEFAULT_TIMEOUT,
    ) -> SnapshotInfo:
        """Create a non-expiring reusable Snapshot and keep this sandbox running."""
        if self._closed:
            raise RuntimeError("sandbox is closed")
        if name is not None and (
            not isinstance(name, str) or not name.strip()
        ):
            raise ValueError("name must be a non-empty string or None")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int):
            raise ValueError("timeout_seconds must be a positive integer")
        if timeout_seconds <= 0 or timeout_seconds > 3600:
            raise ValueError("timeout_seconds must be between 1 and 3600")
        tunnel = getattr(self, "_tunnel_client", None)
        if tunnel is None:
            return self._snapshot_info(
                self._client.create_snapshot(
                    self._sid,
                    name=name.strip() if name is not None else None,
                    timeout_seconds=timeout_seconds,
                )
            )
        with tunnel.checkpoint_inflight():
            return self._snapshot_info(
                self._client.create_snapshot(
                    self._sid,
                    name=name.strip() if name is not None else None,
                    timeout_seconds=timeout_seconds,
                )
            )

    def pause(
        self,
        ttl_seconds: int = 90_000,
        *,
        timeout_seconds: int = ADX_GET_DEFAULT_TIMEOUT,
    ) -> PauseResult:
        """Synchronously pause this sandbox and return its durable snapshot."""
        if isinstance(ttl_seconds, bool) or not isinstance(ttl_seconds, int):
            raise ValueError("ttl_seconds must be a positive integer")
        if ttl_seconds <= 0:
            raise ValueError("ttl_seconds must be a positive integer")
        if isinstance(timeout_seconds, bool) or not isinstance(timeout_seconds, int):
            raise ValueError("timeout_seconds must be a positive integer")
        if timeout_seconds <= 0 or timeout_seconds > 3600:
            raise ValueError("timeout_seconds must be between 1 and 3600")
        if self._closed:
            raise RuntimeError("sandbox is closed")
        result = self._client.pause(
            self._sid, ttl_seconds, timeout_seconds=timeout_seconds
        )
        pause = PauseResult(
            sandbox_id=str(result.get("sandboxId") or ""),
            snapshot_id=str(result.get("snapshotId") or ""),
            size=int(result.get("size") or 0),
            state=str(result.get("state") or ""),
            expires_at=int(result.get("expiresAt") or 0),
        )
        if (pause.sandbox_id != self._sid or pause.state != "paused"
                or not pause.snapshot_id or pause.size <= 0 or pause.expires_at <= 0):
            raise SandboxError("pause response is not an authoritative PAUSED result")
        return pause

    def resume(self) -> ResumeResult:
        """Synchronously resume this sandbox and return the RUNNING route."""
        if self._closed:
            raise RuntimeError("sandbox is closed")
        result = self._client.resume(self._sid)
        mappings = result.get("portMappings") or {}
        if not isinstance(mappings, Mapping):
            raise RuntimeError("resume response portMappings must be an object")
        resume = ResumeResult(
            sandbox_id=str(result.get("sandboxId") or ""),
            state=str(result.get("state") or ""),
            route_address=str(result.get("routeAddress") or ""),
            function_proxy_id=str(result.get("functionProxyId") or ""),
            node_id=str(result.get("nodeId") or ""),
            port_mappings={str(key): int(value) for key, value in mappings.items()},
        )
        if (resume.sandbox_id != self._sid or resume.state != "running"
                or not resume.route_address or not resume.function_proxy_id):
            raise SandboxError("resume response is not an authoritative RUNNING result")
        return resume

    def reload(self) -> bool:
        """Restore this sandbox from its latest local anonymous checkpoint."""
        if self._closed:
            return False
        try:
            return bool(self._client.reload(self._sid).get("success", False))
        except SandboxError:
            return False

    def update_network_policy(
        self, policy: Optional[NetworkPolicy]
    ) -> None:
        """Atomically replace this sandbox's complete network policy.

        Passing None or an empty NetworkPolicy clears the policy and restores
        unrestricted networking.
        """
        if policy is not None and not isinstance(policy, NetworkPolicy):
            raise TypeError("policy must be a NetworkPolicy or None")
        if self._closed:
            raise RuntimeError("sandbox is closed")
        body = (
            {}
            if policy is None or policy.is_empty
            else policy.to_dict()
        )
        result = self._client.update_network_policy(self._sid, body)
        if not bool(result.get("success", False)):
            raise SandboxError("network policy update was not acknowledged")

    def _close(self, *, delete_remote: bool) -> None:
        if self._closed:
            return
        self._closed = True
        if self._tunnel_client is not None:
            try:
                self._tunnel_client.stop()
            except Exception as e:
                logger.debug("tunnel cleanup during close failed: %s", e)
            self._tunnel_client = None
        try:
            self._shells.close()
        except Exception as e:
            logger.debug("shell cleanup during close failed: %s", e)
        try:
            self._pty._close()
        except Exception as e:
            logger.debug("PTY cleanup during close failed: %s", e)
        try:
            if delete_remote and not self._detached:
                self._client.delete(self._sid)
        finally:
            self._client.close()

    def close(self) -> None:
        """Release local clients without deleting the remote sandbox.

        This operation is idempotent. Use :meth:`kill` when the same handle
        should also delete a non-detached remote sandbox.
        """

        self._close(delete_remote=False)

    def kill(self) -> None:
        """Release local clients and delete a non-detached remote sandbox."""

        self._close(delete_remote=True)

    @classmethod
    def delete(
        cls,
        sandbox_id: str,
        *,
        connection: Optional[ConnectionConfig] = None,
    ) -> None:
        if connection is not None and not isinstance(connection, ConnectionConfig):
            raise TypeError("connection must be a ConnectionConfig or None")
        if connection is None:
            client = SandboxClient()
        else:
            client = SandboxClient(connection=connection)
        try:
            client.delete(sandbox_id)
        finally:
            client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.kill()

    def __del__(self):
        try:
            self.kill()
        except Exception:
            pass
