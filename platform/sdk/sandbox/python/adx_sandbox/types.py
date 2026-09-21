import ipaddress
import os
import re
from dataclasses import dataclass, field
from datetime import datetime
from enum import Enum
from typing import Any, Callable, Dict, Mapping, Optional, Sequence, Tuple, Union

import idna

# Default action timeout in seconds.
ADX_GET_DEFAULT_TIMEOUT = 300

# Extra seconds added to the user-specified timeout for the RPC call,
# to account for network overhead and serialization.
ADX_GET_TIMEOUT_BUFFER = 30


@dataclass(frozen=True)
class RestartPolicy:
    """Bounded node-local restart after an unexpected runtime exit."""

    max_attempts: int = 3
    initial_backoff_seconds: int = 1
    max_backoff_seconds: int = 30

    def __post_init__(self):
        for value, limit in ((self.max_attempts, (1 << 32) - 1),
                             (self.initial_backoff_seconds, (1 << 64) - 1),
                             (self.max_backoff_seconds, (1 << 64) - 1)):
            if isinstance(value, bool) or not isinstance(value, int) or not 0 < value <= limit:
                raise ValueError("restart limits must be positive integers within protocol bounds")
        if self.initial_backoff_seconds > self.max_backoff_seconds:
            raise ValueError("initial restart backoff must not exceed maximum backoff")

    def to_dict(self) -> Dict[str, int]:
        return {"maxAttempts": self.max_attempts,
                "initialBackoffSeconds": self.initial_backoff_seconds,
                "maxBackoffSeconds": self.max_backoff_seconds}


_DNS_LABEL_PATTERN = re.compile(r"^[a-z0-9_-]+$")
_NETWORK_ACTIONS = frozenset({"allow", "deny"})
_NETWORK_DIRECTIONS = frozenset({"ingress", "egress", "both"})
_NETWORK_PROTOCOLS = frozenset({"any", "tcp", "udp", "icmp"})
_TRAFFIC_POLICY_MODES = frozenset({"stateless", "stateful"})
_MAX_TRAFFIC_RULES = 256
# UINT32_MAX is reserved for ADX control-plane and published-port
# rules, which must remain effective even when user traffic is default-deny.
_MAX_USER_RULE_PRIORITY = (1 << 32) - 2


def _connection_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"{name} is not set")
    return value


def _connection_env_flag(name: str, default: bool) -> bool:
    raw = os.environ.get(name, "1" if default else "0")
    return raw.strip().lower() not in ("0", "false", "no")


@dataclass(frozen=True)
class ConnectionConfig:
    """Connection settings shared by all transports for one sandbox.

    Passing this object to :class:`adx_sandbox.Sandbox` avoids process-global
    ``ADX_*`` connection state. :meth:`from_env` preserves the environment-based
    configuration used by existing callers.

    ``server_address`` and ``use_tls`` select the frontend control plane.
    ``gateway_address`` selects tunnel, user-port, and PTY routes and falls back
    to ``server_address`` when omitted. ``gateway_use_tls`` selects WSS for the
    reverse tunnel and for PTY when a separate gateway is configured.
    ``verify_tls`` controls frontend HTTP and WebSocket certificate verification.
    """

    server_address: str
    token: str = field(repr=False)
    use_tls: bool = True
    gateway_address: Optional[str] = None
    gateway_use_tls: bool = False
    verify_tls: bool = False
    token_provider: Optional[Callable[[], str]] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        for field_name in ("server_address", "token"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")
        object.__setattr__(
            self,
            "server_address",
            self.server_address.strip().rstrip("/"),
        )
        if not self.server_address:
            raise ValueError("server_address must be a non-empty string")
        object.__setattr__(self, "token", self.token.strip())
        if self.gateway_address is not None:
            if (
                not isinstance(self.gateway_address, str)
                or not self.gateway_address.strip()
            ):
                raise ValueError("gateway_address must be a non-empty string")
            object.__setattr__(
                self,
                "gateway_address",
                self.gateway_address.strip().rstrip("/"),
            )
            if not self.gateway_address:
                raise ValueError("gateway_address must be a non-empty string")
        for field_name in ("use_tls", "gateway_use_tls", "verify_tls"):
            if not isinstance(getattr(self, field_name), bool):
                raise TypeError(f"{field_name} must be a boolean")
        if self.token_provider is not None and not callable(self.token_provider):
            raise TypeError("token_provider must be callable")

    def resolved_token(self) -> str:
        """Return a fresh token when a provider is configured."""
        token = self.token_provider() if self.token_provider is not None else self.token
        if not isinstance(token, str) or not token.strip():
            raise ValueError("token_provider must return a non-empty string")
        return token.strip()

    @classmethod
    def from_env(
        cls,
        *,
        server_address: Optional[str] = None,
        token: Optional[str] = None,
        verify_tls: bool = False,
    ) -> "ConnectionConfig":
        """Build a snapshot of the current ``ADX_*`` connection settings."""

        gateway_address = os.environ.get("ADX_GATEWAY_ADDRESS", "").strip()
        return cls(
            server_address=server_address or _connection_env("ADX_SERVER_ADDRESS"),
            token=token or _connection_env("ADX_TOKEN"),
            use_tls=_connection_env_flag("ADX_TLS", True),
            gateway_address=gateway_address or None,
            gateway_use_tls=_connection_env_flag("ADX_GATEWAY_TLS", False),
            verify_tls=verify_tls,
        )


def _normalize_domain_pattern(pattern: str, description: str) -> str:
    if not isinstance(pattern, str):
        raise TypeError(f"{description} patterns must be strings")
    value = pattern.strip().lower()
    if value.endswith("."):
        value = value[:-1]
    wildcard = value.startswith("*.")
    if wildcard:
        value = value[2:]
    if not value or "*" in value or "?" in value:
        raise ValueError(f"invalid {description} pattern: {pattern!r}")
    try:
        value = idna.encode(value, uts46=True).decode("ascii")
    except idna.IDNAError:
        # Preserve the existing DNS-SD-compatible underscore behavior for
        # ASCII owner names while normalizing ordinary IDNs to punycode.
        if any(ord(char) > 127 for char in value):
            raise ValueError(
                f"invalid {description} pattern: {pattern!r}"
            ) from None
    if len(value) > 253:
        raise ValueError(f"invalid {description} pattern: {pattern!r}")
    for label in value.split("."):
        if (
            not label
            or len(label) > 63
            or label.startswith("-")
            or label.endswith("-")
            or _DNS_LABEL_PATTERN.fullmatch(label) is None
        ):
            raise ValueError(f"invalid {description} pattern: {pattern!r}")
    return f"*.{value}" if wildcard else value


def _normalize_dns_pattern(pattern: str) -> str:
    return _normalize_domain_pattern(pattern, "DNS blacklist")


def _normalize_choice(value: str, name: str, allowed: frozenset[str]) -> str:
    if not isinstance(value, str):
        raise TypeError(f"{name} must be a string")
    normalized = value.strip().lower()
    if normalized not in allowed:
        choices = ", ".join(sorted(allowed))
        raise ValueError(f"{name} must be one of: {choices}")
    return normalized


def _normalize_cidr(value: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError("cidr must be a non-empty IPv4 address or CIDR")
    try:
        network = ipaddress.ip_network(value.strip(), strict=False)
    except ValueError as error:
        raise ValueError(f"invalid IPv4 address or CIDR: {value!r}") from error
    if network.version != 4:
        raise ValueError(f"invalid IPv4 address or CIDR: {value!r}")
    return str(network)


@dataclass(frozen=True)
class PortRange:
    """Inclusive TCP or UDP port range.

    Omit ``last`` to select one port. Both endpoints must be in 1..65535.
    """

    first: int
    last: Optional[int] = None

    def __post_init__(self) -> None:
        last = self.first if self.last is None else self.last
        for name, value in (("first", self.first), ("last", last)):
            if isinstance(value, bool) or not isinstance(value, int):
                raise TypeError(f"port range {name} must be an integer")
            if value < 1 or value > 65535:
                raise ValueError(f"port range {name} must be in 1..65535")
        if self.first > last:
            raise ValueError("port range first must not exceed last")
        object.__setattr__(self, "last", last)

    def to_dict(self) -> Dict[str, int]:
        assert self.last is not None
        return {"first": self.first, "last": self.last}


def _normalize_port_range(
    value: Optional[object], name: str
) -> Optional[PortRange]:
    if value is None or isinstance(value, PortRange):
        return value
    if isinstance(value, bool) or not isinstance(value, int):
        raise TypeError(f"{name} must be an integer, PortRange, or None")
    return PortRange(value)


@dataclass(frozen=True)
class NetworkRule:
    """One IPv4 traffic rule expressed from the sandbox's point of view.

    ``cidr`` and ``domain`` are mutually exclusive. Omitting both matches any
    peer address. A domain is valid only for egress and may be exact or start
    with ``*.``. ``port_range`` selects the peer port and
    ``sandbox_port_range`` selects the local sandbox port.
    """

    action: str = "allow"
    direction: str = "egress"
    protocol: str = "any"
    cidr: Optional[str] = None
    domain: Optional[str] = None
    port_range: Optional[Union[PortRange, int]] = None
    sandbox_port_range: Optional[Union[PortRange, int]] = None
    priority: int = 100

    def __post_init__(self) -> None:
        action = _normalize_choice(self.action, "action", _NETWORK_ACTIONS)
        direction = _normalize_choice(
            self.direction, "direction", _NETWORK_DIRECTIONS
        )
        protocol = _normalize_choice(
            self.protocol, "protocol", _NETWORK_PROTOCOLS
        )
        if self.cidr is not None and self.domain is not None:
            raise ValueError("cidr and domain cannot be combined in one rule")
        cidr = _normalize_cidr(self.cidr) if self.cidr is not None else None
        domain = (
            _normalize_domain_pattern(self.domain, "domain")
            if self.domain is not None
            else None
        )
        if domain is not None and direction != "egress":
            raise ValueError("domain rules are valid only for egress")
        port_range = _normalize_port_range(self.port_range, "port_range")
        sandbox_port_range = _normalize_port_range(
            self.sandbox_port_range, "sandbox_port_range"
        )
        if (port_range is not None or sandbox_port_range is not None) and (
            protocol not in ("tcp", "udp")
        ):
            raise ValueError("port ranges require protocol='tcp' or 'udp'")
        if isinstance(self.priority, bool) or not isinstance(self.priority, int):
            raise TypeError("priority must be an integer")
        if self.priority < 1 or self.priority > _MAX_USER_RULE_PRIORITY:
            raise ValueError(
                f"priority must be in 1..{_MAX_USER_RULE_PRIORITY}"
            )
        object.__setattr__(self, "action", action)
        object.__setattr__(self, "direction", direction)
        object.__setattr__(self, "protocol", protocol)
        object.__setattr__(self, "cidr", cidr)
        object.__setattr__(self, "domain", domain)
        object.__setattr__(self, "port_range", port_range)
        object.__setattr__(self, "sandbox_port_range", sandbox_port_range)

    def to_dict(self) -> Dict[str, Any]:
        value: Dict[str, Any] = {
            "action": self.action,
            "direction": self.direction,
            "protocol": self.protocol,
            "priority": self.priority,
        }
        peer: Dict[str, Any] = {}
        if self.cidr is not None:
            peer["cidr"] = self.cidr
        if self.domain is not None:
            peer["domain"] = self.domain
        if self.port_range is not None:
            assert isinstance(self.port_range, PortRange)
            peer["portRange"] = self.port_range.to_dict()
        if peer:
            value["peer"] = peer
        if self.sandbox_port_range is not None:
            assert isinstance(self.sandbox_port_range, PortRange)
            value["sandboxPortRange"] = self.sandbox_port_range.to_dict()
        return value


@dataclass(frozen=True)
class TrafficPolicy:
    """Generic IPv4 packet policy with independent direction defaults."""

    ingress_default_action: str = "allow"
    egress_default_action: str = "allow"
    rules: Sequence[NetworkRule] = ()
    mode: str = "stateful"

    def __post_init__(self) -> None:
        ingress = _normalize_choice(
            self.ingress_default_action,
            "ingress_default_action",
            _NETWORK_ACTIONS,
        )
        egress = _normalize_choice(
            self.egress_default_action,
            "egress_default_action",
            _NETWORK_ACTIONS,
        )
        mode = _normalize_choice(self.mode, "mode", _TRAFFIC_POLICY_MODES)
        if isinstance(self.rules, (str, bytes)):
            raise TypeError("rules must be a sequence of NetworkRule values")
        rules = tuple(self.rules)
        if len(rules) > _MAX_TRAFFIC_RULES:
            raise ValueError(
                f"traffic policies support at most {_MAX_TRAFFIC_RULES} rules"
            )
        if any(not isinstance(rule, NetworkRule) for rule in rules):
            raise TypeError("rules must contain only NetworkRule values")
        object.__setattr__(self, "ingress_default_action", ingress)
        object.__setattr__(self, "egress_default_action", egress)
        object.__setattr__(self, "rules", rules)
        object.__setattr__(self, "mode", mode)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "ingressDefaultAction": self.ingress_default_action,
            "egressDefaultAction": self.egress_default_action,
            "mode": self.mode,
            "rules": [rule.to_dict() for rule in self.rules],
        }


@dataclass(frozen=True)
class DNSRule:
    """One exact or leading-wildcard DNS query rule."""

    pattern: str
    action: str = "deny"

    def __post_init__(self) -> None:
        object.__setattr__(
            self, "action", _normalize_choice(self.action, "action", _NETWORK_ACTIONS)
        )
        object.__setattr__(
            self, "pattern", _normalize_domain_pattern(self.pattern, "DNS")
        )

    def to_dict(self) -> Dict[str, str]:
        return {"action": self.action, "pattern": self.pattern}


@dataclass(frozen=True)
class DNSPolicy:
    """DNS query policy evaluated by sandboxd's managed DNS proxy."""

    default_action: str = "allow"
    rules: Sequence[DNSRule] = ()

    def __post_init__(self) -> None:
        default = _normalize_choice(
            self.default_action, "default_action", _NETWORK_ACTIONS
        )
        if isinstance(self.rules, (str, bytes)):
            raise TypeError("rules must be a sequence of DNSRule values")
        rules = tuple(self.rules)
        if any(not isinstance(rule, DNSRule) for rule in rules):
            raise TypeError("rules must contain only DNSRule values")
        object.__setattr__(self, "default_action", default)
        object.__setattr__(self, "rules", rules)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "defaultAction": self.default_action,
            "rules": [rule.to_dict() for rule in self.rules],
        }


@dataclass(frozen=True)
class NetworkPolicy:
    """Network policy for sandbox creation and atomic runtime replacement.

    ``block_network`` denies new sandbox network flows except the adx
    control proxy and published sandbox ports required by SDK routes and
    explicit port forwarding. Replies to allowed flows are stateful.
    ``dns_blacklist`` denies conventional DNS queries matching exact names or
    leading ``*.`` suffix patterns. ``traffic`` and ``dns`` expose the generic
    schema v2 policy. Legacy fields and schema v2 sections cannot be mixed.
    """

    block_network: bool = False
    dns_blacklist: Tuple[str, ...] = ()
    traffic: Optional[TrafficPolicy] = None
    dns: Optional[DNSPolicy] = None

    def __post_init__(self) -> None:
        if not isinstance(self.block_network, bool):
            raise TypeError("block_network must be a boolean")
        if isinstance(self.dns_blacklist, (str, bytes)):
            raise TypeError("dns_blacklist must be a sequence of patterns")
        normalized = tuple(
            dict.fromkeys(
                _normalize_dns_pattern(item) for item in self.dns_blacklist
            )
        )
        if self.block_network and normalized:
            raise ValueError(
                "block_network and dns_blacklist cannot be combined"
            )
        if (self.block_network or normalized) and (
            self.traffic is not None or self.dns is not None
        ):
            raise ValueError(
                "legacy and schema v2 network policies cannot be combined"
            )
        if self.traffic is not None and not isinstance(
            self.traffic, TrafficPolicy
        ):
            raise TypeError("traffic must be a TrafficPolicy or None")
        if self.dns is not None and not isinstance(self.dns, DNSPolicy):
            raise TypeError("dns must be a DNSPolicy or None")
        object.__setattr__(self, "dns_blacklist", normalized)

    @classmethod
    def block(cls) -> "NetworkPolicy":
        """Deny new flows except required adx and published-port paths."""
        return cls(block_network=True)

    @classmethod
    def deny_dns(cls, *patterns: str) -> "NetworkPolicy":
        """Deny DNS queries matching the supplied domain patterns."""
        if not patterns:
            raise ValueError("deny_dns requires at least one domain pattern")
        return cls(dns_blacklist=patterns)

    @classmethod
    def allowlist(
        cls,
        rules: Sequence[NetworkRule],
        *,
        default_action: str = "deny",
        ingress_default_action: str = "allow",
        mode: str = "stateful",
    ) -> "NetworkPolicy":
        """Allow selected egress rules and apply a default action to the rest."""

        normalized = tuple(rules)
        if not normalized:
            raise ValueError("allowlist requires at least one NetworkRule")
        return cls(
            traffic=TrafficPolicy(
                ingress_default_action=ingress_default_action,
                egress_default_action=default_action,
                rules=normalized,
                mode=mode,
            )
        )

    @property
    def is_empty(self) -> bool:
        return (
            not self.block_network
            and not self.dns_blacklist
            and self.traffic is None
            and self.dns is None
        )

    def to_dict(self) -> Dict[str, Any]:
        result: Dict[str, Any] = {}
        if self.block_network:
            result["blockNetwork"] = True
        if self.dns_blacklist:
            result["dnsBlacklist"] = list(self.dns_blacklist)
        if self.traffic is not None or self.dns is not None:
            result["schemaVersion"] = 2
            if self.traffic is not None:
                result["traffic"] = self.traffic.to_dict()
            if self.dns is not None:
                result["dns"] = self.dns.to_dict()
        return result


@dataclass(frozen=True)
class PortForwarding:
    """Port-forwarding descriptor.

    Port forwarding is requested at sandbox creation time. The SDK builds
    router URLs as ``http://<gateway>/<safeID>/<port>`` through
    :meth:`adx_sandbox.Sandbox.get_port_url`.
    """

    port: int
    protocol: str = "TCP"


@dataclass(frozen=True)
class DataPlaneSecurityPolicy:
    """Per-sandbox client security for tunnel and port-forwarding.

    Accepted values are ``"tls"`` and ``"tls-token"``. ``None`` inherits the
    server deployment default. Direct access is always ``"tls-token"``.
    """

    tunnel_mode: Optional[str] = None
    port_forward_mode: Optional[str] = None

    def __post_init__(self) -> None:
        for name, value in (
            ("tunnel_mode", self.tunnel_mode),
            ("port_forward_mode", self.port_forward_mode),
        ):
            if value is not None and value not in ("tls", "tls-token"):
                raise ValueError(f"{name} must be 'tls', 'tls-token', or None")

    def to_dict(self) -> Dict[str, str]:
        result: Dict[str, str] = {}
        if self.tunnel_mode is not None:
            result["tunnelSecurityMode"] = self.tunnel_mode
        if self.port_forward_mode is not None:
            result["portForwardSecurityMode"] = self.port_forward_mode
        return result


@dataclass(frozen=True)
class EntryInfo:
    name: str
    path: str
    type: str  # "file" | "dir" | "symlink"
    size: int
    permissions: str
    modified_time: float


class CommandStatus(str, Enum):
    PENDING = "PENDING"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    KILLED = "KILLED"


@dataclass(frozen=True)
class CommandResult:
    stdout: str
    stderr: str
    exit_code: Optional[int]
    status: CommandStatus = CommandStatus.SUCCEEDED
    truncated: bool = False
    error_code: Optional[str] = None
    error_message: Optional[str] = None
    stdout_truncated: bool = False
    stderr_truncated: bool = False


@dataclass(frozen=True)
class SandboxInfo:
    id: str
    state: str  # "running" | "stopped"
    cpu: Optional[int]
    memory: Optional[int]
    image: Optional[str]


@dataclass(frozen=True)
class SnapshotInfo:
    """Stable public identity and aliases of a reusable Snapshot."""

    snapshot_id: str
    names: Tuple[str, ...] = ()


@dataclass(frozen=True)
class PauseResult:
    """Authoritative result of a synchronous sandbox Pause."""

    sandbox_id: str
    snapshot_id: str
    size: int
    state: str
    expires_at: int


@dataclass(frozen=True)
class ResumeResult:
    """Authoritative result of a synchronous sandbox Resume."""

    sandbox_id: str
    state: str
    route_address: str
    function_proxy_id: str
    node_id: str
    port_mappings: Mapping[str, int]


@dataclass(frozen=True)
class S3Config:
    """S3 object storage configuration."""

    endpoint: str
    bucket: str
    object: str
    access_key: Optional[str] = None
    secret_key: Optional[str] = field(default=None, repr=False)

    def __post_init__(self) -> None:
        for field_name in ("endpoint", "bucket", "object"):
            value = getattr(self, field_name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{field_name} must be a non-empty string")

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "endpoint": self.endpoint,
            "bucket": self.bucket,
            "object": self.object,
        }
        if self.access_key is not None:
            d["accessKey"] = self.access_key
        if self.secret_key is not None:
            d["secretKey"] = self.secret_key
        return d


@dataclass(frozen=True)
class Mount:
    """Read-only mount configuration for Sandbox.

    Mounts are always read-only. The source is either a container image
    (``image_url``) or an S3 object (``s3_config``); sandboxd resolves
    the source to a local path and exposes it at ``target``.

    ``type`` selects the in-sandbox filesystem:

    - ``"bind"`` (default): bind-mount the resolved host path (file or
      directory tree) at ``target`` via FDFS.
    - ``"erofs"``: mount the resolved host path as a read-only EROFS
      filesystem. The source must point at an EROFS image file (e.g.
      an S3 object whose content is a ``.img`` EROFS image).

    Exactly one of ``image_url`` or ``s3_config`` must be specified.

    Examples::

        Mount(target="/opt/tool", image_url="registry/tool:v1")
        Mount(target="/weights", type="erofs", s3_config=S3Config(...))
    """

    target: str
    image_url: Optional[str] = None
    s3_config: Optional[S3Config] = None
    type: str = "bind"

    def __post_init__(self) -> None:
        if not isinstance(self.target, str) or not self.target.startswith("/"):
            raise ValueError("target must be an absolute sandbox path")
        sources = [self.image_url, self.s3_config]
        count = sum(1 for s in sources if s is not None)
        if count != 1:
            raise ValueError(
                f"Exactly one of image_url, s3_config must be specified, got {count}"
            )
        if self.image_url is not None and (
            not isinstance(self.image_url, str) or not self.image_url.strip()
        ):
            raise ValueError("image_url must be a non-empty string")
        if self.type not in ("bind", "erofs"):
            raise ValueError(f"type must be 'bind' or 'erofs', got {self.type!r}")

    def to_dict(self) -> Dict[str, Any]:
        d: Dict[str, Any] = {
            "type": self.type,
            "target": self.target,
            "options": ["ro"],
        }
        if self.image_url is not None:
            d["image_url"] = self.image_url
        if self.s3_config is not None:
            d["s3_config"] = self.s3_config.to_dict()
        return d


@dataclass(frozen=True)
class NodeInfo:
    id: str
    status: int
    capacity: Mapping[str, float]
    allocatable: Mapping[str, float]
    labels: Mapping[str, Any]


@dataclass(frozen=True)
class CommandInfo:
    pid: Optional[int]
    command: str
    running: bool
    id: str = ""
    status: CommandStatus = CommandStatus.RUNNING
    exit_code: Optional[int] = None
    created_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    finished_at: Optional[datetime] = None

    @property
    def command_id(self) -> str:
        return self.id
