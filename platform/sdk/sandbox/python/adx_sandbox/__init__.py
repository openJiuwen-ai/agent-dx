from .types import (
    CommandInfo,
    CommandResult,
    CommandStatus,
    ConnectionConfig,
    DataPlaneSecurityPolicy,
    DNSPolicy,
    DNSRule,
    EntryInfo,
    Mount,
    NetworkPolicy,
    NetworkRule,
    NodeInfo,
    PauseResult,
    PortForwarding,
    PortRange,
    S3Config,
    SandboxInfo,
    SnapshotInfo,
    ResumeResult,
    TrafficPolicy,
)

SDK_CAPABILITIES = frozenset(
    {
        "local-close",
        "explicit-connection-config",
        "tunnel-proxy-port",
        "recoverable-commands",
        "multiplexed-command-watch",
    }
)

__all__ = [
    # current API
    "Sandbox",
    "Shell",
    "Shells",
    "CommandHandle",
    "CommandSubmissionError",
    "CommandNotFound",
    "CommandConflict",
    "CommandExpired",
    "CommandWaitTimeout",
    "CommandUnavailable",
    "UnsupportedFeature",
    "ResourceExhausted",
    "SandboxNotFound",
    "PermissionDenied",
    "Pty",
    "PtySession",
    "PtyError",
    "SandboxError",
    # data types
    "ConnectionConfig",
    "EntryInfo",
    "CommandResult",
    "CommandStatus",
    "CommandInfo",
    "DataPlaneSecurityPolicy",
    "SandboxInfo",
    "SnapshotInfo",
    "PauseResult",
    "ResumeResult",
    "Mount",
    "NetworkPolicy",
    "NetworkRule",
    "PortRange",
    "TrafficPolicy",
    "DNSPolicy",
    "DNSRule",
    "S3Config",
    "PortForwarding",
    "NodeInfo",
    "resources",
    "SDK_CAPABILITIES",
]

# Heavy modules are lazy-loaded so lightweight entry points (the adx-sandbox CLI)
# don't pay for the httpx/websockets import up front.
_lazy_imports = {
    "Sandbox": ".sandbox_api",
    "SandboxError": "._transport",
    "Shell": ".shell",
    "Shells": ".shell",
    "CommandHandle": ".commands",
    "CommandSubmissionError": ".commands",
    "CommandNotFound": ".commands",
    "CommandConflict": ".commands",
    "CommandExpired": ".commands",
    "CommandWaitTimeout": ".commands",
    "CommandUnavailable": ".commands",
    "UnsupportedFeature": ".commands",
    "ResourceExhausted": ".commands",
    "SandboxNotFound": "._transport",
    "PermissionDenied": "._transport",
    "Pty": ".pty",
    "PtySession": ".pty",
    "PtyError": ".pty",
    "PortForwarding": ".types",
    "NodeInfo": ".types",
    "CommandInfo": ".types",
    "DataPlaneSecurityPolicy": ".types",
    "NetworkPolicy": ".types",
    "NetworkRule": ".types",
    "PortRange": ".types",
    "TrafficPolicy": ".types",
    "DNSPolicy": ".types",
    "DNSRule": ".types",
    "resources": "._resources",
}


def __getattr__(name):
    module_path = _lazy_imports.get(name)
    if module_path is not None:
        import importlib

        module = importlib.import_module(module_path, __package__)
        value = getattr(module, name)
        globals()[name] = value
        return value
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
