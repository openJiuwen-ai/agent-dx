"""Batch SSH with existing host trust; no global config/key/user modifications.

`remote_prefix` is an optional literal argv run on the remote host before the
private-mount worker, for entering an already owned role (for example a managed
node's namespaces and cgroup). It is quoted together with the worker command.
"""

import re
import shlex
from fixtures.common import Channel, HERE


def remote_prefix(config):
    prefix = config.get("remote_prefix", [])
    if not isinstance(prefix, list) or "remote_prefix" in config and not prefix:
        raise ValueError("remote_prefix must be a non-empty literal argv list")
    if not all(isinstance(s, str) and s and "\n" not in s for s in prefix):
        raise ValueError("remote_prefix entries must be non-empty single-line strings")
    return prefix


def connect(config):
    target = config["ssh_target"]
    if not re.fullmatch(r"[A-Za-z0-9_.@:-]+", target) or target.startswith("-"):
        raise ValueError("invalid SSH target")
    prefix = remote_prefix(config)
    argv = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes",
            "-o", "ServerAliveInterval=15", "-o", "ServerAliveCountMax=8"]
    if config.get("ssh_config"):
        argv += ["-F", config["ssh_config"]]
    command = (["sudo", "-n"] if config.get("sudo", True) else []) + prefix + [
        "unshare",
        "--mount",
        "--propagation",
        "private",
        "--",
        "python3",
        "-u",
        "-c",
        (HERE / "worker.py").read_text(),
    ]
    return Channel(argv + [target, shlex.join(command)])
