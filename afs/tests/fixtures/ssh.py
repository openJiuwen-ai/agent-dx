"""Batch SSH with existing host trust; no global config/key/user modifications."""

import re
import shlex
from fixtures.common import Channel, HERE


def connect(config):
    target = config["ssh_target"]
    if not re.fullmatch(r"[A-Za-z0-9_.@:-]+", target) or target.startswith("-"):
        raise ValueError("invalid SSH target")
    argv = ["ssh", "-T", "-o", "BatchMode=yes", "-o", "ConnectTimeout=10", "-o", "StrictHostKeyChecking=yes"]
    if config.get("ssh_config"):
        argv += ["-F", config["ssh_config"]]
    command = (["sudo", "-n"] if config.get("sudo", True) else []) + [
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
