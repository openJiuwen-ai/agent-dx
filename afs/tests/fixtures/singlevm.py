"""One Linux guest; products share this invocation's private mount namespace."""

import os
import platform
from fixtures.common import Blocked, Channel, HERE


def connect(config):
    if platform.system() != "Linux":
        raise Blocked("singlevm executes inside a Linux VM, not on the macOS controller")
    prefix = [] if os.geteuid() == 0 else ["sudo", "-n"]
    return Channel(
        prefix
        + [
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
    )
