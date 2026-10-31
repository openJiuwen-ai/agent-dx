"""Use caller-provided namespace entrypoints, including a managed VM session.

The prefix must enter an already owned role's network/PID/root namespace. The
worker creates only its own private mount namespace and supervises its children.
"""

from fixtures.common import Channel, HERE


def connect(config):
    prefix = config.get("command_prefix")
    if (
        not isinstance(prefix, list)
        or not prefix
        or not all(isinstance(s, str) and s and "\n" not in s for s in prefix)
    ):
        raise ValueError("exec transport requires a literal command_prefix argv")
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
        ],
        timeout=60,
    )
