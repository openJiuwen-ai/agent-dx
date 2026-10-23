"""Scoped external readiness barrier for a real, admitted restore."""

import json
import re
import subprocess
import threading
from dataclasses import asdict, dataclass
from pathlib import Path


@dataclass(frozen=True)
class TrafficFailure:
    environment: str
    nonce: str
    replica: int
    status: int
    body: str
    seconds: float


class TrafficFailures:
    """Keep the failed response before teardown; probe each guest at most once."""

    def __init__(self, evidence):
        self.evidence = Path(evidence)
        self.probed = set()
        self.lock = threading.Lock()

    def capture(self, failure, probe):
        (self.evidence / f"http-failure-{failure.nonce}.json").write_text(
            json.dumps(asdict(failure), indent=2)
        )
        with self.lock:
            if failure.environment in self.probed:
                return
            self.probed.add(failure.environment)
        try:
            guest = {"output": probe()}
        except Exception as error:
            guest = {"probe_error": repr(error)}
        (self.evidence / f"http-failure-{failure.environment}-guest.json").write_text(
            json.dumps(guest, indent=2)
        )


class ReadinessBarrier:
    def __init__(self, root, node):
        root = Path(root)
        if node not in ("node1", "node2") or not re.fullmatch(r"[A-Za-z0-9_-]+", root.name):
            raise ValueError("fixture node and run name required")
        namespace = json.loads((root / "evidence/topology.json").read_text())["namespaces"][node]
        if not re.fullmatch(r"axt-[a-f0-9]+-node[12]", namespace):
            raise ValueError("owned fixture namespace required")
        self.command = ["nsenter", "--net=/var/run/netns/" + namespace, "iptables"]
        self.rule = [
            "OUTPUT",
            "-p",
            "tcp",
            "--dport",
            "50090",
            "-m",
            "owner",
            "--uid-owner",
            "0",
            "-m",
            "comment",
            "--comment",
            root.name,
            "-j",
            "DROP",
        ]

    def __enter__(self):
        subprocess.run([*self.command, "-I", *self.rule], check=True)
        return self

    def __exit__(self, *_):
        subprocess.run([*self.command, "-D", *self.rule], check=True)
