"""Separate version-conflict fallback, qualified cache reuse, and permissions."""

import json
from pathlib import Path

from fixtures.common import is_single_host

from suites.dfs.cache_probe import assert_cached_reuse as assert_cached_reuse
from suites.dfs.cache_probe import assert_conflict_direct as assert_conflict_direct
from suites.dfs.cache_probe import parse_cache_open as parse_cache_open

PROBE = (Path(__file__).resolve().parent / "cache_probe.py").read_text()


def run_probe(c, action):
    assert is_single_host(c.config), "cache probe needs both real mounts and reader log"
    assert c.case.observe_cache, "cache evidence requires existing Info-level open records"
    c.python("A", "os.chown(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))", c.mount("A"), c.uid, c.gid, owner=False)
    code = (
        'd=os.stat(sys.argv[1]).st_dev;p="/sys/class/bdi/%s:%s/read_ahead_kb"'
        '%(os.major(d),os.minor(d));v=open(p).read().strip();open(p,"w").write("0");print(v)'
    )
    before = c.python("B", code, c.mount("B"), owner=False)["stdout"].strip()
    c.record("cache.mount_readahead", {"before_kib": int(before), "probe_kib": 0, "scope": "owned B mount only"})
    try:
        result = c.python(
            "A", PROBE, action, c.mount("A"), c.mount("B"),
            "http://" + c.address("B") + ":" + str(c.ports("B")[1]) + "/metrics",
            c.root("B") + "/B.log", check=False, timeout=180,
        )
        # Retain evidence emitted before an assertion fails, not only the traceback.
        c.record("cache.probe_result", result)
        stages = [json.loads(line) for line in result["stdout"].splitlines() if line.strip()]
        for stage in stages:
            c.record("cache." + stage["stage"], stage["value"])
        assert result["returncode"] == 0, result
        assert stages[-1] == {"stage": "complete", "value": {
            "action": action, "passed": True, "performance_measurement": False,
        }}, stages[-1:]
    finally:
        c.python(
            "B",
            'd=os.stat(sys.argv[1]).st_dev;p="/sys/class/bdi/%s:%s/read_ahead_kb"'
            '%(os.major(d),os.minor(d));open(p,"w").write(sys.argv[2])',
            c.mount("B"), before, owner=False,
        )
        c.record("cache.mount_readahead_restored", {"kib": int(before)})


def run(c):
    run_probe(c, "conflict")


def reuse(c):
    run_probe(c, "reuse")


def permission(c):
    run_probe(c, "permission")
