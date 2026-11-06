"""One ADX runtime Pod. Fail closed unless cgroup mounts are private to this Pod."""

import json
import os
import re
import signal
import subprocess
import sys
import time
import tempfile
from pathlib import Path
from urllib.parse import quote

S = Path("/secrets")
W = Path("/work")
B = Path("/opt/adx/backend")
C = json.loads(Path("/etc/adx-iac/config.json").read_text())
N = C["node"]
sys.path.insert(0, "/etc/adx-iac")
from node_storage import (
    checkpoint_environment,
    require_executable,
    runtime_configuration,
    validate_storage,
    verify_sfs_mount,
)

validate_storage(N)
runtime_settings = runtime_configuration(N, os.environ["POD_IP"])

node_id = os.environ["POD_NAME"]
if (
    not re.fullmatch(r"adx-node-(0|[1-9][0-9]*)", node_id)
    or int(node_id.rsplit("-", 1)[1]) >= C["node_identities"]["capacity"]
):
    raise SystemExit("POD_NAME is outside the provisioned identity pool")


def run(command, *args):
    subprocess.run([require_executable(command), *args], check=True)


def read(n):
    return (S / n).read_text().strip()


def write(p, v):
    with open(p, "w", opener=lambda filename, flags: os.open(filename, flags, 0o600)) as stream:
        json.dump(v, stream)
    p.chmod(0o600)


if os.geteuid() != 0:
    raise SystemExit("runtime node requires root in its container")
if len(sys.argv) == 1:
    # A new mount+cgroup namespace keeps subsequent mounts inside this Pod.
    os.execv(
        require_executable("unshare"),
        [
            "unshare",
            "--mount",
            "--cgroup",
            "--fork",
            "--kill-child",
            sys.executable,
            __file__,
            "private",
        ],
    )
lines = Path("/proc/self/cgroup").read_text().splitlines()
if any(line.split(":", 2)[2] != "/" for line in lines):
    raise SystemExit("private cgroup namespace not established")
run("mount", "--make-rprivate", "/")
# Kubernetes mounts the NFS volume before entering this private namespace.
verify_sfs_mount(N["sfs"])
cg = Path("/sys/fs/cgroup")
if (cg / "cgroup.controllers").exists():
    run("mount", "-t", "cgroup2", "none", str(cg))
    leaf = cg / "adx-supervisor"
    leaf.mkdir(exist_ok=True)
    for pid in (cg / "cgroup.procs").read_text().split():
        try:
            (leaf / "cgroup.procs").write_text(pid)
        except ProcessLookupError:
            pass
    (cg / "cgroup.subtree_control").write_text("+cpu +memory +pids")
else:
    run("mount", "-t", "tmpfs", "-o", "mode=755", "tmpfs", str(cg))
    for line in lines:
        _, controllers, _ = line.split(":", 2)
        if not controllers:
            continue
        name = controllers.removeprefix("name=")
        path = cg / name
        path.mkdir()
        run("mount", "-t", "cgroup", "-o", controllers, "cgroup", str(path))
        for alias in controllers.split(","):
            if alias != name and not alias.startswith("name="):
                (cg / alias).symlink_to(name)
if Path("/proc/sys/net/bridge/bridge-nf-call-iptables").read_text().strip() != "1":
    raise SystemExit("bridge netfilter is not enabled; no sysctl changes performed")
W.mkdir(exist_ok=True)
R = W / "sandboxd"
R.mkdir(exist_ok=True)
(W / "proxy").mkdir(exist_ok=True)
# Use reachable cluster DNS rather than a per-node link-local resolver inside nested netns.
(W / "resolv.conf").write_text(
    f"nameserver {N['dns_address']}\n"
    f"search {C['namespace']}.svc.cluster.local svc.cluster.local cluster.local\n"
    f"options {' '.join(N['dns_options'])}\n"
)
write(
    R / "registry.json",
    {"registry": {"scheme": "https", "skip_verify": False}, "type": "registry"},
)
write(R / "oss.json", {"oss": {}, "type": "oss"})
write(R / "oss_auths.json", {})
config = f'''rootDir = "{R}/root"
storeDir = "{R}/store"
[plugin.network]
ip_range = "{N["sandbox_gateway_cidr"]}"
nat_backend = "iptables"
enable_local_dnat = true
enable_network_acl = true
[plugin.resource]
disable_cgroup = false
cpu_limit_mode = "quota"
cgroup_cache_size = 1
interface_cache_size = 2
cgroup_root_name = "/{C["namespace"]}"
max_instance_num = {N["max_sandboxes"]}
pids_max = 512
[plugin.runtime]
image_lib_dir = "{R}/images"
filestore_dir = "{R}/filestore"
filestore_dir_size = "{N["filestore_size"]}"
loop_device_dir = "/dev"
overlay_tmpfs_size = "{N["sandbox_rootfs_size"]}"
resolv_conf_path = "/work/resolv.conf"
[plugin.runtime.runsc]
platform = "systrap"
[plugin.runtime.basic_spec]
runsc = "/etc/adx-iac/runsc-base-spec.json"
[plugin.runtime.runtime_binary]
runsc = "/opt/adx/backend/runsc"
[plugin.image]
root = "{R}/image_manager/data"
distill_fs_bin = "/opt/adx/backend/distill_fs"
oss_template = "{R}/oss.json"
nydus_template = "{R}/registry.json"
nydus_suffix = "_nydus_v3"
oss_auths_path = "{R}/oss_auths.json"
registry_auths_path = "/registry/.dockerconfigjson"
cgroup_memory_limit = "0"
'''
(R / "config.toml").write_text(config)


def tls():
    return {
        "ca": str(S / "ca.pem"),
        "certificate": "/node-identity/tls.pem",
        "private_key": "/node-identity/tls.key",
        "server_name": "adx.internal",
        "peers": {
            k: str(S / (v + ".der")) for k, v in {"coordinator": "coordinator", "apiserver": "apiserver"}.items()
        },
    }


ip = os.environ["POD_IP"]
runtime = os.environ["ADX_RUNTIME_PAYLOAD"]
node = {
    "node_id": node_id,
    "listen": "0.0.0.0:17001",
    "advertised_address": ip + ":17001",
    "proxy_address": ip + ":18443",
    "tls": tls(),
    "sandboxd_socket": str(R / "sandboxd.sock"),
    "proxy_socket": str(W / "proxy/route.sock"),
    "resource_source": {"kind": "auto", "disk_path": str(W), "valid_for_seconds": 30},
    "report_interval_seconds": 5,
    "rpc_timeout_seconds": 120,
    "execd_port": 50090,
    "execd_env": {"EXECD_HTTP_TOKEN": read("execd-token")},
    "runtime_logs": {"directory": str(W / "runtime-logs")},
    "metrics_listen": "0.0.0.0:17091",
}
node.update(runtime_settings)
node["checkpoint_storage"] = N["checkpoint_storage"]
checkpoint_env = checkpoint_environment("/obs-credentials")
env = {
    "ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE": "mtls",
    "ADX_DATA_PLANE_RELAY_BIND": "0.0.0.0:18443",
    "ADX_DATA_PLANE_RELAY_ADVERTISE_ADDRESS": ip + ":18443",
    "ADX_DATA_PLANE_RELAY_HEALTH_BIND": "0.0.0.0:19443",
    "ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS": N["sandbox_target_cidr"],
    "ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS": "0.0.0.0/0",
    "ADX_DATA_PLANE_RELAY_TLS_CERT": "/node-identity/tls.pem",
    "ADX_DATA_PLANE_RELAY_TLS_KEY": "/node-identity/tls.key",
    "ADX_DATA_PLANE_RELAY_MTLS_CLIENT_CA": str(S / "ca.pem"),
    "ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR": str(W / "proxy"),
}
profile = {
    "rootfs": {
        "runtime_class": "runsc",
        "type": "local",
        "path": runtime,
        "readonly": False,
    },
    "bootstrap": {
        "type": "erofs",
        "root": runtime,
        "target": "/__adx",
        "entrypoint": ["/__adx/usr/local/bin/adx-execd"],
    },
    "env": {
        "PATH": "/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin",
        "EXECD_HTTP_PORT": "50090",
        "EXECD_HTTP_TOKEN": read("execd-token"),
    },
}
write(
    W / "deployment.yaml",
    {
        "schema_version": 1,
        "package_dir": "/opt/adx",
        "state_dir": tempfile.mkdtemp(prefix="supervisor-", dir=W),
        "redis_url": "redis://:" + quote(read("redis-key"), safe="") + "@" + C["dependencies"]["redis_host"] + "/",
        "namespace": C["platform_namespace"],
        "restart_delay_ms": 1000,
        "stop_timeout_seconds": 30,
        "runtime_profile": profile,
        "services": [{"id": "node", "role": "adxlet", "config": node, "env": env}],
    },
)
os.environ["RUNSC_AKERNEL"] = "1"
backend = subprocess.Popen(
    [
        str(B / "sandboxd"),
        "--root",
        str(R / "root"),
        "--config",
        str(R / "config.toml"),
        "--socket",
        str(R / "sandboxd.sock"),
        "--http-address",
        "127.0.0.1:18081",
        "--pprof-address",
        "127.0.0.1:16061",
        "--log-file",
        str(W / "sandboxd.log"),
    ]
)
supervisor = None
try:
    for _ in range(60):
        if backend.poll() is not None:
            raise RuntimeError("sandboxd exited during startup")
        if (R / "sandboxd.sock").exists():
            break
        time.sleep(1)
    else:
        raise RuntimeError("sandboxd startup timeout")
    supervisor = subprocess.Popen(
        ["/opt/adx/bin/adxctl", "run", "--config", str(W / "deployment.yaml")],
        env={**os.environ, **checkpoint_env},
    )

    def stop(*_):
        if supervisor.poll() is None:
            supervisor.terminate()

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    raise SystemExit(supervisor.wait())
finally:
    if supervisor and supervisor.poll() is None:
        supervisor.terminate()
        supervisor.wait(timeout=45)
    if backend.poll() is None:
        backend.terminate()
        backend.wait(timeout=30)
