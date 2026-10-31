"""Shared product fixture: role operations are identical on local and SSH guests."""

from __future__ import annotations

import base64
import hashlib
import ipaddress
import json
from pathlib import Path
import re
import subprocess
import shutil
import time

from fixtures import singlevm, ssh, namespaces, kubernetes
from fixtures.common import Blocked, sha256, write_json
from fixtures.common import HERE

ROLES = ("M", "A", "B", "C")
NAMES = {"M": "meta", "A": "node-a", "B": "node-b", "C": "node-c"}


def validate(config, cases):
    if config.get("kind") not in ("singlevm", "multivm", "namespaces", "kubernetes"):
        raise ValueError("kind must be singlevm, namespaces, multivm or kubernetes")
    for field in ("uid", "gid", "other_uid", "other_gid"):
        if not isinstance(config.get(field), int) or config[field] <= 0:
            raise ValueError("explicit positive numeric identity required: " + field)
    if config["uid"] == config["other_uid"]:
        raise ValueError("owner and other UID must differ")
    roles = config.get("roles", {})
    hosts = config.get("hosts", {})
    needed = set("MAB")
    if (config["kind"] == "singlevm" and any(c.mode == "dfs" for c in cases)) or any(
        c.id.endswith(".many_readers") for c in cases
    ):
        needed.add("C")
    if not needed <= set(roles):
        raise Blocked("selected cases require roles: " + ",".join(sorted(needed)))
    for role, item in roles.items():
        if role not in ROLES or item.get("host") not in hosts:
            raise ValueError("unknown role or host: " + str(role))
        address = ipaddress.ip_address(item["address"])
        if address.version != 4 or address.is_unspecified:
            raise ValueError("this fixture requires concrete IPv4 role addresses")
        for port in (
            item.get("grpc_port", 26400 if role == "M" else 26500),
            item.get("rest_port", 26401 if role == "M" else 26501),
        ):
            if not isinstance(port, int) or not 1 <= port <= 65535:
                raise ValueError("role ports must be integer 1..65535")
    for name, host in hosts.items():
        if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_-]{0,63}", name):
            raise ValueError("unsafe host ID")
        if host.get("transport") not in ("local", "ssh", "exec", "kubernetes"):
            raise ValueError("unknown transport on " + name)
        if config['kind'] == 'kubernetes' and host.get('transport') != 'kubernetes':
            raise ValueError('Kubernetes topology requires Kubernetes role transport')
        base = Path(host.get("base", ""))
        if (
            not base.is_absolute()
            or ".." in base.parts
            or str(base) == "/"
            or any(c in str(base) for c in ("\n", "\r", '"', "\\"))
        ):
            raise ValueError("host base must be a dedicated existing absolute directory")
    selected_hosts = [roles[r]["host"] for r in needed]
    if config["kind"] in ("multivm", "namespaces", "kubernetes"):
        if len(set(selected_hosts)) != len(needed):
            raise Blocked("multivm requires a distinct guest for every selected role")
        if any(any(t.startswith("local-") for t in c.tags) for c in cases):
            raise Blocked("local-basic requires one VM, including the two-mount Rust contract")
    else:
        if len(set(selected_hosts)) != 1 or any(
            any(t.startswith(("multivm-", "cluster-")) for t in c.tags) for c in cases
        ):
            raise Blocked("singlevm cannot establish multivm evidence")
    return sorted(set(selected_hosts))


class Cluster:
    def __init__(self, config, binary_dir, output, case, root_token):
        self.config = config
        self.binary_dir = Path(binary_dir)
        self.output = Path(output)
        self.case = case
        self.mode = case.mode
        self.token = root_token
        self.channels = {}
        self.roots = {}
        self.evidence = []
        self.started = False
        self.roles = ["M", "A", "B"]
        if config["kind"] == "singlevm" or case.id.endswith(".many_readers"):
            self.roles.append("C")
        if self.mode == "ownerfs":
            self.roles = ["M", "A", "B"]
        self.uid = config["uid"]
        self.gid = config["gid"]
        self.bind = False

    def record(self, name, value):
        self.evidence.append({"check": name, "value": value})
        write_json(self.output / "checks.json", self.evidence)
        return value

    def channel(self, role):
        return self.channels[self.config["roles"][role]["host"]]

    def root(self, role):
        return self.roots[self.config["roles"][role]["host"]]

    def role_dir(self, role):
        return self.root(role) + "/" + NAMES[role]

    def address(self, role):
        return self.config["roles"][role]["address"]

    def ports(self, role):
        entry = self.config["roles"][role]
        return entry.get("grpc_port", 26400 if role == "M" else 26500), entry.get(
            "rest_port", 26401 if role == "M" else 26501
        )

    def command(self, role, argv, *, uid=None, gid=None, env=None, timeout=30, stdin=None, check=True):
        value = self.channel(role).call(
            "command", argv=[str(a) for a in argv], uid=uid, gid=gid, env=env or {}, timeout=timeout, stdin=stdin
        )
        if check and value["returncode"] != 0:
            raise AssertionError(f"{role}: command failed: {value}")
        return value

    def python(self, role, code, *args, owner=True, check=True, stdin=None):
        return self.command(
            role,
            ["python3", "-c", "import os,sys,json,hashlib,errno,stat\n" + code, *args],
            uid=self.uid if owner else None,
            gid=self.gid if owner else None,
            check=check,
            stdin=stdin,
        )

    def mount(self, role):
        return self.role_dir(role) + "/mount/" + self.mode

    def workspace(self, role):
        if self.mode == "ownerfs" and role == "A" and self.bind:
            return self.role_dir(role) + "/egress/workspace"
        return self.mount(role) + "/workspace"

    def connect(self):
        hosts = {self.config["roles"][r]["host"] for r in self.roles}
        infos = {}
        for host in sorted(hosts):
            cfg = self.config["hosts"][host]
            try:
                connector = {"local": singlevm.connect, "ssh": ssh.connect, "exec": namespaces.connect,
                             "kubernetes": kubernetes.connect}[
                    cfg["transport"]
                ]
                self.channels[host] = connector(cfg)
                listeners = [
                    [self.address(r), port]
                    for r in self.roles
                    if self.config["roles"][r]["host"] == host
                    for port in self.ports(r)
                ]
                info = self.channels[host].call("inspect", base=cfg["base"], listeners=listeners)
                if self.config['kind'] == 'kubernetes':
                    if info.get('pod_uid') != cfg['pod_uid']:
                        raise Blocked('worker Pod identity differs from the frozen Kubernetes role')
                    info['kubernetes'] = self.channels[host].kubernetes_identity
            except (OSError, RuntimeError) as exc:
                raise Blocked("cannot establish selected worker prerequisites: " + str(exc)) from exc
            infos[host] = info
            if info["system"] != "Linux" or info["uid"] != 0 or not info["fuse"] or info["filesystem"] != "ext4":
                raise Blocked("Linux root, FUSE device and native ext4 required: " + repr(info))
            if info["free"] < 1024**3 or not all(info["tools"].values()):
                raise Blocked("insufficient space or missing tools: " + repr(info))
        if self.config["kind"] == "multivm":
            ids = [v["machine_id"] for v in infos.values()]
            uuids = [v["product_uuid"] for v in infos.values()]
            boots = [v["boot_id"] for v in infos.values()]
            distinct_boots = len(set(boots)) == len(boots)
            distinct_ids = all(ids) and len(set(ids)) == len(ids)
            distinct_uuids = all(uuids) and len(set(uuids)) == len(uuids)
            if not distinct_boots or not (distinct_ids or distinct_uuids):
                raise Blocked(
                    "independent guest identities are ambiguous; duplicate clone machine-id needs distinct VM UUIDs"
                )
        if self.config["kind"] == "namespaces":
            if len({v["boot_id"] for v in infos.values()}) != 1:
                raise Blocked("namespaces topology must remain inside one actual Linux kernel/VM")
            for field in ("network_namespace", "pid_namespace", "mount_namespace"):
                if len({v[field] for v in infos.values()}) != len(infos):
                    raise Blocked("role namespace isolation is missing: " + field)
        if self.config['kind'] == 'kubernetes':
            pod_ids = [v['kubernetes']['pod_uid'] for v in infos.values()]
            if len(set(pod_ids)) != len(infos):
                raise Blocked('distinct Kubernetes role Pod UIDs are required')
            if len({v['kubernetes']['namespace_uid'] for v in infos.values()}) != 1:
                raise Blocked('Kubernetes role namespace identity differs')
            for field in ('network_namespace', 'pid_namespace', 'mount_namespace'):
                if len({(v['boot_id'], v[field]) for v in infos.values()}) != len(infos):
                    raise Blocked('Kubernetes role namespace isolation is missing: ' + field)
        self.infos = infos
        write_json(
            self.output / "environment.json",
            {"kind": self.config["kind"], "hosts": infos, "roles": self.config["roles"], "mode": self.mode},
        )
        return infos

    def inputs(self, infos):
        binaries = ["afs-meta", "afs-node"] + (["fuse_contract"] if self.case.rust else [])
        for name in binaries:
            p = self.binary_dir / name
            if not p.is_file():
                raise Blocked("missing explicit test/product artifact: " + str(p))
            with p.open("rb") as stream:
                header = stream.read(20)
            if len(header) != 20 or header[:4] != b"\x7fELF" or header[4] != 2 or header[5] not in (1, 2):
                raise Blocked("Linux ELF required: " + str(p))
            machine = int.from_bytes(header[18:20], "little" if header[5] == 1 else "big")
            expected = {"aarch64": 183, "x86_64": 62}
            if any(expected.get(info["machine"]) != machine for info in infos.values()):
                raise Blocked("artifact/guest architecture mismatch: " + name)
        identity = {
            "binaries": {n: sha256(self.binary_dir / n) for n in binaries},
            "case": self.case.id,
            "kind": self.config["kind"],
            "scope": "component integration, not ADX product or performance",
        }
        artifact_identity = self.binary_dir / "artifact-identity.json"
        if not artifact_identity.is_file():
            raise Blocked("artifact-identity.json is required with frozen product source and binary SHA identities")
        manifest = json.loads(artifact_identity.read_text())
        source_identity = manifest.get(
            "compiled_source_logical", manifest.get("source_input_sha256", manifest.get("commit", ""))
        )
        if not isinstance(source_identity, str) or not re.fullmatch(r"[0-9a-f]{40}|[0-9a-f]{64}", source_identity):
            raise Blocked("artifact identity needs an actual full commit or source input SHA256")
        for name in ("afs-meta", "afs-node"):
            if manifest.get("binaries", {}).get(name) != identity["binaries"][name]:
                raise Blocked("artifact manifest binary SHA mismatch: " + name)
        if self.case.rust and manifest.get("test_elf", {}).get("sha256") != identity["binaries"]["fuse_contract"]:
            raise Blocked("selected Rust test ELF SHA identity is missing or mismatched")
        identity["artifact_identity"] = manifest
        identity["artifact_manifest_sha256"] = sha256(artifact_identity)
        write_json(self.output / "identity.json", identity)
        return identity

    def prepare(self):
        infos = self.connect()
        identity = self.inputs(infos)
        binaries = list(identity["binaries"])
        for host, ch in self.channels.items():
            root = (
                self.config["hosts"][host]["base"].rstrip("/")
                + "/"
                + self.token
                + "/"
                + hashlib.sha256(self.case.id.encode()).hexdigest()[:12]
            )
            if len((root + "/node-a/run/node.sock").encode()) >= 108:
                raise Blocked("host base is too long for Linux UNIX socket paths")
            self.roots[host] = root
            ch.call("initialize", root=root, token=self.token)
            for name in binaries:
                actual = ch.upload(self.binary_dir / name, root + "/bin/" + name, 0o755)
                if actual != identity["binaries"][name]:
                    raise Blocked("artifact changed during transfer: " + name)
        write_json(
            self.output / "environment.json",
            {
                "kind": self.config["kind"],
                "hosts": infos,
                "roots": self.roots,
                "roles": self.config["roles"],
                "mode": self.mode,
            },
        )
        self._tls()
        self._configs()
        self.start("M")
        self.start("A")
        if self.mode == "ownerfs":
            self.python(
                "A",
                "os.mkdir(sys.argv[1],0o700);os.chown(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))",
                self.workspace("A"),
                self.uid,
                self.gid,
                owner=False,
            )
            self.stop("A")
            self.bind = True
            self._configs()
            self.start("A")
            self.start("B")
        else:
            self.python(
                "A",
                "os.mkdir(sys.argv[1],0o700);os.chown(sys.argv[1],int(sys.argv[2]),int(sys.argv[3]))",
                self.workspace("A"),
                self.uid,
                self.gid,
                owner=False,
            )
            for role in self.roles[2:]:
                self.start(role)
        self.topology()
        self.started = True

    def _tls(self):
        tls = self.output / "private-tls"
        tls.mkdir(mode=0o700)
        openssl = shutil.which("openssl")
        if openssl is None:
            raise Blocked("missing required controller tool: openssl")
        openssl = str(Path(openssl).resolve())

        def run(*args):
            subprocess.run([openssl, *map(str, args)], check=True, capture_output=True, timeout=30)

        run(
            "req",
            "-x509",
            "-newkey",
            "rsa:2048",
            "-nodes",
            "-days",
            "2",
            "-subj",
            "/CN=afs-test-ca",
            "-keyout",
            tls / "ca.key",
            "-out",
            tls / "ca.pem",
        )
        sans = ",".join(["DNS:afs-meta"] + ["IP:" + self.address(r) for r in self.roles])
        for role in self.roles:
            name = NAMES[role]
            run(
                "req",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-subj",
                "/CN=" + ("afs-meta" if role == "M" else name),
                "-keyout",
                tls / (name + ".key"),
                "-out",
                tls / (name + ".csr"),
            )
            (tls / "extensions").write_text("subjectAltName=" + sans + "\nextendedKeyUsage=serverAuth,clientAuth\n")
            run(
                "x509",
                "-req",
                "-in",
                tls / (name + ".csr"),
                "-CA",
                tls / "ca.pem",
                "-CAkey",
                tls / "ca.key",
                "-CAcreateserial",
                "-days",
                "2",
                "-extfile",
                tls / "extensions",
                "-out",
                tls / (name + ".pem"),
            )
        for host, ch in self.channels.items():
            for p in tls.glob("*"):
                if p.suffix == ".pem" or (
                    p.suffix == ".key"
                    and any(p.stem == NAMES[r] and self.config["roles"][r]["host"] == host for r in self.roles)
                ):
                    ch.upload(p, self.roots[host] + "/tls/" + p.name, 0o600 if p.suffix == ".key" else 0o644)

    def _configs(self):
        for role in self.roles:
            root = self.root(role)
            data = self.role_dir(role)
            grpc, rest = self.ports(role)
            cert = root + "/tls/" + NAMES[role]
            common = (
                f'grpc_listen = "{self.address(role)}:{grpc}"\nrest_listen = "{self.address(role)}:{rest}"\n'
                f'data_dir = "{data}/state"\nlog_level = "warn"\ntrace_enabled = false\n'
                f'tls_ca_certificate = "{root}/tls/ca.pem"\ntls_identity_certificate = "{cert}.pem"\n'
                f'tls_identity_private_key = "{cert}.key"\ntls_server_name = "afs-meta"\n'
            )
            trusted = ",".join(
                NAMES[r] + " = " + json.dumps(root + "/tls/" + NAMES[r] + ".pem") for r in self.roles if r != "M"
            )
            common += "trusted_node_certs = {" + trusted + "}\n"
            if role == "M":
                config = (
                    'id = "meta-ctl"\nfs = "all"\nmeta_store = "local-file"\n'
                    "dfs_desired_copies = 1\ndfs_sync_required_copies = 1\ndfs_min_distinct_nodes = 1\n"
                    'dfs_min_distinct_failure_domains = 1\ndfs_local_copy = "required"\n' + common
                )
            else:
                config = (
                    f'id = "{NAMES[role]}"\nfs = "{self.mode}"\n'
                    f'meta_endpoint = "https://{self.address("M")}:{self.ports("M")[0]}"\n'
                    f'advertise_endpoint = "https://{self.address(role)}:{grpc}"\ndata_mode = "grpc"\n'
                    f'uds_path = "{data}/run/node.sock"\n{self.mode}_mount = "{self.mount(role)}"\n' + common
                )
                if role == "A" and self.mode == "ownerfs" and self.bind:
                    config += (
                        "\nexperimental_ownerfs_workspace_bind = true\nownerfs_workspace_egress_root = "
                        + json.dumps(data + "/egress")
                        + '\n[ownerfs_workspace_bind]\nworkspace = "workspace"\n'
                    )
            path = data + "/config.toml"
            (self.output / ("config-" + role + ("-bind" if self.bind else "") + ".toml")).write_text(config)
            self.channel(role).call("put", path=path, data=base64.b64encode(config.encode()).decode(), mode=0o600)
            self.python(
                role,
                "from pathlib import Path\nfor x in sys.argv[1:]:Path(x).mkdir(parents=True,exist_ok=True)",
                data + "/state",
                data + "/run",
                data + "/egress",
                self.mount(role) if role != "M" else data + "/mount",
                owner=False,
            )
            self.command(
                role, [root + "/bin/afs-" + ("meta" if role == "M" else "node"), "--config", path, "--print-config"]
            )

    def health(self, role, path="/health"):
        code = "import urllib.request\nwith urllib.request.urlopen(sys.argv[1],timeout=2) as r:print(r.read().decode())"
        response = self.python(
            role, code, "http://" + self.address(role) + ":" + str(self.ports(role)[1]) + path, owner=False
        )
        return response["stdout"]

    def start(self, role):
        binary = self.root(role) + "/bin/afs-" + ("meta" if role == "M" else "node")
        listeners = [[self.address(role), port] for port in self.ports(role)]
        self.channel(role).call(
            "start",
            role=role,
            argv=[binary, "--config", self.role_dir(role) + "/config.toml"],
            log=self.root(role) + "/" + role + ".log",
            listeners=listeners,
        )
        deadline = time.monotonic() + 45
        error = None
        while time.monotonic() < deadline:
            state = self.channel(role).call("status")[role]
            if state["returncode"] is not None:
                raise RuntimeError("product exited before ready: " + repr(state))
            try:
                health = json.loads(self.health(role))
                if health.get("status") == "ready" and health.get("ready", True) is not False:
                    return self.record("ready." + role, health)
            except (AssertionError, ValueError) as exc:
                error = str(exc)
            time.sleep(0.2)
        raise RuntimeError("readiness timed out: " + str(error))

    def stop(self, role):
        return self.record("stop." + role, self.channel(role).call("stop", role=role, timeout=35))

    def restart(self, roles):
        for role in reversed(roles):
            self.stop(role)
        for role in roles:
            self.start(role)

    def topology(self):
        for role in self.roles[1:]:
            path = self.workspace(role) if role == "A" and self.mode == "ownerfs" else self.mount(role)
            try:
                value = self.command(role, ["findmnt", "-J", "--mountpoint", path, "-o", "TARGET,SOURCE,FSTYPE,ID"])
            except AssertionError:
                if role == 'A' and self.mode == 'ownerfs':
                    try:
                        diagnostic = self.python(role, (HERE / 'bind_probe.py').read_text(), self.root(role),
                                                 self.token, owner=False, check=False)
                        self.record('ownerfs.bind_diagnostic', diagnostic)
                    except Exception as error:
                        self.record('ownerfs.bind_diagnostic', {'diagnostic_error': str(error)})
                raise
            mount = json.loads(value["stdout"])["filesystems"][0]
            if role == "A" and self.mode == "ownerfs":
                assert mount["fstype"] == "ext4", mount
                value = self.python(
                    role,
                    (
                        "from pathlib import Path\np=Path(sys.argv[1]);a=os.stat(sys.argv[2]);"
                        'v=list(p.glob("root-*-e*"));assert len(v)==1,v;b=v[0].stat();assert '
                        "(a.st_dev,a.st_ino)==(b.st_dev,b.st_ino)"
                    ),
                    self.role_dir(role) + "/state/ownerfs",
                    path,
                    owner=False,
                )
            else:
                assert mount["source"] == "afs-" + self.mode, mount
            self.record("topology." + role, mount)

    def read(self, role, name, data):
        value = self.python(
            role,
            (
                'd=open(sys.argv[1],"rb").read();assert len(d)==int(sys.argv[2]);asse'
                'rt hashlib.sha256(d).hexdigest()==sys.argv[3];print(json.dumps({"byt'
                'es":len(d),"sha256":hashlib.sha256(d).hexdigest()}))'
            ),
            self.workspace(role) + "/" + name,
            len(data),
            hashlib.sha256(data).hexdigest(),
        )
        self.record("read." + role, json.loads(value["stdout"]))

    def write(self, role, name, data):
        self.python(
            role,
            'p=sys.argv[1];d=bytes.fromhex(sys.stdin.read());f=open(p,"wb");f.write(d);f.flush();os.fsync(f.fileno());f.close()',
            self.workspace(role) + "/" + name,
            stdin=data.hex(),
        )
        self.record("write." + role, {"bytes": len(data), "sha256": hashlib.sha256(data).hexdigest()})

    def cleanup(self):
        results = {}
        errors = []
        # Stop all Nodes on all guests before stopping Meta, even after partial setup.
        for role in reversed(self.roles[1:]):
            if self.config["roles"][role]["host"] not in self.channels:
                continue
            try:
                self.channel(role).call("stop", role=role, timeout=35)
            except Exception as exc:
                errors.append(str(exc))
        nodes_confirmed_gone = True
        for ch in self.channels.values():
            try:
                if any(role in ch.call("status") for role in self.roles[1:]):
                    nodes_confirmed_gone = False
            except Exception as exc:
                nodes_confirmed_gone = False
                errors.append("cannot confirm Node exit: " + str(exc))
        for host, ch in self.channels.items():
            try:
                result = ch.call("cleanup", timeout=80, allow_meta=nodes_confirmed_gone)
                results[host] = result
                errors.extend(result["errors"])
                if host in self.roots:
                    for name, value in ch.call("collect").items():
                        (self.output / (host + "-" + name)).write_bytes(base64.b64decode(value["data"]))
            except Exception as exc:
                errors.append(host + ": cleanup unconfirmed: " + str(exc))
            try:
                ch.close()
            except Exception as exc:
                errors.append(str(exc))
        private = self.output / "private-tls"
        if not errors and private.is_dir():
            shutil.rmtree(private)
        write_json(self.output / "waits.json", {h: v["waits"] for h, v in results.items()})
        value = {"status": "FAIL" if errors else "PASS", "hosts": results, "errors": errors}
        write_json(self.output / "cleanup.json", value)
        return value
