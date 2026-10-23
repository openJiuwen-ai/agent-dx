"""Split an existing FC fixture into control and two isolated process nodes."""

import copy
import json
import os
from pathlib import Path
from storage_fixture import public as public_storage

HOST = "10.240.0.1"
ADDRESSES = {"node1": "10.240.0.11", "node2": "10.240.0.12"}


def configure(root):
    root = Path(root)
    original = json.loads((root / "deployment.yaml").read_text())
    template = next(s for s in original["services"] if s["role"] == "adxlet")
    if template["config"].get("proxy_mode") != "embedded":
        raise ValueError("transfer fixture requires embedded Relay")

    def relocate(value, destination):
        if isinstance(value, dict):
            return {key: relocate(item, destination) for key, item in value.items()}
        if isinstance(value, list):
            return [relocate(item, destination) for item in value]
        if isinstance(value, str) and str(root) in value and str(root / "secrets") not in value:
            return value.replace(str(root), str(destination))
        return value

    for name, address in ADDRESSES.items():
        folder = root / name
        folder.mkdir()
        service = relocate(copy.deepcopy(template), folder)
        service["id"] = name
        c = service["config"]
        c.update(
            node_id=name, advertised_address=f"{address}:17001", proxy_address=f"{address}:18443"
        )
        cert = "node" if name == "node1" else "node2"
        for key, suffix in [("certificate", "pem"), ("private_key", "key")]:
            c["tls"][key] = str(root / "secrets/tls" / f"{cert}.{suffix}")
        if public_storage(root)["kind"] == "minio":
            c["checkpoint_storage"]["endpoint"] = f"http://{HOST}:19090"
        if public_storage(root)["kind"] in ("afs", "juicefs"):
            from filesystem_fixture import storage_config

            c["checkpoint_storage"] = storage_config(
                os.environ.get("ADX_FC_AFS_ROOT", ""), name, root.name
            )
        env = service["env"]
        env["ADX_DATA_PLANE_RELAY_TLS_CERT"] = c["tls"]["certificate"]
        env["ADX_DATA_PLANE_RELAY_TLS_KEY"] = c["tls"]["private_key"]
        env["ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS"] = f"{HOST}/32"
        env["ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS"] = f"10.231.{16 if name == 'node1' else 32}.0/20"
        d = {
            **original,
            "state_dir": str(folder / "state"),
            "services": [service],
            "redis_url": original["redis_url"].replace("127.0.0.1", HOST),
        }
        path = folder / "deployment.yaml"
        path.write_text(json.dumps(d, indent=2))
        path.chmod(0o600)
        backend = folder / "sandboxd"
        backend.mkdir()
        for filename in ("oss.json", "registry.json", "oss_auths.json", "registry_auths.json"):
            (backend / filename).write_bytes((root / "sandboxd" / filename).read_bytes())
        text = (root / "sandboxd/config.toml").read_text().replace(str(root), str(folder))
        if name == "node2":
            text = text.replace('ip_range = "10.231.16.1/20"', 'ip_range = "10.231.32.1/20"')
        text = text.replace('cgroup_root_name = "/adx-fc-', f'cgroup_root_name = "/{name}-adx-fc-')
        if public_storage(root)["kind"] in ("afs", "juicefs", "native_xfs"):
            # Private writable runtime files share the filesystem, but never
            # share the immutable checkpoint directory or another executor's state.
            runtime_root = (
                os.environ["ADX_STORAGE_NATIVE_XFS_ROOT"]
                if public_storage(root)["kind"] == "native_xfs"
                else c["checkpoint_storage"]["mount_root"]
            )
            runtime = Path(runtime_root) / "runtime" / root.name / name
            runtime.mkdir(parents=True, exist_ok=True)
            text = text.replace(
                f'filestore_dir = "{backend}/filestore"', f'filestore_dir = "{runtime}"'
            )
        (backend / "config.toml").write_text(text)
    control = copy.deepcopy(original)
    control["services"] = [s for s in control["services"] if s["role"] not in ("adxlet", "relay")]
    coordinator = next(s for s in control["services"] if s["role"] == "coordinator")["config"]
    coordinator.update(advertised_address=f"https://{HOST}:17000", heartbeat_timeout_seconds=12)
    if os.getenv("ADX_FC_SUITE") == "storage":
        ingress = copy.deepcopy(next(s for s in control["services"] if s["role"] == "ingress"))
        ingress["id"] = "ingress2"
        ingress["env"].update(
            ADX_DATA_PLANE_INGRESS_TLS_BIND="0.0.0.0:9443",
            ADX_DATA_PLANE_INGRESS_PLAIN_BIND="127.0.0.1:9080",
            ADX_DATA_PLANE_INGRESS_HEALTH_BIND="127.0.0.1:19080",
        )
        replica = root / "ingress2"
        replica.mkdir()
        (replica / "deployment.yaml").write_text(
            json.dumps(
                {**control, "services": [ingress], "state_dir": str(replica / "state")}, indent=2
            )
        )
        (replica / "deployment.yaml").chmod(0o600)
    (root / "deployment.yaml").write_text(json.dumps(control, indent=2))
    path = root / "registry.yaml"
    path.write_text(path.read_text().replace("127.0.0.1:5000", "0.0.0.0:5000"))
