"""Build ADX runtime configuration from public settings and mounted Secrets."""

import json
import os
from pathlib import Path
import sys
import tempfile
from urllib.parse import quote

SETTINGS = json.loads(Path("/etc/adx-iac/config.json").read_text())
SECRETS = Path("/secrets")
WORK = Path("/work")
WORK.mkdir(exist_ok=True)
TENANT = SETTINGS["tenant"]
ROLE = sys.argv[1]


def read(name):
    return (SECRETS / name).read_text().strip()


def write(name, value):
    path = WORK / name
    # Only the service user can read generated configurations containing credentials.
    with open(path, "w", opener=lambda p, flags: os.open(p, flags, 0o600)) as stream:
        json.dump(value, stream)
    path.chmod(0o600)
    return str(path)


def tls(name, peers):
    return {
        "ca": str(SECRETS / "ca.pem"),
        "certificate": str(SECRETS / (name + ".pem")),
        "private_key": str(SECRETS / (name + ".key")),
        "server_name": "adx.internal",
        "peers": {key: str(SECRETS / (value + ".der")) for key, value in peers.items()},
    }


redis = "redis://:" + quote(read("redis-key"), safe="") + "@" + SETTINGS["dependencies"]["redis_host"] + "/"
runtime = os.environ["ADX_RUNTIME_PAYLOAD"]
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
services = []


def add(name, role, service_config, service_env=None):
    services.append({"id": name, "role": role, "config": service_config, "env": service_env or {}})


if ROLE == "coordinator":
    add(
        "coordinator",
        "coordinator",
        {
            "listen": "0.0.0.0:17000",
            "advertised_address": "https://coordinator:17000",
            "scheduler_shards": 1,
            "placement": SETTINGS["scheduling"]["placement"],
            "rpc_timeout_seconds": 60,
            "tls": tls(
                "coordinator",
                {
                    "apiserver": "apiserver",
                    "ingress": "ingress",
                    **{
                        "node:adx-node-" + str(i): "/node-peers/adx-node-" + str(i)
                        for i in range(SETTINGS["node_identities"]["capacity"])
                    },
                },
            ),
            "bootstrap_credentials": [
                {
                    "key_file": str(SECRETS / "admin-key"),
                    "tenant_id": "admin",
                    "administrator": True,
                    "expires_at_unix_seconds": 0,
                },
                {
                    "key_file": str(SECRETS / "tenant-key"),
                    "tenant_id": TENANT,
                    "administrator": False,
                    "expires_at_unix_seconds": 0,
                },
            ],
            "metrics_listen": "0.0.0.0:17090",
        },
    )
    if "storage_domains" in SETTINGS:
        services[-1]["config"]["storage_domains"] = SETTINGS["storage_domains"]
    if "checkpoint_reclaimers" in SETTINGS:
        services[-1]["config"]["checkpoint_reclaimers"] = SETTINGS["checkpoint_reclaimers"]
        for credential_name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY"):
            os.environ[credential_name] = (Path("/obs-credentials") / credential_name).read_text().strip()
        os.environ["AWS_EC2_METADATA_DISABLED"] = "true"
        os.environ["AWS_VIRTUAL_HOSTED_STYLE_REQUEST"] = "true"
elif ROLE == "apiserver":
    add(
        "api",
        "apiserver",
        {
            "listen": "0.0.0.0:8888",
            "loopback_http": False,
            "ingress_mode": "standalone",
            "internal_security": "mtls",
            "ca": str(SECRETS / "ca.pem"),
            "certificate": str(SECRETS / "apiserver.pem"),
            "private_key": str(SECRETS / "apiserver.key"),
            "server_name": "adx.internal",
            "rpc_timeout_seconds": 120,
            "cache_entries": 1000,
            "auth_cache_ttl_seconds": 10,
        },
    )
elif ROLE == "activator":
    config = write(
        "activator.json",
        {
            "listen": "0.0.0.0:8091",
            "redis_url": redis + "1",
            "namespace": SETTINGS["agent_namespace"],
            "environment_url": "https://apiserver:8888",
            "sandbox_network": SETTINGS["sandbox_network"],
            "environment_api_key_envs": {TENANT: "ADX_PLATFORM_TENANT_API_KEY"},
            "allow_plaintext_transport": True,
            "environment_ca_file": str(SECRETS / "ca.pem"),
            "request_timeout_seconds": 120,
            "registration": {
                "instance_id": os.environ["POD_NAME"],
                "advertise_url": "http://" + os.environ["POD_IP"] + ":8091",
                "lease_seconds": 15,
                "heartbeat_seconds": 5,
            },
        },
    )
    os.environ.update(
        {
            "ADX_ACTIVATOR_CONFIG": config,
            "ADX_ACTIVATOR_SERVICE_TOKEN": read("service-token"),
            "ADX_PLATFORM_TENANT_API_KEY": read("tenant-key"),
        }
    )
    os.execv("/opt/adx/bin/adx-activator", ["adx-activator"])
elif ROLE == "ingress":
    agent = write(
        "agent.json",
        {
            "timeout_seconds": 120,
            "activator": {
                "discovery": {
                    "redis_url": redis + "1",
                    "namespace": SETTINGS["agent_namespace"],
                    "refresh_seconds": 2,
                },
                "token_env": "ADX_ACTIVATOR_SERVICE_TOKEN",
                "allow_plaintext": True,
            },
        },
    )
    files = write(
        "environment-files.json",
        {
            "port": 50090,
            "token_env": "ADX_EXECD_HTTP_TOKEN",
            "directory": {
                "url": "https://apiserver:8888",
                "allow_http": False,
                "ca_file": str(SECRETS / "ca.pem"),
            },
            "api_key_envs": {TENANT: "ADX_PLATFORM_TENANT_API_KEY"},
        },
    )
    # This platform chart never enables the DigitalMateGateway account/application layer.
    for disabled_key in (
        "ADX_JIUWEN_CONFIG",
        "ADX_ACCOUNT_CONFIG",
        "ADX_ACCOUNT_DATABASE_URL",
        "ADX_LITELLM_ADMIN_KEY",
        "ADX_CREDENTIAL_KEY_FILE",
        "ADX_AGC_API_CLIENT_SECRET",
        "HUAWEI_OAUTH_CLIENT_ID",
        "HUAWEI_OAUTH_CLIENT_SECRET",
    ):
        os.environ.pop(disabled_key, None)
    env = {
        "ADX_ENVIRONMENT_FILES_CONFIG": files,
        "ADX_PLATFORM_TENANT_API_KEY": read("tenant-key"),
        "ADX_AGENT_CONFIG": agent,
        "ADX_EXECD_HTTP_TOKEN": read("execd-token"),
        "ADX_ACTIVATOR_SERVICE_TOKEN": read("service-token"),
        "ADX_DATA_PLANE_INGRESS_TLS_BIND": "0.0.0.0:8443",
        "ADX_DATA_PLANE_INGRESS_PLAIN_BIND": "0.0.0.0:8080",
        "ADX_DATA_PLANE_INGRESS_HEALTH_BIND": "0.0.0.0:18080",
        "ADX_DATA_PLANE_INGRESS_TLS_CERT": str(SECRETS / "ingress.pem"),
        "ADX_DATA_PLANE_INGRESS_TLS_KEY": str(SECRETS / "ingress.key"),
        "ADX_DATA_PLANE_INGRESS_ALLOWED_CLIENT_CIDRS": "0.0.0.0/0",
        "ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS": "127.0.0.1:18080",
        "ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ROUTES": "exact:/healthz",
        "ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE": "mtls",
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_CA": str(SECRETS / "ca.pem"),
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_SERVER_NAME": "adx.internal",
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_CERT": str(SECRETS / "ingress.pem"),
        "ADX_DATA_PLANE_INGRESS_NODE_TLS_CLIENT_KEY": str(SECRETS / "ingress.key"),
    }
    add(
        "ingress",
        "ingress",
        {
            "tls": tls("ingress", {"coordinator": "coordinator"}),
            "rpc_timeout_seconds": 10,
            "refresh_seconds": 2,
            "auth_cache_seconds": 10,
            "auth_cache_entries": 1000,
        },
        env,
    )
else:
    raise SystemExit("Unknown control role")

deployment_path = write(
    "deployment.yaml",
    {
        "schema_version": 1,
        "package_dir": "/opt/adx",
        "state_dir": tempfile.mkdtemp(prefix="supervisor-", dir=WORK),
        "redis_url": redis,
        "namespace": SETTINGS["platform_namespace"],
        "restart_delay_ms": 1000,
        "stop_timeout_seconds": 30,
        "services": services,
        "runtime_profile": profile,
        "logging": {
            "enabled": True,
            "max_file_bytes": 16777216,
            "rotate_seconds": 3600,
            "compress": False,
            "max_files": 3,
            "max_age_seconds": 86400,
            "max_total_bytes": 67108864,
        },
    },
)
os.execv("/opt/adx/bin/adxctl", ["adxctl", "run", "--config", deployment_path])
