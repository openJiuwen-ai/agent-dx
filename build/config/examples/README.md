# Service configuration examples

`deployment.yaml` is the unified `adxctl` input; JSON deployment input is not supported. `adxctl config init` creates `/opt/adx/config/deployment.yaml` from the managed-Redis standalone template by default. Add `--compact` to write a versioned profile reference and use `service_overrides` for host-specific differences; `adxctl config dump` prints the fully resolved YAML. Use `--profile standalone-external-redis`, `coordinator`, `node`, or `ingress-api` for the other topologies. The individual JSON files describe generated service entrypoints rather than the operator-facing deployment format. Replace addresses and absolute certificate paths for your deployment. Coordinator, Adxlet and API Server use `--config /absolute/path/config.json`. In the default mode API Server embeds Ingress and receives its listener environment; explicit `ingress_mode: standalone` renders the Ingress control JSON and starts `adx-ingress` separately.

Shipped deployment examples:

| File | Host roles | Redis mode |
| --- | --- | --- |
| `deployment.yaml` | Single-host Coordinator, Adxlet with embedded Relay, API Server and Ingress | External Redis |
| `deployment-standalone-managed-redis.yaml` | Same single-host roles plus Redis | `adxctl`-managed local Redis with AOF |
| `deployment-coordinator.yaml` | Coordinator control host | Shared external Redis |
| `deployment-node.yaml` | One Worker's Adxlet with embedded Relay | Shared external Redis |
| `deployment-ingress-api.yaml` | Ingress and API Server ingress host | Shared external Redis |

Optional Agent FS (AFS) examples are under `afs/`. They are copied into release
packages only when `ADX_WITH_AFS=1` is used for the explicit filesystem package.
Default ADX packages do not include `afs-meta`, `afs-node`, AFS ports, data
directories, mount points or filesystem probes. The standalone AFS deployment
example uses `with_afs: true` and absolute AFS TOML paths; copy those TOML files
to `/opt/adx/config/afs/` or update the paths before running `adxctl`.

See the [`adxctl` deployment guide](../../../docs/deployment/adxctl.md) for exact commands, role boundaries and split-host startup order. `adx-apiserver` serves the public control API and embeds Ingress by default. The release package also includes `adx-ingress` for explicit split-process deployment with `ingress_mode: standalone`.

String values in deployment YAML may use `${VAR}` or `${VAR:-default}`. Expansion happens after YAML parsing, so environment values remain scalar strings and cannot inject mappings or lists. Missing variables without defaults fail configuration loading. Numeric and boolean fields remain native YAML values rather than implicitly converting environment strings.

Compact `service_overrides` keys are local role names. The default Node profile has one `adxlet` service that embeds Relay, so Proxy environment overrides also belong under `adxlet`. Each worker has its own deployment YAML; its cluster-wide identity is the `adxlet` configuration's `node_id`, not the supervisor service ID.

Certificates and private keys are PEM. Peer identity files are DER leaf
certificates, supplied by deployment. Certificates must include the configured
`server_name` as a DNS SAN. The API Server certificate also covers its public HTTPS
name. Keep private keys and bootstrap API key files readable only by the service.
API keys contain 32–512 bytes. Configurations do not embed the raw key.

The shipped deployment selects `resource_source.kind=auto`; `kind=sandboxd` selects the external resource collector. See [resource sources](../../../docs/testing/node-lifecycle.md). The compatible, mutually exclusive `capacity_file` input consumes an observation file with this shape:

```json
{
  "capacity": {"cpu_millis": 4000, "memory_bytes": 8589934592, "disk_bytes": 107374182400},
  "devices": [],
  "valid_until_unix_seconds": 0
}
```

The resource producer must atomically replace that file with measured capacity,
physical devices and a future expiry; `0` above is an expired placeholder. A
missing/invalid update uses the last observation until expiry, then closes new
admission. The file reader is separate from the implemented sandboxd collector and automatic capacity source. Do not publish a configured resource budget as a measurement.

Node startup obtains the complete authoritative catalog before reconciling
managed runtimes. It restores committed running Environments and cleans runtimes
confirmed unowned or left by uncommitted starts. An unavailable Coordinator never
means an empty catalog. Explicit CLI stop performs local Environment cleanup before service shutdown.


Coordinator publishes its `advertised_address` in Redis with a renewable lease
and a default TTL of 15 seconds. Node and Environment API examples discover it
through the same Redis namespace. An explicit `coordinator_address` is an
alternative; configure only one discovery mode. Coordinator's heartbeat timeout
defaults to 30 seconds, so Node reports should arrive much more frequently.
After restart, a Node registers as reconciling and admits new allocations only
after recovering the authoritative catalog. See [discovery and recovery](../../../docs/testing/recovery-discovery.md).

Ingress's `ingress-control.json` uses the same Redis namespace as Coordinator,
whose `tls.peers` must include the Ingress certificate. The default embedded
deployment sets `ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR=/opt/adx/run/node` in
Adxlet's `env`, with the matching `proxy_socket` at `/opt/adx/run/node/route.sock`.
Relay closes data admission during initial startup and resynchronization,
opening it after the full Adxlet binding sync. See [route publication and local synchronization](../../../docs/testing/route-publication.md).

See [process deployment](../../../docs/testing/process-deployment.md) for foreground supervisor commands, managed Redis AOF configuration and explicit stop cleanup. The deployment example requires environment-provided certificates, sandboxd and a configured resource source; it is not a self-contained E2E environment.

For a co-located Ingress, Environment API can set `loopback_http: true` with a literal
loopback `listen` address such as `127.0.0.1:8888`. Ingress forwards control requests
there; public SDK traffic still terminates TLS at Ingress and internal RPC remains
mTLS. HTTPS is the default. Wildcard, hostname and non-loopback HTTP listeners are
rejected.

Managed Redis listening beyond loopback requires `password_file`, an absolute
path to a deployment-owned secret (32–512 printable non-space ASCII bytes).
`adxctl` renders `requirepass` into its private configuration and keeps Redis
protected mode enabled. Set the matching credentials in the shared `redis_url`
(URI-escape special characters); protect the deployment file because that URL
contains a secret. Components use that URL for both storage and discovery.

Coordinator and Adxlet examples enable loopback `metrics_listen` on ports 19090 and 19091. See [instance and resource metrics](../../../docs/testing/environment-resource-metrics.md) for metric definitions and external collection.

Deployment examples enable component log rotation and gzip compression.
Retention limits apply per component. See [component log rotation and compression](../../../docs/testing/log-rotation.md)
for configuration and failure handling.

Adxlet's `runtime_logs.directory` is separate from Supervisor component logs.
Standalone and node examples use the corresponding `state_dir/logs/runtime`,
so the Collector example can read `.out/.err` files from `$ADX_LOG_DIR/runtime`.
Terminated Runtime logs are retained for 24 hours, with at most 50 pairs and a
total 1 GiB budget. See [Runtime logs](../../../docs/testing/runtime-logs.md)
for backend limits on files belonging to running Runtimes.

Coordinator examples enable `autoscaling_listen: 127.0.0.1:19092` for direct HTTP queue, node admission and node Environment queries. This endpoint has no authentication or authorization checks. See [Coordinator autoscaling HTTP](../../../docs/deployment/coordinator-autoscaling.md) for request and shutdown semantics.

For S3-compatible, AFS, or JuiceFS checkpoint storage, replace `checkpoint_dir` with the `checkpoint_storage` object in the [checkpoint storage example](../../../docs/testing/snapshot-storage.md). Its optional `cache_max_entries` and `cache_budget_bytes` default to 500 and 53,687,091,200 (50 GiB). They bound reusable local copies; restore downloads and runtime-owned files can exceed those cache limits and still require real free disk space.

Configure matching `checkpoint_reclaimers` on Coordinator for every shared storage alias. Coordinator deletes shared objects/directories; adxlet only manages local files and cache eviction. AFS requires the same DFS namespace mounted on Coordinator and workers.

`node-jiuwenbox.json` 是 Jiuwenbox／Conch 的 adxlet 配置示例；替换 EROFS SHA256、Conch socket、镜像绑定和证书路径后使用。Linux 前置条件、配置语义及未完成的运行时验收见 [Jiuwenbox P0](../../../docs/testing/jiuwenbox-runtime.md)。
