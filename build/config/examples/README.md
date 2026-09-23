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

See the [`adxctl` deployment guide](../../../docs/deployment/adxctl.md) for exact commands, role boundaries and split-host startup order. `adx-apiserver` is the current control-plane Frontend and hosts Ingress by default; `adx-ingress` remains available for explicit process isolation.

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


Coordinator 配置 `advertised_address` 发布可续期的 Redis 地址，默认 TTL 15 秒。Node 与 Sandbox API 示例通过相同 Redis namespace 发现它；也可改用显式 `coordinator_address`，不能同时配置两种方式。Coordinator 心跳超时默认 30 秒，Node 报告间隔应显著小于该值。Node 重启先注册为对账中，完成权威目录恢复后才开放新分配。完整契约见 [发现与恢复](../../../docs/testing/recovery-discovery.md)。

Ingress 的 `ingress-control.json` 与 Coordinator 使用相同 Redis namespace；Coordinator `tls.peers` 必须登记 Ingress 证书。默认共进程部署在 Adxlet 的 `env` 中设置 `ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR=/opt/adx/run/node`，其 `proxy_socket` 相应为 `/opt/adx/run/node/route.sock`。代理首次启动与重新同步期间关闭数据准入，完成 Adxlet 全量绑定同步后开放。详见 [路由发布与本机同步](../../../docs/testing/route-publication.md)。

See [process deployment](../../../docs/testing/process-deployment.md) for foreground supervisor commands, managed Redis AOF configuration and explicit stop cleanup. The deployment example requires environment-provided certificates, sandboxd and a configured resource source; it is not a self-contained E2E environment.

For a co-located Ingress, Sandbox API can set `loopback_http: true` with a literal
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

部署示例已启用组件日志滚动与 gzip 压缩，所有历史保留限制按组件计算。配置和异常处理见[组件日志滚动与压缩](../../../docs/testing/log-rotation.md)。
