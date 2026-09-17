# Service configuration examples

`deployment.json` is the unified `adxctl` input; individual JSON files also describe each service entrypoint. Copy and replace addresses and absolute certificate paths for
your deployment. Master, Node Manager and Sandbox API use `--config /absolute/path/config.json`. Edge uses `ADX_EDGE_CONTROL_CONFIG=/absolute/path/edge-control.json` alongside its listener and data-path environment settings.

Certificates and private keys are PEM. Peer identity files are DER leaf
certificates, supplied by deployment. Certificates must include the configured
`server_name` as a DNS SAN. The Frontend certificate also covers its public HTTPS
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
managed runtimes. It restores committed running Instances and cleans runtimes
confirmed unowned or left by uncommitted starts. An unavailable Master never
means an empty catalog. Explicit CLI stop performs local Instance cleanup before service shutdown.


Master 配置 `advertised_address` 发布可续期的 Redis 地址，默认 TTL 15 秒。Node 与 Sandbox API 示例通过相同 Redis namespace 发现它；也可改用显式 `master_address`，不能同时配置两种方式。Master 心跳超时默认 30 秒，Node 报告间隔应显著小于该值。Node 重启先注册为对账中，完成权威目录恢复后才开放新分配。完整契约见 [发现与恢复](../../../docs/testing/recovery-discovery.md)。

Edge 的 `edge-control.json` 与 Master 使用相同 Redis namespace；Master `tls.peers` 必须登记 Edge 证书。Node Proxy 设置 `ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR=/run/adx`，Node Manager 的 `proxy_socket` 相应为 `/run/adx/route.sock`。代理首次启动与重新同步期间关闭数据准入，完成 Node Manager 全量绑定同步后开放。详见 [路由发布与本机同步](../../../docs/testing/route-publication.md)。

See [process deployment](../../../docs/testing/process-deployment.md) for foreground supervisor commands, managed Redis AOF configuration and explicit stop cleanup. The deployment example requires environment-provided certificates, sandboxd and a configured resource source; it is not a self-contained E2E environment.

For a co-located Edge, Sandbox API can set `loopback_http: true` with a literal
loopback `listen` address such as `127.0.0.1:8888`. Edge forwards control requests
there; public SDK traffic still terminates TLS at Edge and internal RPC remains
mTLS. HTTPS is the default. Wildcard, hostname and non-loopback HTTP listeners are
rejected.

Managed Redis listening beyond loopback requires `password_file`, an absolute
path to a deployment-owned secret (32–512 printable non-space ASCII bytes).
`adxctl` renders `requirepass` into its private configuration and keeps Redis
protected mode enabled. Set the matching credentials in the shared `redis_url`
(URI-escape special characters); protect the deployment file because that URL
contains a secret. Components use that URL for both storage and discovery.

Master and Node Manager examples enable loopback `metrics_listen` on ports 19090 and 19091. See [instance and resource metrics](../../../docs/testing/instance-resource-metrics.md) for metric definitions and external collection.

部署示例已启用组件日志滚动与 gzip 压缩，所有历史保留限制按组件计算。配置和异常处理见[组件日志滚动与压缩](../../../docs/testing/log-rotation.md)。
