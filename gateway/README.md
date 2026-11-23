# Rust Gateway

`gateway/` owns shared ingress. The nested [`apiserver`](apiserver/README.md)
crate serves the public Environment control API and embeds Ingress by default. The
root `data-plane-gateway` crate contains the reusable Ingress-to-Node data plane
gateway and the standalone `adx-ingress` fallback. Its core
CONNECT protocol identifies a workload and target endpoint; environment routing is
the first adapter, not part of the generic relay contract:

* `adx-relay` accepts HTTP/2 `CONNECT` and relays one stream to one
  admitted `targetIP:targetPort` TCP connection.
* `DataPlaneL4Connector` is the Ingress-side connector used by HTTP, WebSocket,
  SSH and port-forwarding adapters.
* `IngressRouteResolver` reads the shared in-memory `RouteStore`. The production Ingress discovers Coordinator through Redis, receives an initial full route snapshot followed by gRPC deltas, and verifies API keys through Coordinator with a bounded short-lived cache.

With `agent-api`, managed Agent requests cache immutable templates. In standalone
Activator mode, AgentBinding operations use deterministic rendezvous hashing over individual
instance URLs or Redis membership discovery; discovery refresh runs in the
background and requests use a local member snapshot. Embedded Activator mode calls
the local module without cross-instance AgentBinding affinity. Both modes share AgentBinding activation caching
and explicit bypass semantics. Both modes automatically start membership leases, change subscriptions and background reconciliation when `registration` is configured; omitting it keeps request-driven processing. There is no separate `reconciliation` configuration field. Embedded background ownership does not change local request calls. Registration does not expose an internal HTTP listener; remote Activator discovery must exclude embedded members without that interface. See the
[Agent deployment and cache contract](../agent/README.md#activator-%E5%8F%91%E7%8E%B0%E4%B8%8E-agentbinding-%E4%BA%B2%E5%92%8C%E8%B7%AF%E7%94%B1).

The typed public Ingress → Relay → EXECD operations are defined by the
[`data-plane.yaml`](../platform/api/openapi/data-plane.yaml) OpenAPI contract.
Generic port forwarding and reverse tunnels carry application-defined protocols
and remain outside that typed API.

The default `activity-client` feature builds the managed Ingress and Relay entrypoints. Embedded and standalone Ingress both use `ingress::IngressService`; only the owning process changes. Standalone Ingress requires `ADX_INGRESS_CONTROL_CONFIG` pointing to [ingress-control.json](../build/config/examples/ingress-control.json). Coordinator must map the Ingress client certificate to the `ingress` role. Public listener TLS and Ingress-to-Node security remain separate settings. The optional `etcd-watch` library and legacy test fixtures are retained separately; the managed Ingress entrypoint uses the new Coordinator route service.

业务网关如何使用 Agent v2 可参见[最小示例](examples/business-gateway/README.md)。

The gateway is a member of the root Cargo workspace. Run from repository root:

```sh
make data-plane-gateway-dev
make data-plane-gateway-ut
cargo build --locked -p data-plane-gateway --all-features --release
```

Root targets stage host-native binaries in `target/` (or `CARGO_TARGET_DIR`). Optional Linux static builds use `gateway/scripts/build-static-linux.sh` and `build/images/Dockerfile.gateway-static`; these require a Linux builder and are not part of the native migration checks. The source adx split-wheel/Buildkite/image harness is recorded in the migration manifest; it depended on the old full control-plane tree.

The node never resolves DNS, accepts a user API Key, or chooses an arbitrary target
address. The target IP must be present in
`ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS`; for the current environment adapter this is
the node's environment bridge CIDR. The Ingress chooses the TCP port. Ingress-to-Node
defaults to plaintext H2/TCP inside a network-isolated trust boundary; optional
mTLS, Node peer CIDR admission, and Ingress client CIDR admission are implemented
in-process. Production configuration fails closed when required CIDR or mTLS
settings are absent.

Example development launch:

```text
ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR=/run/adx \
ADX_DATA_PLANE_RELAY_BIND=0.0.0.0:8443 \
ADX_DATA_PLANE_RELAY_ADVERTISE_ADDRESS=node-a.internal:8443 \
ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS=10.88.0.0/16 \
ADX_DATA_PLANE_ALLOWED_INGRESS_CIDRS=127.0.0.1/32 \
ADX_DATA_PLANE_INGRESS_NODE_SECURITY_MODE=network \
cargo run --bin adx-relay
```

Ingress-to-Node has one shared deployment mode. `network` (the default) uses
plaintext H2/TCP and relies on a mandatory Ingress-only network boundary; `mtls`
adds mutually authenticated TLS. Untrusted or cross-cloud networks must use
mTLS.

The remaining platform ACL TODO is to install an immutable environment-egress
policy that rejects protected internal CIDRs, with only platform-managed DNS
and runtime-control exceptions. Until that is implemented, deployments must
provide equivalent environment-veth/host-firewall isolation before enabling
`network` mode. User NetworkPolicy must not be able to weaken that baseline.
`ADX_DATA_PLANE_RELAY_ALLOW_ANY_INGRESS=1` /
`ADX_DATA_PLANE_INGRESS_ALLOW_ANY_CLIENT=1` remain development-only ACL escapes.
By default, Ingress has separate TLS and plaintext listeners. Direct is accepted only on the
TLS listener and always requires a user token. The generic relay supports anonymous tunnel and port-forwarding/SSH routes. The managed Coordinator route resolver forces token authentication for port-forwarding/SSH; such routes require TLS. A published `portForwardRoutes` entry can
require a token for one target port; that port is then rejected on the plaintext
listener and authenticated on the TLS listener. Plaintext requests carrying
`Authorization`, `X-Auth`, or a token query parameter are rejected so credentials
cannot accidentally cross the clear-text entrypoint.
When `ADX_DATA_PLANE_INGRESS_PORT_HOST_DOMAIN=example.com` is set, the HTTP
port-forwarding route also accepts `Host: <instance-id>-<port>.example.com` and
passes the complete request path and query to the instance. A matching Host
route takes precedence over management and configured proxy paths. Unrelated
Hosts retain the existing path routing. Deployments serving this form to
browsers need wildcard DNS and a TLS certificate covering the subdomains; the
Ingress still applies the same route publication and authentication checks.
When E2B routing is enabled, a Host valid in both the native `<instance-id>-<port>`
and E2B `<port>-<sandbox-id>` forms defaults to the native target. To select the
E2B target instead, supply both `E2b-Sandbox-Id` and `E2b-Sandbox-Port` matching
its E2B Host interpretation. The selected target retains its published access
policy; an unavailable or unauthorized native target does not fall back to E2B.
Frontend is not a data-plane hop. Ingress removes credentials before opening Node
streams, so user API Keys never reach Node or the workload.

New Ingress-to-Node physical H2 connections complete a PING/PONG exchange before
entering the pool. TCP, optional TLS, and this protocol check share the pool's
connect timeout. A timeout or cancellation of the opening future closes the
socket without retaining a connection driver. This adds one round trip when
creating a physical connection; reused connections do not repeat the check.

Ordinary Direct HTTP requests use a bounded keep-alive pool keyed by the full
endpoint identity `(node, instance, workload, environment IP, port)`.
Consequently, a connection never moves between environments or execution bindings.
WebSocket/Upgrade and raw CONNECT traffic remain one logical H2 stream per client
connection. Route changes and Ingress drain invalidate matching idle HTTP
connections. Active sessions record the route-store revision at admission;
queued events from before admission cannot cancel a newly opened download.
Later revocations still close old sessions, including when an identical endpoint
is subsequently restored. HTTP pool keys include that instance's revision so a
new request cannot reuse a connection admitted before revocation; changes to
other instances do not change its pool key. Requests waiting for a connection
recheck the admitted revision and authorization metadata before forwarding.
An obsolete, unsent admission may retry the current route once within its original
deadline and authorization scope. Authorization changes are rejected; transient
route failures return a retryable response with `requestForwarded: false`.
Requests already sent are never replayed. Delayed revocations invalidate only
older pool revisions. Planned node upgrades wait for natural stream inactivity,
then close admission against the observed activity revision before checkpointing.

The pool defaults to 64 concurrent connections per endpoint, 64 idle
connections per endpoint, 1024 idle connections globally, a 3 second acquire
timeout, and a 5 second idle lifetime. An idle pooled HTTP connection is still
an active Node CONNECT stream, so the short lifetime deliberately bounds how
long pooling can defer environment idle reclamation. Continuously reused backend
HTTP connections also have a 30 second maximum lifetime: expiry prevents reuse
**after the current response completes**, and never truncates its body. This can
expose idle gaps without closing admission while clients remain active; overlapping
connections or continuous traffic can still prevent a planned upgrade. A long-lived
CONNECT, upgraded session or file transfer can exceed the pause budget; the
upgrade fails safely and reopens the source admission instead of forcibly closing
that stream. A pause budget shorter than the pool lifetime may likewise time out.
The connection and idle limits are tunable:

```text
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS=1024
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_IDLE_TIMEOUT_SEC=5
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_ACQUIRE_TIMEOUT_MS=3000
```

For an HTTP request carrying `X-ADX-Processing-Timeout-Ms` (one positive integer,
1–86400000 milliseconds), Ingress bounds authentication, route lookup, resume,
committed-route delivery and pool/CONNECT admission by the initial deadline.
Once forwarding starts, each non-empty request-body data frame renews the window
for waiting for upstream response headers. After the last upload data, the
upstream must still return its response headers within that window.
Successful `2xx` responses finish the processing window at their response headers.
Their file, tar, port-forwarded HTTP or SSE bodies may continue streaming beyond
that window. SDK read-inactivity timeouts and single-file Range retries still
apply. Error response bodies retain the latest processing deadline; WebSocket
sessions after `101` retain their independent lifetime.
Requests without this header retain their existing forwarding/streaming behavior.
The header is removed before forwarding. A cancelled or expired wake RPC returns
HTTP 503 `ENV_RESUMING` only while application bytes have not been sent, with
`Retry-After: 1`, a reserved `X-ADX-Error-Code` marker, and a structured error
containing `retry: same_operation`, `outcome: not_started`, `requestForwarded: false`
and the request/Environment IDs. Ingress strips that reserved response marker
from upstream replies. HTTP 504 after forwarding instead reports an unknown
outcome and `retry: never`; an expired error response stream with already-sent
headers is closed. Rejected checkpoint restoration reports HTTP 409 `ENV_UNAVAILABLE`.

A request's timeout or disconnection does not cancel a durable wake intent.
Independent Ingress replicas coalesce through Coordinator and can deliver retries
using their own route cache and connection pool. Without the HTTP deadline header,
wake plus committed-route delivery share one default 300-second deadline;
forwarding and upgraded-connection lifetimes otherwise retain their existing policy.
See the [SDK timeout and demand retry contract](../platform/sdk/environment/python/README.md).

Pool admission timeout returns HTTP 429. Metrics expose current idle
connections plus opened, reused, discarded, and acquire-timeout totals. The L4
connector presents the H2 CONNECT stream directly as `AsyncRead + AsyncWrite`;
there is no intermediate `DuplexStream` or per-stream byte-copy relay task.

### Configurable reverse proxy

Ingress uses a shared HTTP reverse proxy for Frontend and additional applications.
The existing `CONTROL_PLANE_ADDRESS` and `CONTROL_PLANE_ROUTES` settings continue
to select Frontend requests. To mount other applications, set
`ADX_DATA_PLANE_INGRESS_PROXY_ROUTES_FILE` to a JSON file:

```json
[
  {
    "name": "grafana",
    "path_prefix": "/grafana",
    "upstream": "http://grafana:3000",
    "strip_prefix": true
  }
]
```

Each route has a unique `name`, a `path_prefix`, and an HTTP origin `upstream`
(without a path or credentials). `strip_prefix` defaults to false. An optional
`host` restricts matching to a public hostname, ignoring case and the incoming
port. Routes are loaded and validated at startup; restart Ingress after editing the
file. Invalid or duplicate routes fail startup.

Configure these environment variables on the `ingress` service in the unified deployment JSON, then run `adxctl` as described in [process deployment](../docs/testing/process-deployment.md):

```text
ADX_DATA_PLANE_INGRESS_PROXY_ROUTES_FILE=/etc/adx/ingress-proxy-routes.json
ADX_DATA_PLANE_INGRESS_PROXY_MAX_IDLE_CONNECTIONS=512
ADX_DATA_PLANE_INGRESS_PROXY_IDLE_TIMEOUT_SEC=30
ADX_DATA_PLANE_INGRESS_PROXY_CONNECT_TIMEOUT_SEC=5
```

`adx-ingress` reads its environment; it does not accept the former `-s values.ingress...` CLI overrides. Supply TLS, `ADX_INGRESS_CONTROL_CONFIG`, authentication and deployment network settings alongside them.

Prefixes match whole path segments: `/grafana` matches `/grafana` and
`/grafana/api/live/`, but not `/grafana2`. A trailing slash in the configured
prefix is normalized. With `strip_prefix: true`, `/grafana/api/live/?x=1` becomes
`/api/live/?x=1`; with false the original path is retained. Query strings and
escaped path bytes are preserved. Host-specific routes take precedence over
host-independent routes; within that group the longest matching prefix wins.
Explicit application routes precede the legacy Frontend and environment HTTP routes.
The command-watch endpoint and CONNECT handling remain reserved. A `/` route is
therefore a catch-all for other HTTP requests on its matching host.

Application routes require the TLS ingress and use its existing client ACL.
Applications handle their own login/authorization, as Frontend does. The current
upstream transport is HTTP/1.1 over plaintext HTTP, suitable for internal HTTP
services behind Ingress's TLS termination. Upstream HTTPS is rejected at startup.

The proxy preserves the public Host, Authorization, Location, and separate
Set-Cookie headers. It replaces forwarding metadata with the client peer address,
public host, and HTTPS scheme, and adds `X-Forwarded-Prefix` when stripping a
prefix. Connection-specific headers are removed in both directions. Response
bodies are streamed, including SSE. WebSocket upgrades retain a dedicated
connection for the upgraded session and close both sides when either relay ends.
Upgrade requests open a fresh upstream connection rather than reusing an idle
HTTP connection, including when the downstream client reuses its connection.

Ingress HTTP listeners also accept legacy `GET ... HTTP/1.0` WebSocket
handshakes with `Connection: upgrade` and `Upgrade: websocket`. Compatibility
applies only when `Content-Length` is absent or zero and `Transfer-Encoding`
is absent. Header names and upgrade tokens are case-insensitive. Ingress
normalizes the request version before Hyper parses it, so both the client
connection and the HTTP/1.1 upstream connection can upgrade normally. Successful
handshakes receive `HTTP/1.1 101 Switching Protocols`.

The handshake can follow earlier requests on the same keep-alive connection,
including requests with fixed-length or chunked bodies and trailers. Ordinary
request bodies and upgraded WebSocket bytes pass through unchanged. A rejected
upgrade can be retried on the same connection when the response permits reuse.
Pre-read buffering uses Hyper's default HTTP/1 read-buffer bound; unrecognized
framing passes to Hyper for validation without normalization.

The standalone acceptance script uses a loopback TLS Ingress fixture, Execd
tunnel-only listeners, and the Python SDK `TunnelClient`. It sends HTTP/1.0
handshakes on the wire for control and application WebSockets, verifies
connection reuse after tunneled HTTP, and checks `tunnel hello`, text, binary
data, and ping/pong. With the Environment SDK dependencies and OpenSSL installed:

```bash
export CARGO_TARGET_DIR="$PWD/out/tunnel-http10-target"
cargo build --locked -p adx-execd --bin adx-execd -j 2
cargo build --locked -p data-plane-gateway --example tunnel_http10 -j 2
EXECD_RUNTIME="$CARGO_TARGET_DIR/debug/adx-execd" \
INGRESS_RUNTIME="$CARGO_TARGET_DIR/debug/examples/tunnel_http10" \
PYTHONPATH=platform/sdk/environment/python \
python3 gateway/tests/tunnel_http10_interop.py --output out/tunnel-http10
```

The script generates short-lived fixture certificates and writes handshake
evidence to `result.json` inside the output directory. `--tls` accepts an
existing directory containing `ca.pem`, `ingress.pem`, and `ingress.key`.

For Grafana with the stripping route above, configure its external URL, for
example `GF_SERVER_ROOT_URL=https://example.com/grafana/`, and its internal server
protocol as HTTP (`GF_SERVER_PROTOCOL=http`). Keep `serve_from_sub_path` false
with this prefix-stripping configuration. Grafana then generates the correct
subpath links, redirects, and cookie paths; Ingress does not rewrite HTML or
application-generated Location/Cookie paths. Grafana Live uses the same route
at `/grafana/api/live/`. See the
[Grafana reverse proxy guide](https://grafana.com/tutorials/run-grafana-behind-a-proxy/).

Connections are reused per upstream origin. Transport failures return to the
caller without automatically replaying requests. Pool defaults and overrides:

```text
ADX_DATA_PLANE_INGRESS_PROXY_MAX_IDLE_CONNECTIONS=512
ADX_DATA_PLANE_INGRESS_PROXY_IDLE_TIMEOUT_SEC=30
ADX_DATA_PLANE_INGRESS_PROXY_CONNECT_TIMEOUT_SEC=5
```

The idle limit is per origin and controls retained connections, not concurrent
requests. Set it to 0 to disable keep-alive reuse for comparisons. Busy responses
can open additional connections. Idle and TCP connect timeouts must be non-zero;
they do not impose a response deadline on long-running create or SSE requests.
A response connection is reused only after the HTTP message completes; canceling
an incomplete response discards that connection.

The data-plane processes raise their inherited soft `RLIMIT_NOFILE` to 65,536
by default without exceeding the hard limit or reducing a higher inherited
value. Override that target with `ADX_NOFILE_SOFT_LIMIT`; Node stream
admission is calculated only after the limit is applied. TCP listeners retry
transient accept failures with bounded exponential backoff, so an FD pressure
event does not permanently remove an ingress or health listener.

Both Rust processes can write bounded service logs. Ingress additionally writes a
separate access/audit file containing request ID, peer, ingress security,
access kind, instance/target port, status and duration. CONNECT completion
records include bytes in both directions and the close outcome. URI query
strings and credentials are never logged.

```text
ADX_DATA_PLANE_LOG_DIR=<directory>             # enables rolling files
ADX_DATA_PLANE_LOG_MAX_SIZE_MB=40              # size per active/rotated file
ADX_DATA_PLANE_LOG_MAX_FILES=10                # retained rotated files
ADX_DATA_PLANE_LOG_QUEUE_CAPACITY=32768         # bounded records per file writer
ADX_DATA_PLANE_LOG_FLUSH_INTERVAL_MS=200        # background flush interval
ADX_DATA_PLANE_LOG_STDOUT=true                 # true by default
ADX_DATA_PLANE_LOG_COMPRESSION=gzip            # gzip (default) or none
ADX_DATA_PLANE_INGRESS_ACCESS_LOG_ENABLED=true
```

The resulting files are `ingress-frontend.log`, `ingress-frontend-access.log`, and
`relay.log`, with `.1.gz` through `.N.gz` suffixes for older generations.
Gzip compression is the default; `ADX_DATA_PLANE_LOG_COMPRESSION=none` disables it. Each
tracing event is formatted into one record and offered to a bounded queue
without waiting for disk I/O. A dedicated writer thread performs rotation and
flushes to the operating-system page cache every 200 ms by default; it does not
`fsync` each record. Queue overflow drops the new record and emits a rate-limited
warning to stderr instead of stalling the data plane. Rotation hands the closed
file to a separate compressor thread, so gzip cannot pause queue consumption.
Compression failure retains the uncompressed staging file and reports an error
rather than deleting log data. When a dedicated access file exists, enabled access/audit events go there. Without a file sink, enabled events enter stdout. The access and audit enable switches are independent; disabling one drops its corresponding events.

Ingress also replaces Traefik for the existing public control-plane
surface. Only a fixed set of paths such as `/api/environment`, `/functions`,
`/serverless`, `/terminal`, `/invocations`, and `/global-scheduler` is proxied to the configured
`ADX_DATA_PLANE_INGRESS_CONTROL_PLANE_ADDRESS`. These paths are accepted
only on the TLS listener. Their authentication remains owned by Frontend and
the original Authorization headers are preserved on that hop.

The Ingress directly exposes HTTP/direct and standard HTTP CONNECT ingress; it is
not placed behind Traefik. Native clients such as SSH use the included local adapter, which opens
CONNECT and presents a local TCP socket:

```text
adx-data-plane-forward port-forward \
  ingress.example.com:8080 <instance-id> 22 127.0.0.1:10022
ssh -p 10022 user@127.0.0.1
```

Client-to-Ingress TLS is selected by port: the TLS listener defaults to `8443` and
the plaintext tunnel/port-forwarding listener defaults to `8080`. Ingress
terminates TLS itself; configure the adapter with `ADX_DATA_PLANE_FORWARD_TLS_CA` and optionally
`ADX_DATA_PLANE_FORWARD_TLS_SERVER_NAME`. Direct TLS is mandatory. The
canonical internal header spelling follows Frontend style:
`X-Adx-Instance-Id`, `X-Adx-Workload-Id`, `X-Adx-Target-Ip`,
`X-Adx-Endpoint-Generation`, `X-Adx-Target-Port`, and `X-Request-Id`. HTTP field
names are case-insensitive; HTTP/2 requires lowercase names on the wire, so the
Rust h2 implementation encodes the same names as `x-adx-*` / `x-request-id`.

Set `ADX_DATA_PLANE_RELAY_ACTIVITY_UDS_DIR` to a dedicated node-local
control directory. Adxlet's `proxy_socket` must be `<dir>/route.sock`.
The managed Relay starts with admission closed. `GetBindingState`,
`BeginBindings`, and `ReplaceBindings` establish a complete binding snapshot;
only successful replacement opens admission. Beginning synchronization retires
existing relay sessions. Individual updates carry the proxy process UUID,
synchronization epoch, ownership generation and binding revision. A previous
controller or proxy session cannot reactivate an old binding. Adxlet
replays its complete local catalog when the proxy process restarts.
See [publication and recovery contract](../docs/testing/route-publication.md).

Activity snapshots go to Adxlet at `<dir>/adxlet.sock` using
`adx.node.v1.NodeActivityService`. Each complete snapshot carries the registered
proxy session and an increasing sequence. The reporting interval is configured
with `ADX_DATA_PLANE_RELAY_ACTIVITY_INTERVAL_SEC` (default 30 seconds).
Stream changes trigger a batch after a 10 ms coalescing window. Connection
failures retry at most once per second without stopping data traffic.

Adxlet validates the session and snapshot order. An absent or expired
observation is unknown, never evidence of idleness. Session registration, startup reconciliation and process assembly are wired by Adxlet in both process modes. See [protocol boundaries](../platform/api/proto/README.md).

The source layout follows the process and responsibility boundary:

```text
src/bin/       Node, Ingress, and local port-forward process entrypoints
src/common/    CONNECT metadata and RouteInfo model
src/node/      H2 admission, TCP relay, activity tracking/client
src/ingress/      route store/watcher, resolver, H2 and backend HTTP pools,
               direct H2 L4 connector, HTTP/WebSocket and CONNECT serving
```

Run the complete local protocol matrix with:

```text
cargo test --locked --all-features --test mock_e2e
```

It starts real in-process Node and Ingress listeners plus mock environment HTTP and TCP
services and covers readiness, tenant admission, direct HTTP, WebSocket,
CONNECT tunnel, port-forwarding, SSH/raw TCP, route retirement, status errors,
drain, H2 reuse, and activity-count convergence.

The historical `gateway/tests/real_process_mock.sh` and Lima harness below target the previous etcd/IAM bootstrap; they need adaptation before use with the managed Ingress entrypoint. For the new Redis/mTLS route and real H2/TCP collaboration check, run `python3 build/ci/run.py control-rpc` with `ADX_TEST_REDIS_SERVER` set. This fixture does not launch sandboxd or EXECD.

The reusable Lima topology has a separate project harness:

```text
LIMA_HOME=~/.lima-adx-local-3vm \
LIMACTL=/path/to/limactl \
bash gateway/tests/local_3vm_e2e.sh
```

It deploys one Ingress on `adx-coordinator`, one Relay on each worker,
and environment-like services behind worker-local netns/veth environment IPs. It covers
TLS direct/auth, plaintext standard CONNECT port forwarding, static control
proxying, two-node routing, a 65-second byte-idle stream, relay byte metrics,
latency/throughput, secret-free access/audit records, and size rotation. The
VM IPs are rediscovered on every run and all processes/netns are removed before
the reusable VMs are stopped. Evidence is retained under
`.adx-cache/data-plane-gateway-3vm/<run-id>/`.

For the current public Environment SDK platform acceptance, use [build/e2e](../build/e2e/README.md). The old full-cluster AIO harness was not imported and is not a runnable command in this repository.

The relay library supports tunnel, SSH and configured user-port forwarding. Public create validates forwarded ports and data-plane security settings, and Coordinator publishes the resulting routes. Generic reverse-proxy routes can forward Agent traffic to a separately supplied service; they do not implement the Agent backend.

For current production logging, use [structured collection](../docs/testing/log-collection.md) and [supervisor rotation](../docs/testing/log-rotation.md). Gateway's optional own file writer above is an alternative sink; the unified JSON collection deployment leaves it disabled to avoid double writing. Trace propagation and export are implemented in Ingress and Relay, see [distributed traces](../docs/testing/distributed-traces.md). Ingress `/metrics` exposes aggregate HTTP response classes and response-header latency buckets from 1 ms through 120 s, plus `+Inf`; it includes control-plane routes and does not measure the completion time of a streaming response body. Relay `/metrics` is served on its separate health listener, which defaults to `127.0.0.1:18443` unless `ADX_DATA_PLANE_RELAY_HEALTH_BIND` is set. Ingress also exports the owning Linux process's CPU time, resident/virtual memory, open file descriptors, threads and uptime through the shared `adx-observability` sampler. With embedded Ingress, these samples cover the API Server and Ingress together under one `process_pid`; with standalone Ingress, they cover the Ingress process. Scrape that endpoint once per process rather than treating the embedded roles as two processes.

When Relay rejects an obsolete execution binding before CONNECT opens, Ingress
waits up to 50 ms for that environment's route delta and opens the replacement
once. Unrelated updates do not extend this budget. Tenant or security-policy
changes reject reuse of the previous authorization; unchanged conflicts remain
errors. This recovery sends no application bytes to the rejected execution and
does not replay a command or an HTTP request already delivered to Execd.

### Management boundary

Ingress forwards data and calls Activator for AgentBinding activation. Standalone
Activator uses the API Server's existing Environment REST API; embedded Activator uses
its application service. The Inline HTTP/JWT compatibility adapter and Ingress
`/api/environment/v2/instances` endpoint have been removed. Configure standalone
Activator with an API Server management URL and tenant-scoped API key environment
references (see [Activator](../agent/activator/README.md)). Common Environment file
access uses the API Server instance directory for admission and the shared data
plane for Relay/Execd transport. PlatformEnvironment and its runtime lookup have been
removed.

### Explicit Environment HTTP/WS access

A Gateway built with `agent-api` also accepts
`/agent/http[/business/path]?target=urn:adx:instance:<environment-id>&port=<port>`
and `/agent/ws` with the same query. Agent v2 authenticates exclusively with X-ADX-AUTH. HTTP and WS forwarding consume that header and preserve application Authorization. Instance access requires an explicit nonzero
port, HTTPS and a tenant API Key authorized for the existing Environment.
URN components are percent-encoded before URL query encoding. HTTP rejects
WebSocket Upgrade; WS requires GET and WebSocket Upgrade.

This path does not need `ADX_AGENT_CONFIG`, a Template, Binding or Activator. The caller first creates the Environment through the public
Environment API. Ingress reuses the normal authorized port-forwarding route,
including route publication and traffic-triggered resume. It strips platform
credentials, initialization headers and routing query parameters before
forwarding the business path, query, HTTP body or WebSocket traffic. Instance
access neither creates a Binding nor emits Binding identity headers. Managed
Template/Binding access keeps its existing activation behavior.

### Alias targets

Managed HTTP/WS/SSH targets use `urn:adx:template:<template>:<reference>`;
reference selects a numeric version or a named alias. HTTP/WS selects a binding
with the optional `X-ADX-Binding-Id` header; SSH uses a separate `binding_id`
route option. Omitting the selector generates a new ID. Binding URNs are not
accepted. The ID is percent-encoded UTF-8 on the wire; invalid/empty/duplicate
headers and binding headers on instance targets are rejected. Gateway consumes
the request header and returns the selected `X-ADX-Binding-Id`, overriding backend
values and removing the obsolete `X-ADX-Binding-Urn` response header. See the
[Agent access contract](../agent/README.md#统一-httpws-访问).
The long-lived Binding scope stores an explicit version or alias reference; its
current Environment carries the actual immutable template version and ID. Service
validation and Alias version selection run inside Activator before Binding/Environment creation. Gateway forwards the scope/protocol/port and uses the returned target/port; a hot resolve requires one RPC for both numeric and Alias scopes. An unsent refresh can observe a newer current
without changing Binding generation; an implicit port is selected again from the
new version, while an explicit port remains a caller constraint. No request or
handshake is replayed after bytes are sent.

Management GET/DELETE and list operations also accept
`/api/agent/v2/templates/<template>/aliases/<alias>/bindings[/<binding_id>]`.
Alias configuration uses tenant-authenticated `GET/PUT /api/agent/v2/templates/<template>/aliases/<alias>` in both modes. PUT accepts `{seed, versions, expected_revision?}` and returns `{alias:{config,revision}}`; template/alias come from the URL and tenant comes from the verified key. The private aliases/put endpoint is an internal service RPC;
see [alias setup](../agent/activator/README.md#别名收敛配置).
Platform graceful Environment shutdown is deferred; current deletion can interrupt
old connections. Activator requests deletion without observing connection counts.

### Managed Agent service access

With `agent-api`, `AgentApi::service_access()` supplies `AgentV2Access` backed by
its existing ManagedService. `select` validates the authenticated tenant's binding
scope and declared service, retaining the request deadline and binding generation.
`connect_service` opens an authorized Relay stream and can refresh the same
generation once after an unsent route/connection failure. Before any backend WS handshake or business bytes are sent, WS admission may refresh once when authoritative metadata confirms that the Binding was deleted or rebuilt, retaining its original deadline and initialization input. HTTP/SSH remain generation-fenced. Authorization failures do
not reactivate the target. Returned streams never replay application writes.

Managed HTTP keeps the backend connection pool. When a cold service refuses a connection before receiving any request bytes, Ingress waits for the listener within the original deadline.

### Common Environment file access

With `agent-api`, set `ADX_ENVIRONMENT_FILES_CONFIG` to a JSON configuration:
[standalone](examples/environment-files.json) or
[embedded](examples/environment-files-embedded.json). `token_env` references a protected
process environment variable containing the server-owned Execd token; no inline
secret is stored in JSON. `port` must equal the Ingress `default_direct_port`.
Standalone Ingress uses `directory` and `api_key_envs` to call the existing API
Server `GET /api/instances?instance_id=...`; keys must be tenant-scoped, never admin
keys. Embedded Ingress receives `EnvironmentService` directly and rejects HTTP directory
configuration. Both require the requested Environment to belong to the tenant and be
Running or a resumable Pausing/Paused/Resuming state. Authorized transfers use the
shared Gateway EnsureRunning path before opening Execd; directory reads alone never wake.
Each frontend upload/download request owns one `Files` context: its first
backend operation queries the directory, and the result is reused for all metadata,
secret/registration reads, body chunks, and upload/commit operations in that request.
A multipart request also shares admission across its files. A new frontend request
must query again; failed admission is not repeatedly queried within the same context.
Neither lookup activates an Environment or returns credentials.

The context pins the admitted runtime route. Each operation checks local route
currency and retained route-change events; a target change, removal, or lost event
history ends the transfer, even if the old route is subsequently restored. In-flight
I/O retains the existing cancellation checks. File connections do not follow a
replacement execution, and failed operations are not automatically replayed. All
operations share the original deadline.

`Files` owns bounded reads, metadata, upload and commit over shared authorized
Relay streams.
Public `/direct` requests targeting the configured Execd port use the same
`ExecdAccess` authorization and server-owned credential; clients do not supply
Execd tokens. Files never pass through API Server.

API Server runtime profile, node and Ingress must use the same Execd port and
Secret. Cross-process configuration agreement is not automatically verified: node
environment overrides can change the actual
credential. Different per-node credentials and one-sided rotation are unsupported.
There is no old `spec.env` credential fallback; all participating processes must
use the new configuration before using this binary.

### Network policy for embedded template sandboxes

Both Activator deployment modes read platform ACL from `egress_isolation`. Put the
shared settings under `embedded` in `ADX_AGENT_CONFIG` for embedded deployment or
at the root of `ADX_ACTIVATOR_CONFIG` for standalone deployment. Both combine
platform policy and template egress rules in the Environment create request. See the
[Activator network configuration](../agent/activator/README.md#模板沙箱网络隔离).

命令状态订阅 `/api/environment/v1/commands/watch` 在已配置 Execd access 时，会在后端 WebSocket 握手前检查 Environment 归属并注入服务端 Execd 令牌；调用方的业务凭据不会转发给 Execd。该认证与 `/direct` HTTP 路径共用同一配置。


### Explicit HTTP E2B deployments

E2B management and envd/application routes require TLS by default. An operator
may explicitly set `ADX_DATA_PLANE_INGRESS_E2B_ALLOW_HTTP=true` together with
E2B enabled to admit those routes on the existing plaintext listener, including
HTTP/2. API keys, sandbox envd tokens, private-port traffic credentials, route
fencing and Relay restrictions remain enforced. Native control/administrator,
Execd, Agent and CONNECT policies remain unchanged. No forwarded header can
assert this opt-in. See the [configuration and limits](../docs/deployment/e2b-compatibility.md#显式-http-入口).

### Explicit ingress listeners

`ADX_DATA_PLANE_INGRESS_LISTENERS` optionally replaces the default public listeners with a JSON array. Each entry has `listen`, `tls` (default false), `allow_http_credentials` (default false), and optional `allowed_routes`. With the variable absent, existing TLS/plaintext binds and certificate requirements remain unchanged. An explicit HTTP-only list does not require public TLS certificates. Internal RPC security is configured independently.

```json
[{"listen":"127.0.0.1:8080","tls":false,"allow_http_credentials":true,
  "allowed_routes":[{"method":"GET","path":"exact:/healthz"},{"path":"prefix:/api/environment"},{"path":"prefix:/direct"}]}]
```

Credential-enabled HTTP must be explicitly selected on trusted interfaces. Authentication and tenant checks remain active. It does not enable E2B HTTP unless the E2B HTTP option is also enabled. The default plaintext listener continues rejecting credentials. HTTPS listeners cannot set `allow_http_credentials`.

Route filters run before dispatch. `method` is an optional HTTP method; `path` uses existing `exact:` or segment-bounded `prefix:` syntax. Missing `allowed_routes` retains all routes; an empty list denies every route. These restrictions apply to that public listener only, including health endpoints. They do not configure host firewall or network reachability.
