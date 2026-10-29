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
Ingress has separate TLS and plaintext listeners. Direct is accepted only on the
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
Frontend is not a data-plane hop. Ingress removes credentials before opening Node
streams, so user API Keys never reach Node or the workload.

New Ingress-to-Node physical H2 connections complete a PING/PONG exchange before
entering the pool. TCP, optional TLS, and this protocol check share the pool's
connect timeout. A timeout or cancellation of the opening future closes the
socket without retaining a connection driver. This adds one round trip when
creating a physical connection; reused connections do not repeat the check.

Ordinary Direct HTTP requests use a bounded keep-alive pool keyed by the full
endpoint identity `(node, instance, workload, environment IP, port)`.
Consequently, a connection can never move between environments or survive an
route change. WebSocket/Upgrade and raw CONNECT traffic remain
one logical H2 stream per client connection. Route changes and explicit Node
Proxy retirement,
and drain immediately invalidate matching idle HTTP connections.

The pool defaults to 64 concurrent connections per endpoint, 64 idle
connections per endpoint, 1024 idle connections globally, a 3 second acquire
timeout, and a 5 second idle lifetime. An idle pooled HTTP connection is still
an active Node CONNECT stream, so the short lifetime deliberately bounds how
long pooling can defer environment idle reclamation. These settings are tunable:

```text
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS=1024
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_IDLE_TIMEOUT_SEC=5
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_ACQUIRE_TIMEOUT_MS=3000
```

For an HTTP request carrying `X-ADX-Processing-Timeout-Ms` (one positive integer,
1–86400000 milliseconds), Ingress creates one deadline after receiving its headers.
Authentication, route lookup, resume, committed-route delivery, pool/CONNECT
admission, upstream processing and response streaming consume that same deadline.
The header is removed before forwarding. A cancelled or expired wake RPC returns
HTTP 503 `ENV_RESUMING` only while application bytes have not been sent, with
`Retry-After: 1`, a reserved `X-ADX-Error-Code` marker, and a structured error
containing `retry: same_operation`, `outcome: not_started`, `requestForwarded: false`
and the request/Environment IDs. Ingress strips that reserved response marker
from upstream replies. HTTP 504 after forwarding instead reports an unknown
outcome and `retry: never`; an expired stream with already-sent response headers
is closed. Rejected checkpoint restoration reports HTTP 409 `ENV_UNAVAILABLE`.

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
and `/agent/ws` with the same query. Instance access requires an explicit nonzero
port, HTTPS and a tenant API Key authorized for the existing Environment.
URN components are percent-encoded before URL query encoding. HTTP rejects
WebSocket Upgrade; WS requires GET and WebSocket Upgrade.

This path does not need `ADX_AGENT_CONFIG`, a Template, Binding, Activator or the
Jiuwen module. The caller first creates the Environment through the public
Environment API. Ingress reuses the normal authorized port-forwarding route,
including route publication and traffic-triggered resume. It strips platform
credentials, initialization headers and routing query parameters before
forwarding the business path, query, HTTP body or WebSocket traffic. Instance
access neither creates a Binding nor emits Binding identity headers. Managed
Template/Binding access keeps its existing activation behavior.

### Alias targets

Managed HTTP/WS/SSH accepts `urn:adx:binding:<template>:<alias>:<binding_id>`.
The long-lived Binding scope stores an explicit version or alias reference; its
current Environment carries the actual immutable template version and ID. Service
validation and Alias version selection run inside Activator before Binding/Environment creation. Gateway forwards the scope/protocol/port and uses the returned target/port; a hot resolve requires one RPC for both numeric and Alias scopes. An unsent refresh can observe a newer current
without changing Binding generation; an implicit port is selected again from the
new version, while an explicit port remains a caller constraint. No request or
handshake is replayed after bytes are sent.

Management GET/DELETE/resolve and list operations also accept
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
Managed WS uses this path; managed HTTP shares selection/retry while retaining the
backend HTTP pool. The Jiuwen codecs, bounded session/request state and E2A socket
driver are implemented. Its internal connection adapter uses the shared authorized
Relay stream, bounds the WS handshake by the original admission deadline, and
observes route revocation during both handshake and streaming. The driver never
reconnects or replays business writes. Each frontend owns one independent E2A
connection and reuses it for its requests. Multiple frontends may connect to the
same binding, including while another handshake is pending. The gateway does not
arbitrate per-binding connection concurrency; the business service owns concurrent
session semantics. Dropping a connection releases only its own Relay stream.
Authentication, route revocation and the existing transport resource limits still
apply to each connection. Each Jiuwen frontend has separate bounded request and
output queues (`queue_capacity`, default 64). When the output queue fills, the
backend driver waits up to `output_queue_timeout_seconds` (default 5) for space;
a longer stall closes that WebSocket with code 1011. This wait is independent of
`write_timeout_seconds`, which bounds writes to the frontend socket.

The backend handshake uses `/`, Host `127.0.0.1:<service-port>` and Origin
`http://127.0.0.1:<service-port>` on the selected stream; it does not dial that URL
or forward frontend credentials. If AgentServer Origin checks are enabled, its
allowlist must include `127.0.0.1`. Local Relay/mock-backend component tests cover
authorized WS reuse, handshake timeout and route revocation. The embedded stack
uses a 2 MiB worker-stack regression fixture. See the
[design](../docs/development/jiuwenswarm-adx-gateway-design.md) for the contract and
remaining validation boundaries.

Jiuwen download settings are decoded from the selected `TemplateVersion.env` by
`digitalmategateway::download_config::DownloadConfig::from_template`. Upload/download obtain Binding identity and its actual template together from Activator's read-only bindings/view, without creating a Binding or Environment. Gateway does not choose a version from Alias weights.
The same env is already passed to AgentServer through ExecutionSpec. Templates
must explicitly set absolute `JIUWENSWARM_WORKSPACE` and
`JIUWENSWARM_DOWNLOAD_ASSET_ROOT` paths. A nonempty
`JIUWENSWARM_FILE_DOWNLOAD_SECRET` takes precedence and must contain at least 32
characters; it is kept verbatim and redacted in Debug. If absent or empty, the
adapter selects `<workspace>/config/.file_download_secret` in the authorized
Environment. No second Ingress configuration or process-environment probing is used.
Public identity and binding routing use authenticated business sessions.

`digitalmategateway::download_token` implements HMAC verification over the original Base64URL
text, claim/expiry checks, and verified registration/file-metadata comparisons.
`SignedDownload::check_file` requires active matching registration and actual file
type/size for verified assets before returning `CheckedDownload`. Digest matching
uses the registration; it does not hash the entire download. Verification input
limits are 16 KiB for a token and 64 KiB each for secret/registration contents.
`digitalmategateway::download_runtime::Reader::authorize` combines these checks with bounded
reads from the common `environment_files::Files` capability. The common data plane
checks Environment ownership/state through the API Server and supplies Execd
credentials internally; Jiuwen never receives them. It reads a key file only when needed, a registration only for verified
assets, and path metadata before admission. The original deadline covers lookup,
connect and body reads; revocation or caller cancellation drops the owned HTTP
connection immediately. Errors do not echo upstream bodies or credentials.
`digitalmategateway::download_http::Download::respond` implements internal GET/HEAD attachment
responses. Verified assets support single byte ranges (206/416) and inline;
ordinary attachments retain full 200 responses and ignore Range/inline. Both
use no-store, encoded UTF-8 filenames and extension-based MIME detection.
Execd reads are pulled in chunks of at most 64 KiB, each with fresh business
signature/registration admission, expiry checks and exact upstream range/length
validation. API Server ownership/state admission is shared by the entire HTTP
transfer, including metadata reads and all chunks. Buffered first bytes
are admitted again when consumed; backpressure does not start background reads.
The caller's deadline covers the whole transfer. Errors before headers produce
bounded JSON responses; later errors abort the body without a successful truncation.
Local Relay/simulated-Execd tests cover these paths; real Environment validation and
production account integration remains subject to the validation below. CheckedDownload is a
point-in-time result, not a reusable authorization credential.

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
operations share the original deadline; business session and download signature/
registration checks remain active throughout the transfer.

`Files` owns bounded reads, metadata, upload and commit over shared authorized
Relay streams. Jiuwen owns its business protocol and signature/workspace checks.
Public `/direct` requests targeting the configured Execd port use the same
`ExecdAccess` authorization and server-owned credential; clients do not supply
Execd tokens. Files never pass through API Server.

API Server runtime profile, node and Ingress must use the same Execd port and
Secret. Cross-process configuration agreement is not automatically verified: node
environment overrides can change the actual
credential. Different per-node credentials and one-sided rotation are unsupported.
There is no old `spec.env` credential fallback; all participating processes must
use the new configuration before using this binary.

## DigitalMateGateway authenticated entrypoints

### Transparent business WebSocket

The `digitalmategateway` business module loads `ADX_JIUWEN_CONFIG` with a
shared top-level `tenant` and peer application blocks. The required `jiuwen` block configures `/ws` and file routes; the optional
`orbit` block enables `GET /orbit/ws`. Each block owns its target and request timeout:

```json
{
  "tenant": "your-tenant",
  "jiuwen": {
    "agent_type": "assistant",
    "template": "jiuwen",
    "version": "1"
  },
  "orbit": {
    "template": "orbit",
    "template_ref": "production",
    "backend_path": "/ws",
    "service_port": 18092
  }
}
```

Both applications use the same `ADX_ACCOUNT_CONFIG`, Huawei/AGC login and
business Bearer sessions. The gateway owns one fixed Platform tenant; accounts
and sessions have no tenant field. They share account identities; the token
is not bound to an application. Business and shared account routes do not
restrict admission by Host or Origin. Deployment routing controls the public
domains and paths; business routes still require a valid session.

`jiuwen.request_timeout_seconds` and `orbit.request_timeout_seconds` each default
to 60 seconds (valid range 1–86400); Jiuwen's remaining queue and protocol timeout
settings also live inside `jiuwen`. Environment direct-port host routing retains
its existing precedence. Without `orbit`, `/orbit/ws` returns 404.
Apply the configuration with its matching Ingress build. Existing Jiuwen
binding IDs and workspace parameters stay unchanged.

Both business applications support TLS termination at an external load balancer.
Generic `/agent/http` and `/agent/ws` remain tenant API Key routes requiring the
Ingress TLS listener; business sessions do not authorize them.

Use `/orbit/ws` and `Authorization: Bearer <business-token>`.
The binding is fixed per verified user: its internal ID is `orbit-` plus the
full hexadecimal SHA-256 of compact JSON `[tenant,user_id]`. Template and
`template_ref` remain separate scope fields. `orbit.template_ref` uses the shared
reference parser: a canonical positive numeric string selects a version, and a
non-numeric string selects an Alias; for example `"12"` or `"production"`.
An Alias must exist before admission and its reconciliation requires configured
Activator registration and Redis notifications; registration automatically starts reconciliation. Reconnects reuse the same scope; different users
are isolated. There is no client-selected binding ID or binding ID response
header, ownership field, or mapping table.

The target is selected solely from the gateway configuration and verified session.
Query parameters do not participate in gateway target or binding selection, even
when named `target`, `template`, `port`, `user_id` or `binding_id`. Only the path is
replaced by the configured backend path; the query string is forwarded unchanged,
without parsing, re-encoding, name filtering or an Orbit-specific length limit.
This includes `token` and `authorization`: URL parameters never authenticate the
gateway and may have application-defined meaning. Orbit interprets its own query
parameters. General HTTP request limits still apply.
Authentication credentials (including Cookie), `X-User-Id` and `X-ADX-*`
headers are removed before forwarding. The original Host and Origin headers are
preserved; backend origin checks remain the application's responsibility. The
backend connection address and port come from the fixed service configuration.
WebSocket subprotocol negotiation and business frames remain opaque.

The entrypoint uses shared Agent activation, declared-service validation,
cold-start readiness and Gateway HTTP Upgrade/byte forwarding. It does not
implement a separate Relay client or translate Jiuwen/Orbit messages. Only
unsent connection admission may retry; sent requests and frames are not replayed.
For a missing binding it obtains `model_api_key` and `model_api_base` from the
same account model service as Jiuwen, and injects `workspace` using the shared
Jiuwen rule: `w-` plus the full SHA-256 of compact JSON `[tenant,template,user_id]`,
using Orbit's configured template. The value is stable across template versions
and distinct from a different user's or template's workspace. The
[Orbit template example](../agent/activator/examples/orbit-sfs-template.json)
omits the server-assigned version from its publish input and retains the same
application image, using a shared managed
Volume `orbit-workspace-${workspace}` in domain `sfs`, relative path
`workspaces/${workspace}`, mounted at `/data/state`. With domain root `/mnt/sfs`,
this preserves the existing workspace directory. Model parameters remain in the
environment and existing binding parameters remain unchanged. Volume initialization
declares `create_if_missing: true` and UID/GID 10001, mode `0700`; existing directory
ownership and permissions are retained. Coordinator storage-domain authorization,
node storage mounts and the SFS parent must be configured before activation.
Shared Volume access does not provide application write isolation. The old direct
bind Env does not acquire managed occupancy retroactively.

Changing a fixed-version scope to an Alias scope changes its storage key. Deploy
the matching `template_ref` configuration with this binary; the old `orbit.version`
field is not accepted. Existing string template versions become same-name Aliases
through an explicitly reviewed offline migration, with no automatic data migration.
Preserve the existing Binding and data during that maintenance operation; changing
the config alone can cause first traffic to create another Binding.
Local tests cover typed references, authenticated Alias forwarding and managed
Volume activation with a simulated Platform. Real Orbit/SFS deployment and the
manual metadata migration remain operational acceptance work.

Agent activation, connection, the post-connect session recheck and HTTP Upgrade
share the original Orbit admission deadline; the recheck cannot add its separate
account I/O timeout to the budget. The same session watcher remains active across
these stages, with route cancellation watched once the backend stream exists.
Admission expiry returns 504 and does not limit an established WebSocket's lifetime.

Missing/invalid/revoked sessions return 401. Before Upgrade, session invalidation
returns 401 and route
revocation returns `502 ROUTE_REVOKED`; backend admission failure returns
`502 AGENT_SERVER_UNAVAILABLE` and deadline expiry returns 504. After Upgrade,
session invalidation or route revocation closes the shared forwarding stream.
Account expiry/revocation checks use the existing session guard (at most a
30-second polling interval for revocation).

### Existing Jiuwen protocol and accounts

Build with `--features agent-api`. Configure `ADX_JIUWEN_CONFIG` using
[jiuwen.json](examples/jiuwen.json), and `ADX_ACCOUNT_CONFIG` using
[accounts.json](examples/accounts.json). Both standalone and embedded Ingress
require Agent configuration and `ADX_ENVIRONMENT_FILES_CONFIG` as described above. Fixed-user `local_fixed` mode
has been removed; Jiuwen requires business session tokens independently of the
listener transport. It neither checks TLS state nor trusts `X-Forwarded-Proto`
for authorization. Both HTTP and HTTPS listeners use the same business checks.
Origin is not checked.

For TLS termination at an external load balancer, bind the existing HTTP listener
with `ADX_DATA_PLANE_INGRESS_PLAIN_BIND=0.0.0.0:8080`, route the business Service
port 80 to port 8080 using HTTP, and retain HTTPS/WSS at the public listener.
Restrict the HTTP port to the deployment's internal network and configure public
domains and paths at the load balancer. This does not change the existing TLS-only
management routes or Ingress-to-Node mTLS.

`POST /api/v1/auth/huawei/login` exchanges an Account Kit authorization code and returns
an opaque business token. `POST /api/v1/auth/agc/login` accepts
`{"agcAccessToken":"...","agreementVersion":"1"}` and exchanges a current AGC Auth
user access token for the same kind of business session. First registration through
either provider requires `agreementVersion`; clients never submit an external UID,
phone number, ADX user ID, tenant, or project.

AGC verification is enabled by the optional `accounts.agc` block. The checked-in example
uses the Jiuwen CN project and reads the API client secret from
`ADX_AGC_API_CLIENT_SECRET`. `api_client_id` and that secret are server REST credentials,
not a user token. Ingress obtains a short-lived service credential from the fixed CN AGC
endpoint and calls the official `verify-token` API for every login, so revoked user tokens
are not accepted. It also requires the returned `productId` and the remotely verified JWT
`iss`, `aud`, `sub`, and `exp` to agree with the configured project and UID. The JWT
`agc.sign_in_provider` value is not used as an account-type decision because it is not the
client SDK's anonymous-account flag. Invalid, expired, revoked, and wrong-project credentials
return `401 INVALID_AGC_ACCESS_TOKEN`; AGC/API-client/network failures return
`503 AUTH_UNAVAILABLE`. The service credential is cached until shortly before expiry;
user access tokens and verification results are not cached. Each failed verification emits one
sanitized warning with its stage, failure reason, and available upstream HTTP status/return code;
the user token, UID, profile data, and credentials are never logged.

The actual Ingress route includes the version prefix:
`/api/v1/auth/agc/login`. With a public `authBaseUrl` ending in `/api/v1`, the client
appends `/auth/agc/login`. There is no separate unversioned `/auth/agc/login` route in
the current Gateway implementation.

`POST /api/v1/auth/logout` revokes that session only. `GET /api/v1/auth/quota`
requires the business Bearer token and returns the same quota payload as
Jiuwenswarm: `{"ok":true,"available":true,"quota":{"total":30,"balance":22.5,
"used":7.5,"low_balance":false,"exhausted":false,"reset_period":"",
"reset_at":""}}`. `litellm.points_per_budget_unit` scales LiteLLM key
`max_budget`, `spend`, and their remaining balance into displayed points. It
defaults to `1.0` and must be finite and positive. With the [account example](examples/accounts.json),
`max_budget: 15.0` and `points_per_budget_unit: 2.0`, a LiteLLM budget of 15
displays as 30 points; spending 3.75 displays 7.5 used and 22.5 remaining.
Remaining points are clamped to zero: spending 20 against a budget of 15
displays 40 used and zero remaining, with `exhausted: true`.
When model prices use RMB, this means one point represents RMB 0.50. This
conversion does not change LiteLLM prices, spend, key budgets, rate limits,
or reset schedules. Missing total or used values remain `-1`; when either
is unknown, remaining points are zero without marking the budget exhausted.
Unrepresentable converted
amounts return `503 AUTH_UNAVAILABLE`. Low-balance and exhaustion flags retain
their budget semantics. Concurrent model calls may overshoot the budget.
The multiplier is read at Ingress startup; restarting with a new value changes
subsequent quota responses for both new and existing keys.
`budget_duration: null` gives a one-time key budget;
a configured duration enables resets. Existing keys retain their LiteLLM
budget policy; changes to `max_budget` and `budget_duration` apply to newly issued keys. The query
creates or reconciles the user's persisted key if needed, without creating a Sandbox. It does not
accept query parameters and returns `Cache-Control: no-store`.
`/ws`, `GET/HEAD
/file-api/download?token=...`, and `POST /file-api/upload` require
`Authorization: Bearer <business token>`.
User identity comes only from the session. On `/ws`, the optional `user_id`
query parameter is ignored, including empty or repeated values; it cannot select
another user or binding. Other query parameters and query strings over 1024 bytes
return `400 INVALID_QUERY`. Download retains its own file-token validation.
Upload accepts `multipart/form-data` with one or more `file` parts, optional
`dir`, `session_id`, `user_id`, and `agent_type` fields, and at most 64 MiB
of total request body (up to 20 files). The optional upload query parameter
`token` is ignored; it does not authorize the request or select the target.
`session_id` is accepted for client compatibility but does not change the
selected binding. The `user_id` field and `X-User-Id` header, if present, must match the
authenticated user. Upload paths are relative to the immutable template's
`JIUWENSWARM_WORKSPACE` and written to that user's bound Environment through
Relay and Execd. The response has Jiuwen's
`{"ok":true,"files":[{"filename":"...","path":"...","mime_type":"...","size_bytes":0}],"errors":[]}`
shape. Upload does not open another AgentServer WS connection.
ADX management and service credentials
remain separate. Use a Jiuwen hostname outside the direct-port host domain.

Accounts, hashed sessions and encrypted per-user LiteLLM credentials use a shared
PostgreSQL schema without tenant columns. Apply [schema.sql](src/ingress/accounts/schema.sql)
for a fresh database.
The schema version remains 1. Runtime SQL has no old-column detection or fallback.
The gateway's unchanged configured tenant is only a fixed identity namespace for
Agent scopes, workspace hashes, LiteLLM owners and encrypted-key associated data.
Keep that value and the credential encryption key unchanged to reuse existing data.

The first WS for a missing binding, or a quota query, provisions/reconciles
an account model key. The shared activation path obtains ordinary rendering parameters
from the business adapter only for a missing Binding. Jiuwen supplies `workspace`,
`model_api_key` and `model_api_base`; its Template declares `API_KEY: "${model_api_key}"`
and `API_BASE: "${model_api_base}"`. Activator renders the Template using persisted
parameters instead of overlaying a second private launch configuration. Parameters are
currently plaintext metadata; future application encryption belongs to the business
Gateway and AgentServer. Warm access does not fetch new model credentials. Model names
and `JIUWEN_PLATFORM_MODELS` remain deployment Template values, interpreted by the image.

User keys are created with `models: ["all-proxy-models"]`, covering current and
future LiteLLM proxy models. Initializing a Binding or querying quota also reconciles
an existing owner's key through `/key/update`, changing only model access and the display alias. Key
identity, spend, budget and rate limits remain unchanged. Ownership mismatch or a
blocked key is rejected before an update; failed or lost responses are retried with
the same key. Warm binding access does not call LiteLLM management: existing warm
users acquire expanded access on a quota query, new Binding initialization, or an
explicit reconciliation. The template's list controls the models shown
in Jiuwen, not the LiteLLM key's authorization boundary.

Huawei login reads the optional `display_name` from the verified ID token when
the authorization includes `profile`. The nickname is persisted and refreshed on
login; an absent or invalid nickname does not erase an existing one. Client-supplied
nicknames are not accepted. Key aliases use `huawei-<nickname>-<first 8 user-ID chars>`
with a bounded, sanitized nickname, or `huawei-<first 8 user-ID chars>` when unavailable.
AGC accounts currently do not import a display name and use
`agc-<first 8 user-ID chars>`.
Quota queries and new-binding preparation synchronize aliases for existing keys,
including requests with older valid sessions. Historical users need a new Huawei
login to populate a nickname that was never stored. Alias changes alone do not
change LiteLLM's native client error text; that requires a LiteLLM error callback.

Downloads resolve existing bindings without provisioning keys or creating Environments.

WS authenticates before upgrade and checks sessions every 30 seconds while connected;
messages do not query the account database. Logout immediately rejects new connections,
while existing WS connections close on their next periodic check or absolute expiry.
Download streams also stop on expiry/revocation. Multiple independent WS connections may
use the same binding; business concurrency is owned by AgentServer.
Platform-wide target connection limits remain deferred, as does
Execd's path/open race. Ordinary and verified download token/registry checks remain.

At the normal INFO filter, Jiuwen WebSocket logs include `connection_id`, client/
backend request mapping, response parse/projection errors, terminal handling and
socket-write results. The select winner and safe error category are logged before
connection cleanup; backend EOF/close records include the last response's request
ID, sequence and final flag. These diagnostics do not change forwarding or close
behavior and do not print message bodies, tool arguments, credentials or peer
close reasons. A completed socket write is not a peer delivery acknowledgement.

The two authentication boundaries, client requirements and verification limits are documented in the
[Jiuwen gateway design](../docs/development/jiuwenswarm-adx-gateway-design.md).


### Network policy for embedded template sandboxes

When `ADX_AGENT_CONFIG` selects an embedded Activator, `embedded.sandbox_network`
is required. API Server passes this configured policy to the Agent-side
`LocalEnvironment` adapter; the Environment API's public network contract is unchanged.
The standalone Activator uses the same mapping. See the
[Activator network configuration](../agent/activator/README.md#模板沙箱网络隔离)
for address pools, public egress and explicit internal service exceptions.

### Jiuwen persistent workspace parameters

Jiuwen prepares each new AgentBinding with `parameters.workspace = "w-" + SHA256(JSON([tenant, template, user_id]))`, using the authenticated internal user ID and the full hexadecimal digest. The directory identity excludes template version, agent_type, Binding generation and Sandbox ID. Activator receives a generic parameter map; it does not receive a new user/owner identity. Existing Bindings reuse their stored parameters.

Use the [Jiuwen SFS template example](../agent/activator/examples/jiuwen-sfs-template.json) to render a node-visible Local source into `/home/jiuwen`. Different template versions can refer to the same directory, but this mapping does not enforce exclusive access. Until workspace admission is implemented, the caller must stop the previous Sandbox completely before starting another with the same directory. The source directories must already exist; this code does not provision storage or migrate existing user data.

`jiuwen.workspace_identity` in `ADX_JIUWEN_CONFIG` defaults to `scoped` (the existing tenant/template/user
hash). AgentOS deployments can explicitly set `user` to reuse
`/home/agentos/users/${workspace}` in a template. In this mode an authenticated user
must be a safe single directory component; template/version/agent type changes
keep the same user directory while each binding retains its execution identity.
Configure a tenant-specific mount root when user IDs are not globally unique.


`GET /api/v1/auth/session` requires a valid business Bearer session and returns
its `userId` without starting an Environment. It follows DigitalMateGateway's
business authentication; management API keys remain separate from business sessions.

命令状态订阅 `/api/environment/v1/commands/watch` 在已配置 Execd access 时，会在后端 WebSocket 握手前检查 Environment 归属并注入服务端 Execd 令牌；调用方的业务凭据不会转发给 Execd。该认证与 `/direct` HTTP 路径共用同一配置。
