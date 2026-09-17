# Rust Data Plane Gateway

This crate contains the reusable Edge-to-Node data plane gateway. Its core
CONNECT protocol identifies a workload and target endpoint; sandbox routing is
the first adapter, not part of the generic relay contract:

* `adx-node-proxy` accepts HTTP/2 `CONNECT` and relays one stream to one
  admitted `targetIP:targetPort` TCP connection.
* `DataPlaneL4Connector` is the Edge-side connector used by HTTP, WebSocket,
  SSH and port-forwarding adapters.
* `EdgeRouteResolver` reads the shared in-memory `RouteStore`. The production Edge discovers Master through Redis, receives an initial full route snapshot followed by gRPC deltas, and verifies API keys through Master with a bounded short-lived cache.

The default `activity-client` feature builds the managed Edge and Node Proxy entrypoints. Edge requires `ADX_EDGE_CONTROL_CONFIG` pointing to [edge-control.json](../build/config/examples/edge-control.json). Master must map the Edge client certificate to the `edge` role. Public listener TLS and Edge-to-Node security remain separate settings. The optional `etcd-watch` library and legacy test fixtures are retained separately; the managed Edge entrypoint uses the new Master route service.

The gateway is a member of the root Cargo workspace. Run from repository root:

```sh
make data-plane-gateway-dev
make data-plane-gateway-ut
cargo build --locked -p data-plane-gateway --all-features --release
```

Root targets stage host-native binaries in `target/` (or `CARGO_TARGET_DIR`). Optional Linux static builds use `gateway/scripts/build-static-linux.sh` and `build/images/Dockerfile.gateway-static`; these require a Linux builder and are not part of the native migration checks. The source adx split-wheel/Buildkite/image harness is recorded in the migration manifest; it depended on the old full control-plane tree.

The node never resolves DNS, accepts a user API Key, or chooses an arbitrary target
address. The target IP must be present in
`ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS`; for the current sandbox adapter this is
the node's sandbox bridge CIDR. The Edge chooses the TCP port. Edge-to-Node
defaults to plaintext H2/TCP inside a network-isolated trust boundary; optional
mTLS, Node peer CIDR admission, and Edge client CIDR admission are implemented
in-process. Production configuration fails closed when required CIDR or mTLS
settings are absent.

Example development launch:

```text
ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR=/run/adx \
ADX_DATA_PLANE_NODE_PROXY_BIND=0.0.0.0:8443 \
ADX_DATA_PLANE_NODE_PROXY_ADVERTISE_ADDRESS=node-a.internal:8443 \
ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS=10.88.0.0/16 \
ADX_DATA_PLANE_ALLOWED_EDGE_CIDRS=127.0.0.1/32 \
ADX_DATA_PLANE_EDGE_FRONTEND_NODE_SECURITY_MODE=network \
cargo run --bin adx-node-proxy
```

Edge-to-Node has one shared deployment mode. `network` (the default) uses
plaintext H2/TCP and relies on a mandatory Edge-only network boundary; `mtls`
adds mutually authenticated TLS. Untrusted or cross-cloud networks must use
mTLS.

The remaining platform ACL TODO is to install an immutable sandbox-egress
policy that rejects protected internal CIDRs, with only platform-managed DNS
and runtime-control exceptions. Until that is implemented, deployments must
provide equivalent sandbox-veth/host-firewall isolation before enabling
`network` mode. User NetworkPolicy must not be able to weaken that baseline.
`ADX_DATA_PLANE_NODE_PROXY_ALLOW_ANY_EDGE=1` /
`ADX_DATA_PLANE_EDGE_FRONTEND_ALLOW_ANY_CLIENT=1` remain development-only ACL escapes.
Edge has separate TLS and plaintext listeners. Direct is accepted only on the
TLS listener and always requires a user token. The generic relay supports anonymous tunnel and port-forwarding/SSH routes. The managed Master route resolver forces token authentication for port-forwarding/SSH; such routes require TLS. The new control backend does not publish user ports. A `portForwardRoutes` entry can
require a token for one target port; that port is then rejected on the plaintext
listener and authenticated on the TLS listener. Plaintext requests carrying
`Authorization`, `X-Auth`, or a token query parameter are rejected so credentials
cannot accidentally cross the clear-text entrypoint.
Frontend is not a data-plane hop. Edge removes credentials before opening Node
streams, so user API Keys never reach Node or the workload.

New Edge-to-Node physical H2 connections complete a PING/PONG exchange before
entering the pool. TCP, optional TLS, and this protocol check share the pool's
connect timeout. A timeout or cancellation of the opening future closes the
socket without retaining a connection driver. This adds one round trip when
creating a physical connection; reused connections do not repeat the check.

Ordinary Direct HTTP requests use a bounded keep-alive pool keyed by the full
endpoint identity `(node, instance, workload, sandbox IP, port)`.
Consequently, a connection can never move between sandboxes or survive an
route change. WebSocket/Upgrade and raw CONNECT traffic remain
one logical H2 stream per client connection. Route changes and explicit Node
Proxy retirement,
and drain immediately invalidate matching idle HTTP connections.

The pool defaults to 64 concurrent connections per endpoint, 64 idle
connections per endpoint, 1024 idle connections globally, a 3 second acquire
timeout, and a 5 second idle lifetime. An idle pooled HTTP connection is still
an active Node CONNECT stream, so the short lifetime deliberately bounds how
long pooling can defer sandbox idle reclamation. These settings are tunable:

```text
ADX_DATA_PLANE_EDGE_FRONTEND_BACKEND_HTTP_MAX_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_EDGE_FRONTEND_BACKEND_HTTP_MAX_IDLE_CONNECTIONS=1024
ADX_DATA_PLANE_EDGE_FRONTEND_BACKEND_HTTP_MAX_IDLE_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_EDGE_FRONTEND_BACKEND_HTTP_IDLE_TIMEOUT_SEC=5
ADX_DATA_PLANE_EDGE_FRONTEND_BACKEND_HTTP_ACQUIRE_TIMEOUT_MS=3000
```

Pool admission timeout returns HTTP 429. Metrics expose current idle
connections plus opened, reused, discarded, and acquire-timeout totals. The L4
connector presents the H2 CONNECT stream directly as `AsyncRead + AsyncWrite`;
there is no intermediate `DuplexStream` or per-stream byte-copy relay task.

### Configurable reverse proxy

Edge uses a shared HTTP reverse proxy for Frontend and additional applications.
The existing `CONTROL_PLANE_ADDRESS` and `CONTROL_PLANE_ROUTES` settings continue
to select Frontend requests. To mount other applications, set
`ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_ROUTES_FILE` to a JSON file:

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
port. Routes are loaded and validated at startup; restart Edge after editing the
file. Invalid or duplicate routes fail startup.

Configure these environment variables on the `edge` service in the unified deployment JSON, then run `adxctl` as described in [process deployment](../docs/testing/process-deployment.md):

```text
ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_ROUTES_FILE=/etc/adx/edge-proxy-routes.json
ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_MAX_IDLE_CONNECTIONS=512
ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_IDLE_TIMEOUT_SEC=30
ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_CONNECT_TIMEOUT_SEC=5
```

`adx-edge-frontend` reads its environment; it does not accept the former `-s values.edge_frontend...` CLI overrides. Supply TLS, `ADX_EDGE_CONTROL_CONFIG`, authentication and deployment network settings alongside them.

Prefixes match whole path segments: `/grafana` matches `/grafana` and
`/grafana/api/live/`, but not `/grafana2`. A trailing slash in the configured
prefix is normalized. With `strip_prefix: true`, `/grafana/api/live/?x=1` becomes
`/api/live/?x=1`; with false the original path is retained. Query strings and
escaped path bytes are preserved. Host-specific routes take precedence over
host-independent routes; within that group the longest matching prefix wins.
Explicit application routes precede the legacy Frontend and sandbox HTTP routes.
The command-watch endpoint and CONNECT handling remain reserved. A `/` route is
therefore a catch-all for other HTTP requests on its matching host.

Application routes require the TLS ingress and use its existing client ACL.
Applications handle their own login/authorization, as Frontend does. The current
upstream transport is HTTP/1.1 over plaintext HTTP, suitable for internal HTTP
services behind Edge's TLS termination. Upstream HTTPS is rejected at startup.

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
subpath links, redirects, and cookie paths; Edge does not rewrite HTML or
application-generated Location/Cookie paths. Grafana Live uses the same route
at `/grafana/api/live/`. See the
[Grafana reverse proxy guide](https://grafana.com/tutorials/run-grafana-behind-a-proxy/).

Connections are reused per upstream origin. Transport failures return to the
caller without automatically replaying requests. Pool defaults and overrides:

```text
ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_MAX_IDLE_CONNECTIONS=512
ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_IDLE_TIMEOUT_SEC=30
ADX_DATA_PLANE_EDGE_FRONTEND_PROXY_CONNECT_TIMEOUT_SEC=5
```

The idle limit is per origin and controls retained connections, not concurrent
requests. Set it to 0 to disable keep-alive reuse for comparisons. Busy responses
can open additional connections. Idle and TCP connect timeouts must be non-zero;
they do not impose a response deadline on long-running create or SSE requests.
A response connection is reused only after the HTTP message completes; canceling
an incomplete response discards that connection.

The data-plane processes raise their inherited soft `RLIMIT_NOFILE` to 65,536
by default without exceeding the hard limit or reducing a higher inherited
value. Override that target with `ADX_DATA_PLANE_NOFILE_SOFT_LIMIT`; Node stream
admission is calculated only after the limit is applied. TCP listeners retry
transient accept failures with bounded exponential backoff, so an FD pressure
event does not permanently remove an ingress or health listener.

Both Rust processes can write bounded service logs. Edge additionally writes a
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
ADX_DATA_PLANE_EDGE_FRONTEND_ACCESS_LOG_ENABLED=true
```

The resulting files are `edge-frontend.log`, `edge-frontend-access.log`, and
`node-proxy.log`, with `.1.gz` through `.N.gz` suffixes for older generations.
Gzip compression is the default; `ADX_DATA_PLANE_LOG_COMPRESSION=none` disables it. Each
tracing event is formatted into one record and offered to a bounded queue
without waiting for disk I/O. A dedicated writer thread performs rotation and
flushes to the operating-system page cache every 200 ms by default; it does not
`fsync` each record. Queue overflow drops the new record and emits a rate-limited
warning to stderr instead of stalling the data plane. Rotation hands the closed
file to a separate compressor thread, so gzip cannot pause queue consumption.
Compression failure retains the uncompressed staging file and reports an error
rather than deleting log data. When a dedicated access file exists, enabled access/audit events go there. Without a file sink, enabled events enter stdout. The access and audit enable switches are independent; disabling one drops its corresponding events.

Edge Frontend also replaces Traefik for the existing public control-plane
surface. Only a fixed set of paths such as `/api/sandbox`, `/functions`,
`/serverless`, `/terminal`, `/invocations`, and `/global-scheduler` is proxied to the configured
`ADX_DATA_PLANE_EDGE_FRONTEND_CONTROL_PLANE_ADDRESS`. These paths are accepted
only on the TLS listener. Their authentication remains owned by Frontend and
the original Authorization headers are preserved on that hop.

The Edge directly exposes HTTP/direct and standard HTTP CONNECT ingress; it is
not placed behind Traefik. Native clients such as SSH use the included local adapter, which opens
CONNECT and presents a local TCP socket:

```text
adx-data-plane-forward port-forward \
  edge.example.com:8080 <instance-id> 22 127.0.0.1:10022
ssh -p 10022 user@127.0.0.1
```

Client-to-Edge TLS is selected by port: the TLS listener defaults to `8443` and
the plaintext tunnel/port-forwarding listener defaults to `8080`. Edge
terminates TLS itself; configure the adapter with `ADX_DATA_PLANE_FORWARD_TLS_CA` and optionally
`ADX_DATA_PLANE_FORWARD_TLS_SERVER_NAME`. Direct TLS is mandatory. The
canonical internal header spelling follows Frontend style:
`X-Adx-Instance-Id`, `X-Adx-Workload-Id`, `X-Adx-Target-Ip`,
`X-Adx-Endpoint-Generation`, `X-Adx-Target-Port`, and `X-Request-Id`. HTTP field
names are case-insensitive; HTTP/2 requires lowercase names on the wire, so the
Rust h2 implementation encodes the same names as `x-adx-*` / `x-request-id`.

Set `ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_UDS_DIR` to a dedicated node-local
control directory. Node Manager's `proxy_socket` must be `<dir>/route.sock`.
The managed Node Proxy starts with admission closed. `GetBindingState`,
`BeginBindings`, and `ReplaceBindings` establish a complete binding snapshot;
only successful replacement opens admission. Beginning synchronization retires
existing relay sessions. Individual updates carry the proxy process UUID,
synchronization epoch, ownership generation and binding revision. A previous
controller or proxy session cannot reactivate an old binding. Node Manager
replays its complete local catalog when the proxy process restarts.
See [publication and recovery contract](../docs/testing/route-publication.md).

Activity snapshots go to Node Manager at `<dir>/node-manager.sock` using
`adx.node.v1.NodeActivityService`. Each complete snapshot carries the registered
proxy session and an increasing sequence. The reporting interval is configured
with `ADX_DATA_PLANE_NODE_PROXY_ACTIVITY_INTERVAL_SEC` (default 30 seconds).
Stream changes trigger a batch after a 10 ms coalescing window. Connection
failures retry at most once per second without stopping data traffic.

Node Manager validates the session and snapshot order. An absent or expired
observation is unknown, never evidence of idleness. Session registration, startup reconciliation and process assembly are wired by Node Manager in both process modes. See [protocol boundaries](../platform/api/proto/README.md).

The source layout follows the process and responsibility boundary:

```text
src/bin/       Node, Edge, and local port-forward process entrypoints
src/common/    CONNECT metadata and RouteInfo model
src/node/      H2 admission, TCP relay, activity tracking/client
src/edge/      route store/watcher, resolver, H2 and backend HTTP pools,
               direct H2 L4 connector, HTTP/WebSocket and CONNECT serving
```

Run the complete local protocol matrix with:

```text
cargo test --locked --all-features --test mock_e2e
```

It starts real in-process Node and Edge listeners plus mock sandbox HTTP and TCP
services and covers readiness, tenant admission, direct HTTP, WebSocket,
CONNECT tunnel, port-forwarding, SSH/raw TCP, route retirement, status errors,
drain, H2 reuse, and activity-count convergence.

The historical `gateway/tests/real_process_mock.sh` and Lima harness below target the previous etcd/IAM bootstrap; they need adaptation before use with the managed Edge entrypoint. For the new Redis/mTLS route and real H2/TCP collaboration check, run `python3 build/ci/run.py control-rpc` with `ADX_TEST_REDIS_SERVER` set. This fixture does not launch sandboxd or RRT.

The reusable Lima topology has a separate project harness:

```text
LIMA_HOME=~/.lima-adx-local-3vm \
LIMACTL=/path/to/limactl \
bash gateway/tests/local_3vm_e2e.sh
```

It deploys one Edge Frontend on `adx-master`, one Node Proxy on each worker,
and sandbox-like services behind worker-local netns/veth sandbox IPs. It covers
TLS direct/auth, plaintext standard CONNECT port forwarding, static control
proxying, two-node routing, a 65-second byte-idle stream, relay byte metrics,
latency/throughput, secret-free access/audit records, and size rotation. The
VM IPs are rediscovered on every run and all processes/netns are removed before
the reusable VMs are stopped. Evidence is retained under
`.adx-cache/data-plane-gateway-3vm/<run-id>/`.

For the current public Sandbox SDK platform acceptance, use [build/e2e](../build/e2e/README.md). The old full-cluster AIO harness was not imported and is not a runnable command in this repository.

The relay library supports tunnel, SSH and configured user-port forwarding; that does not imply the new control plane publishes user ports. Current public create rejects user-port publication and per-Instance data-plane security overrides. Generic reverse-proxy routes can forward Agent traffic to a separately supplied service; they do not implement the Agent backend.

For current production logging, use [structured collection](../docs/testing/log-collection.md) and [supervisor rotation](../docs/testing/log-rotation.md). Gateway's optional own file writer above is an alternative sink; the unified JSON collection deployment leaves it disabled to avoid double writing. Trace propagation and export are implemented in Edge and Node Proxy, see [distributed traces](../docs/testing/distributed-traces.md).
