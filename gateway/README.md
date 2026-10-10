# Rust Gateway

`gateway/` owns shared ingress. The nested [`apiserver`](apiserver/README.md)
crate serves the public Sandbox control API and embeds Ingress by default. The
root `data-plane-gateway` crate contains the reusable Ingress-to-Node data plane
gateway and the standalone `adx-ingress` fallback. Its core
CONNECT protocol identifies a workload and target endpoint; sandbox routing is
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
and explicit bypass semantics. See the
[Agent deployment and cache contract](../agent/README.md#activator-发现与-agentbinding-亲和路由).

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
`ADX_DATA_PLANE_ALLOWED_TARGET_CIDRS`; for the current sandbox adapter this is
the node's sandbox bridge CIDR. The Ingress chooses the TCP port. Ingress-to-Node
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

The remaining platform ACL TODO is to install an immutable sandbox-egress
policy that rejects protected internal CIDRs, with only platform-managed DNS
and runtime-control exceptions. Until that is implemented, deployments must
provide equivalent sandbox-veth/host-firewall isolation before enabling
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
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS=1024
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_MAX_IDLE_CONNECTIONS_PER_ENDPOINT=64
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_IDLE_TIMEOUT_SEC=5
ADX_DATA_PLANE_INGRESS_BACKEND_HTTP_ACQUIRE_TIMEOUT_MS=3000
```

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
Explicit application routes precede the legacy Frontend and sandbox HTTP routes.
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
surface. Only a fixed set of paths such as `/api/sandbox`, `/functions`,
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

It starts real in-process Node and Ingress listeners plus mock sandbox HTTP and TCP
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
and sandbox-like services behind worker-local netns/veth sandbox IPs. It covers
TLS direct/auth, plaintext standard CONNECT port forwarding, static control
proxying, two-node routing, a 65-second byte-idle stream, relay byte metrics,
latency/throughput, secret-free access/audit records, and size rotation. The
VM IPs are rediscovered on every run and all processes/netns are removed before
the reusable VMs are stopped. Evidence is retained under
`.adx-cache/data-plane-gateway-3vm/<run-id>/`.

For the current public Sandbox SDK platform acceptance, use [build/e2e](../build/e2e/README.md). The old full-cluster AIO harness was not imported and is not a runnable command in this repository.

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
Activator uses the API Server's existing Sandbox REST API; embedded Activator uses
its application service. The Inline HTTP/JWT compatibility adapter and Ingress
`/api/sandbox/v2/instances` endpoint have been removed. Configure standalone
Activator with an API Server management URL and tenant-scoped API key environment
references (see [Activator](../agent/activator/README.md)). Common Sandbox file
access uses the API Server instance directory for admission and the shared data
plane for Relay/Execd transport. PlatformSandbox and its runtime lookup have been
removed. This source change does not redeploy the cluster.

### Managed Agent service access

With `agent-api`, `AgentApi::service_access()` supplies `AgentV2Access` backed by
its existing ManagedService. `select` validates the authenticated tenant's binding
scope and declared service, retaining the request deadline and binding generation.
`connect_service` opens an authorized Relay stream and can refresh the same
generation once after an unsent route/connection failure. Authorization failures do
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
apply to each connection.

The backend handshake uses `/`, Host `127.0.0.1:<service-port>` and Origin
`http://127.0.0.1:<service-port>` on the selected stream; it does not dial that URL
or forward frontend credentials. If AgentServer Origin checks are enabled, its
allowlist must include `127.0.0.1`. Local Relay/mock-backend component tests cover
authorized WS reuse, handshake timeout and route revocation. After the narrowly
scoped Execd entrypoint-lock fix, real isolated Sandbox creation through the
independent Activator passed, including AgentServer argv/working directory and
service listening. The earlier embedded stack overflow was fixed and covered by a 2 MiB worker-stack
regression. Standalone cloud validation covers new Sandbox creation, cross-replica
file transfers, and real DeepSeek responses through LiteLLM. The subsequent upstream
merge passed local regression checks and has not been redeployed; see the
[design](../docs/development/jiuwenswarm-adx-gateway-design.md).
Real Huawei login and beegent device validation remain
incomplete.

Jiuwen download settings are decoded from the selected `TemplateVersion.env` by
`jiuwen::download_config::DownloadConfig::load`, using the shared ManagedService
and its tenant/template/version cache without creating a binding or Sandbox.
The same env is already passed to AgentServer through ExecutionSpec. Templates
must explicitly set absolute `JIUWENSWARM_WORKSPACE` and
`JIUWENSWARM_DOWNLOAD_ASSET_ROOT` paths. A nonempty
`JIUWENSWARM_FILE_DOWNLOAD_SECRET` takes precedence and must contain at least 32
characters; it is kept verbatim and redacted in Debug. If absent or empty, the
adapter selects `<workspace>/config/.file_download_secret` in the authorized
Sandbox. No second Ingress configuration or process-environment probing is used.
Public identity and binding routing use authenticated business sessions.

`jiuwen::download_token` implements HMAC verification over the original Base64URL
text, claim/expiry checks, and verified registration/file-metadata comparisons.
`SignedDownload::check_file` requires active matching registration and actual file
type/size for verified assets before returning `CheckedDownload`. Digest matching
uses the registration; it does not hash the entire download. Verification input
limits are 16 KiB for a token and 64 KiB each for secret/registration contents.
`jiuwen::download_runtime::Reader::authorize` combines these checks with bounded
reads from the common `sandbox_files::Files` capability. The common data plane
checks Sandbox ownership/state through the API Server and supplies Execd
credentials internally; Jiuwen never receives them. It reads a key file only when needed, a registration only for verified
assets, and path metadata before admission. The original deadline covers lookup,
connect and body reads; revocation or caller cancellation drops the owned HTTP
connection immediately. Errors do not echo upstream bodies or credentials.
`jiuwen::download_http::Download::respond` implements internal GET/HEAD attachment
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
Local Relay/simulated-Execd tests cover these paths; real Sandbox validation and
production account integration remains subject to the validation below. CheckedDownload is a
point-in-time result, not a reusable authorization credential.

### Common Sandbox file access

With `agent-api`, set `ADX_SANDBOX_FILES_CONFIG` to a JSON configuration:
[standalone](examples/sandbox-files.json) or
[embedded](examples/sandbox-files-embedded.json). `token_env` references a protected
process environment variable containing the deployment-owned Execd token; no inline
secret is stored in JSON. `port` must equal the Ingress `default_direct_port`.
Standalone Ingress uses `directory` and `api_key_envs` to call the existing API
Server `GET /api/instances?instance_id=...`; keys must be tenant-scoped, never admin
keys. Embedded Ingress receives `SandboxService` directly and rejects HTTP directory
configuration. Both require the requested Sandbox to belong to the tenant and be
Running. Each frontend upload/download request owns one `Files` context: its first
backend operation queries the directory, and the result is reused for all metadata,
secret/registration reads, body chunks, and upload/commit operations in that request.
A multipart request also shares admission across its files. A new frontend request
must query again; failed admission is not repeatedly queried within the same context.
Neither lookup activates a Sandbox or returns credentials.

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
Secret. Cross-process configuration agreement is a deployment requirement, not an
automatically verified property: node environment overrides can change the actual
credential. Different per-node credentials and one-sided rotation are unsupported.
There is no old `spec.env` credential fallback. Existing deployment manifests must
be updated to the new configuration before using this binary.

## Jiuwen authenticated entrypoints

Build with `--features agent-api`. Configure `ADX_JIUWEN_CONFIG` using
[jiuwen.json](examples/jiuwen.json), and `ADX_ACCOUNT_CONFIG` using
[accounts.json](examples/accounts.json). Both standalone and embedded Ingress
require Agent configuration and `ADX_SANDBOX_FILES_CONFIG` as described above. Fixed-user `local_fixed` mode
has been removed; Jiuwen requires business session tokens independently of the
listener transport. It neither checks TLS state nor trusts `X-Forwarded-Proto`
for authorization. Both HTTP and HTTPS listeners use the same business checks.
`allowed_origins` accepts exact HTTP or HTTPS origins, including non-default ports.

For TLS termination at an external load balancer, bind the existing HTTP listener
with `ADX_DATA_PLANE_INGRESS_PLAIN_BIND=0.0.0.0:8080`, route the business Service
port 80 to port 8080 using HTTP, and retain HTTPS/WSS at the public listener.
Restrict the HTTP port to the deployment's internal network and configure the
public hostname in `allowed_hosts`. This does not change the existing TLS-only
management routes or Ingress-to-Node mTLS.

`POST /auth/huawei/login` exchanges an Account Kit authorization code and returns
an opaque business token. First registration requires `agreementVersion`.
`POST /auth/logout` revokes that session only. `/ws`, `GET/HEAD
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
`JIUWENSWARM_WORKSPACE` and written to that user's bound Sandbox through
Relay and Execd. The response has Jiuwen's
`{"ok":true,"files":[{"filename":"...","path":"...","mime_type":"...","size_bytes":0}],"errors":[]}`
shape. Upload does not open another AgentServer WS connection.
ADX management and service credentials
remain separate. Use a Jiuwen hostname outside the direct-port host domain.

Accounts, hashed sessions and encrypted per-user LiteLLM credentials use a shared
PostgreSQL schema. Apply [schema.sql](src/ingress/accounts/schema.sql) before
startup. Ingress and standalone Activators share `ADX_CREDENTIAL_KEY_FILE`, a
protected file containing a persistent 32-byte encryption key as 64 hex characters.
Huawei application secrets and LiteLLM administrative keys remain on Ingress.

The first WS for a new binding provisions/reconciles a user model key, then
prepares an immutable private binding launch configuration. Activator overlays
that configuration on the shared template environment before Sandbox creation.
Retries and rebuilds reuse it; public Binding metadata does not return it.
Downloads resolve existing bindings without provisioning keys or creating Sandboxes.

WS checks sessions before business messages and periodically while idle; download
streams also stop on expiry/revocation. Multiple independent WS connections may
use the same binding; business concurrency is owned by AgentServer.
Platform-wide target connection limits remain deferred, as does
Execd's path/open race. Ordinary and verified download token/registry checks remain.

Deployment, two authentication boundaries, client requirements and verification
limits are documented in the
[Jiuwen gateway design](../docs/development/jiuwenswarm-adx-gateway-design.md).
