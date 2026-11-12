# Coordinator discovery

`RedisDiscovery` atomically reads the renewable Coordinator endpoint and the
control header's epoch. It validates the schema, endpoint and matching epoch
before returning an address. Discovery does not make Environment ownership
decisions or authenticate API keys.

A lookup uses one absolute deadline derived from its configured timeout. The
deadline includes the connection-cache mutex, connection establishment and the
Redis query. A dropped/reset connection or a transport timeout can cause at most
one retry of this fixed, read-only Lua script. The retry uses the remaining
budget. A missing or malformed record, invalid endpoint, epoch mismatch, Redis
authentication/configuration error or command rejection does not trigger that
retry. An exhausted deadline starts no new attempt.

Cached transports have independent failure flags. A failed, cancelled or timed
out read retires its own transport. Reconnection is serialized, while ordinary
reads release the cache mutex and use Redis multiplexing. A late failure from an
old transport cannot invalidate a healthy replacement. Cache invalidation does
not wait for a mutex after the deadline expires.

Attempt failures log structured `phase`, Redis error `category`, `attempt`,
`retry` and elapsed time. Deadline and record-validation failures are classified
separately. Raw Redis error text, Redis URLs, record contents and API keys are
excluded from these diagnostics.

API Server's uncached credential verification applies its original RPC budget
to discovery, channel acquisition and `VerifyApiKey` together. The RPC owns the
same absolute deadline so expiry records its failure result, error code/message
and failed trace status. Unrepresentable timeouts return a structured error.
The HTTP boundary
continues to reject unavailable authentication with 503 and invalid credentials
with 401. This recovery does not replay API create, capture, restore or other
mutations; their existing operation-identity contracts remain authoritative.

## Component regression coverage

```sh
cargo test --locked -p adx-discovery -j 2
cargo test --locked -p adx-apiserver --lib clients::discovery_tests -j 2
```

These tests use a scripted RESP peer, loopback sockets and a test gRPC auth
service. They cover cached idle close, concurrent replacement, a late old read,
healthy reuse, repeated transport failures, one shared retry deadline, lock and
connection-setup timeout, cancellation, permanent command errors, malformed and
missing records, invalid schema/address and epoch mismatch. The HTTP test checks
that the first authenticated `GET /api/instances` after an injected close returns
200 and reaches `VerifyApiKey`; invalid credentials remain rejected. A separate
test consumes part of the auth budget in discovery and verifies that the RPC
does not receive a fresh full budget.

Hanging discovery and a hanging auth RPC both retain HTTP 503 with the
`UNAVAILABLE` error code. Their tests also check that discovery expiry never
invokes `VerifyApiKey`, an auth RPC timeout records failure attribution in its
span, and unrepresentable budgets return errors rather than panicking.

The storage suite additionally uses a real Redis process behind a
connection-scoped proxy and verifies recovery after a Redis crash/restart:

```sh
export ADX_TEST_REDIS_SERVER=/absolute/path/to/redis-server
python3 build/ci/run.py storage --jobs 2
```

For only discovery storage cases, provide an evidence directory and run:

```sh
export ADX_TEST_EVIDENCE=/absolute/path/to/new-evidence-directory
mkdir -p "$ADX_TEST_EVIDENCE"
cargo test --locked -p adx-coordinator --test storage discovery_ -j 2 -- --ignored
```

The real-Redis fixture creates a private Unix socket and process for each case.
It never disconnects deployment clients. Component tests do not establish the
cause of a historical deployment disconnect or replace deployment acceptance
with the resulting release artifacts.
