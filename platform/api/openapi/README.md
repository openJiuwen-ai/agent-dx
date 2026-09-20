# ADX public HTTP contracts

This directory contains the machine-readable contracts for the public ADX HTTP
surface. The Rust API Server and Gateway/RRT implementations remain the source
of truth; changes to a public route must update the matching contract in the
same change.

- [`sandbox.yaml`](sandbox.yaml) describes the public Sandbox management API
  served by API Server through Edge and the service-authenticated v2 Sandbox
  capability used by Agent components.
- [`data-plane.yaml`](data-plane.yaml) describes the typed runtime data API
  forwarded by Edge and Node Proxy to RRT: health, invocation, resumable upload,
  download and command-watch WebSocket setup.

The generic `/{instanceId}/{port}/{path}` and `/tunnel/{instanceId}/...`
surfaces proxy application-defined protocols. They are routing contracts rather
than typed ADX payloads and are not expanded as OpenAPI operations. RRT
checkpoint cooperation is an internal Node Manager contract documented in
[`../http/runtime-control.md`](../http/runtime-control.md).
