# ADX public HTTP contracts

This directory contains the machine-readable contracts for the public ADX HTTP
surface. The Rust API Server and Gateway/EXECD implementations remain the source
of truth; changes to a public route must update the matching contract in the
same change.

- [`sandbox.yaml`](sandbox.yaml) describes the public Sandbox management API,
  the current schedulable node-resource view used by the Sandbox SDK, and the
  service-authenticated v2 Sandbox capability used by Agent components. API
  Server serves these routes through Ingress.
- [`data-plane.yaml`](data-plane.yaml) describes the typed runtime data API
  forwarded by Ingress and Relay to EXECD: health, invocation, resumable upload,
  download, PTY and command-watch WebSocket setup.

The generic `/{instanceId}/{port}/{path}` and `/tunnel/{instanceId}/...`
surfaces proxy application-defined protocols. They are routing contracts rather
than typed ADX payloads and are not expanded as OpenAPI operations. EXECD
checkpoint cooperation is an internal Adxlet contract documented in
[`../http/runtime-control.md`](../http/runtime-control.md).
