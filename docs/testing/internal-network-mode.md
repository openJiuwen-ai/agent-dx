# Internal network mode validation

Date: 2026-09-22.

`internal_security: network` is an explicit profile setting. Master, Node
Manager, API Server and Edge use plaintext internal RPC; Edge-to-Node Proxy
forwarding uses the existing network transport. Component roles and node IDs
are caller declarations on the deployment network, not authenticated identities.
Node sessions, capsule ownership, tenant checks and API key verification remain.
The default is mTLS; clients reject an endpoint with the wrong scheme instead
of falling back. Public Edge HTTPS configuration is unchanged.

## Automated checks

- RED: the new network identity tests failed before `Peers::network` existed.
- Protocol, transport, discovery and deployment tests: 58 passed.
- API Server, Master and Node Manager normal tests: 203 passed, 48 ignored.
- Workspace/all-targets check and all-features Clippy with `-D warnings`: passed.
- Formatting and documentation checks: passed.

Logs are under `out/ci/internal-network/`: `red.log`, `unit.log`,
`services2.log`, `check3.log` and `clippy-final.log`.

## Real Redis and RPC

The Linux RPC suite passed all 20 tests against an isolated Redis process.
The new plaintext case covers registration/reconciliation, node identity versus
request consistency, API key verification, creation, persistence and tenant
isolation. Existing mTLS route publication, lifecycle and recovery cases passed.
The route fixture explicitly publishes its token-protected target port.

Log: `out/ci/internal-network/linux-rpc-final.log`. Optional API Server HTTPS
subprocess/Python branches were not run because `ADX_TEST_API_SERVER` was unset;
the real API/Edge path is covered by the standalone runs below.

## Real standalone validation

AKernel ran on its dedicated Linux x86_64 host with a new data directory. Its
certificate generator produced only the public HTTPS certificate and key.
There were no internal certificate files. Existing SDK server-address and token
configuration was used unchanged.

- gVisor: 6 passed, 1 custom OCI case skipped (51.158 seconds).
- Firecracker: 6 passed, 1 custom OCI case skipped (72.260 seconds).
- Both covered commands, files, PTY and the workload checkpoint Unix socket,
  reload and reverse-tunnel recovery.
- Test image: `akernel-adx-validation:network-mode`.
- Image ID: `sha256:1e6f257b828b17a735c7d8e12b59e73471ba6d326fe2b06387942c397b145e44`.
- Remote log: `/var/log/akernel-network-mode-e2e.log`.

This is an overlay of locally built ADX binaries onto the reviewed AKernel
checkpoint image. AKernel's formal #71 artifact lock still needs an updated
release containing the network-mode and workload-checkpoint changes. These
results do not establish custom OCI, live Kubernetes or cross-node validation.
