# Installed deployment example acceptance

This driver runs the package installer, verifies `/opt/adx/current`, and starts the shipped
`etc/examples/deployment.yaml` at `/opt/adx/config/deployment.yaml`. It compares the
source and installed file hashes. The five ADX roles use that configuration;
only external sandboxd, Redis, registry and test certificates are prepared by
the fixture. The SDK uses Firecracker with one vCPU and 512 MiB.

Run on the selected **dedicated** Linux KVM host as root. The driver refuses to
replace any existing `/opt/adx`, `/usr/local/bin/adxctl`, or sandboxd socket. These paths are owned for the duration of this test and
removed during cleanup. It requires unused example ports and sufficient disk
for two sequential Firecracker instances.

Stage a directory outside the installation paths, with:

- `package/`: verified release package for the host architecture;
- `execd.tar`: OCI image archive containing the same package's EXECD;
- `client/`: Python virtualenv containing Sandbox SDK dependencies; the driver
  installs the package wheel offline into it;
- `tools/`: `docker-registry`, `redis-cli` and `virtiofsd`;
- `e2e/example/`: this directory's Python files;
- `e2e/firecracker/configure.py`, `e2e/publish.py`,
  `e2e/rpc_certificates.py` (from `build/ci/`), and `e2e/package.py`
  (from `build/release/`).

The external runtime must already be prepared at `/opt/adx-fc`: `bin/sandboxd`,
`bin/sbox`, `bin/firecracker` and their helpers; `artifacts/Image` and
`artifacts/initrd.img`. See [Firecracker prerequisites](../firecracker/README.md).
The prerequisite helper also generates an auxiliary deployment configuration;
it is not used to launch the ADX roles in this test.

```sh
sudo env ADX_EXAMPLE_BASE=/opt/adx-example \
  python3 -u /opt/adx-example/e2e/example/run.py /var/lib/adx-example-run-001
```

The run directory must be new. `export/evidence/result.json` requires all six
cases: CLI validation/rendering, five live roles and automatic resource
observation, administrator-created tenant key through HTTPS Ingress, SDK command
and binary file round-trip, explicit deletion, and supervisor stop deleting a
remaining instance. Redis terminal state, empty backend inventory, independent
sandboxd/Redis survival after platform stop and clean fixture shutdown are
required. Component logs, status, package manifest, SDK IDs and final catalog
are exported. Private keys/configuration stay outside exported evidence;
API keys are redacted.

A pass proves the single-host process installation example. Kubernetes and
cross-node recovery have separate acceptance drivers.
