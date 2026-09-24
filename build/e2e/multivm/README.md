# Three-VM process acceptance

This directory defines the result contract for one control VM and two worker
VMs. It does not provision machines. The deployment adapter must install one
verified release on all three machines, configure the selected internal RPC
security mode, start external sandboxd on both workers, and run the public SDK
through the control VM's Ingress endpoint.

The inventory must record three unique machine IDs, hostnames and addresses,
plus `ssh_target` for each machine, `node_id` for each worker, and the clean
source commit, release SHA256 and target. Addresses must be IP addresses
assigned inside their respective VMs. A passing full result must
run all checks in `contract.REQUIRED`, prove actual placement on both worker
VMs, stop workers before the control VM, and record empty sandboxd inventories
and an empty route catalog.

Inventory shape (replace every identity, address and digest with observed
values; `ssh_target` may be an SSH config alias):

```text
{
  "schema_version": 1,
  "machines": [
    {"role": "control", "machine_id": "...", "hostname": "...", "address": "10.0.0.10", "ssh_target": "adx-control"},
    {"role": "worker-1", "machine_id": "...", "hostname": "...", "address": "10.0.0.11", "ssh_target": "adx-worker-a", "node_id": "node1"},
    {"role": "worker-2", "machine_id": "...", "hostname": "...", "address": "10.0.0.12", "ssh_target": "adx-worker-b", "node_id": "node2"}
  ],
  "artifacts": {"commit": "40 lowercase hex characters", "release_sha256": "64 lowercase hex characters", "target": "x86_64-unknown-linux-gnu"}
}
```

The control VM may set `deployment_config` when its YAML is not at
`/opt/adx/config/deployment.yaml`; each worker may set `sandboxd_socket` when
it differs from the CLI's `--socket` default.

Validate completed evidence with:

```sh
python3 build/e2e/multivm/verify.py \
  --inventory out/e2e/3vm/inventory.json \
  --result out/e2e/3vm/result.json
```

The first automated deployment adapter should wrap the repository's public SDK
assertions and `adxctl`; it must not replace them with SSH process checks. A VM
ping, supervisor PID or three unique machine IDs is only deployment evidence,
not a passing business acceptance result.

`sdk_accept.py` is a runnable subset after the three VMs have been deployed.
Run it on a client with the installed `adx-sandbox` SDK, access to Ingress and
non-interactive SSH access to all three guests. Each worker needs `sbox` on
`PATH`. The CLI verifies the release archive SHA256, each guest's machine
identity, assigned IP and installed manifest. It creates one pinned Sandbox
on each worker through the public SDK, reads the persisted assignment through
`adx-inspect` on the control VM, confirms each physical sandboxd backend
on the expected VM and its absence on the other VM, runs commands and binary
file round-trips through Ingress, and deletes its own instances.

```sh
python3 build/e2e/multivm/sdk_accept.py \
  --inventory out/e2e/3vm/inventory.json \
  --release /path/to/adx-release.tar.gz \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/sdk
```

`sdk-accept-result.json` is not a full `result.json`: it covers live VM
identity, resource discovery, persisted and physical two-worker placement, and cross-VM
command/file transport. Deployment, scheduler capacity/queue, local-first,
fault recovery, route-catalog inspection and ordered service shutdown still
require their own executable checks before `verify.py` can report full
Multi-VM acceptance. The SDK subset does not stop the shared services.
