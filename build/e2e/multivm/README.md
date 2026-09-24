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

`worker_failure.py` exercises heartbeat expiry and returning-worker cleanup.
Run it only on dedicated test VMs after the SDK subset passes. The configured
SSH account on worker 2 needs non-interactive `sudo -n kill` permission. The
script stops only the adxlet process with `SIGSTOP` and always sends `SIGCONT`
on its failure path. It asserts that an initially working route is invalidated,
worker 1 continues serving, worker 2 clears the stale sandboxd backend before
readmission with a new session, and a fresh pinned Sandbox works on worker 2.
Its wait budgets are 90 seconds for heartbeat expiry and 90 seconds for
reconciliation, plus creation and cleanup time.

```sh
python3 build/e2e/multivm/worker_failure.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/worker-failure
```

This produces `worker-failure-result.json`. It is the `MV-06` subset, not the
complete Multi-VM result contract. Run `sdk_accept.py` first with the same
inventory and release archive.

`capacity_queue.py` covers the resource-exhaustion and queue-wakeup portion
of `MV-03`. It measures each worker's current allocatable CPU through the
public SDK, reserves that amount with one pinned Sandbox per worker, confirms
an additional create appears in the Coordinator's admin-only waiting queue,
releases one holder, then requires the queued create to run on that worker.
It verifies the final persisted resource state and physical backend cleanup.
Run it only on dedicated workers with at least 500 millicores and 512 MiB
currently allocatable on each. The `--admin-token-file` is separate from the
tenant token and must have administrator permission. The deployment Ingress
configuration must route `/global-scheduler/` to API Server.

```sh
python3 build/e2e/multivm/capacity_queue.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --admin-token-file /path/to/admin-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/capacity
```

Pack/Spread configuration and preference scoring remain separate `MV-03`
checks; `capacity-queue-result.json` alone is not a full `MV-03` pass.

`worker_restart.py` covers the quick-restart portion of `MV-07`: it kills only
worker 2's adxlet child, lets `adxctl` restart it, and checks that the new
node session reattaches the same sandboxd backend and assignment generation
while both Sandboxes continue serving. Its reattachment budget is 25 seconds,
below the default 30-second heartbeat expiry. Run it on dedicated VMs where
the worker 2 SSH account has non-interactive `sudo -n kill` permission.

```sh
python3 build/e2e/multivm/worker_restart.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/worker-restart
```

The old-session write-fencing assertion is not in this SDK subset; it needs
a protocol-level stale-session probe before `MV-07` is fully covered.
