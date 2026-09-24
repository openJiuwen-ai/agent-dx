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

`placement_policy.py` checks the Pack/Spread part of `MV-03` through two
unpinned public SDK creates and persisted/physical placement. Run it twice,
once against a deployment with Coordinator `placement: pack` and once after
deploying `placement: spread`. API Server must use central create mode; a
local-first entry bypasses global scoring. The case checks the resolved
deployment configuration and rejects nodes whose initial free-resource
fractions differ enough to make the two-request expectation ambiguous. Each
worker needs at least 1,000 millicores and 1,024 MiB free. The test releases
both Sandboxes and checks persisted deletion and resource/backend cleanup.

```sh
python3 build/e2e/multivm/placement_policy.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --placement pack \
  --output out/e2e/3vm/placement-pack
```

Repeat with a separately deployed `spread` configuration and matching
`--placement spread`. `capacity-queue-result.json` and both placement results
are separate evidence; none has yet passed on three real VMs. Node preference
scoring remains a separate `MV-03` check.

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

`local_first.py` covers `MV-04` on a dedicated three-VM deployment whose
API Server uses `create_mode: local_first`. It checks entry rotation, same-ID
concurrent claim convergence, conflicting specification rejection, actual
backend placement, Coordinator claim-log evidence, a constrained request with
an explicit Adxlet forward event into central scheduling, and exactly-once CPU
reservation in the scheduler ledger. The control inventory entry may set
`coordinator_log_dir` (default `/opt/adx/run/control/logs`). Each worker
may set `adxlet_log_dir` (default `/opt/adx/run/node/logs`). Keep those log
directories for the duration of the run. Worker 1 needs at least 1,000
millicores of available CPU; worker 2 needs at least 2,000. No other client
should allocate during the test.

```sh
python3 build/e2e/multivm/local_first.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/local-first
```

This is a runnable case, not evidence that it has passed on real VMs.

`control_restart.py` covers the default embedded-Ingress `MV-08` profile on a
pre-provisioned control VM with supervised Coordinator, API Server, and managed
Redis. It keeps one Sandbox on each worker while killing and restarting those
three child processes in turn. Each restart must preserve persisted ownership,
generation, physical backend IDs, file contents, and public SDK commands.
Coordinator restart must advance the epoch; API Server restart must not. After
each restart, both workers must again be routable. The test removes its two
Sandboxes and verifies persisted release and physical cleanup. It requires
the supervisor to restart each child within 90 seconds and does not stop the
control VM or the whole deployment.

```sh
python3 build/e2e/multivm/control_restart.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/control-restart
```

This case has not yet passed on three real VMs. A separate Ingress process
needs its own restart acceptance in the split-process profile.

`stop.py` is the final destructive `MV-09` case for dedicated VMs. It refuses
to run without `--confirm-dedicated` and requires both worker sandboxd
inventories to be empty before creating its own Sandboxes. It stops worker 2,
then worker 1, then the control VM through each supervisor. After each worker
stop it checks the backend inventory, persisted deletion, route withdrawal,
and continued service on any remaining worker. Run this only after all other
three-VM cases; the deployment will be stopped at the end.

```sh
python3 build/e2e/multivm/stop.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/stop \
  --confirm-dedicated
```

This case has not yet passed on real VMs.
