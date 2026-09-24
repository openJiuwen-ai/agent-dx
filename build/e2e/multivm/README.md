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

`auth_accept.py` extends the L0 check across the public HTTPS Ingress. It
requires the administrator key in addition to the owner tenant key. The case
creates a temporary key for another tenant, verifies that invalid and
cross-tenant keys cannot read or delete the owner's Sandbox, verifies the
administrator-only key API, revokes the temporary key and waits for cached
authentication to expire. The report contains no key material. Run it before
fault injection on a deployed three-VM environment:

```sh
python3 build/e2e/multivm/auth_accept.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --admin-token-file /path/to/admin-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/auth
```

Its result is `auth-result.json`; the case has not yet run on real VMs.

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
are separate evidence; none has yet passed on three real VMs.

`node_preferences.py` extends `MV-03` on a central deployment. It pins one
labelled anchor to each worker, then checks weighted and ordered node
preferences, Environment affinity OR, anti-affinity, and an explicit node
constraint combined with OR conditions. Each public SDK result is checked
against the persisted assignment, the single physical sandboxd backend and
a real command. It deletes its own instances and verifies terminal cleanup.

```sh
python3 build/e2e/multivm/node_preferences.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/node-preferences
```

This case has not yet passed on real VMs.

`runtime_affinity.py` tests the runtime-fit rule merged into `refactor` on
a heterogeneous deployment. Both workers must report live sandboxd runtime
inventories, and exactly one must advertise `runsc`. The case creates an
unpinned public SDK Sandbox with `runtime='runsc'`, then checks persisted
ownership, the sole physical backend, command execution and cleanup. It
fails its preflight before creating anything on a runc-only deployment.

```sh
python3 build/e2e/multivm/runtime_affinity.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/runtime-affinity
```

This case has not yet passed on heterogeneous real VMs.

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

For the old-session write-fencing check, build the test-only Coordinator RPC
probe for the client machine's Linux target before starting the three-hour
suite budget:

```sh
cargo build -p adx-coordinator --example stale_session_probe
```

Set `coordinator_rpc_address` on the control machine inventory entry to the
Coordinator RPC address reachable from the test client. If internal RPC uses
mTLS, also set `session_probe_tls` on that entry with `ca`, `cert`, `key` and
`server_name`; the certificate must authenticate as worker 2's Node identity.
The file paths refer to files on the test client. For a deployment without
internal mTLS, leave `session_probe_tls` unset. Then run the dedicated case:

```sh
python3 build/e2e/multivm/suite.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/suite \
  --case session-fence \
  --session-probe target/debug/examples/stale_session_probe
```

The case repeats quick worker replacement, then sends `CommitEnvironment`
with the retired Node session. It requires `FailedPrecondition` and an
unchanged persisted record while both instances still serve. This case has
not yet run on three real VMs.

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

For a split-process control deployment, restart the independently supervised
Ingress with the same ownership, backend, file and public SDK route checks.
The case requires `adxctl status` to report a distinct `ingress` process;
the default embedded profile cannot satisfy this preflight.

```sh
python3 build/e2e/multivm/control_restart.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/ingress-restart \
  --role ingress
```

Neither deployment profile has passed this case on three real VMs yet.

`stop.py` is the final destructive `MV-09` case for dedicated VMs. It refuses
to run without `--confirm-dedicated` and requires both worker sandboxd
inventories to be empty before creating its own Sandboxes. It stops worker 2,
then worker 1, then the control VM through each supervisor. After each worker
stop it checks the backend inventory, persisted deletion, route withdrawal,
and continued service on any remaining worker. Run this only after all other
three-VM cases; the deployment will be stopped at the end. To record the
complete result contract's published-route count, build the test-only RPC
reader for the Linux client before the timed run:

```sh
cargo build -p adx-coordinator --example route_snapshot_probe
```

The control inventory entry needs `coordinator_rpc_address`. When internal
RPC uses mTLS, set `route_probe_tls` on that entry with `ca`, `cert`, `key`
and `server_name` paths on the client; this certificate must authenticate
as Ingress. The probe reads a full Coordinator publication snapshot after
both workers have drained and before stopping control. The per-instance
public route withdrawal checks still run after each worker stop.

```sh
python3 build/e2e/multivm/stop.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --ca /path/to/ingress-ca.pem \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/stop \
  --confirm-dedicated \
  --route-probe target/debug/examples/route_snapshot_probe
```

Without `--route-probe`, `stop.py` remains a narrower shutdown subset and
cannot supply `final_state.published_routes` for full `result.json`. This
case has not yet passed on real VMs.

After confirming the three dedicated VM identities and release digest, start
the non-resettable budget **before installing or configuring the first profile**:

```sh
python3 build/e2e/multivm/budget.py \
  --inventory out/e2e/3vm/inventory.json \
  --output out/e2e/3vm/suite \
  --budget-seconds 10800
```

This writes `budget-state.json` exclusively. Re-running the command cannot
reset its start time. The inventory and output directory must remain the same
for every profile. Without this explicit start, `suite.py` can run selected
cases with a cases-only budget, but `assemble.py` cannot mark the complete
three-VM deployment acceptance as passed.

`suite.py` runs selected cases against an already deployed profile. It writes
each case's complete `case.log` and JSON result, plus a resumable
`budget-state.json` and `suite-result.json`. With the explicit start, the
shared budget is 10,800 seconds (three hours), including deployment, failed
cases and time spent switching profiles. A case has
its own smaller timeout; on failure the remaining selected cases continue,
so failure diagnosis and regression can happen after the coverage pass.
The budget file is bound to the inventory digest and can be reused across
separate central Pack, central Spread and local-first deployments. An
interrupted runner records the in-flight time before resuming and refuses to
start another case while the previous process group is still live.
`stop` must be selected last with `--confirm-dedicated` and should only run
after the other profiles have finished. The runner reports selected-case
results; it does not claim the complete `contract.REQUIRED` result on its own.

For example, on a central Pack deployment:

```sh
python3 build/e2e/multivm/suite.py \
  --inventory out/e2e/3vm/inventory.json \
  --endpoint control.example:8443 \
  --token-file /path/to/tenant-key \
  --admin-token-file /path/to/admin-key \
  --ca /path/to/ingress-ca.pem \
  --release /path/to/adx-release.tar.gz \
  --image registry.example/team/rrt@sha256:DIGEST \
  --output out/e2e/3vm/suite \
  --case sdk --case auth --case capacity --case placement-pack --case node-preferences \
  --case worker-failure --case worker-restart --case control-restart
```

After applying central Spread, call the same runner with the same `--output`
and `--case placement-spread`; after applying local-first use
`--case local-first`. For a split-process profile, select
`--case ingress-restart` after its separate Ingress has started. Run
`--case runtime-affinity` only on a heterogeneous profile with one runsc
worker. Select `--case session-fence --session-probe PATH` for the old-session
RPC assertion after the quick-restart subset. Select
`--case stop --confirm-dedicated --route-probe PATH` last, against the
chosen final deployment. All cases remain unverified on real three-VM hosts.

After `stop`, assemble the complete result from the same suite output. The
assembler requires `sdk`, `auth`, `capacity`, both placement policies,
`node-preferences`, `local-first`, `worker-failure`, `worker-restart`,
`session-fence`, `control-restart`, `ingress-restart`, and `stop`. It checks
each case's actual assertions, release and VM identities, physical placement,
the three-hour case-runtime and wall-clock ledgers, and the final backend and published-route
counts. If `runtime-affinity` was selected, its heterogeneous inventory and
physical placement report must also pass validation. Missing or failed cases
produce a failed `result.json` with explicit
gaps; a subset report cannot be promoted to full acceptance.

```sh
python3 build/e2e/multivm/assemble.py \
  --inventory out/e2e/3vm/inventory.json \
  --suite-output out/e2e/3vm/suite
python3 build/e2e/multivm/verify.py \
  --inventory out/e2e/3vm/inventory.json \
  --result out/e2e/3vm/suite/result.json
```

Prepare the verified release, three-VM inventory and Linux test-only RPC probes
before starting the shared budget. Start it before deploying the initial
profile, then install and run every profile within that window. VM provisioning
before the inventory is fixed is outside the acceptance clock.
