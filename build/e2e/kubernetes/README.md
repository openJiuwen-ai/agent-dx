# Kubernetes process-deployment acceptance

The Buildkite gate uses `run.py` here. `manifest.py` defines two node Pods,
Services, volume/credential references and node placement. The Pods run ADX as
processes from the unified release. `publish_images.py` runs on the build worker
and hands immutable registry references to the deployment worker.

```sh
# Build stage, after build/e2e/prepare.py. Docker registry authentication is
# configured externally on the builder.
python3 build/e2e/kubernetes/publish_images.py \
  --bundle out/buildkite/bundle --repository registry.example/team/adx-e2e

# Deployment stage. Optional --registry-auth is a Docker config JSON file;
# optional --context explicitly selects a context from the supplied kubeconfig.
python3 build/e2e/kubernetes/run.py \
  --bundle out/buildkite/bundle/bundle.json \
  --registry-images out/buildkite/bundle/registry-images.json \
  --kubeconfig /path/to/target-kubeconfig \
  --output out/e2e/kubernetes-run-001
```

Every invocation requires a new evidence directory and creates a unique test
namespace. Target images must be digest pinned. Registry TLS verification is
on; private registry credentials are mounted read-only and supplied to both
Kubernetes and sandboxd. No cluster selection or credential material is embedded
in the source tree.

Five scenario groups are shared with the local driver. Kubernetes-specific
contract tests cover manifests, Secret projection, Service addresses, immutable
artifact handoff and namespace ownership/cleanup failures. These tests do not
execute a Kubernetes cluster. Pod readiness alone does not pass the platform
readiness gate, and deleting a namespace alone cannot satisfy the physical
instance-cleanup scenario.

State and artifact cache are ephemeral for this acceptance deployment. Redis
still writes AOF to its Pod-local state volume; persistence across Pod deletion
is outside these scenarios. Kubernetes requests/limits bound the node container;
the fixture reads its cgroup limits before advertising ADX capacity.

See [Buildkite setup](../../../.buildkite/README.md) for the reused CI workers, target kubeconfig mount and SWR secrets.

The target worker pool must already provide EROFS and bridge netfilter with
`bridge-nf-call-iptables=1`. The fixture checks these read-only before starting
sandboxd. Select eligible nodes with repeated `--node-name` arguments, or set
`ADX_E2E_NODE_NAMES` to comma-separated Kubernetes node names in Buildkite.
The scheduler still applies the Linux/architecture and resource requirements.
A one-node pool runs two isolated Pods on one host; it does not validate
cross-host networking. Actual host placement is retained in the acceptance logs.

The E2E job log streams deployment commands and their output as they run, with
10-second progress notices for long waits. It shows resource creation, Pod/image
readiness, node prerequisites, sandboxd readiness, ADX registration/routes and
cleanup. Structured Kubernetes JSON is retained in numbered artifact logs;
readable Pod placement is printed in the job log. Secret payloads remain on stdin
and known credential values are redacted before writing or streaming output.

Each scenario emits RUN, PASS/FAIL and elapsed time. Its child-process output is
streamed without buffering, including SDK instance IDs, command/file assertions,
authentication checks, capacity wait/resume, placement rules with expected/actual
node assignments, and restart recovery checks.
`case-results.json` retains per-case outcomes and duration; JUnit lists all seven
scenarios separately, with unexecuted scenarios marked skipped and cleanup
reported independently. A failed command or timeout still fails the acceptance.
