# Kubernetes load acceptance

This directory holds optional two-worker Full deployment load cases. They are
not part of Standalone or the basic Buildkite gate. Run each with
`build/e2e/kubernetes/targeted_suite.py` using one verified immutable bundle and
`--budget-seconds` no greater than 10800. The suite records failures and
continues to the next case only after namespace cleanup is verified. Diagnose
and rerun failures after all selected cases have been attempted.

| Case | Workload | Pass condition |
|---|---|---|
| `load-performance` | 90 seconds of concurrent command/file operations and alternating create/delete on two physical workers | Public SDK operations remain correct; record create/delete/command/file counts, P50/P95/P99, maximum latency and operations per second. No fixed latency SLA is imposed on an unspecified cluster. |
| `load-pressure` | Two 2000m CPU holders fill both workers, then eight concurrent 2000m create requests wait in the central queue before the holders are released | All eight are observed waiting, none overcommits, all drain with distinct identities and correct physical ownership, Coordinator/Node resource metrics agree, and cleanup leaves no physical backend. |
| `mixed-soak` | Existing five-minute concurrent command/file traffic plus instance churn | Minimum operation counts, zero business errors, placement on both workers, percentile report and final cleanup. |

The Kubernetes driver rejects two Pods on the same physical host. Each run
uses a new namespace, retains per-case logs, result JSON, JUnit and actual Pod
placement, and must confirm namespace deletion. The three cases are optional:
use `--profile full --case load-performance`, `load-pressure`, or `mixed-soak`
for a single run; use `targeted_suite.py --case ...` repeatedly to share one
three-hour budget across all three. Do not copy these cases into the
Standalone profile without defining and validating a single-host capacity
contract.
