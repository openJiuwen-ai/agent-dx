# Sandbox runtime failure responses

Sandboxrouter returns a lifecycle response when an authoritative instance observation says the sandbox cannot accept a request. The response applies to HTTP requests and WebSocket handshakes before response headers have been sent.

| Instance state | HTTP | `code` | `retryable` |
| --- | --- | --- | --- |
| FATAL | 410 | SANDBOX_EXITED | false |
| FAILED | 503 | SANDBOX_RECOVERING | true |
| SCHEDULE_FAILED | 409 | SANDBOX_SCHEDULE_FAILED | false |

FATAL includes a normal process return. Inspect `exit_code` and `exit_type` to distinguish normal and abnormal exits. The message and numeric fields come from `InstanceInfo.instanceStatus`; sandboxrouter does not infer OOM from exit code 137 or parse reason text into a new classification.

An authenticated owner calling the RRT control port receives:

```json
{
  "code": "SANDBOX_EXITED",
  "message": "sandbox was oom-killed by the kernel (memory cgroup limit exceeded)",
  "instance_id": "example-sandbox",
  "state": "FATAL",
  "exit_code": 137,
  "exit_type": 5,
  "err_code": 100,
  "retryable": false
}
```

The numbers above illustrate the wire format. Actual values are copied from the instance state. Responses use `application/json` and `Cache-Control: no-store`. Empty state messages receive a generic lifecycle description. When router authentication is disabled, control-port details follow that deployment setting. Public service ports and public tunnel handshakes receive the lifecycle code, state, retryability and a generic message; tenant identity, process exit metadata and diagnostic text are omitted.

PAUSED continues to return HTTP 409. When no lifecycle rejection applies, a missing instance or unpublished port continues to return 404. Failure to read authoritative state returns 503 `route unavailable`. An upstream transport failure stays 502/504 unless the observed failure belongs to the exact instance and runtime dispatched to; a replacement runtime's failure must not explain an earlier request.

## State convergence

Running port routes remain the fast path. Non-running observations trigger the existing bounded etcd read-through, with concurrent reads coalesced by sanitized instance ID. A newer watch event wins over an in-flight read. Within one instance generation, older versions are rejected; etcd revisions also order observations across generations. Raw instance IDs are retained alongside the sanitized routing index. Ambiguous sanitized identities reject resolution without exposing either instance's diagnostics.

After an instance record is deleted, failure information is retained for ten minutes from the first observed deletion, with at most 4,096 deleted records. Repeated deletion or absent-route reads do not extend that period. New instance generations or recovered RUNNING state replace the old failure. Expiration and capacity eviction clear the corresponding frontend summary. This is a process-local bounded retention window; it does not provide durable exit history across frontend restarts or replicas that never observed the failure.

Retained FATAL and SCHEDULE_FAILED summaries do not preemptively reject a new create using the same name. The backend remains responsible for rejecting a name that is still occupied. FAILED and PAUSED summaries continue to prevent conflicting creates. A successful replacement's instance observation replaces the retained diagnostic.

## SDK compatibility

The Python SDK treats 410 and 409 as immediate `SandboxError` results and includes the response body. They do not increment route-miss counters or disable direct invocation. A FAILED response uses the existing bounded 503 retry behavior and preserves the request ID. Only 404 retains the existing direct-route fallback semantics. Upload/download HTTP errors also include the failure body.

## Validation

Run `go test -race ./sandboxrouter/...` from `pkg/frontend` with the repository's configured module/cache environment. Coverage includes state-to-HTTP mapping, authentication and ownership, public-port redaction, runtime identity at transport failure, deletion retention/expiry/capacity, restored routes, sanitized identity collisions, and stale/concurrent observations.

The SDK tests in `python/tests/test_transport_direct.py` check that a terminal OOM response is returned after one request without fallback and that a recovering response converges through the existing direct retry path.

For deployment acceptance, create a dedicated runsc sandbox with a small memory cgroup limit and launch a process that exceeds that limit. Record the sandboxd Wait result and the matching FunctionSystem runtime/instance identity. Wait for the instance's FATAL observation, then invoke through `/direct`: require HTTP 410, the same exit code and reason, and a single SDK failure without fallback. Confirm the summary survives the backend record's deletion, then clean up the dedicated sandbox. A killed child process alone is insufficient: acceptance requires sandboxd Wait to report the sandbox exit. Repeat with recovery enabled to verify a fresh runtime becomes routable. Keep this deployment test distinct from injected-state HTTP tests.
