# DigitalMate transport observation candidate

Integration partner: DigitalMate PR #3058 / RFC #2921, HTR-C9.
Source baseline: agent-dx master d4a9cdc258105831a24cc0aa3e893aa50c723506.
This is an opt-in performance capture, not a new execution or authentication path.

Set DIGITALMATE_PROFILING_MODE to summary or trace on adx-ingress to enable it.
Unset, off and invalid values disable it. Summary emits completed connection
stages and per-message metadata; trace additionally emits stage starts.
Non-DigitalMate application upgrades retain their original forwarding path.

The authorized application entry places callbacks in local Request extensions,
never in client-controlled headers. The HTTP 101 upgraded byte streams retain
copy_bidirectional_with_sizes, flush, cancellation and half-close behavior.
Read observation follows accepted reads; write observation follows accepted
local writes. A Gateway write is not a client-receipt acknowledgement.

Connection stages: gateway.connection_setup, gateway.selection,
gateway.service_ready and gateway.proxy_handshake. They share a connection ID
and are charged once per connection, rather than to every subsequent message.
Selection includes existing binding/activation work; readiness includes the
existing service wait. These spans do not split unavailable Activator or
Environment-internal suboperations into invented durations.

Four message boundaries use independent bounded passive RFC6455 decoders:
gateway.client_message_received, gateway.sandbox_write_accepted,
gateway.sandbox_message_received and gateway.client_write_accepted.
Same-direction decoder sequence and connection identity pair the same message
across forwarding. Connection elapsed milliseconds come from one Instant;
wall timestamps are descriptive only. Payload assembly duration starts once
the frame header is complete; it is not an application-processing duration.

Only bounded ASCII scalar IDs, fixed classification fields, lengths and timing
metadata are serialized. No authorization, query string, prompt, tool arguments,
reply body or attachment contents are persisted by this observer. Up to 64 KiB
of a text message is transiently buffered for JSON metadata parsing. Binary,
compressed, oversized and invalid JSON messages produce a coverage gap; the
original bytes still pass through unchanged. Headers are at most 14 bytes.
Malformed framing disables observation rather than changing transport behavior.
There is no timing-based request join and no new runtime/Store writer.

The x-digitalmate-observation-id response header is a generated correlation hint,
not a routing key, identity or permission. Request RPC/submission/message IDs are
still needed for exact joins. Missing/ambiguous joins remain unavailable.

Focused tests: passive masked/fragmented/oversized/malformed frame capture;
transparent bidirectional backpressure/half-close; mode and connection lifecycle.
Current Linux dev-profile binary builds. Package all-targets lint is blocked by
pre-existing test dead_code diagnostics; dependency lint additionally reports
pre-existing OperationScope::new_without_default. Those gates are not weakened.
Cluster capture, real phone timing and full workspace checks must be recorded
separately; local tests do not imply complete end-to-end acceptance.
