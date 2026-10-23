# Firecracker storage diagnostics

`vcpu_wait.py` runs on the dedicated Linux host, selects Firecracker processes whose sandboxd parent
belongs to the supplied trial directory, and samples vCPU CPU accounting, run-queue time, wait channel
and kernel stack. It records no application payload. Its output is diagnostic evidence and must not
be mixed into ordinary performance trials. `cpu_ns` and `run_ns` include the whole guest and host KVM
execution; they are not exclusive guest-agent CPU measurements.

`agent_cpu.py` consumes optional external sandboxd instrumentation. It expects host records named
`firecracker: guest agent CPU profile` with `vsock`, `message_type`, `guest_handler_wall_ms`,
`guest_process_user_ms`, `guest_process_system_ms` and `host_response_ms`. Restore-window correlation
also requires `firecracker: restore agent phase` records with `sandbox_id`, `phase` and `elapsed_ms`.
The validation-only sandboxd patch is retained with the experiment evidence, outside the ADX source.
When adding guest response fields, build matching sandboxd and firecracker-agent/initrd binaries:
the host decoder rejects unknown fields. Use fresh snapshots, record runtime hashes, and restore
the original runtime before normal performance trials.

Guest CPU uses `getrusage(RUSAGE_SELF)`, excluding
sandbox child processes but including other agent goroutines. Handler wall time begins after request
parsing and ends before serializing the response. The host-response interval outside it also contains
scheduling, transport and serialization; it cannot be labeled entirely as storage wait. Correlate it
with the independent vCPU samples and restore-stage timestamps. Sampling provides estimates of wait
time, not exact accounting of every blocked interval.

Guest `getrusage` is guest CPU accounting, not a measurement of physical host CPU. In the observed
shared-file restore, a 277 ms release phase recorded 275 ms of guest process CPU but only about
8 ms of host vCPU execution over the sampled window. Preserve both values; do not classify that
phase as 275 ms of agent computation. Host `schedstat` deltas bound the whole vCPU's actual execution,
including other guest tasks and KVM overhead, and cannot isolate an individual agent goroutine.

For this experiment the host log timezone is Asia/Shanghai. The optional `--vcpu` input joins
explicit health/network/release phase windows by sandbox identity and the runtime's SHA-256
directory name; it excludes replies from initial creation and other instances. Gaps exceeding
200 ms are excluded from wait estimates. Preserve the coverage alongside every estimate.

```bash
python3 agent_cpu.py node1-sandboxd-service.log node2-sandboxd-service.log \
  --vcpu vcpu.jsonl --output agent-cpu.json
```

AFS exports `afs_fuse_write_duration_seconds` by filesystem, write source (`page_cache` or
`write_call`) and stage (`queue` or `execute`). Compare before/after counter and histogram
deltas for the same capture/wake window. Sums across handles are concurrent work totals,
not a partition of the request's wall clock. Kernel page-cache writes use the bounded owned
dispatch queue so a slow backend acknowledgement does not hold the FUSE receive thread;
same-handle FIFO and acknowledgement durability remain unchanged.

`rgw_observer.go` is the optional S3 request observer. Its socket-written byte counters differ from
client-consumed bytes when requests are canceled; object/layout attribution is handled by
`object_read_profile.py`.
