#[cfg(feature = "hixl")]
use super::{Command, HixlRuntime};
use super::{
    HIXL_UNSUPPORTED, HixlErrorKind, HixlPhase, NativeBackend, NativeCallError, NativePoll,
    NativeReadWindow, NativeRequest, RuntimeCore, RuntimeOptions, RuntimeState, map_native_error,
};
#[cfg(feature = "hixl")]
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::time::{Duration, Instant};

#[derive(Default)]
struct FakeBackend {
    next_request: u64,
    connect_delay: Option<Duration>,
    polls: usize,
    poll_result: Option<NativePoll>,
    poll_sdk_transfer: Option<Duration>,
    poll_output_copy: Option<Duration>,
    poll_error: Option<NativeCallError>,
    sync_timeout_ms: u32,
    sync_sdk_transfer: Option<Duration>,
    sync_output_copy: Option<Duration>,
    batch_submissions: Vec<Vec<NativeReadWindow>>,
    sync_batches: Vec<Vec<NativeReadWindow>>,
    submit_batch_error: Option<NativeCallError>,
    sync_batch_error: Option<NativeCallError>,
    shutdown_calls: usize,
}

impl NativeBackend for FakeBackend {
    #[cfg(feature = "hixl")]
    fn start(_: &RuntimeOptions) -> Result<Self, NativeCallError> {
        Ok(Self::default())
    }

    fn hccs_domain_id(&self) -> Option<u64> {
        Some(7)
    }

    fn connect(&mut self, _: &str, _: u32) -> Result<(), NativeCallError> {
        if let Some(delay) = self.connect_delay {
            std::thread::sleep(delay);
        }
        Ok(())
    }

    fn prepare_export(&mut self, _: u64, _: u64, _: &[u8]) -> Result<u64, NativeCallError> {
        Ok(0x1000)
    }

    fn release_export(&mut self, _: u64, _: u64) -> Result<(), NativeCallError> {
        Ok(())
    }

    fn poll_read(
        &mut self,
        _: NativeRequest,
        output: &mut [u8],
    ) -> Result<NativePoll, NativeCallError> {
        self.polls += 1;
        if let Some(error) = &self.poll_error {
            return Err(error.clone());
        }
        let default_timing = super::NativeReadTiming {
            sdk_transfer: self.poll_sdk_transfer,
            output_copy: self.poll_output_copy,
        };
        match self.poll_result.clone().unwrap_or(NativePoll::Completed {
            timing: default_timing,
        }) {
            NativePoll::Completed { timing } => {
                output[..4].copy_from_slice(b"read");
                Ok(NativePoll::Completed { timing })
            }
            result => Ok(result),
        }
    }

    fn submit_read_batch(
        &mut self,
        windows: &[NativeReadWindow],
        _peer: &str,
    ) -> Result<NativeRequest, NativeCallError> {
        if let Some(error) = self.submit_batch_error.take() {
            return Err(error);
        }
        self.batch_submissions.push(windows.to_vec());
        self.next_request += 1;
        Ok(NativeRequest(self.next_request))
    }

    fn read_sync_batch(
        &mut self,
        windows: &[NativeReadWindow],
        _peer: &str,
        output: &mut [u8],
        timeout_ms: u32,
    ) -> Result<super::NativeReadTiming, NativeCallError> {
        self.sync_timeout_ms = timeout_ms;
        if let Some(error) = self.sync_batch_error.take() {
            return Err(error);
        }
        self.sync_batches.push(windows.to_vec());
        output[..4].copy_from_slice(b"read");
        Ok(super::NativeReadTiming {
            sdk_transfer: self.sync_sdk_transfer,
            output_copy: self.sync_output_copy,
        })
    }

    fn shutdown(&mut self) -> Result<(), NativeCallError> {
        self.shutdown_calls += 1;
        Ok(())
    }
}

fn options() -> RuntimeOptions {
    RuntimeOptions {
        device_id: 0,
        engine_id: "127.0.0.1:7920".into(),
        window_bytes: 4,
        receive_slots: 1,
        export_slots: 1,
        ..RuntimeOptions::default()
    }
}

#[cfg(feature = "hixl")]
fn runtime_with_fake_backend(backend: FakeBackend, options: RuntimeOptions) -> HixlRuntime {
    let mut core = RuntimeCore::new(backend, options).unwrap();
    core.connect("peer", Instant::now() + Duration::from_secs(1))
        .unwrap();
    let info = core.info();
    let status = Arc::new(RwLock::new(core.status()));
    let owner_status = Arc::clone(&status);
    let (sender, receiver) = mpsc::sync_channel(8);
    let owner = std::thread::spawn(move || {
        super::owner_loop(core, receiver, owner_status);
    });
    HixlRuntime {
        info,
        sender,
        status,
        owner: Mutex::new(Some(owner)),
    }
}

#[test]
#[cfg(feature = "hixl")]
fn timed_hccs_read_returns_owner_queue_sdk_and_copy_durations() {
    let mut read_options = options();
    read_options.transport = super::ReadTransport::HccsBufferPool;
    let runtime = runtime_with_fake_backend(
        FakeBackend {
            connect_delay: Some(Duration::from_millis(40)),
            sync_sdk_transfer: Some(Duration::from_millis(9)),
            sync_output_copy: Some(Duration::from_millis(2)),
            ..FakeBackend::default()
        },
        read_options,
    );

    let (reply, _response) = mpsc::sync_channel(1);
    runtime
        .send(
            Command::Connect {
                peer: "queue-blocker".to_owned(),
                deadline: Instant::now() + Duration::from_secs(1),
                reply,
            },
            HixlPhase::Connect,
        )
        .unwrap();
    let outcome = runtime
        .read_batch_timed_into(
            "peer",
            &[(0xfeed, 4)],
            vec![0; 4],
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

    assert_eq!(outcome.bytes, b"read");
    assert!(outcome.timing.owner_queue >= Duration::from_millis(15));
    assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(9)));
    assert_eq!(outcome.timing.output_copy, Some(Duration::from_millis(2)));
    runtime.shutdown().unwrap();
}

#[test]
#[cfg(feature = "hixl")]
fn timed_roce_read_returns_dispatch_completion_and_copy_durations() {
    let runtime = runtime_with_fake_backend(
        FakeBackend {
            poll_sdk_transfer: Some(Duration::from_millis(11)),
            poll_output_copy: Some(Duration::from_millis(3)),
            ..FakeBackend::default()
        },
        options(),
    );
    let outcome = runtime
        .read_batch_timed_into(
            "peer",
            &[(0xfeed, 4)],
            vec![0; 4],
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

    assert_eq!(outcome.bytes, b"read");
    assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(11)));
    assert_eq!(outcome.timing.output_copy, Some(Duration::from_millis(3)));
    runtime.shutdown().unwrap();
}

#[test]
#[cfg(feature = "hixl")]
fn timed_read_preserves_native_uncertainty_errors() {
    let mut read_options = options();
    read_options.transport = super::ReadTransport::HccsBufferPool;
    let runtime = runtime_with_fake_backend(
        FakeBackend {
            sync_batch_error: Some(NativeCallError {
                status: Some(super::HIXL_TIMEOUT),
                message: "uncertain synchronous completion".to_owned(),
                accepted: true,
            }),
            ..FakeBackend::default()
        },
        read_options,
    );
    let error = runtime
        .read_batch_timed_into(
            "peer",
            &[(0xfeed, 4)],
            vec![0; 4],
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap_err();

    assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
    assert!(error.read_may_be_in_flight);
    assert_eq!(runtime.status().state, RuntimeState::Quarantined);
}

#[test]
fn runtime_options_default_device_listen_port_is_unset() {
    assert_eq!(RuntimeOptions::default().device_listen_port, None);
}

#[test]
fn runtime_core_rejects_zero_device_listen_port_before_backend_entry() {
    let mut opts = options();
    opts.device_listen_port = Some(0);
    let error = match RuntimeCore::new(FakeBackend::default(), opts) {
        Ok(_) => panic!("zero device listen port must be rejected"),
        Err(error) => error,
    };
    assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
    assert!(error.message.contains("device_listen_port"));
}

#[test]
fn runtime_core_preserves_max_device_listen_port() {
    let mut opts = options();
    opts.device_listen_port = Some(u16::MAX);
    let core = RuntimeCore::new(FakeBackend::default(), opts).unwrap();
    assert_eq!(core.options.device_listen_port, Some(u16::MAX));
}

#[test]
fn runtime_core_uses_a_generation_created_before_native_startup() {
    let core = RuntimeCore::new_with_generation(FakeBackend::default(), options(), 42).unwrap();
    assert_eq!(core.info().generation, 42);
}

#[test]
fn separate_runtime_instances_have_distinct_nonzero_generations() {
    let first = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let second = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    assert_ne!(first.info().generation, 0);
    assert_ne!(first.info().generation, second.info().generation);
}

#[test]
fn hccs_relay_budget_counts_both_internal_hbm_pools() {
    let mut opts = options();
    opts.transport = super::ReadTransport::HccsBufferPool;
    opts.buffer_pool_count = 4;
    opts.buffer_pool_size_mib = 8;
    let core = RuntimeCore::new(FakeBackend::default(), opts).unwrap();
    assert_eq!(core.status().relay_hbm_budget_bytes, 64 * 1024 * 1024);
    assert_eq!(core.status().allocated_bytes, 8);
    assert_eq!(core.info().transport, super::ReadTransport::HccsBufferPool);
}

#[test]
fn roce_does_not_budget_an_unused_hbm_relay_pool() {
    let core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    assert_eq!(core.status().transport, super::ReadTransport::Roce);
    assert_eq!(core.status().relay_hbm_budget_bytes, 0);
    assert_eq!(core.info().buffer_pool_count, 0);
    assert_eq!(core.info().buffer_pool_size_mib, 0);
}

#[test]
fn hccs_rejects_hbm_budget_overflow_and_exhaustion_before_backend_entry() {
    for (count, size) in [
        (0, 8),
        (4, 0),
        (65, 8),
        (4, 65),
        (64, 8),
        (usize::MAX, usize::MAX),
    ] {
        let mut opts = options();
        opts.transport = super::ReadTransport::HccsBufferPool;
        opts.buffer_pool_count = count;
        opts.buffer_pool_size_mib = size;
        assert!(RuntimeCore::new(FakeBackend::default(), opts).is_err());
    }
}

#[test]
#[cfg(feature = "hixl")]
fn bounded_owner_queue_refuses_pressure_without_native_dispatch() {
    let core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let (sender, _receiver) = mpsc::sync_channel(1);
    let runtime = HixlRuntime {
        info: core.info(),
        sender,
        status: Arc::new(RwLock::new(core.status())),
        owner: Mutex::new(None),
    };
    let (reply, _response) = mpsc::sync_channel(1);
    runtime
        .send(Command::Quarantine { reply }, HixlPhase::Quarantine)
        .unwrap();
    let error = runtime
        .read_batch_timed_into(
            "peer",
            &[(0xfeed, 4)],
            vec![0; 4],
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::ResourceExhausted);
    assert!(!error.read_may_be_in_flight);
    assert_eq!(runtime.status().state, RuntimeState::Running);
    assert_eq!(core.backend.shutdown_calls, 0);
}

#[test]
#[cfg(feature = "hixl")]
fn lost_owner_reply_does_not_prove_a_read_was_never_submitted() {
    let (reply, receiver) =
        mpsc::sync_channel::<Result<super::HixlReadOutcome, super::HixlError>>(1);
    drop(reply);
    let error = super::recv_reply(receiver, HixlPhase::PollRead).unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
    assert!(error.read_may_be_in_flight);
}

#[test]
fn export_pool_capacity_and_generation_prevent_stale_release() {
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let first = core
        .prepare_export_slice(
            b"one",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
    let full = core
        .prepare_export_slice(
            b"two",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap_err();
    assert_eq!(full.kind, HixlErrorKind::ResourceExhausted);
    core.release_export(first.slot_id, first.generation)
        .unwrap();
    let second = core
        .prepare_export_slice(
            b"new",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
    assert_ne!(first.generation, second.generation);
    let stale = core
        .release_export(first.slot_id, first.generation)
        .unwrap_err();
    assert_eq!(stale.kind, HixlErrorKind::InvalidArgument);
}

#[test]
fn explicit_quarantine_retains_unresolved_export_generation() {
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let window = core
        .prepare_export_slice(
            b"one",
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(core.status().allocated_bytes, 8);
    assert_eq!(core.status().quarantined_bytes, 0);

    core.quarantine().unwrap();

    let status = core.status();
    assert_eq!(status.state, RuntimeState::Quarantined);
    assert_eq!(status.allocated_bytes, 8);
    assert_eq!(status.quarantined_bytes, 4);
    assert_eq!(status.export_slots_in_use, 1);
    assert_eq!(status.receive_slots_in_use, 0);
    let release = core
        .release_export(window.slot_id, window.generation)
        .unwrap_err();
    assert_eq!(release.kind, HixlErrorKind::EnginePoisoned);
    let shutdown = core.shutdown().unwrap_err();
    assert_eq!(shutdown.kind, HixlErrorKind::ShutdownIncomplete);
    assert_eq!(core.backend.shutdown_calls, 0);
}

#[test]
fn failed_start_cleanup_cannot_be_a_safe_unsupported_capability() {
    let error = map_native_error(
        HixlPhase::Initialize,
        NativeCallError {
            status: Some(HIXL_UNSUPPORTED),
            message: "registered startup memory remains retained".to_owned(),
            accepted: true,
        },
        HixlErrorKind::NativeFailure,
    );
    assert_eq!(error.kind, HixlErrorKind::ShutdownIncomplete);
    assert_eq!(error.native_status, Some(HIXL_UNSUPPORTED));
}

const MIB: usize = 1024 * 1024;

fn batch_options(receive_slots: usize) -> RuntimeOptions {
    RuntimeOptions {
        window_bytes: 4 * MIB,
        receive_slots,
        export_slots: 1,
        ..options()
    }
}

fn batch_windows(count: usize, len: usize) -> Vec<(u64, usize)> {
    (0..count)
        .map(|index| (0x10_0000 + (index * len) as u64, len))
        .collect()
}

fn connected_batch_core(mut core: RuntimeCore<FakeBackend>) -> RuntimeCore<FakeBackend> {
    core.connect("peer", Instant::now() + Duration::from_secs(1))
        .unwrap();
    core
}

#[test]
fn batch_read_submits_25_windows_as_one_100_mib_native_call() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(25)).unwrap());
    let windows = batch_windows(25, 4 * MIB);
    let outcome = core
        .read_batch(
            "peer",
            &windows,
            vec![0; 100 * MIB],
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap();

    assert_eq!(outcome.bytes.len(), 104_857_600);
    assert_eq!(core.backend.batch_submissions.len(), 1);
    let submitted = &core.backend.batch_submissions[0];
    assert_eq!(submitted.len(), 25);
    let mut local_offset = 0;
    for (index, window) in submitted.iter().enumerate() {
        assert_eq!(window.slot_id, index as u64);
        assert_eq!(window.remote_addr, windows[index].0);
        assert_eq!(window.len, 4 * MIB);
        assert_eq!(window.local_offset, local_offset);
        local_offset += 4 * MIB;
    }
    assert_eq!(local_offset, 104_857_600);
    assert_eq!(core.status().receive_slots_in_use, 0);
    assert_eq!(core.status().state, RuntimeState::Running);
    core.shutdown().unwrap();
}

#[test]
fn batch_read_rejects_invalid_window_sets_before_native_entry() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(4)).unwrap());
    let deadline = Instant::now() + Duration::from_secs(1);
    let cases: Vec<(Vec<(u64, usize)>, usize)> = vec![
        (Vec::new(), 0),
        (vec![(0x1000, 4 * MIB); 5], 20 * MIB),
        (vec![(0x1000, 4 * MIB + 1)], 4 * MIB + 1),
        (vec![(0x1000, 4)], 3),
        (vec![(0, 4)], 4),
        (vec![(0x1000, 0)], 0),
    ];
    for (windows, output_len) in cases {
        let error = core
            .read_batch(
                "peer",
                &windows,
                vec![0; output_len],
                deadline,
                Duration::ZERO,
            )
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
    }
    assert!(core.backend.batch_submissions.is_empty());
    assert_eq!(core.status().receive_slots_in_use, 0);
}

#[test]
fn hccs_batch_read_uses_one_sync_call_and_reports_timing() {
    let mut opts = batch_options(2);
    opts.transport = super::ReadTransport::HccsBufferPool;
    let backend = FakeBackend {
        sync_sdk_transfer: Some(Duration::from_millis(7)),
        sync_output_copy: Some(Duration::from_millis(2)),
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(backend, opts).unwrap());
    let outcome = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            Instant::now() + Duration::from_secs(1),
            Duration::from_millis(3),
        )
        .unwrap();

    assert_eq!(core.backend.sync_batches.len(), 1);
    assert_eq!(core.backend.sync_batches[0].len(), 2);
    assert_eq!(core.backend.polls, 0);
    assert!((1..=1000).contains(&core.backend.sync_timeout_ms));
    assert_eq!(outcome.timing.owner_queue, Duration::from_millis(3));
    assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(7)));
    assert_eq!(outcome.timing.output_copy, Some(Duration::from_millis(2)));
    assert_eq!(core.status().receive_slots_in_use, 0);
    core.shutdown().unwrap();
}

#[test]
fn hccs_batch_expired_deadline_releases_every_slot_before_native_entry() {
    let mut opts = batch_options(2);
    opts.transport = super::ReadTransport::HccsBufferPool;
    let mut core = connected_batch_core(RuntimeCore::new(FakeBackend::default(), opts).unwrap());
    let deadline = Instant::now()
        .checked_sub(Duration::from_millis(1))
        .unwrap();
    let error = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            deadline,
            Duration::ZERO,
        )
        .unwrap_err();

    assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
    assert!(!error.read_may_be_in_flight);
    assert_eq!(core.status().state, RuntimeState::Running);
    assert_eq!(core.status().receive_slots_in_use, 0);
    assert!(core.backend.sync_batches.is_empty());
    core.shutdown().unwrap();
}

#[test]
fn hccs_batch_uncertain_sync_quarantines_every_slot() {
    for status in [super::HIXL_TIMEOUT, 1] {
        let mut opts = batch_options(2);
        opts.transport = super::ReadTransport::HccsBufferPool;
        let backend = FakeBackend {
            sync_batch_error: Some(NativeCallError {
                status: Some(status),
                message: "batch sync outcome is uncertain".into(),
                accepted: true,
            }),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(backend, opts).unwrap());
        let error = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                vec![0; 8 * MIB],
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap_err();

        assert!(error.read_may_be_in_flight);
        assert_eq!(core.status().state, RuntimeState::Quarantined);
        assert_eq!(core.status().receive_slots_in_use, 2);
        assert_eq!(core.status().quarantined_bytes, 8 * MIB);
        assert_eq!(core.backend.sync_batches.len(), 0);
        assert_eq!(core.backend.shutdown_calls, 0);
        assert!(core.shutdown().is_err());
    }
}

#[test]
fn batch_submit_rejected_before_native_entry_releases_every_slot() {
    let backend = FakeBackend {
        submit_batch_error: Some(NativeCallError::new(Some(503_900), "batch submit")),
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(backend, batch_options(2)).unwrap());
    let error = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap_err();

    assert_eq!(error.kind, HixlErrorKind::NativeFailure);
    assert!(!error.read_may_be_in_flight);
    assert_eq!(core.status().state, RuntimeState::Running);
    assert_eq!(core.status().receive_slots_in_use, 0);
}

#[test]
fn batch_submit_error_after_native_entry_quarantines_every_slot() {
    let backend = FakeBackend {
        submit_batch_error: Some(NativeCallError {
            status: Some(503_900),
            message: "uncertain batch submission".into(),
            accepted: true,
        }),
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(backend, batch_options(2)).unwrap());
    let error = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap_err();

    assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
    assert!(error.read_may_be_in_flight);
    assert_eq!(core.status().receive_slots_in_use, 2);
    assert_eq!(core.status().quarantined_bytes, 8 * MIB);
    assert_eq!(core.backend.shutdown_calls, 0);
    assert!(core.shutdown().is_err());
}

#[test]
fn batch_deadline_or_unknown_poll_quarantines_every_slot() {
    let waiting = FakeBackend {
        poll_result: Some(NativePoll::Waiting),
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(waiting, batch_options(2)).unwrap());
    let error = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            Instant::now() + Duration::from_millis(2),
            Duration::ZERO,
        )
        .unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
    assert!(error.read_may_be_in_flight);
    assert_eq!(core.status().quarantined_bytes, 8 * MIB);
    assert_eq!(core.status().receive_slots_in_use, 2);

    let unknown = FakeBackend {
        poll_error: Some(NativeCallError {
            status: None,
            message: "unknown batch completion".into(),
            accepted: true,
        }),
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(unknown, batch_options(2)).unwrap());
    let error = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::CompletionFailed);
    assert!(error.read_may_be_in_flight);
    assert_eq!(core.status().quarantined_bytes, 8 * MIB);
    assert_eq!(core.status().receive_slots_in_use, 2);
    let reuse = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap_err();
    assert_eq!(reuse.kind, HixlErrorKind::EnginePoisoned);
    assert!(!reuse.read_may_be_in_flight);
    assert_eq!(core.backend.shutdown_calls, 0);
}

#[test]
fn batch_success_releases_slots_for_the_next_generation_of_reads() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(2)).unwrap());
    for _ in 0..3 {
        core.read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            vec![0; 8 * MIB],
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap();
        assert_eq!(core.status().receive_slots_in_use, 0);
    }
    assert_eq!(core.backend.batch_submissions.len(), 3);
    core.shutdown().unwrap();
}
