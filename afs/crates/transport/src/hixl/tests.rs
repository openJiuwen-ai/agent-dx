#[cfg(feature = "hixl")]
use super::{Command, HixlRuntime};
use super::{
    HIXL_UNSUPPORTED, HixlErrorKind, HixlPhase, NativeBackend, NativeCallError, NativePoll,
    NativeReadWindow, NativeRequest, RuntimeCore, RuntimeOptions, RuntimeState, map_native_error,
    validate_options,
};
#[cfg(feature = "hixl")]
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::time::{Duration, Instant};

/// Bytes every fake receive slot holds once a READ completes.
static RECEIVED: [u8; 4] = *b"read";

#[derive(Default)]
struct FakeBackend {
    next_request: u64,
    connect_delay: Option<Duration>,
    polls: usize,
    poll_result: Option<NativePoll>,
    poll_sdk_transfer: Option<Duration>,
    poll_error: Option<NativeCallError>,
    sync_timeout_ms: u32,
    sync_sdk_transfer: Option<Duration>,
    zero_receive_address: bool,
    released_receives: usize,
    release_receive_error: Option<NativeCallError>,
    batch_submissions: Vec<Vec<NativeReadWindow>>,
    sync_batches: Vec<Vec<NativeReadWindow>>,
    submit_batch_error: Option<NativeCallError>,
    sync_batch_error: Option<NativeCallError>,
    shutdown_calls: usize,
    shutdown_error: Option<NativeCallError>,
    reserve_error: Option<NativeCallError>,
    reserve_address: Option<u64>,
}

impl FakeBackend {
    fn fill_addresses(&self, addresses: &mut [u64]) {
        let address = if self.zero_receive_address {
            0
        } else {
            RECEIVED.as_ptr().expose_provenance() as u64
        };
        addresses.fill(address);
    }
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

    fn reserve_export(&mut self, _: u64, _: u64, _: usize) -> Result<u64, NativeCallError> {
        match self.reserve_error.clone() {
            Some(error) => Err(error),
            None => Ok(self.reserve_address.unwrap_or(0x1000)),
        }
    }

    fn release_export(&mut self, _: u64, _: u64) -> Result<(), NativeCallError> {
        Ok(())
    }

    fn release_receives(&mut self, slot_ids: &[u64]) -> Result<(), NativeCallError> {
        if let Some(error) = self.release_receive_error.take() {
            return Err(error);
        }
        self.released_receives += slot_ids.len();
        Ok(())
    }

    fn poll_read(
        &mut self,
        _: NativeRequest,
        addresses: &mut [u64],
    ) -> Result<NativePoll, NativeCallError> {
        self.polls += 1;
        if let Some(error) = &self.poll_error {
            return Err(error.clone());
        }
        let default_timing = super::NativeReadTiming {
            sdk_transfer: self.poll_sdk_transfer,
        };
        match self.poll_result.clone().unwrap_or(NativePoll::Completed {
            timing: default_timing,
        }) {
            NativePoll::Completed { timing } => {
                self.fill_addresses(addresses);
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
        addresses: &mut [u64],
        timeout_ms: u32,
    ) -> Result<super::NativeReadTiming, NativeCallError> {
        self.sync_timeout_ms = timeout_ms;
        if let Some(error) = self.sync_batch_error.take() {
            return Err(error);
        }
        self.sync_batches.push(windows.to_vec());
        self.fill_addresses(addresses);
        Ok(super::NativeReadTiming {
            sdk_transfer: self.sync_sdk_transfer,
        })
    }

    fn shutdown(&mut self) -> Result<(), NativeCallError> {
        self.shutdown_calls += 1;
        match self.shutdown_error.take() {
            Some(error) => Err(error),
            None => Ok(()),
        }
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
            Vec::with_capacity(4),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

    assert_eq!(outcome.bytes, b"read");
    assert!(outcome.timing.owner_queue >= Duration::from_millis(15));
    assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(9)));
    assert!(outcome.timing.output_copy.is_some());
    assert_eq!(runtime.status().receive_slots_in_use, 0);
    runtime.shutdown().unwrap();
}

#[test]
#[cfg(feature = "hixl")]
fn timed_roce_read_returns_dispatch_completion_and_copy_durations() {
    let runtime = runtime_with_fake_backend(
        FakeBackend {
            poll_sdk_transfer: Some(Duration::from_millis(11)),
            ..FakeBackend::default()
        },
        options(),
    );
    let outcome = runtime
        .read_batch_timed_into(
            "peer",
            &[(0xfeed, 4)],
            Vec::with_capacity(4),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();

    assert_eq!(outcome.bytes, b"read");
    assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(11)));
    assert!(outcome.timing.output_copy.is_some());
    assert_eq!(runtime.status().receive_slots_in_use, 0);
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
            Vec::with_capacity(4),
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
fn runtime_options_reserve_one_gib_of_host_slots() {
    let reserved = RuntimeOptions {
        window_bytes: 4 * 1024 * 1024,
        receive_slots: 128,
        export_slots: 128,
        ..RuntimeOptions::default()
    };
    assert!(validate_options(&reserved).is_ok());
    assert!(
        validate_options(&RuntimeOptions {
            receive_slots: 129,
            ..reserved.clone()
        })
        .is_err()
    );
    assert!(
        validate_options(&RuntimeOptions {
            export_slots: 129,
            ..reserved
        })
        .is_err()
    );
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
            Vec::with_capacity(4),
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
fn completed_read_release_survives_full_owner_queue() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    core.connect("peer", deadline).unwrap();
    let pinned = core
        .read_batch(
            "peer",
            &[(0xfeed, 4)],
            Vec::with_capacity(4),
            deadline,
            Duration::ZERO,
        )
        .unwrap();
    assert_eq!(core.status().receive_slots_in_use, 1);
    let (sender, receiver) = mpsc::sync_channel(1);
    let runtime = HixlRuntime {
        info: core.info(),
        sender,
        status: Arc::new(RwLock::new(core.status())),
        owner: Mutex::new(None),
    };
    let (reply, response) = mpsc::sync_channel(1);
    runtime
        .send(
            Command::Connect {
                peer: "peer".to_owned(),
                deadline,
                reply,
            },
            HixlPhase::Connect,
        )
        .unwrap();

    let owner_status = Arc::clone(&runtime.status);
    let (started, start) = mpsc::sync_channel(0);
    let (finished, finish) = mpsc::channel();
    let slots = pinned.slot_ids();
    let owner = std::thread::scope(|scope| {
        let runtime = &runtime;
        scope.spawn(move || {
            started.send(()).unwrap();
            runtime.release_pinned(slots);
            finished.send(()).unwrap();
        });
        start.recv().unwrap();
        // The queue is full until the fake owner starts. The releasing caller
        // must retain responsibility while waiting for space and the result.
        let finished_early = finish.recv_timeout(Duration::from_millis(100)).is_ok();
        let owner = std::thread::spawn(move || super::owner_loop(core, receiver, owner_status));
        response
            .recv_timeout(Duration::from_secs(2))
            .unwrap()
            .unwrap();
        assert!(
            !finished_early,
            "release returned while the owner queue was full"
        );
        finish.recv_timeout(Duration::from_secs(2)).unwrap();
        owner
    });
    assert_eq!(runtime.status().receive_slots_in_use, 0);
    runtime.shutdown().unwrap();
    owner.join().unwrap();
}

#[test]
#[cfg(feature = "hixl")]
fn completed_read_release_marks_disconnected_owner_quarantined() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    core.connect("peer", deadline).unwrap();
    let pinned = core
        .read_batch(
            "peer",
            &[(0xfeed, 4)],
            Vec::with_capacity(4),
            deadline,
            Duration::ZERO,
        )
        .unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    drop(receiver);
    let runtime = HixlRuntime {
        info: core.info(),
        sender,
        status: Arc::new(RwLock::new(core.status())),
        owner: Mutex::new(None),
    };

    runtime.release_pinned(pinned.slot_ids());

    let status = runtime.status();
    assert_eq!(status.state, RuntimeState::Quarantined);
    assert_eq!(status.receive_slots_in_use, 1);
    assert_eq!(status.quarantined_bytes, status.allocated_bytes);
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
fn export_batch_rolls_back_partial_allocation_and_rejects_stale_release() {
    let mut opts = options();
    opts.export_slots = 2;
    let mut core = RuntimeCore::new(FakeBackend::default(), opts).unwrap();
    let deadline = Instant::now() + Duration::from_secs(2);
    let error = core.reserve_exports(&[1, 0], deadline).unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
    assert_eq!(core.status().export_slots_in_use, 0);
    let pair = core.reserve_exports(&[3, 2], deadline).unwrap();
    assert_eq!((pair[0].length, pair[1].length), (3, 2));
    let ids: Vec<_> = pair.iter().map(|s| (s.slot_id, s.generation)).collect();
    core.release_exports(&ids).unwrap();
    let _new = core.reserve_exports(&[1, 1], deadline).unwrap();
    assert!(core.release_exports(&ids).is_err());
    assert_eq!(core.status().export_slots_in_use, 2);
}

#[test]
fn export_pool_capacity_and_generation_prevent_stale_release() {
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let first = core
        .reserve_export_slot(
            3,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap();
    let full = core
        .reserve_export_slot(
            3,
            std::time::Instant::now() + std::time::Duration::from_secs(1),
        )
        .unwrap_err();
    assert_eq!(full.kind, HixlErrorKind::ResourceExhausted);
    core.release_export(first.slot_id, first.generation)
        .unwrap();
    let second = core
        .reserve_export_slot(
            3,
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
fn reserving_an_export_pins_its_slot_without_copying() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let first = core.reserve_export_slot(3, deadline).unwrap();
    assert_eq!((first.address, first.length), (0x1000, 3));
    assert_eq!(core.status().export_slots_in_use, 1);
    assert_eq!(
        core.reserve_export_slot(1, deadline).unwrap_err().kind,
        HixlErrorKind::ResourceExhausted
    );

    core.release_export(first.slot_id, first.generation)
        .unwrap();
    let second = core.reserve_export_slot(4, deadline).unwrap();
    assert_ne!(first.generation, second.generation);
    assert_eq!(
        core.release_export(first.slot_id, first.generation)
            .unwrap_err()
            .kind,
        HixlErrorKind::InvalidArgument
    );
}

#[test]
fn reserving_an_export_rejects_lengths_outside_the_slot() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    for length in [0, 5] {
        assert_eq!(
            core.reserve_export_slot(length, deadline).unwrap_err().kind,
            HixlErrorKind::InvalidArgument
        );
    }
    assert_eq!(core.status().export_slots_in_use, 0);
}

#[test]
fn a_rejected_reservation_frees_its_slot() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let backend = FakeBackend {
        reserve_error: Some(NativeCallError {
            status: None,
            message: "rejected".to_owned(),
            accepted: false,
        }),
        ..FakeBackend::default()
    };
    let mut core = RuntimeCore::new(backend, options()).unwrap();
    assert!(core.reserve_export_slot(1, deadline).is_err());
    assert_eq!(core.status().export_slots_in_use, 0);
    assert_eq!(core.status().state, RuntimeState::Running);
}

#[test]
fn a_reservation_without_an_address_quarantines_the_generation() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let backend = FakeBackend {
        reserve_address: Some(0),
        ..FakeBackend::default()
    };
    let mut core = RuntimeCore::new(backend, options()).unwrap();
    assert_eq!(
        core.reserve_export_slot(1, deadline).unwrap_err().kind,
        HixlErrorKind::EnginePoisoned
    );
    assert_eq!(core.status().quarantined_bytes, 4);
}

#[test]
fn an_export_reservation_writes_through_to_its_memory() {
    let mut memory = vec![0_u8; 4];
    {
        let window = super::ExportWindow {
            slot_id: 0,
            generation: 1,
            address: memory.as_ptr().expose_provenance() as u64,
            length: 3,
        };
        // SAFETY: the initialized `memory` outlives the reservation and
        // nothing else accesses it during this scope.
        let mut reservation =
            unsafe { super::ReservedExport::from_raw(window, memory.as_mut_ptr()) };
        assert_eq!(reservation.as_mut_slice().len(), 3);
        reservation.as_mut_slice().copy_from_slice(b"abc");
    }
    assert_eq!(memory, b"abc\0");
}

#[test]
fn explicit_quarantine_retains_unresolved_export_generation() {
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let window = core
        .reserve_export_slot(
            3,
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
fn batch_read_submits_100_and_128_mib_in_one_native_call_each() {
    for count in [25, 32] {
        let mut core = connected_batch_core(
            RuntimeCore::new(FakeBackend::default(), batch_options(count)).unwrap(),
        );
        let windows = batch_windows(count, 4 * MIB);
        let length = count * 4 * MIB;
        let outcome = core
            .read_batch(
                "peer",
                &windows,
                Vec::with_capacity(length),
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap();

        assert!(outcome.output.is_empty());
        assert!(outcome.output.capacity() >= length);
        assert_eq!(core.backend.batch_submissions.len(), 1);
        let submitted = &core.backend.batch_submissions[0];
        assert_eq!(submitted.len(), count);
        for (index, window) in submitted.iter().enumerate() {
            assert_eq!(window.slot_id, index as u64);
            assert_eq!(window.remote_addr, windows[index].0);
            assert_eq!(window.len, 4 * MIB);
        }
        assert_eq!(core.status().receive_slots_in_use, count);
        core.release_receives(&outcome.slot_ids()).unwrap();
        assert_eq!(core.status().receive_slots_in_use, 0);
        assert_eq!(core.status().state, RuntimeState::Running);
        core.shutdown().unwrap();
    }
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
                Vec::with_capacity(output_len),
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
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(backend, opts).unwrap());
    let outcome = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            Vec::with_capacity(8 * MIB),
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
    assert_eq!(outcome.timing.output_copy, None);
    core.release_receives(&outcome.slot_ids()).unwrap();
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
            Vec::with_capacity(8 * MIB),
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
                Vec::with_capacity(8 * MIB),
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
#[cfg(feature = "hixl")]
fn failed_start_error_retains_the_native_runtime() {
    let before = super::retained_start_count();
    let error = super::failed_start_error(
        std::ptr::without_provenance_mut::<std::ffi::c_void>(0x2000),
        HIXL_UNSUPPORTED,
        "HIXL Initialize failed".to_owned(),
    );
    assert!(error.accepted, "a retained handle is never a safe refusal");
    assert_eq!(error.status, Some(HIXL_UNSUPPORTED));
    assert!(
        error
            .message
            .contains("retained with its device resources until process exit"),
        "{}",
        error.message
    );
    assert_eq!(super::retained_start_count(), before + 1);
    let retained = super::retained_starts();
    let last = retained.last().expect("the failed start is retained");
    assert_eq!(last.address, 0x2000);
    assert_eq!(last.status, Some(HIXL_UNSUPPORTED));

    // A null handle is a clean failure: nothing is retained and preferred
    // mode may still treat the status as a capability refusal.
    let clean = super::failed_start_error(
        std::ptr::null_mut(),
        HIXL_UNSUPPORTED,
        "HIXL Initialize failed".to_owned(),
    );
    assert!(!clean.accepted);
    assert!(!clean.message.contains("retained"));
    assert_eq!(super::retained_start_count(), before + 1);
}

#[test]
#[cfg(feature = "hixl")]
fn failed_native_cleanup_stays_retryable_until_shutdown_succeeds() {
    let runtime = runtime_with_fake_backend(
        FakeBackend {
            shutdown_error: Some(NativeCallError::new(Some(9), "aclrtResetDevice failed")),
            ..FakeBackend::default()
        },
        options(),
    );

    let error = runtime.shutdown().unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::ShutdownIncomplete);
    assert_eq!(runtime.status().state, RuntimeState::ShutdownFailed);

    // Cleanup stays retryable, but no data-plane operation may re-enter.
    let blocked = runtime
        .read_batch_timed_into(
            "peer",
            &[(0xfeed, 4)],
            Vec::with_capacity(4),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap_err();
    assert_eq!(blocked.kind, HixlErrorKind::ShutdownIncomplete);

    runtime.shutdown().unwrap();
    assert_eq!(runtime.status().state, RuntimeState::Shutdown);
    // A completed shutdown stays idempotent.
    runtime.shutdown().unwrap();
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
            Vec::with_capacity(8 * MIB),
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
            Vec::with_capacity(8 * MIB),
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
            Vec::with_capacity(8 * MIB),
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
            Vec::with_capacity(8 * MIB),
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
            Vec::with_capacity(8 * MIB),
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
        let pinned = core
            .read_batch(
                "peer",
                &batch_windows(2, 4 * MIB),
                Vec::with_capacity(8 * MIB),
                Instant::now() + Duration::from_secs(1),
                Duration::ZERO,
            )
            .unwrap();
        core.release_receives(&pinned.slot_ids()).unwrap();
        assert_eq!(core.status().receive_slots_in_use, 0);
    }
    assert_eq!(core.backend.batch_submissions.len(), 3);
    assert_eq!(core.backend.released_receives, 6);
    core.shutdown().unwrap();
}

#[test]
#[cfg(feature = "hixl")]
fn batch_submission_publishes_only_the_filled_buffer() {
    let runtime = runtime_with_fake_backend(FakeBackend::default(), options());
    let deadline = Instant::now() + Duration::from_secs(1);
    let output = runtime.prepare_read_batch_buffer(4).unwrap();
    assert!(output.is_empty());
    let outcome = runtime
        .read_batch_timed_into("peer", &[(0xfeed, 4)], output, deadline)
        .unwrap();
    assert_eq!(outcome.bytes, b"read");
    assert_eq!(runtime.status().receive_slots_in_use, 0);
    runtime.shutdown().unwrap();
}

#[test]
fn a_batch_rejects_initialized_or_insufficient_output_before_native_entry() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(2)).unwrap());
    let deadline = Instant::now() + Duration::from_secs(1);
    for output in [vec![0; 8], Vec::with_capacity(7)] {
        let error = core
            .read_batch(
                "peer",
                &batch_windows(2, 4),
                output,
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
fn a_completed_read_keeps_its_slots_pinned_until_they_are_released() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(2)).unwrap());
    let read = |core: &mut RuntimeCore<FakeBackend>| {
        core.read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            Vec::with_capacity(8 * MIB),
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
    };
    let pinned = read(&mut core).unwrap();
    assert_eq!(pinned.slots.len(), 2);
    assert!(pinned.slots.iter().all(|slot| slot.address != 0));
    assert_eq!(pinned.slots[1].length, 4 * MIB);
    assert_eq!(core.status().receive_slots_in_use, 2);

    assert_eq!(
        read(&mut core).unwrap_err().kind,
        HixlErrorKind::ResourceExhausted
    );
    assert_eq!(
        core.shutdown().unwrap_err().kind,
        HixlErrorKind::ShutdownIncomplete
    );

    core.release_receives(&pinned.slot_ids()).unwrap();
    assert_eq!(core.status().receive_slots_in_use, 0);
    core.shutdown().unwrap();
}

#[test]
fn releasing_receive_slots_that_are_not_pinned_is_rejected() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(2)).unwrap());
    assert_eq!(
        core.release_receives(&[0]).unwrap_err().kind,
        HixlErrorKind::InvalidArgument
    );
    assert_eq!(
        core.release_receives(&[9]).unwrap_err().kind,
        HixlErrorKind::InvalidArgument
    );
    assert_eq!(core.backend.released_receives, 0);
}

#[test]
fn a_read_completed_without_an_address_quarantines_its_slots() {
    let backend = FakeBackend {
        zero_receive_address: true,
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(backend, batch_options(2)).unwrap());
    let error = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            Vec::with_capacity(8 * MIB),
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
    assert_eq!(core.status().quarantined_bytes, 8 * MIB);
}

#[test]
fn a_failed_receive_release_quarantines_the_slots() {
    let backend = FakeBackend {
        release_receive_error: Some(NativeCallError {
            status: None,
            message: "release refused".into(),
            accepted: false,
        }),
        ..FakeBackend::default()
    };
    let mut core = connected_batch_core(RuntimeCore::new(backend, batch_options(2)).unwrap());
    let pinned = core
        .read_batch(
            "peer",
            &batch_windows(2, 4 * MIB),
            Vec::with_capacity(8 * MIB),
            Instant::now() + Duration::from_secs(1),
            Duration::ZERO,
        )
        .unwrap();
    assert_eq!(
        core.release_receives(&pinned.slot_ids()).unwrap_err().kind,
        HixlErrorKind::EnginePoisoned
    );
    assert_eq!(core.status().quarantined_bytes, 8 * MIB);
}

#[test]
#[cfg(feature = "hixl")]
fn copying_pinned_slots_fills_the_output_in_offset_order() {
    const SLOT: usize = 1024;
    let memory = (0..8_u8).map(|byte| vec![byte; SLOT]).collect::<Vec<_>>();
    let slots = memory
        .iter()
        .enumerate()
        .map(|(index, bytes)| super::PinnedSlot {
            slot_id: index,
            address: bytes.as_ptr().expose_provenance(),
            length: SLOT,
        })
        .collect::<Vec<_>>();
    let mut output = Vec::with_capacity(SLOT * 8);
    super::copy_pinned(&slots, &mut output, SLOT * 8).unwrap();
    assert_eq!(output.len(), SLOT * 8);
    for (index, chunk) in output.chunks(SLOT).enumerate() {
        assert!(chunk.iter().all(|byte| *byte == index as u8));
    }
}

#[test]
#[cfg(feature = "hixl")]
fn copying_pinned_slots_rejects_lengths_that_do_not_tile_the_output() {
    let bytes = [1_u8; 8];
    let slot = |length| super::PinnedSlot {
        slot_id: 0,
        address: bytes.as_ptr().expose_provenance(),
        length,
    };
    for length in [4, 9] {
        let mut output = Vec::with_capacity(8);
        assert_eq!(
            super::copy_pinned(&[slot(length)], &mut output, 8)
                .unwrap_err()
                .kind,
            HixlErrorKind::Internal
        );
        assert!(output.is_empty());
    }
}
