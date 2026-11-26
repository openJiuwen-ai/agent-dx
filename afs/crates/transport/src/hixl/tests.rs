#[cfg(feature = "hixl")]
use super::{Command, HixlRuntime};
use super::{
    HIXL_UNSUPPORTED, HixlErrorKind, HixlPhase, NativeBackend, NativeCallError,
    NativeTransferWindow, RuntimeCore, RuntimeOptions, RuntimeState, map_native_error,
    validate_options,
};
#[cfg(feature = "hixl")]
use std::sync::{Arc, Mutex, RwLock, mpsc};
use std::time::{Duration, Instant};

/// Bytes every fake receive slot holds once a READ completes.
static RECEIVED: [u8; 4] = *b"read";

#[derive(Default)]
struct FakeBackend {
    connect_delay: Option<Duration>,
    sync_timeout_ms: u32,
    sync_sdk_transfer: Option<Duration>,
    zero_receive_address: bool,
    released_receives: usize,
    release_receive_error: Option<NativeCallError>,
    release_export_error: Option<NativeCallError>,
    sync_batches: Vec<Vec<NativeTransferWindow>>,
    writes: Vec<(Vec<NativeTransferWindow>, Vec<u8>)>,
    write_error: Option<NativeCallError>,
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
        self.release_export_error.take().map_or(Ok(()), Err)
    }

    fn release_receives(&mut self, slot_ids: &[u64]) -> Result<(), NativeCallError> {
        if let Some(error) = self.release_receive_error.take() {
            return Err(error);
        }
        self.released_receives += slot_ids.len();
        Ok(())
    }

    fn read_sync_batch(
        &mut self,
        windows: &[NativeTransferWindow],
        _peer: &str,
        addresses: &mut [u64],
        timeout_ms: u32,
    ) -> Result<super::NativeReadTiming, NativeCallError> {
        self.sync_batches.push(windows.to_vec());
        self.sync_timeout_ms = timeout_ms;
        if let Some(error) = self.sync_batch_error.take() {
            return Err(error);
        }
        self.fill_addresses(addresses);
        Ok(super::NativeReadTiming {
            sdk_transfer: self.sync_sdk_transfer,
        })
    }

    fn write_batch(
        &mut self,
        windows: &[NativeTransferWindow],
        _peer: &str,
        bytes: &[u8],
        _timeout_ms: u32,
    ) -> Result<(), NativeCallError> {
        self.writes.push((windows.to_vec(), bytes.to_vec()));
        self.write_error.take().map_or(Ok(()), Err)
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

#[cfg(feature = "hixl")]
fn read_one_slot(runtime: &HixlRuntime) -> Result<super::HixlReadOutcome, super::HixlError> {
    runtime.read_batch_timed_into(
        "peer",
        &[(0xfeed, RECEIVED.len())],
        Vec::with_capacity(RECEIVED.len()),
        Instant::now() + Duration::from_secs(1),
    )
}

#[test]
#[cfg(feature = "hixl")]
fn timed_hccs_read_returns_owner_queue_sdk_and_copy_durations() {
    let mut read_options = options();
    read_options.transport = super::ReadTransport::Hccs;
    read_options.buffer_pool = true;
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
    let outcome = read_one_slot(&runtime).unwrap();

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
            sync_sdk_transfer: Some(Duration::from_millis(11)),
            ..FakeBackend::default()
        },
        options(),
    );
    let outcome = read_one_slot(&runtime).unwrap();

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
    read_options.transport = super::ReadTransport::Hccs;
    read_options.buffer_pool = true;
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
    let error = read_one_slot(&runtime).unwrap_err();

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
fn pooled_links_budget_both_device_pools_and_publish_canonical_identity() {
    for transport in [super::ReadTransport::Roce, super::ReadTransport::Hccs] {
        let core = RuntimeCore::new(
            FakeBackend::default(),
            RuntimeOptions {
                transport,
                buffer_pool: true,
                buffer_pool_count: 4,
                buffer_pool_size_mib: 8,
                ..options()
            },
        )
        .unwrap();
        assert_eq!(core.status().relay_hbm_budget_bytes, 64 * MIB);
        assert_eq!(core.status().allocated_bytes, 8);
        assert_eq!(core.info().transport, transport);
        assert_eq!(core.info().buffer_pool_count, 4);
        assert_eq!(core.info().buffer_pool_size_mib, 8);
        assert_eq!(
            core.info().hccs_domain_id,
            (transport == super::ReadTransport::Hccs).then_some(7)
        );
    }
}

#[test]
fn direct_roce_ignores_unused_pool_dimensions() {
    let core = RuntimeCore::new(
        FakeBackend::default(),
        RuntimeOptions {
            buffer_pool_count: 0,
            buffer_pool_size_mib: usize::MAX,
            ..options()
        },
    )
    .unwrap();
    assert_eq!(core.status().transport, super::ReadTransport::Roce);
    assert_eq!(core.status().relay_hbm_budget_bytes, 0);
    assert_eq!(core.info().buffer_pool_count, 0);
    assert_eq!(core.info().buffer_pool_size_mib, 0);
    assert_eq!(core.info().hccs_domain_id, None);
}

#[test]
fn hccs_without_pool_is_rejected_during_validation() {
    let error = validate_options(&RuntimeOptions {
        transport: super::ReadTransport::Hccs,
        ..options()
    })
    .unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
    assert_eq!(error.phase, HixlPhase::Initialize);
}

#[test]
fn pooled_links_reject_invalid_dimensions_and_overbudget_hbm() {
    for transport in [super::ReadTransport::Roce, super::ReadTransport::Hccs] {
        for (count, size, kind) in [
            (0, 8, HixlErrorKind::InvalidArgument),
            (4, 0, HixlErrorKind::InvalidArgument),
            (65, 8, HixlErrorKind::InvalidArgument),
            (4, 65, HixlErrorKind::InvalidArgument),
            (64, 8, HixlErrorKind::ResourceExhausted),
            (usize::MAX, usize::MAX, HixlErrorKind::InvalidArgument),
        ] {
            let error = validate_options(&RuntimeOptions {
                transport,
                buffer_pool: true,
                buffer_pool_count: count,
                buffer_pool_size_mib: size,
                ..options()
            })
            .unwrap_err();
            assert_eq!(error.kind, kind);
        }
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
    let error = read_one_slot(&runtime).unwrap_err();
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
fn lost_owner_reply_preserves_read_and_write_uncertainty() {
    for phase in [HixlPhase::CompleteRead, HixlPhase::WriteBatch] {
        let (reply, receiver) = mpsc::sync_channel::<Result<(), super::HixlError>>(1);
        drop(reply);
        let error = super::recv_reply(receiver, phase).unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
        assert!(error.read_may_be_in_flight);
    }
}

#[test]
#[cfg(feature = "hixl")]
fn completed_export_release_survives_full_owner_queue() {
    let deadline = Instant::now() + Duration::from_secs(5);
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let first = core.reserve_export_slot(4, deadline).unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    let runtime = Arc::new(HixlRuntime {
        info: core.info(),
        sender,
        status: Arc::new(RwLock::new(core.status())),
        owner: Mutex::new(None),
    });
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

    let releasing_runtime = Arc::clone(&runtime);
    let (started, start) = mpsc::sync_channel(0);
    let (finished, finish) = mpsc::channel();
    let releasing = std::thread::spawn(move || {
        started.send(()).unwrap();
        // SAFETY: no Rust reservation was created and the slot was never published.
        let result =
            unsafe { releasing_runtime.release_exports(vec![(first.slot_id, first.generation)]) };
        finished.send(result).unwrap();
    });
    start.recv_timeout(Duration::from_secs(2)).unwrap();
    let early = finish.recv_timeout(Duration::from_millis(100));
    let owner_status = Arc::clone(&runtime.status);
    let owner = std::thread::spawn(move || super::owner_loop(core, receiver, owner_status));
    response
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(early, Err(mpsc::RecvTimeoutError::Timeout));
    finish
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    releasing.join().unwrap();
    let status = runtime.status();
    assert_eq!(status.state, RuntimeState::Running);
    assert_eq!(status.export_slots_in_use, 0);
    assert_eq!(status.quarantined_bytes, 0);

    // Request the descriptor directly so the fake address is never wrapped as a Rust buffer.
    let (reply, response) = mpsc::sync_channel(1);
    runtime
        .send(
            Command::ReserveExports {
                lengths: vec![4],
                deadline: Instant::now() + Duration::from_secs(2),
                reply,
            },
            HixlPhase::PrepareExport,
        )
        .unwrap();
    let windows = response
        .recv_timeout(Duration::from_secs(2))
        .unwrap()
        .unwrap();
    assert_eq!(windows.len(), 1);
    let second = &windows[0];
    assert_eq!(second.slot_id, first.slot_id);
    assert_ne!(second.generation, first.generation);
    // SAFETY: this slot was also never published or borrowed.
    unsafe { runtime.release_exports(vec![(second.slot_id, second.generation)]) }.unwrap();
    runtime.shutdown().unwrap();
    owner.join().unwrap();
}

#[test]
#[cfg(feature = "hixl")]
fn completed_export_release_marks_disconnected_owner_quarantined() {
    let mut core = RuntimeCore::new(FakeBackend::default(), options()).unwrap();
    let slot = core
        .reserve_export_slot(4, Instant::now() + Duration::from_secs(1))
        .unwrap();
    let (sender, receiver) = mpsc::sync_channel(1);
    drop(receiver);
    let runtime = HixlRuntime {
        info: core.info(),
        sender,
        status: Arc::new(RwLock::new(core.status())),
        owner: Mutex::new(None),
    };

    // SAFETY: the slot was never published or borrowed.
    let error =
        unsafe { runtime.release_exports(vec![(slot.slot_id, slot.generation)]) }.unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
    let status = runtime.status();
    assert_eq!(status.state, RuntimeState::Quarantined);
    assert_eq!(status.export_slots_in_use, 1);
    assert_eq!(status.quarantined_bytes, status.allocated_bytes);
}

#[test]
fn native_export_release_failure_quarantines_slot() {
    let deadline = Instant::now() + Duration::from_secs(1);
    let backend = FakeBackend {
        release_export_error: Some(NativeCallError {
            status: None,
            message: "native release failed".to_owned(),
            accepted: true,
        }),
        ..FakeBackend::default()
    };
    let mut core = RuntimeCore::new(backend, options()).unwrap();
    let slot = core.reserve_export_slot(4, deadline).unwrap();
    let error = core
        .release_export(slot.slot_id, slot.generation)
        .unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::EnginePoisoned);
    let status = core.status();
    assert_eq!(status.state, RuntimeState::Quarantined);
    assert_eq!(status.export_slots_in_use, 1);
    assert_eq!(status.quarantined_bytes, 4);
    assert!(core.reserve_export_slot(4, deadline).is_err());
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
fn export_reservation_pins_the_slot_and_rejects_stale_release_after_reuse() {
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

fn read_two_slots(
    core: &mut RuntimeCore<FakeBackend>,
) -> Result<super::PinnedRead, super::HixlError> {
    core.read_batch(
        "peer",
        &batch_windows(2, 4 * MIB),
        Vec::with_capacity(8 * MIB),
        Instant::now() + Duration::from_secs(1),
        Duration::ZERO,
    )
}

#[test]
fn batch_read_transfers_100_and_128_mib_in_one_sync_call_each() {
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
        assert_eq!(core.backend.sync_batches.len(), 1);
        let submitted = &core.backend.sync_batches[0];
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
        (vec![(u64::MAX, 4)], 4),
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
    assert!(core.backend.sync_batches.is_empty());
    assert_eq!(core.status().receive_slots_in_use, 0);
}

#[test]
fn both_planes_use_one_sync_read_call_and_report_timing() {
    for (transport, buffer_pool) in [
        (super::ReadTransport::Roce, false),
        (super::ReadTransport::Roce, true),
        (super::ReadTransport::Hccs, true),
    ] {
        let mut opts = batch_options(2);
        opts.transport = transport;
        opts.buffer_pool = buffer_pool;
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

        assert_eq!(core.backend.sync_batches.len(), 1, "{transport:?}");
        assert_eq!(core.backend.sync_batches[0].len(), 2);
        assert!((1..=1000).contains(&core.backend.sync_timeout_ms));
        assert_eq!(outcome.timing.owner_queue, Duration::from_millis(3));
        assert_eq!(outcome.timing.sdk_transfer, Some(Duration::from_millis(7)));
        assert_eq!(outcome.timing.output_copy, None);
        core.release_receives(&outcome.slot_ids()).unwrap();
        assert_eq!(core.status().receive_slots_in_use, 0);
        core.shutdown().unwrap();
    }
}

#[test]
fn expired_read_deadline_leaves_slots_available_without_native_dispatch() {
    for (transport, buffer_pool) in [
        (super::ReadTransport::Roce, false),
        (super::ReadTransport::Roce, true),
        (super::ReadTransport::Hccs, true),
    ] {
        let mut opts = batch_options(2);
        opts.transport = transport;
        opts.buffer_pool = buffer_pool;
        let mut core =
            connected_batch_core(RuntimeCore::new(FakeBackend::default(), opts).unwrap());
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
}

#[test]
fn uncertain_sync_read_quarantines_every_slot_on_both_planes() {
    for (transport, buffer_pool) in [
        (super::ReadTransport::Roce, false),
        (super::ReadTransport::Roce, true),
        (super::ReadTransport::Hccs, true),
    ] {
        for (status, kind) in [
            (Some(super::HIXL_TIMEOUT), HixlErrorKind::DeadlineExceeded),
            (None, HixlErrorKind::EnginePoisoned),
        ] {
            let mut opts = batch_options(2);
            opts.transport = transport;
            opts.buffer_pool = buffer_pool;
            let backend = FakeBackend {
                sync_batch_error: Some(NativeCallError {
                    status,
                    message: "batch sync outcome is uncertain".into(),
                    accepted: true,
                }),
                ..FakeBackend::default()
            };
            let mut core = connected_batch_core(RuntimeCore::new(backend, opts).unwrap());
            let error = read_two_slots(&mut core).unwrap_err();
            assert_eq!(error.kind, kind, "{transport:?}");
            assert!(error.read_may_be_in_flight);
            assert_eq!(core.status().state, RuntimeState::Quarantined);
            assert_eq!(core.status().receive_slots_in_use, 2);
            assert_eq!(core.status().quarantined_bytes, 8 * MIB);
            let reuse = read_two_slots(&mut core).unwrap_err();
            assert_eq!(reuse.kind, HixlErrorKind::EnginePoisoned);
            assert!(!reuse.read_may_be_in_flight);
            assert_eq!(core.backend.sync_batches.len(), 1);
            assert_eq!(core.backend.shutdown_calls, 0);
            assert!(core.shutdown().is_err());
        }
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
    let blocked = read_one_slot(&runtime).unwrap_err();
    assert_eq!(blocked.kind, HixlErrorKind::ShutdownIncomplete);

    runtime.shutdown().unwrap();
    assert_eq!(runtime.status().state, RuntimeState::Shutdown);
    // A completed shutdown stays idempotent.
    runtime.shutdown().unwrap();
}

#[test]
fn sync_read_rejection_before_dispatch_rolls_back_all_slots() {
    for (transport, buffer_pool) in [
        (super::ReadTransport::Roce, false),
        (super::ReadTransport::Roce, true),
        (super::ReadTransport::Hccs, true),
    ] {
        let mut opts = batch_options(2);
        opts.transport = transport;
        opts.buffer_pool = buffer_pool;
        let backend = FakeBackend {
            sync_batch_error: Some(NativeCallError::new(Some(503_900), "batch rejected")),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(backend, opts).unwrap());
        let error = read_two_slots(&mut core).unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::NativeFailure);
        assert!(!error.read_may_be_in_flight);
        assert_eq!(core.status().state, RuntimeState::Running);
        assert_eq!(core.status().receive_slots_in_use, 0);
        assert_eq!(core.backend.released_receives, 0);
        let pinned = read_two_slots(&mut core).unwrap();
        assert_eq!(pinned.slot_ids(), [0, 1]);
        core.release_receives(&pinned.slot_ids()).unwrap();
        assert_eq!(core.backend.sync_batches.len(), 2);
        assert_eq!(core.backend.released_receives, 2);
        core.shutdown().unwrap();
    }
}

#[test]
fn batch_success_releases_slots_for_the_next_generation_of_reads() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(2)).unwrap());
    for _ in 0..3 {
        let pinned = read_two_slots(&mut core).unwrap();
        core.release_receives(&pinned.slot_ids()).unwrap();
        assert_eq!(core.status().receive_slots_in_use, 0);
    }
    assert_eq!(core.backend.sync_batches.len(), 3);
    assert_eq!(core.backend.released_receives, 6);
    core.shutdown().unwrap();
}

#[test]
#[cfg(feature = "hixl")]
fn sync_batch_publishes_only_the_filled_buffer() {
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
    assert!(core.backend.sync_batches.is_empty());
    assert_eq!(core.status().receive_slots_in_use, 0);
}

#[test]
fn a_completed_read_keeps_its_slots_pinned_until_they_are_released() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), batch_options(2)).unwrap());
    let pinned = read_two_slots(&mut core).unwrap();
    assert_eq!(pinned.slots.len(), 2);
    assert!(pinned.slots.iter().all(|slot| slot.address != 0));
    assert_eq!(pinned.slots[1].length, 4 * MIB);
    assert_eq!(core.status().receive_slots_in_use, 2);

    assert_eq!(
        read_two_slots(&mut core).unwrap_err().kind,
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
    let error = read_two_slots(&mut core).unwrap_err();
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
    let pinned = read_two_slots(&mut core).unwrap();
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

#[test]
fn write_batch_validates_payload_and_reuses_staging_slots() {
    let mut core =
        connected_batch_core(RuntimeCore::new(FakeBackend::default(), options()).unwrap());
    let deadline = Instant::now() + Duration::from_secs(1);
    for (windows, bytes) in [
        (&[(0x1000, 4)][..], &b"abc"[..]),
        (&[(u64::MAX, 4)][..], &b"abcd"[..]),
    ] {
        let error = core
            .write_batch("peer", windows, bytes, deadline)
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
        assert!(!error.read_may_be_in_flight);
    }
    assert!(core.backend.writes.is_empty());
    let error = core
        .write_batch("peer", &[(0x1000, 4)], b"abcd", Instant::now())
        .unwrap_err();
    assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
    assert!(!error.read_may_be_in_flight);
    for bytes in [b"abcd", b"efgh"] {
        core.write_batch("peer", &[(0x1000, 4)], bytes, deadline)
            .unwrap();
    }
    assert_eq!(core.backend.writes.len(), 2);
    let (windows, bytes) = &core.backend.writes[1];
    assert_eq!(
        &windows[..],
        &[NativeTransferWindow {
            slot_id: 0,
            remote_addr: 0x1000,
            len: 4
        }]
    );
    assert_eq!(bytes, b"efgh");
    assert_eq!(core.status().receive_slots_in_use, 0);
    core.shutdown().unwrap();
}

#[test]
fn write_batch_quarantines_only_when_native_outcome_is_uncertain() {
    for accepted in [false, true] {
        let mut options = options();
        options.receive_slots = 2;
        let backend = FakeBackend {
            write_error: Some(NativeCallError {
                status: Some(super::HIXL_TIMEOUT),
                message: "injected WRITE failure".to_owned(),
                accepted,
            }),
            ..FakeBackend::default()
        };
        let mut core = connected_batch_core(RuntimeCore::new(backend, options).unwrap());
        let deadline = Instant::now() + Duration::from_secs(1);
        let windows = [(0x1000, 4), (0x2000, 3)];
        let error = core
            .write_batch("peer", &windows, b"abcdefg", deadline)
            .unwrap_err();
        assert_eq!(error.kind, HixlErrorKind::DeadlineExceeded);
        assert_eq!(error.read_may_be_in_flight, accepted);
        if accepted {
            assert_eq!(core.status().state, RuntimeState::Quarantined);
            assert_eq!(core.status().quarantined_bytes, 8);
            assert!(core.shutdown().is_err());
            assert_eq!(core.backend.shutdown_calls, 0);
        } else {
            assert_eq!(core.status().state, RuntimeState::Running);
            core.write_batch("peer", &windows, b"abcdefg", deadline)
                .unwrap();
            core.shutdown().unwrap();
        }
    }
}

#[cfg(feature = "hixl")]
#[test]
fn write_batch_runs_through_the_owner_thread() {
    let runtime = runtime_with_fake_backend(FakeBackend::default(), options());
    runtime
        .write_batch(
            "peer",
            &[(0x1000, 4)],
            b"data".to_vec(),
            Instant::now() + Duration::from_secs(1),
        )
        .unwrap();
    assert_eq!(runtime.status().receive_slots_in_use, 0);
    runtime.shutdown().unwrap();
}
