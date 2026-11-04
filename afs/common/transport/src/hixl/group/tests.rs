use super::{HixlErrorKind, ReadTransport, RuntimeOptions, validate_group_options};

fn options(device_id: i32, port: u16) -> RuntimeOptions {
    RuntimeOptions {
        device_id,
        engine_id: format!("127.0.0.1:{}", 7920 + device_id),
        device_listen_port: Some(port),
        ..RuntimeOptions::default()
    }
}

#[test]
fn group_options_validate_listener_and_canonical_engine_identity() {
    let first = options(0, 27000);
    let mut second = options(1, 27001);
    second.device_listen_port = None;
    assert!(validate_group_options(&[first.clone(), second.clone()]).is_err());
    second.device_listen_port = Some(27001);
    let mut ipv6 = first;
    ipv6.engine_id = "[0:0:0:0:0:0:0:1]:7920".into();
    second.engine_id = "[::1]:7920".into();
    assert!(validate_group_options(&[ipv6, second]).is_err());
}

#[test]
fn group_options_require_one_to_sixteen_lanes() {
    assert_eq!(
        validate_group_options(&[]).unwrap_err().kind,
        HixlErrorKind::InvalidArgument
    );
    let too_many = (0..17)
        .map(|device_id| options(device_id, 27000 + device_id as u16))
        .collect::<Vec<_>>();
    assert_eq!(
        validate_group_options(&too_many).unwrap_err().kind,
        HixlErrorKind::InvalidArgument
    );
}

#[test]
fn group_options_reject_duplicate_devices_engines_and_listener_ports() {
    let first = options(0, 27000);

    let duplicate_device = vec![first.clone(), options(0, 27001)];
    assert_eq!(
        validate_group_options(&duplicate_device).unwrap_err().kind,
        HixlErrorKind::InvalidArgument
    );

    let mut duplicate_engine = options(1, 27001);
    duplicate_engine.engine_id = first.engine_id.clone();
    assert_eq!(
        validate_group_options(&[first.clone(), duplicate_engine])
            .unwrap_err()
            .kind,
        HixlErrorKind::InvalidArgument
    );

    let duplicate_port = vec![first, options(1, 27000)];
    assert_eq!(
        validate_group_options(&duplicate_port).unwrap_err().kind,
        HixlErrorKind::InvalidArgument
    );
}

#[test]
fn group_options_require_matching_window_transport_and_pool_dimensions() {
    let first = options(0, 27000);

    let mut different_window = options(1, 27001);
    different_window.window_bytes /= 2;
    assert_eq!(
        validate_group_options(&[first.clone(), different_window])
            .unwrap_err()
            .kind,
        HixlErrorKind::InvalidArgument
    );

    let mut different_transport = options(1, 27001);
    different_transport.transport = ReadTransport::HccsBufferPool;
    assert_eq!(
        validate_group_options(&[first.clone(), different_transport])
            .unwrap_err()
            .kind,
        HixlErrorKind::InvalidArgument
    );

    let mut relay = first.clone();
    relay.transport = ReadTransport::HccsBufferPool;
    let mut different_pool = relay.clone();
    different_pool.device_id = 1;
    different_pool.engine_id = "127.0.0.1:7921".to_owned();
    different_pool.device_listen_port = Some(27001);
    different_pool.buffer_pool_count += 1;
    assert_eq!(
        validate_group_options(&[relay, different_pool])
            .unwrap_err()
            .kind,
        HixlErrorKind::InvalidArgument
    );
}

#[test]
fn sixteen_lanes_accept_the_per_lane_budget() {
    let options = (0..16)
        .map(|device_id| RuntimeOptions {
            device_id,
            engine_id: format!("127.0.0.1:{}", 7920 + device_id),
            device_listen_port: Some(27000 + device_id as u16),
            receive_slots: 64,
            export_slots: 64,
            transport: ReadTransport::HccsBufferPool,
            buffer_pool_count: 32,
            buffer_pool_size_mib: 8,
            ..RuntimeOptions::default()
        })
        .collect::<Vec<_>>();
    assert!(validate_group_options(&options).is_ok());
}

#[cfg(feature = "hixl")]
mod lifecycle {
    use super::super::*;
    use crate::hixl::Command;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::sync::{Arc, Mutex, RwLock, mpsc};

    fn options(device_id: i32, transport: ReadTransport) -> RuntimeOptions {
        RuntimeOptions {
            device_id,
            engine_id: format!("127.0.0.1:{}", 7920 + device_id),
            device_listen_port: Some(27000 + device_id as u16),
            transport,
            ..RuntimeOptions::default()
        }
    }

    fn fake_runtime(
        options: &RuntimeOptions,
        domain: Option<u64>,
        shutdown_error: Option<HixlError>,
        shutdown_calls: Arc<AtomicUsize>,
    ) -> Arc<HixlRuntime> {
        let host_bytes = options.window_bytes * (options.receive_slots + options.export_slots);
        let (buffer_pool_count, buffer_pool_size_mib) = options.buffer_pool_dims();
        let info = EngineInfo {
            engine_id: options.engine_id.clone(),
            generation: 1 + options.device_id as u64,
            window_bytes: options.window_bytes,
            transport: options.transport,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id: domain,
        };
        let status = Arc::new(RwLock::new(RuntimeStatus {
            state: RuntimeState::Running,
            generation: info.generation,
            allocated_bytes: host_bytes,
            quarantined_bytes: 0,
            export_slots: options.export_slots,
            receive_slots_in_use: 0,
            export_slots_in_use: 0,
            transport: options.transport,
            relay_hbm_budget_bytes: options.relay_hbm_budget_bytes(),
            hccs_domain_id: domain,
        }));
        let (sender, receiver) = mpsc::sync_channel(4);
        let owner_status = Arc::clone(&status);
        let owner = std::thread::spawn(move || {
            fake_owner(receiver, owner_status, shutdown_error, shutdown_calls)
        });
        Arc::new(HixlRuntime {
            info,
            sender,
            status,
            owner: Mutex::new(Some(owner)),
        })
    }

    fn fake_owner(
        receiver: mpsc::Receiver<Command>,
        status: Arc<RwLock<RuntimeStatus>>,
        mut shutdown_error: Option<HixlError>,
        shutdown_calls: Arc<AtomicUsize>,
    ) {
        while let Ok(command) = receiver.recv() {
            match command {
                Command::Shutdown { reply } => {
                    shutdown_calls.fetch_add(1, Ordering::SeqCst);
                    let result = if let Some(error) = shutdown_error.take() {
                        Err(error)
                    } else {
                        if let Ok(mut status) = status.write() {
                            status.state = RuntimeState::Shutdown;
                        }
                        Ok(())
                    };
                    let should_stop = result.is_ok();
                    let _ = reply.send(result);
                    if should_stop {
                        break;
                    }
                }
                Command::Quarantine { reply } => {
                    if let Ok(mut status) = status.write() {
                        status.state = RuntimeState::Quarantined;
                    }
                    let _ = reply.send(Ok(()));
                }
                _ => {}
            }
        }
    }

    fn shutdown_error(message: &str) -> HixlError {
        HixlError::new(
            HixlPhase::Shutdown,
            HixlErrorKind::NativeFailure,
            None,
            message,
        )
    }

    #[test]
    fn later_lane_start_failure_shuts_down_every_started_lane() {
        let calls = Arc::new(AtomicUsize::new(0));
        let closure_calls = Arc::clone(&calls);
        let options = vec![
            options(0, ReadTransport::Roce),
            options(1, ReadTransport::Roce),
        ];
        let result = HixlRuntimeGroup::start_with(options, move |options| {
            if options.device_id == 1 {
                return Err(HixlError::new(
                    HixlPhase::Initialize,
                    HixlErrorKind::NativeFailure,
                    None,
                    "later lane failed",
                ));
            }
            Ok(fake_runtime(
                &options,
                None,
                None,
                Arc::clone(&closure_calls),
            ))
        });
        let error = match result {
            Ok(_) => panic!("second lane startup should fail"),
            Err(error) => error,
        };
        assert_eq!(error.kind, HixlErrorKind::NativeFailure);
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn incomplete_partial_start_cleanup_is_not_a_safe_fallback() {
        let calls = Arc::new(AtomicUsize::new(0));
        let closure_calls = Arc::clone(&calls);
        let result = HixlRuntimeGroup::start_with(
            vec![
                options(0, ReadTransport::Roce),
                options(1, ReadTransport::Roce),
                options(2, ReadTransport::Roce),
            ],
            move |options| {
                if options.device_id == 2 {
                    return Err(HixlError::new(
                        HixlPhase::Initialize,
                        HixlErrorKind::Unsupported,
                        None,
                        "second lane unavailable",
                    ));
                }
                Ok(fake_runtime(
                    &options,
                    None,
                    (options.device_id == 0).then(|| shutdown_error("first lane did not drain")),
                    Arc::clone(&closure_calls),
                ))
            },
        );
        let error = match result {
            Ok(_) => panic!("incomplete cleanup must fail group startup"),
            Err(error) => error,
        };
        assert_eq!(error.kind, HixlErrorKind::ShutdownIncomplete);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    #[test]
    fn hccs_domain_mismatch_cleans_all_started_lanes() {
        let calls = Arc::new(AtomicUsize::new(0));
        let closure_calls = Arc::clone(&calls);
        let result = HixlRuntimeGroup::start_with(
            vec![
                options(0, ReadTransport::HccsBufferPool),
                options(1, ReadTransport::HccsBufferPool),
            ],
            move |options| {
                Ok(fake_runtime(
                    &options,
                    Some(7 + options.device_id as u64),
                    None,
                    Arc::clone(&closure_calls),
                ))
            },
        );
        let error = match result {
            Ok(_) => panic!("different HCCS domains must fail group startup"),
            Err(error) => error,
        };
        assert_eq!(error.kind, HixlErrorKind::InvalidArgument);
        assert_eq!(calls.load(Ordering::SeqCst), 2);
    }

    /// A lane that records each shutdown with its device id, in order.
    fn recording_runtime(
        options: &RuntimeOptions,
        order: Arc<Mutex<Vec<i32>>>,
    ) -> Arc<HixlRuntime> {
        let device_id = options.device_id;
        let (buffer_pool_count, buffer_pool_size_mib) = options.buffer_pool_dims();
        let info = EngineInfo {
            engine_id: options.engine_id.clone(),
            generation: 1 + device_id as u64,
            window_bytes: options.window_bytes,
            transport: options.transport,
            buffer_pool_count,
            buffer_pool_size_mib,
            hccs_domain_id: None,
        };
        let status = Arc::new(RwLock::new(RuntimeStatus {
            state: RuntimeState::Running,
            generation: info.generation,
            allocated_bytes: 0,
            quarantined_bytes: 0,
            export_slots: options.export_slots,
            receive_slots_in_use: 0,
            export_slots_in_use: 0,
            transport: options.transport,
            relay_hbm_budget_bytes: options.relay_hbm_budget_bytes(),
            hccs_domain_id: None,
        }));
        let (sender, receiver) = mpsc::sync_channel(4);
        let owner_status = Arc::clone(&status);
        let owner = std::thread::spawn(move || {
            while let Ok(command) = receiver.recv() {
                match command {
                    Command::Shutdown { reply } => {
                        order.lock().unwrap().push(device_id);
                        if let Ok(mut status) = owner_status.write() {
                            status.state = RuntimeState::Shutdown;
                        }
                        let _ = reply.send(Ok(()));
                        break;
                    }
                    _ => {}
                }
            }
        });
        Arc::new(HixlRuntime {
            info,
            sender,
            status,
            owner: Mutex::new(Some(owner)),
        })
    }

    #[test]
    fn a_failed_start_rolls_back_started_lanes_newest_first() {
        let order = Arc::new(Mutex::new(Vec::new()));
        let result = HixlRuntimeGroup::start_with(
            vec![
                options(0, ReadTransport::Roce),
                options(1, ReadTransport::Roce),
                options(2, ReadTransport::Roce),
            ],
            {
                let order = Arc::clone(&order);
                move |options| {
                    if options.device_id == 2 {
                        return Err(HixlError::new(
                            HixlPhase::Initialize,
                            HixlErrorKind::NativeFailure,
                            None,
                            "latest lane failed",
                        ));
                    }
                    Ok(recording_runtime(&options, Arc::clone(&order)))
                }
            },
        );
        assert!(result.is_err());
        assert_eq!(
            *order.lock().unwrap(),
            vec![1, 0],
            "rollback must shut down the newest started lane first"
        );
    }

    #[test]
    fn shutdown_attempts_every_lane_and_returns_the_first_error() {
        let first_calls = Arc::new(AtomicUsize::new(0));
        let second_calls = Arc::new(AtomicUsize::new(0));
        let group = HixlRuntimeGroup::start_with(
            vec![
                options(0, ReadTransport::Roce),
                options(1, ReadTransport::Roce),
            ],
            {
                let first_calls = Arc::clone(&first_calls);
                let second_calls = Arc::clone(&second_calls);
                move |options| {
                    let calls = if options.device_id == 0 {
                        Arc::clone(&first_calls)
                    } else {
                        Arc::clone(&second_calls)
                    };
                    let failure = (options.device_id == 0)
                        .then(|| shutdown_error("first lane shutdown failed"));
                    Ok(fake_runtime(&options, None, failure, calls))
                }
            },
        )
        .unwrap();
        let error = group.shutdown().unwrap_err();
        assert_eq!(error.message, "first lane shutdown failed");
        assert_eq!(first_calls.load(Ordering::SeqCst), 1);
        assert_eq!(second_calls.load(Ordering::SeqCst), 1);
    }

    #[test]
    fn quarantine_attempts_every_lane() {
        let calls = Arc::new(AtomicUsize::new(0));
        let group = HixlRuntimeGroup::start_with(
            vec![
                options(0, ReadTransport::Roce),
                options(1, ReadTransport::Roce),
            ],
            {
                let calls = Arc::clone(&calls);
                move |options| Ok(fake_runtime(&options, None, None, Arc::clone(&calls)))
            },
        )
        .unwrap();
        group.lane(0).unwrap().quarantine().unwrap();
        assert_eq!(group.status().state, RuntimeState::Quarantined);
        group.quarantine().unwrap();
        assert!(
            group
                .lane_statuses()
                .iter()
                .all(|status| status.state == RuntimeState::Quarantined)
        );
        assert_eq!(group.status().state, RuntimeState::Quarantined);
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[test]
    fn from_single_preserves_lane_access_and_aggregate_status() {
        let runtime = fake_runtime(
            &options(0, ReadTransport::Roce),
            None,
            None,
            Arc::new(AtomicUsize::new(0)),
        );
        let group = HixlRuntimeGroup::from_single(Arc::clone(&runtime));
        assert_eq!(group.len(), 1);
        assert!(!group.is_empty());
        assert!(Arc::ptr_eq(&group.lane(0).unwrap(), &runtime));
        assert!(group.lane(1).is_none());
        assert_eq!(group.info(), runtime.info());
        assert_eq!(group.infos(), vec![runtime.info()]);
        assert_eq!(
            group.status().allocated_bytes,
            runtime.status().allocated_bytes
        );
        assert_eq!(group.lane_statuses(), vec![runtime.status()]);
    }

    #[test]
    fn status_aggregates_host_hbm_and_slot_capacity_across_lanes() {
        let group = HixlRuntimeGroup::start_with(
            vec![
                options(0, ReadTransport::HccsBufferPool),
                options(1, ReadTransport::HccsBufferPool),
            ],
            |options| {
                Ok(fake_runtime(
                    &options,
                    Some(7),
                    None,
                    Arc::new(AtomicUsize::new(0)),
                ))
            },
        )
        .unwrap();
        let status = group.status();
        assert_eq!(group.len(), 2);
        assert_eq!(group.infos().len(), 2);
        assert_eq!(status.allocated_bytes, 256 * 1024 * 1024);
        assert_eq!(status.relay_hbm_budget_bytes, 128 * 1024 * 1024);
        assert_eq!(status.export_slots, 32);
        assert_eq!(status.state, RuntimeState::Running);
    }
}
