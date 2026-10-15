#[cfg(any(feature = "hixl", test))]
use super::ReadTransport;
use super::{
    EngineInfo, HIXL_RESOURCE_EXHAUSTED, HixlError, HixlErrorKind, HixlPhase, HixlRuntime,
    MAX_POOL_BYTES, RuntimeOptions, RuntimeState, RuntimeStatus, validate_options,
};
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

const MAX_GROUP_LANES: usize = 16;
const MAX_GROUP_POOL_BYTES: u128 = (MAX_POOL_BYTES as u128) * MAX_GROUP_LANES as u128;

/// A bounded set of independent HIXL engines, one per local device lane.
pub struct HixlRuntimeGroup {
    lanes: Vec<Arc<HixlRuntime>>,
    primary_info: EngineInfo,
}

impl HixlRuntimeGroup {
    /// Starts one independent HIXL runtime for each configured device lane.
    /// Startup is all-or-clean: a partially started group is returned only if
    /// every lane started, and a failed rollback is surfaced as incomplete.
    pub fn start(options: Vec<RuntimeOptions>) -> Result<Arc<Self>, HixlError> {
        validate_group_options(&options).map_err(|mut error| {
            error.phase = HixlPhase::Start;
            error
        })?;
        #[cfg(not(feature = "hixl"))]
        {
            let _ = options;
            Err(HixlError::unsupported(
                HixlPhase::Start,
                "afs-transport was built without the hixl feature",
            ))
        }
        #[cfg(feature = "hixl")]
        {
            Self::start_with(options, HixlRuntime::start)
        }
    }

    /// Wraps an already-started single runtime without changing its owner.
    #[must_use]
    pub fn from_single(runtime: Arc<HixlRuntime>) -> Arc<Self> {
        let primary_info = runtime.info();
        Arc::new(Self {
            lanes: vec![runtime],
            primary_info,
        })
    }

    /// Primary lane identity used by the authenticated control session.
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        self.primary_info.clone()
    }

    /// Stable engine identity for every lane in configuration order.
    #[must_use]
    pub fn infos(&self) -> Vec<EngineInfo> {
        self.lanes.iter().map(|lane| lane.info()).collect()
    }

    /// Returns a lane by its zero-based configuration index.
    #[must_use]
    pub fn lane(&self, index: usize) -> Option<Arc<HixlRuntime>> {
        self.lanes.get(index).cloned()
    }

    /// Number of device lanes in this group.
    #[must_use]
    pub fn len(&self) -> usize {
        self.lanes.len()
    }

    /// Whether this group has no device lanes.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.lanes.is_empty()
    }

    /// Aggregates configured capacity and reports the least healthy lane state.
    #[must_use]
    pub fn status(&self) -> RuntimeStatus {
        let Some(first) = self.lanes.first() else {
            return RuntimeStatus {
                state: RuntimeState::Stopped,
                generation: self.primary_info.generation,
                allocated_bytes: 0,
                quarantined_bytes: 0,
                export_slots: 0,
                receive_slots_in_use: 0,
                export_slots_in_use: 0,
                transport: self.primary_info.transport,
                relay_hbm_budget_bytes: 0,
                hccs_domain_id: self.primary_info.hccs_domain_id,
            };
        };
        let mut aggregate = first.status();
        for lane in self.lanes.iter().skip(1) {
            let status = lane.status();
            if state_severity(status.state) > state_severity(aggregate.state) {
                aggregate.state = status.state;
            }
            aggregate.allocated_bytes = aggregate
                .allocated_bytes
                .saturating_add(status.allocated_bytes);
            aggregate.quarantined_bytes = aggregate
                .quarantined_bytes
                .saturating_add(status.quarantined_bytes);
            aggregate.export_slots = aggregate.export_slots.saturating_add(status.export_slots);
            aggregate.receive_slots_in_use = aggregate
                .receive_slots_in_use
                .saturating_add(status.receive_slots_in_use);
            aggregate.export_slots_in_use = aggregate
                .export_slots_in_use
                .saturating_add(status.export_slots_in_use);
            aggregate.relay_hbm_budget_bytes = aggregate
                .relay_hbm_budget_bytes
                .saturating_add(status.relay_hbm_budget_bytes);
        }
        aggregate
    }

    /// Per-lane lifecycle and resource snapshots in configuration order.
    #[must_use]
    pub fn lane_statuses(&self) -> Vec<RuntimeStatus> {
        self.lanes.iter().map(|lane| lane.status()).collect()
    }

    /// Attempts shutdown on every lane and returns the first lane error.
    pub fn shutdown(&self) -> Result<(), HixlError> {
        let mut first_error = None;
        for lane in &self.lanes {
            if let Err(error) = lane.shutdown()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    /// Quarantines every lane and returns the first lane error.
    pub fn quarantine(&self) -> Result<(), HixlError> {
        let mut first_error = None;
        for lane in &self.lanes {
            if let Err(error) = lane.quarantine()
                && first_error.is_none()
            {
                first_error = Some(error);
            }
        }
        first_error.map_or(Ok(()), Err)
    }

    #[cfg(feature = "hixl")]
    fn start_with<F>(
        options: Vec<RuntimeOptions>,
        mut start_lane: F,
    ) -> Result<Arc<Self>, HixlError>
    where
        F: FnMut(RuntimeOptions) -> Result<Arc<HixlRuntime>, HixlError>,
    {
        validate_group_options(&options).map_err(|mut error| {
            error.phase = HixlPhase::Start;
            error
        })?;
        let mut lanes = Vec::with_capacity(options.len());
        for options in options {
            match start_lane(options) {
                Ok(lane) => lanes.push(lane),
                Err(start_error) => return Err(rollback_start(&lanes, start_error)),
            }
        }
        if lanes[0].info().transport == ReadTransport::HccsBufferPool {
            let expected_domain = lanes[0].info().hccs_domain_id;
            if expected_domain.is_none()
                || lanes
                    .iter()
                    .skip(1)
                    .any(|lane| lane.info().hccs_domain_id != expected_domain)
            {
                let error = HixlError::new(
                    HixlPhase::Start,
                    HixlErrorKind::InvalidArgument,
                    None,
                    "HCCS HIXL lanes must report the same verified device domain",
                );
                return Err(rollback_start(&lanes, error));
            }
        }
        let primary_info = lanes[0].info();
        Ok(Arc::new(Self {
            lanes,
            primary_info,
        }))
    }
}

/// Validates lane count, identities, shared transfer dimensions, and aggregate
/// Host/HBM pool budgets without starting a native runtime.
pub fn validate_group_options(options: &[RuntimeOptions]) -> Result<(), HixlError> {
    if !(1..=MAX_GROUP_LANES).contains(&options.len()) {
        return Err(group_option_error(format!(
            "HIXL runtime group must contain 1..={MAX_GROUP_LANES} lanes"
        )));
    }
    let mut devices = HashSet::with_capacity(options.len());
    let mut engines = HashSet::with_capacity(options.len());
    let mut listen_ports = HashSet::with_capacity(options.len());
    let first = &options[0];
    let mut host_budget = 0_u128;
    let mut hbm_budget = 0_u128;
    for lane in options {
        validate_options(lane)?;
        if !devices.insert(lane.device_id) {
            return Err(group_option_error(format!(
                "duplicate HIXL device id {} in runtime group",
                lane.device_id
            )));
        }
        let endpoint: SocketAddr = lane
            .engine_id
            .parse()
            .map_err(|_| group_option_error("HIXL lane engine_id must be an explicit IP:port"))?;
        if endpoint.port() == 0
            || endpoint.ip().is_unspecified()
            || endpoint.ip().is_multicast()
            || matches!(endpoint.ip(), IpAddr::V4(address) if address.is_broadcast())
        {
            return Err(group_option_error(
                "HIXL lane endpoint must be a concrete unicast address",
            ));
        }
        if !engines.insert(endpoint) {
            return Err(group_option_error(format!(
                "duplicate HIXL engine endpoint {} in runtime group",
                lane.engine_id
            )));
        }
        if options.len() > 1 && lane.device_listen_port.is_none() {
            return Err(group_option_error(
                "multi-device HIXL requires an explicit listener port per lane",
            ));
        }
        if let Some(port) = lane.device_listen_port
            && !listen_ports.insert(port)
        {
            return Err(group_option_error(format!(
                "duplicate HIXL device listen port {port} in runtime group"
            )));
        }
        if lane.window_bytes != first.window_bytes
            || lane.receive_slots != first.receive_slots
            || lane.export_slots != first.export_slots
            || lane.max_inflight_per_peer != first.max_inflight_per_peer
            || lane.transport != first.transport
            || lane.buffer_pool_count != first.buffer_pool_count
            || lane.buffer_pool_size_mib != first.buffer_pool_size_mib
        {
            return Err(group_option_error(
                "HIXL lanes must share window, slots, transport, and BufferPool dimensions",
            ));
        }
        let slots = lane
            .receive_slots
            .checked_add(lane.export_slots)
            .ok_or_else(|| group_option_error("HIXL group Host slot count overflows"))?;
        let lane_host_budget = lane
            .window_bytes
            .checked_mul(slots)
            .ok_or_else(|| group_option_error("HIXL group Host pool budget overflows"))?;
        host_budget = host_budget.saturating_add(lane_host_budget as u128);
        hbm_budget = hbm_budget.saturating_add(lane.relay_hbm_budget_bytes() as u128);
    }
    if host_budget > MAX_GROUP_POOL_BYTES || hbm_budget > MAX_GROUP_POOL_BYTES {
        return Err(HixlError::new(
            HixlPhase::Initialize,
            HixlErrorKind::ResourceExhausted,
            Some(HIXL_RESOURCE_EXHAUSTED),
            "HIXL runtime group exceeds its bounded aggregate Host/HBM budget",
        ));
    }
    Ok(())
}

fn group_option_error(message: impl Into<String>) -> HixlError {
    HixlError::new(
        HixlPhase::Initialize,
        HixlErrorKind::InvalidArgument,
        None,
        message,
    )
}

#[cfg(feature = "hixl")]
fn rollback_start(lanes: &[Arc<HixlRuntime>], start_error: HixlError) -> HixlError {
    let cleanup_error = shutdown_all(lanes).err();
    match cleanup_error {
        Some(cleanup_error) => HixlError::new(
            HixlPhase::Start,
            HixlErrorKind::ShutdownIncomplete,
            cleanup_error.native_status,
            format!(
                "{}; partial HIXL group cleanup is incomplete: {}",
                start_error.message, cleanup_error.message
            ),
        ),
        None => start_error,
    }
}

#[cfg(feature = "hixl")]
fn shutdown_all(lanes: &[Arc<HixlRuntime>]) -> Result<(), HixlError> {
    let mut first_error = None;
    for lane in lanes.iter().rev() {
        if let Err(error) = lane.shutdown()
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

fn state_severity(state: RuntimeState) -> u8 {
    match state {
        RuntimeState::Running => 0,
        RuntimeState::Shutdown => 1,
        RuntimeState::Stopped => 2,
        RuntimeState::Degraded => 3,
        RuntimeState::Quarantined => 4,
    }
}

#[cfg(test)]
mod tests {
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
    fn sixteen_lanes_at_the_per_lane_budget_remain_group_bounded() {
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
            let info = EngineInfo {
                engine_id: options.engine_id.clone(),
                generation: 1 + options.device_id as u64,
                window_bytes: options.window_bytes,
                transport: options.transport,
                buffer_pool_count: if options.transport == ReadTransport::HccsBufferPool {
                    options.buffer_pool_count
                } else {
                    0
                },
                buffer_pool_size_mib: if options.transport == ReadTransport::HccsBufferPool {
                    options.buffer_pool_size_mib
                } else {
                    0
                },
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
                        (options.device_id == 0)
                            .then(|| shutdown_error("first lane did not drain")),
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
}
