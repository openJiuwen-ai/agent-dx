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
mod tests;
