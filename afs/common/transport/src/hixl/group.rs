#[cfg(any(feature = "hixl", test))]
use super::ReadTransport;
use super::{
    EngineInfo, HixlError, HixlErrorKind, HixlPhase, HixlRuntime, RuntimeOptions, RuntimeState,
    RuntimeStatus, validate_options,
};
use std::collections::HashSet;
use std::net::{IpAddr, SocketAddr};
use std::sync::Arc;

const MAX_GROUP_LANES: usize = 16;

/// A bounded set of independent HIXL engines, one per local device lane.
pub struct HixlRuntimeGroup {
    lanes: Vec<Arc<HixlRuntime>>,
}

impl HixlRuntimeGroup {
    /// Starts one independent HIXL runtime for each configured device lane.
    /// Startup is all-or-clean: a partially started group is returned only if
    /// every lane started, and a failed rollback is surfaced as incomplete.
    pub fn start(options: Vec<RuntimeOptions>) -> Result<Arc<Self>, HixlError> {
        #[cfg(not(feature = "hixl"))]
        {
            validate_group_options(&options).map_err(start_phase)?;
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
        Arc::new(Self {
            lanes: vec![runtime],
        })
    }

    /// Primary lane identity used by the authenticated control session.
    #[must_use]
    pub fn info(&self) -> EngineInfo {
        self.lanes[0].info()
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
        let mut aggregate = self.lanes[0].status();
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
        first_lane_error(self.lanes.iter(), HixlRuntime::shutdown)
    }

    /// Quarantines every lane and returns the first lane error.
    pub fn quarantine(&self) -> Result<(), HixlError> {
        first_lane_error(self.lanes.iter(), HixlRuntime::quarantine)
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
        Ok(Arc::new(Self { lanes }))
    }
}

/// Validates lane count, identities and shared transfer dimensions without
/// starting a native runtime. Per-lane Host/HBM budgets bound the whole group.
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
            || lane.transport != first.transport
            || lane.buffer_pool_count != first.buffer_pool_count
            || lane.buffer_pool_size_mib != first.buffer_pool_size_mib
        {
            return Err(group_option_error(
                "HIXL lanes must share window, slots, transport, and BufferPool dimensions",
            ));
        }
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

/// Reports group validation failures as a start-phase error. Only the build
/// without the hixl feature validates before handing off to `start_with`.
#[cfg(not(feature = "hixl"))]
fn start_phase(mut error: HixlError) -> HixlError {
    error.phase = HixlPhase::Start;
    error
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

/// Runs one lane operation on every lane in order and returns the first
/// error; a lane that fails does not stop the remaining lanes.
fn first_lane_error<'a>(
    lanes: impl Iterator<Item = &'a Arc<HixlRuntime>>,
    lane_op: impl Fn(&HixlRuntime) -> Result<(), HixlError>,
) -> Result<(), HixlError> {
    let mut first_error = None;
    for lane in lanes {
        if let Err(error) = lane_op(lane)
            && first_error.is_none()
        {
            first_error = Some(error);
        }
    }
    first_error.map_or(Ok(()), Err)
}

/// Rolls a failed group start back, newest lane first.
#[cfg(feature = "hixl")]
fn shutdown_all(lanes: &[Arc<HixlRuntime>]) -> Result<(), HixlError> {
    first_lane_error(lanes.iter().rev(), HixlRuntime::shutdown)
}

fn state_severity(state: RuntimeState) -> u8 {
    match state {
        RuntimeState::Running => 0,
        RuntimeState::Shutdown => 1,
        RuntimeState::Stopped => 2,
        RuntimeState::Quarantined => 3,
    }
}

#[cfg(test)]
mod tests;
