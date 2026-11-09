//! Native HIXL boundary literals shared by the DFS and OwnerFs HIXL tests.

use afs_transport::hixl::{EngineInfo, HixlError, HixlErrorKind, HixlPhase, ReadTransport};

/// Negotiations a source answered, admitted or refused.
pub(crate) fn negotiations(registry: &afs_metrics::Registry) -> u64 {
    registry
        .gather()
        .into_iter()
        .filter(|family| family.name() == "afs_hixl_operation_duration_seconds")
        .flat_map(|family| family.get_metric().to_vec())
        .filter(|metric| {
            metric
                .get_label()
                .iter()
                .any(|label| label.name() == "phase" && label.value() == "negotiate")
        })
        .map(|metric| metric.get_histogram().get_sample_count())
        .sum()
}

/// A direct RoCE engine identity with no relay pool.
pub(crate) fn engine(engine_id: &str, generation: u64, window_bytes: usize) -> EngineInfo {
    EngineInfo {
        engine_id: engine_id.to_owned(),
        generation,
        window_bytes,
        transport: ReadTransport::Roce,
        buffer_pool_count: 0,
        buffer_pool_size_mib: 0,
        hccs_domain_id: None,
    }
}

/// A definite native failure: the READ is known not to be in flight.
pub(crate) fn native_error(phase: HixlPhase, kind: HixlErrorKind, message: &str) -> HixlError {
    HixlError {
        phase,
        kind,
        native_status: None,
        message: message.into(),
        read_may_be_in_flight: false,
    }
}
