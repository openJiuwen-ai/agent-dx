//! Confirmed cancellation of an unassigned central scheduling request.
use adx_core::scheduling::SchedulingFailure;
use tonic::{Code, Status};

const CANCELLED: &str = "adx-scheduling-cancelled";

/// Called only after the Coordinator has cancelled an unassigned request.
pub fn scheduling_deadline(failure: SchedulingFailure) -> Status {
    let reasons = failure
        .rejections
        .iter()
        .map(|(reason, count)| format!("{reason}={count}"))
        .collect::<Vec<_>>()
        .join(", ");
    let message = format!(
        "central scheduling queue deadline exceeded; request CPU={}m memory={} bytes disk={} bytes; \
         max worker capacity CPU={}m memory={} bytes disk={} bytes; \
         max available CPU={}m memory={} bytes disk={} bytes; \
         placement rejections: [{}]; eligible workers={}",
        failure.requested.cpu_millis, failure.requested.memory_bytes, failure.requested.disk_bytes,
        failure.max_capacity.cpu_millis, failure.max_capacity.memory_bytes, failure.max_capacity.disk_bytes,
        failure.max_available.cpu_millis, failure.max_available.memory_bytes, failure.max_available.disk_bytes,
        reasons, failure.eligible_nodes,
    );
    match serde_json::to_vec(&failure) {
        Ok(details) => {
            let mut status = Status::with_details(Code::DeadlineExceeded, message, details.into());
            status.metadata_mut().insert(
                CANCELLED,
                tonic::metadata::MetadataValue::from_static("true"),
            );
            status
        }
        Err(_) => Status::internal("scheduling failure encoding failed"),
    }
}

/// Ordinary deadlines and malformed diagnostics never confirm cancellation.
pub fn scheduling_failure(status: &Status) -> Option<SchedulingFailure> {
    if status.code() != Code::DeadlineExceeded
        || status
            .metadata()
            .get(CANCELLED)
            .is_none_or(|value| value != "true")
    {
        return None;
    }
    serde_json::from_slice(status.details()).ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_marked_valid_scheduling_deadlines_confirm_cancellation() {
        let failure = SchedulingFailure::default();
        let status = scheduling_deadline(failure.clone());
        assert_eq!(scheduling_failure(&status), Some(failure));
        assert!(crate::deadline_failure(&status).is_none());
        let plain = Status::with_details(
            Code::DeadlineExceeded,
            "timeout",
            status.details().to_vec().into(),
        );
        assert!(scheduling_failure(&plain).is_none());
        let mut malformed = Status::deadline_exceeded("unconfirmed");
        malformed
            .metadata_mut()
            .insert(CANCELLED, "true".parse().unwrap());
        assert!(scheduling_failure(&malformed).is_none());
        let runtime = crate::status(adx_core::Error::DeadlineExceeded("runtime start".into()));
        assert!(scheduling_failure(&runtime).is_none());
        assert!(crate::deadline_failure(&runtime).is_some());
    }
}
