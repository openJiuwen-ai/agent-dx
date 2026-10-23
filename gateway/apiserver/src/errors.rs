use adx_error::{ErrorCode, OperationOutcome, RetryDirective};
use serde::Serialize;
use tonic::{Code, Status};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorDetail<'a> {
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "sandbox_ids")]
    pub sandbox_ids: Vec<String>,
    #[serde(skip_serializing_if = "Vec::is_empty", rename = "template_ids")]
    pub template_ids: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub probe_failure: Option<adx_core::probes::ProbeFailure>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub scheduling_failure: Option<adx_core::scheduling::SchedulingFailure>,
    pub code: ErrorCode,
    pub retry: RetryDirective,
    pub outcome: OperationOutcome,
    pub request_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment_id: Option<&'a str>,
}

impl<'a> ErrorDetail<'a> {
    pub fn from_status(
        status: &Status,
        request_id: &'a str,
        operation_id: Option<&'a str>,
        environment_id: Option<&'a str>,
        execution_may_have_started: bool,
    ) -> Self {
        let probe_failure =
            serde_json::from_slice::<adx_core::probes::ProbeFailure>(status.details()).ok();
        let blockers = serde_json::from_slice::<serde_json::Value>(status.details())
            .ok()
            .filter(|v| v["code"] == "SNAPSHOT_IN_USE");
        let scheduling_failure = adx_protocol::scheduling_failure(status);
        let terminal_deadline = adx_protocol::deadline_failure(status).is_some();
        let (code, retry, outcome) = if blockers.is_some() {
            (
                ErrorCode::SnapshotInUse,
                RetryDirective::Never,
                OperationOutcome::NotStarted,
            )
        } else if scheduling_failure.is_some() {
            (
                ErrorCode::DeadlineExceeded,
                RetryDirective::Never,
                OperationOutcome::NotStarted,
            )
        } else if terminal_deadline {
            (
                ErrorCode::DeadlineExceeded,
                RetryDirective::Never,
                OperationOutcome::Terminal,
            )
        } else if status.code() == Code::ResourceExhausted
            && status
                .metadata()
                .get("adx-backend-failure")
                .is_some_and(|value| value == "true")
        {
            (
                ErrorCode::ResourceExhausted,
                RetryDirective::Never,
                OperationOutcome::Terminal,
            )
        } else if probe_failure.is_some() {
            (
                ErrorCode::Unavailable,
                RetryDirective::Never,
                OperationOutcome::Terminal,
            )
        } else {
            match status.code() {
                Code::InvalidArgument | Code::OutOfRange => (
                    ErrorCode::InvalidArgument,
                    RetryDirective::Never,
                    OperationOutcome::NotStarted,
                ),
                Code::Unauthenticated => (
                    ErrorCode::Unauthenticated,
                    RetryDirective::Never,
                    OperationOutcome::NotStarted,
                ),
                Code::PermissionDenied => (
                    ErrorCode::PermissionDenied,
                    RetryDirective::Never,
                    OperationOutcome::NotStarted,
                ),
                Code::NotFound => (
                    ErrorCode::NotFound,
                    RetryDirective::Never,
                    OperationOutcome::Terminal,
                ),
                Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => (
                    ErrorCode::Conflict,
                    RetryDirective::Never,
                    OperationOutcome::NotStarted,
                ),
                Code::ResourceExhausted => (
                    ErrorCode::ResourceExhausted,
                    RetryDirective::AfterBackoff,
                    OperationOutcome::NotStarted,
                ),
                Code::Unimplemented => (
                    ErrorCode::Unsupported,
                    RetryDirective::Never,
                    OperationOutcome::NotStarted,
                ),
                Code::DeadlineExceeded => (
                    ErrorCode::DeadlineExceeded,
                    RetryDirective::SameOperation,
                    if execution_may_have_started {
                        OperationOutcome::Unknown
                    } else {
                        OperationOutcome::NotStarted
                    },
                ),
                Code::Unavailable if !execution_may_have_started => (
                    ErrorCode::Unavailable,
                    RetryDirective::AfterBackoff,
                    OperationOutcome::NotStarted,
                ),
                Code::DataLoss => (
                    ErrorCode::DataLoss,
                    RetryDirective::Never,
                    OperationOutcome::Terminal,
                ),
                Code::Cancelled | Code::Unknown | Code::Unavailable | Code::Internal
                    if execution_may_have_started =>
                {
                    (
                        ErrorCode::OutcomeUnknown,
                        RetryDirective::SameOperation,
                        OperationOutcome::Unknown,
                    )
                }
                _ => (
                    ErrorCode::Internal,
                    RetryDirective::AfterBackoff,
                    OperationOutcome::NotStarted,
                ),
            }
        };
        Self {
            sandbox_ids: blockers
                .as_ref()
                .and_then(|v| serde_json::from_value(v["sandbox_ids"].clone()).ok())
                .unwrap_or_default(),
            template_ids: blockers
                .as_ref()
                .and_then(|v| serde_json::from_value(v["template_ids"].clone()).ok())
                .unwrap_or_default(),
            probe_failure,
            scheduling_failure,
            code,
            retry,
            outcome,
            request_id,
            operation_id,
            environment_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backend_resource_rejection_is_terminal_while_admission_remains_retryable() {
        let backend = adx_protocol::status(adx_protocol::backend_error(
            Status::resource_exhausted("image registry rate limited the pull"),
        ));
        let rejected = ErrorDetail::from_status(&backend, "request", None, None, true);
        assert_eq!(rejected.code, ErrorCode::ResourceExhausted);
        assert_eq!(rejected.retry, RetryDirective::Never);
        assert_eq!(rejected.outcome, OperationOutcome::Terminal);
        let admission = ErrorDetail::from_status(
            &Status::resource_exhausted("node capacity"),
            "request",
            None,
            None,
            true,
        );
        assert_eq!(admission.retry, RetryDirective::AfterBackoff);
        assert_eq!(admission.outcome, OperationOutcome::NotStarted);
    }

    #[test]
    fn final_probe_failure_retains_metadata_and_terminal_retry_contract() {
        let failure = adx_core::probes::ProbeFailure::timeout("readiness deadline exceeded");
        let status = adx_protocol::status(adx_core::Error::ProbeFailed(failure.clone()));
        let detail = ErrorDetail::from_status(&status, "create-a", None, Some("env-a"), true);
        assert_eq!(detail.retry, RetryDirective::Never);
        assert_eq!(detail.outcome, OperationOutcome::Terminal);
        assert_eq!(detail.probe_failure, Some(failure));
    }

    #[test]
    fn stable_failures_are_not_retryable() {
        let detail = ErrorDetail::from_status(
            &Status::invalid_argument("bad request"),
            "request-a",
            None,
            None,
            false,
        );
        assert_eq!(detail.code, ErrorCode::InvalidArgument);
        assert_eq!(detail.retry, RetryDirective::Never);
        assert_eq!(detail.outcome, OperationOutcome::NotStarted);
    }

    #[test]
    fn transport_failure_after_submission_preserves_unknown_outcome() {
        let detail = ErrorDetail::from_status(
            &Status::unavailable("reply lost"),
            "request-a",
            Some("pause-a"),
            Some("instance-a"),
            true,
        );
        assert_eq!(detail.code, ErrorCode::OutcomeUnknown);
        assert_eq!(detail.retry, RetryDirective::SameOperation);
        assert_eq!(detail.outcome, OperationOutcome::Unknown);
    }

    #[test]
    fn deadline_after_submission_retains_timeout_classification() {
        let detail = ErrorDetail::from_status(
            &Status::deadline_exceeded("create deadline exceeded during node initialization"),
            "create-a",
            None,
            Some("env-a"),
            true,
        );
        assert_eq!(detail.code, ErrorCode::DeadlineExceeded);
        // A transport wait timeout alone does not establish a terminal result.
        assert_eq!(detail.outcome, OperationOutcome::Unknown);
        assert_eq!(detail.retry, RetryDirective::SameOperation);
    }

    #[test]
    fn confirmed_create_timeout_is_terminal_and_preserves_its_message() {
        let status = adx_protocol::status(adx_core::Error::DeadlineExceeded(
            "create deadline exceeded during backend preparation/runtime start".into(),
        ));
        let detail = ErrorDetail::from_status(&status, "create-a", None, Some("env-a"), true);
        assert_eq!(detail.code, ErrorCode::DeadlineExceeded);
        assert_eq!(detail.retry, RetryDirective::Never);
        assert_eq!(detail.outcome, OperationOutcome::Terminal);
        assert!(status
            .message()
            .contains("backend preparation/runtime start"));
    }

    #[test]
    fn unavailable_before_submission_uses_backoff() {
        let detail = ErrorDetail::from_status(
            &Status::unavailable("dependency unavailable"),
            "request-a",
            None,
            None,
            false,
        );
        assert_eq!(detail.code, ErrorCode::Unavailable);
        assert_eq!(detail.retry, RetryDirective::AfterBackoff);
        assert_eq!(detail.outcome, OperationOutcome::NotStarted);
    }

    #[test]
    fn every_documented_grpc_class_has_a_stable_mapping() {
        let cases = [
            (Code::InvalidArgument, false, "INVALID_ARGUMENT"),
            (Code::OutOfRange, false, "INVALID_ARGUMENT"),
            (Code::Unauthenticated, false, "UNAUTHENTICATED"),
            (Code::PermissionDenied, false, "PERMISSION_DENIED"),
            (Code::NotFound, false, "NOT_FOUND"),
            (Code::AlreadyExists, false, "CONFLICT"),
            (Code::FailedPrecondition, false, "CONFLICT"),
            (Code::Aborted, false, "CONFLICT"),
            (Code::ResourceExhausted, false, "RESOURCE_EXHAUSTED"),
            (Code::Unimplemented, false, "UNSUPPORTED"),
            (Code::Unavailable, false, "UNAVAILABLE"),
            (Code::DeadlineExceeded, false, "DEADLINE_EXCEEDED"),
            (Code::DataLoss, false, "DATA_LOSS"),
            (Code::Internal, false, "INTERNAL"),
            (Code::Unavailable, true, "OUTCOME_UNKNOWN"),
            (Code::DeadlineExceeded, true, "DEADLINE_EXCEEDED"),
            (Code::Cancelled, true, "OUTCOME_UNKNOWN"),
            (Code::Unknown, true, "OUTCOME_UNKNOWN"),
            (Code::Internal, true, "OUTCOME_UNKNOWN"),
        ];
        for (grpc, submitted, expected) in cases {
            let detail = ErrorDetail::from_status(
                &Status::new(grpc, "test"),
                "request-a",
                None,
                None,
                submitted,
            );
            assert_eq!(
                detail.code.as_str(),
                expected,
                "gRPC {grpc:?}, submitted={submitted}"
            );
        }
    }
}
