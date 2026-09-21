use serde::Serialize;
use tonic::{Code, Status};

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryDirective {
    Never,
    SameOperation,
    AfterBackoff,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationOutcome {
    NotStarted,
    Unknown,
    Terminal,
}

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorDetail<'a> {
    pub code: &'static str,
    pub retry: RetryDirective,
    pub outcome: OperationOutcome,
    pub request_id: &'a str,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub operation_id: Option<&'a str>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<&'a str>,
}

impl<'a> ErrorDetail<'a> {
    pub fn from_status(
        status: &Status,
        request_id: &'a str,
        operation_id: Option<&'a str>,
        instance_id: Option<&'a str>,
        execution_may_have_started: bool,
    ) -> Self {
        let (code, retry, outcome) = match status.code() {
            Code::InvalidArgument | Code::OutOfRange => (
                "INVALID_ARGUMENT",
                RetryDirective::Never,
                OperationOutcome::NotStarted,
            ),
            Code::Unauthenticated => (
                "UNAUTHENTICATED",
                RetryDirective::Never,
                OperationOutcome::NotStarted,
            ),
            Code::PermissionDenied => (
                "PERMISSION_DENIED",
                RetryDirective::Never,
                OperationOutcome::NotStarted,
            ),
            Code::NotFound => (
                "NOT_FOUND",
                RetryDirective::Never,
                OperationOutcome::Terminal,
            ),
            Code::AlreadyExists | Code::FailedPrecondition | Code::Aborted => (
                "CONFLICT",
                RetryDirective::Never,
                OperationOutcome::NotStarted,
            ),
            Code::ResourceExhausted => (
                "RESOURCE_EXHAUSTED",
                RetryDirective::AfterBackoff,
                OperationOutcome::NotStarted,
            ),
            Code::Unimplemented => (
                "UNSUPPORTED",
                RetryDirective::Never,
                OperationOutcome::NotStarted,
            ),
            Code::DeadlineExceeded if !execution_may_have_started => (
                "DEADLINE_EXCEEDED",
                RetryDirective::SameOperation,
                OperationOutcome::NotStarted,
            ),
            Code::Unavailable if !execution_may_have_started => (
                "UNAVAILABLE",
                RetryDirective::AfterBackoff,
                OperationOutcome::NotStarted,
            ),
            Code::DataLoss => (
                "DATA_LOSS",
                RetryDirective::Never,
                OperationOutcome::Terminal,
            ),
            Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::Unavailable
            | Code::Internal
                if execution_may_have_started =>
            {
                (
                    "OUTCOME_UNKNOWN",
                    RetryDirective::SameOperation,
                    OperationOutcome::Unknown,
                )
            }
            _ => (
                "INTERNAL",
                RetryDirective::AfterBackoff,
                OperationOutcome::NotStarted,
            ),
        };
        Self {
            code,
            retry,
            outcome,
            request_id,
            operation_id,
            instance_id,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn stable_failures_are_not_retryable() {
        let detail = ErrorDetail::from_status(
            &Status::invalid_argument("bad request"),
            "request-a",
            None,
            None,
            false,
        );
        assert_eq!(detail.code, "INVALID_ARGUMENT");
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
        assert_eq!(detail.code, "OUTCOME_UNKNOWN");
        assert_eq!(detail.retry, RetryDirective::SameOperation);
        assert_eq!(detail.outcome, OperationOutcome::Unknown);
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
        assert_eq!(detail.code, "UNAVAILABLE");
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
            (Code::DeadlineExceeded, true, "OUTCOME_UNKNOWN"),
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
                detail.code, expected,
                "gRPC {grpc:?}, submitted={submitted}"
            );
        }
    }
}
