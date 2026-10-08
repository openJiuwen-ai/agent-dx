use adx_error::{ErrorCode, OperationOutcome, RetryDirective};
use serde::Serialize;
use tonic::{Code, Status};

#[derive(Clone, Debug, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorDetail<'a> {
    pub code: ErrorCode,
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
            Code::DeadlineExceeded if !execution_may_have_started => (
                ErrorCode::DeadlineExceeded,
                RetryDirective::SameOperation,
                OperationOutcome::NotStarted,
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
            Code::Cancelled
            | Code::Unknown
            | Code::DeadlineExceeded
            | Code::Unavailable
            | Code::Internal
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
                detail.code.as_str(),
                expected,
                "gRPC {grpc:?}, submitted={submitted}"
            );
        }
    }
}
