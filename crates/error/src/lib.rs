//! Stable error semantics shared across ADX component and public API adapters.
//!
//! Transport-specific status codes and HTTP response bodies are mapped at the
//! boundary. This crate owns only the meaning callers use to decide whether an
//! operation can be retried and whether it may already have taken effect.

use serde::{Deserialize, Serialize};

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum ErrorCode {
    InvalidArgument,
    Unauthenticated,
    PermissionDenied,
    NotFound,
    Conflict,
    ResourceExhausted,
    Unsupported,
    DeadlineExceeded,
    Unavailable,
    DataLoss,
    OutcomeUnknown,
    Internal,
}

impl ErrorCode {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::NotFound => "NOT_FOUND",
            Self::Conflict => "CONFLICT",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::Unsupported => "UNSUPPORTED",
            Self::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Self::Unavailable => "UNAVAILABLE",
            Self::DataLoss => "DATA_LOSS",
            Self::OutcomeUnknown => "OUTCOME_UNKNOWN",
            Self::Internal => "INTERNAL",
        }
    }
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum RetryDirective {
    Never,
    SameOperation,
    AfterBackoff,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum OperationOutcome {
    NotStarted,
    Unknown,
    Committed,
    Terminal,
}

#[derive(Clone, Copy, Debug, Deserialize, Eq, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ErrorSemantics {
    pub code: ErrorCode,
    pub retry: RetryDirective,
    pub outcome: OperationOutcome,
}

impl ErrorSemantics {
    pub const fn new(code: ErrorCode, retry: RetryDirective, outcome: OperationOutcome) -> Self {
        Self {
            code,
            retry,
            outcome,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn serialization_is_the_public_error_contract() {
        let value = serde_json::to_value(ErrorSemantics::new(
            ErrorCode::OutcomeUnknown,
            RetryDirective::SameOperation,
            OperationOutcome::Unknown,
        ))
        .unwrap();
        assert_eq!(value["code"], "OUTCOME_UNKNOWN");
        assert_eq!(value["retry"], "same_operation");
        assert_eq!(value["outcome"], "unknown");
    }

    #[test]
    fn stable_code_strings_do_not_depend_on_serde() {
        assert_eq!(ErrorCode::ResourceExhausted.as_str(), "RESOURCE_EXHAUSTED");
        assert_eq!(ErrorCode::DeadlineExceeded.as_str(), "DEADLINE_EXCEEDED");
    }
}
