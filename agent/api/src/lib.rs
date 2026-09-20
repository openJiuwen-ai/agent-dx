//! Agent APIs embedded by Gateway. Inline needs only the injected Sandbox boundary, not ADX Redis or Dispatcher.
pub mod dispatcher;
pub mod managed;
pub mod management;
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "kind", content = "message", rename_all = "snake_case")]
pub enum Error {
    Invalid(String),
    Unsupported(String),
    NotFound,
    NotReady(String),
    Conflict(String),
    Unavailable(String),
    OutcomeUnknown(String),
}
impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self:?}")
    }
}
impl std::error::Error for Error {}
pub type Result<T> = std::result::Result<T, Error>;
impl From<adx_agent_store::Error> for Error {
    fn from(value: adx_agent_store::Error) -> Self {
        use adx_agent_store::Error as E;
        match value {
            E::Invalid(s) => Self::Invalid(s),
            E::Conflict(s) => Self::Conflict(s),
            E::Unavailable(s) => Self::Unavailable(s),
            E::OutcomeUnknown(s) => Self::OutcomeUnknown(s),
            E::Corrupt(_) => Self::Unavailable("invalid stored Agent state".into()),
        }
    }
}
impl From<adx_agent_core::sandbox::SandboxError> for Error {
    fn from(value: adx_agent_core::sandbox::SandboxError) -> Self {
        use adx_agent_core::sandbox::SandboxError as E;
        match value {
            E::NotFound => Self::NotFound,
            E::Invalid(s) => Self::Invalid(s),
            E::Unsupported(s) => Self::Unsupported(s),
            E::Conflict(s) => Self::Conflict(s),
            E::Unavailable(s) => Self::Unavailable(s),
            E::OutcomeUnknown(s) => Self::OutcomeUnknown(s),
        }
    }
}
