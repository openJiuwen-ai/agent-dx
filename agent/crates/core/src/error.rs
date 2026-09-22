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
impl From<crate::sandbox::SandboxError> for Error {
    fn from(value: crate::sandbox::SandboxError) -> Self {
        use crate::sandbox::SandboxError as E;
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
