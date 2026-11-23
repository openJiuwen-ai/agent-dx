pub mod cli;
pub mod config;
mod external;
pub mod rollout;
pub mod supervisor;
pub type Result<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub use adx_observability::capture as logging;
