//! Shared wire/storage limits and default budgets. These are not interchangeable limits.
use std::time::Duration;

pub const IDENTIFIER_BYTES: usize = 512;
pub const ENVIRONMENT_PAGE_SIZE: usize = 100;
pub const SERVICE_TOKEN_MIN_BYTES: usize = 32;
pub const HTTP_JSON_BYTES: usize = 1024 * 1024;
pub const CAS_ATTEMPTS: usize = 32;
pub const TRANSACTION_CHECKS: usize = 128;
pub const REDIS_MAX_INFLIGHT: usize = 4096;
pub const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
pub const SANDBOX_REQUEST_TIMEOUT: Duration = AGENT_REQUEST_TIMEOUT;
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
