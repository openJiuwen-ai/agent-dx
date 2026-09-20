//! Shared wire/storage limits and default budgets. These are not interchangeable limits.
use std::time::Duration;

pub const IDENTIFIER_BYTES: usize = 512;
pub const SERVICE_TOKEN_MIN_BYTES: usize = 32;
pub const HTTP_JSON_BYTES: usize = 1024 * 1024;
pub const INTERNAL_REQUEST_BYTES: usize = 64 * 1024;
pub const RRT_RESPONSE_BYTES: usize = 128 * 1024;
pub const TEMPLATE_CACHE_ENTRIES: usize = 1024;
pub const SESSION_CACHE_ENTRIES: usize = 10_000;
pub const AFFINITY_CACHE_ENTRIES: usize = 256;
pub const CAS_ATTEMPTS: usize = 32;
pub const TRANSACTION_CHECKS: usize = 128;
pub const REDIS_MAX_INFLIGHT: usize = 4096;
pub const SCAN_MAX_COUNT: u32 = 1000;
pub const SCAN_PAGE_COUNT: u32 = 100;
pub const AGENT_REQUEST_TIMEOUT: Duration = Duration::from_secs(60);
pub const SANDBOX_REQUEST_TIMEOUT: Duration = Duration::from_secs(30);
pub const DISPATCHER_FINISH_TIMEOUT: Duration = Duration::from_secs(10);
pub const CREATE_TIMEOUT: Duration = Duration::from_secs(60);
pub const CREATE_TIMEOUT_MESSAGE: &str = "instance creation timed out; deletion requested";
pub const CONNECT_TIMEOUT: Duration = Duration::from_secs(3);
