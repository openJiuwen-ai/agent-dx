//! Stable span attribute names shared by every ADX component.
//!
//! The bare names mirror the core-flow-tracing contract. Component-local
//! aliases such as `environment.id` remain in place and are written alongside
//! the bare name when both are meaningful.

pub const RESULT: &str = "result";
pub const ERROR_CODE: &str = "error.code";
pub const ERROR_MESSAGE: &str = "error.message";
pub const INSTANCE_ID: &str = "instance_id";
pub const ENVIRONMENT_ID: &str = "environment.id";
pub const AGENT_TYPE: &str = "agent.type";
pub const USER_ID: &str = "user.id";
pub const NODE_ID: &str = "node.id";
pub const SANDBOX_ID: &str = "sandbox.id";
pub const RETRY_COUNT: &str = "retry.count";
pub const RETRY_FIRST_ERROR_CODE: &str = "retry.first_error_code";
pub const RETRY_FIRST_ERROR_MESSAGE: &str = "retry.first_error_message";
pub const SOURCE: &str = "source";
pub const COMPONENT: &str = "adx.component";

pub const HTTP_REQUEST_METHOD: &str = "http.request.method";
pub const HTTP_ROUTE: &str = "http.route";
pub const HTTP_RESPONSE_STATUS_CODE: &str = "http.response.status_code";
pub const RPC_METHOD: &str = "rpc.method";
