//! Agent-side conversion to the public Sandbox management protocol.
//! No Platform model or RPC client is used here.
use adx_agent_core::{limits, sandbox::*};
use serde_json::{json, Value};
use sha2::{Digest, Sha256};
use std::time::Duration;

pub(crate) fn validate_identity(value: &str) -> Result<(), SandboxError> {
    if value.is_empty()
        || value.len() > limits::IDENTIFIER_BYTES
        || matches!(value, "." | "..")
        || value
            .chars()
            .any(|c| c.is_control() || c == '/' || c == '\\')
    {
        return Err(SandboxError::Invalid("invalid Sandbox identity".into()));
    }
    Ok(())
}
pub(crate) fn observation(tenant: &str, id: &str, phase: SandboxPhase) -> SandboxObservation {
    SandboxObservation {
        id: id.into(),
        tenant: tenant.into(),
        ready: phase == SandboxPhase::Running,
        phase,
        runtime_id: None,
        message: None,
    }
}
pub fn validate_timeout(timeout: Duration) -> Result<(), SandboxError> {
    if timeout.is_zero() || i64::try_from(timeout.as_secs()).is_err() {
        return Err(SandboxError::Invalid("invalid Sandbox timeout".into()));
    }
    Ok(())
}
pub fn validate_execution(execution: &ExecutionSpec) -> Result<(), SandboxError> {
    execution.validate().map_err(SandboxError::Invalid)?;
    if !execution.inherit_entrypoint {
        return Err(SandboxError::Unsupported(
            "API Server adapter requires image process inheritance".into(),
        ));
    }
    if execution
        .env
        .keys()
        .any(|key| key.starts_with("ADX_") || key == "EXECD_HTTP_TOKEN")
    {
        return Err(SandboxError::Invalid("reserved environment key".into()));
    }
    if execution.resources.cpu_millis > i64::MAX as u64
        || execution.resources.memory_mib > (i64::MAX as u64 / 1048576)
    {
        return Err(SandboxError::Invalid(
            "Sandbox resources exceed API Server limits".into(),
        ));
    }
    Ok(())
}
/// Both HTTP and local adapters submit the same request body. Deadline is transport metadata.
pub fn create_input(request: &CreateSandbox, timeout: Duration) -> Result<Value, SandboxError> {
    validate_execution(&request.execution)?;
    validate_identity(&request.tenant)?;
    validate_identity(&request.id)?;
    validate_timeout(timeout)?;
    // AgentBinding generations use adx-<UUID>. The public API builds namespace-name IDs.
    let name = request
        .id
        .strip_prefix("adx-")
        .filter(|name| !name.is_empty() && name.len() <= 256)
        .ok_or_else(|| SandboxError::Invalid("managed Sandbox ID must be adx-<name>".into()))?;
    let execution = &request.execution;
    // Keep the submitted specification stable across retries. The caller's absolute
    // deadline bounds HTTP waiting; API Server owns its independent create budget.
    Ok(json!({
        "namespace":"adx", "name":name, "image":execution.image, "runtime":execution.isolation_runtime,
        "cpu":execution.resources.cpu_millis, "memory":execution.resources.memory_mib,
        "inheritEntrypoint":true, "env":execution.env,
        "createTimeoutSeconds":timeout.as_secs().max(1),
    }))
}
pub fn operation_id(action: &str, tenant: &str, id: &str) -> Result<String, SandboxError> {
    validate_identity(tenant)?;
    validate_identity(id)?;
    let identity = serde_json::to_vec(&(tenant, id))
        .map_err(|_| SandboxError::Invalid("invalid Sandbox identity".into()))?;
    Ok(format!("activator-{action}-{:x}", Sha256::digest(identity)))
}
pub(crate) fn created(
    tenant: &str,
    id: &str,
    value: &Value,
) -> Result<SandboxObservation, SandboxError> {
    if value["sandboxId"] != id || value["instanceId"] != id || value["status"] != "running" {
        return Err(SandboxError::OutcomeUnknown(
            "invalid API Server create response".into(),
        ));
    }
    Ok(observation(tenant, id, SandboxPhase::Running))
}
pub(crate) fn observed(
    tenant: &str,
    id: &str,
    row: &Value,
) -> Result<SandboxObservation, SandboxError> {
    if row["id"] != id {
        return Err(SandboxError::Unavailable(
            "API Server instance identity mismatch".into(),
        ));
    }
    let phase = match row["status"].as_str() {
        Some("running") => SandboxPhase::Running,
        Some("pending" | "starting") => SandboxPhase::Creating,
        Some("failed") => SandboxPhase::Failed,
        Some("paused" | "pausing" | "resuming" | "deleting") => {
            return Err(SandboxError::Conflict(
                "Sandbox is not available for Agent activation".into(),
            ))
        }
        _ => {
            return Err(SandboxError::Unavailable(
                "unknown API Server instance state".into(),
            ))
        }
    };
    Ok(observation(tenant, id, phase))
}
