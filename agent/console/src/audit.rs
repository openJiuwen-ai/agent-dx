//! Credential-free audit events emitted by the independent console process.
use crate::session::{Role, SessionRecord};
use serde::Serialize;

#[derive(Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Outcome { Success, Failure, Unknown }

#[derive(Default, Serialize)]
pub struct AuditTarget {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub slot: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub environment_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub mount_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub key_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub purpose: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub template: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub ref_kind: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reference: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub binding_id: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<String>,
}
#[derive(Serialize)]
pub struct Actor { subject_id: String, role: Role, session_id: String }
#[derive(Serialize)]
pub struct AuditEvent {
    ts: u64,
    actor: Actor,
    target_tenant_id: String,
    action: &'static str,
    target: AuditTarget,
    outcome: Outcome,
}
impl AuditEvent {
    pub fn new(actor: &SessionRecord, tenant: &str, action: &'static str, target: AuditTarget, outcome: Outcome) -> Self {
        Self { ts: std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).map_or(0, |v| v.as_secs()),
            actor: Actor { subject_id:actor.subject_id.clone(), role:actor.role, session_id:actor.session_id.clone() },
            target_tenant_id:tenant.into(), action, target, outcome }
    }
}
pub fn audit(event: AuditEvent) {
    tracing::info!(target: "adx_console_audit", ts = event.ts, actor.subject_id = %event.actor.subject_id,
        actor.role = ?event.actor.role, actor.session_id = %event.actor.session_id,
        target_tenant_id = %event.target_tenant_id, action = event.action,
        target = %serde_json::to_string(&event.target).unwrap_or_default(),
        outcome = %match event.outcome { Outcome::Success => "success", Outcome::Failure => "failure", Outcome::Unknown => "unknown" });
}

/// Cancellation drops the stream and records an unknown delivery outcome.
pub struct DownloadAudit {
    actor: SessionRecord,
    tenant: String,
    target: AuditTarget,
    outcome: Outcome,
}
impl DownloadAudit {
    pub fn new(actor: SessionRecord, tenant: String, target: AuditTarget) -> Self {
        Self { actor, tenant, target, outcome: Outcome::Unknown }
    }
    pub fn finished(&mut self) { self.outcome = Outcome::Success; }
    pub fn failed(&mut self) { self.outcome = Outcome::Failure; }
}
impl Drop for DownloadAudit {
    fn drop(&mut self) {
        audit(AuditEvent::new(&self.actor, &self.tenant, "console.file.download",
            std::mem::take(&mut self.target), std::mem::replace(&mut self.outcome, Outcome::Unknown)));
    }
}
