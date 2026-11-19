//! Immutable restore input and tenant/product rollout policy.
use crate::{identifier, parameters, version_number, ValidationResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrewarmRecipe {
    pub snapshot_id: String,
    pub image_digest: String,
    pub restore_profile_digest: String,
    pub handoff_contract_version: String,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub restore_env: BTreeMap<String, String>,
}

impl PrewarmRecipe {
    pub fn validate(&self, image: &str) -> ValidationResult {
        identifier(&self.snapshot_id, "snapshot_id")?;
        identifier(&self.handoff_contract_version, "handoff_contract_version")?;
        if self.snapshot_id.trim() != self.snapshot_id
            || matches!(self.snapshot_id.as_str(), "." | "..")
            || self.snapshot_id.contains(['/', '\\'])
            || !sha256(&self.image_digest)
            || !sha256(&self.restore_profile_digest)
            || image
                .rsplit_once('@')
                .is_none_or(|(_, digest)| digest != self.image_digest)
        {
            return Err("invalid fixed Golden restore input".into());
        }
        if self.restore_env.len() > 16 {
            return Err("too many restore-only variables".into());
        }
        for (key, value) in &self.restore_env {
            let name = key.to_ascii_uppercase();
            if key.is_empty()
                || key.len() > 128
                || value.len() > 512
                || !key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
                || name.starts_with("ADX_")
                || name.starts_with("EXECD_")
                || matches!(name.as_str(), "HOME" | "PATH" | "API_KEY")
                || ["TOKEN", "PASSWORD", "PRIVATE_KEY", "SECRET", "API_KEY"]
                    .iter()
                    .any(|marker| name.contains(marker))
                || value.contains('\0')
                || parameters::render(value, None).is_err()
                || value.contains("${")
            {
                return Err("invalid restore-only variable".into());
            }
        }
        Ok(())
    }
}

pub fn sha256(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(workspace_digest)
}

pub fn workspace_digest(hex: &str) -> bool {
    hex.len() == 64
        && hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SnapshotPreparation {
    Pending,
    Ready,
    Failed { reason: String },
}

/// Fixed and sanitized execution input, captured in the publication transaction.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SeedExecution {
    pub execution: crate::environment::ExecutionSpec,
    pub volume: crate::volumes::VolumeSpec,
    /// Native request captured by the publishing adapter, including deployment policy.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_input: Option<serde_json::Value>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "status", rename_all = "snake_case", deny_unknown_fields)]
pub enum SeedResult {
    Pending,
    Ready { snapshot_id: String },
    Failed,
}

/// Only supplied by an authenticated product entrypoint; timestamp is from account storage.
/// The stable subject is the caller's canonical product Binding id, independent of template reference.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EligibleStart {
    pub product: String,
    pub registered_at_ms: Option<u64>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PrewarmPolicy {
    pub enabled: bool,
    pub first_enabled_at_ms: Option<u64>,
    /// Older revisions lack proof that every creation path writes the assignment ledger.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub assignment_tracking_since_ms: Option<u64>,
    pub template: String,
    pub version: String,
    pub validation_ref: String,
}
impl PrewarmPolicy {
    pub fn allows(&self, subject: &EligibleStart) -> bool {
        self.enabled
            && self
                .first_enabled_at_ms
                .zip(self.assignment_tracking_since_ms)
                .zip(subject.registered_at_ms)
                .is_some_and(|((cut, tracked), registration)| registration > cut.max(tracked))
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyScope {
    pub tenant: String,
    pub product: String,
}
impl PolicyScope {
    pub fn validate(&self) -> ValidationResult {
        identifier(&self.tenant, "tenant")?;
        identifier(&self.product, "product")
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicyUpdate {
    pub scope: PolicyScope,
    pub expected_revision: Option<String>,
    pub enabled: bool,
    pub template: String,
    pub version: String,
    /// Receipt for the operator's immutable backend/volume/serving acceptance.
    pub validation_ref: String,
}
impl PolicyUpdate {
    pub fn validate(&self) -> ValidationResult {
        self.scope.validate()?;
        identifier(&self.template, "template")?;
        version_number(&self.version)?;
        identifier(&self.validation_ref, "validation_ref")?;
        if self.validation_ref.trim() != self.validation_ref {
            return Err("invalid validation reference".into());
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoldenRetirement {
    pub tenant: String,
    pub snapshot_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PolicySnapshot {
    pub revision: String,
    pub policy: PrewarmPolicy,
}
