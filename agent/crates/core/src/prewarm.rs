//! Immutable restore and preparation inputs for a template version.
use crate::{identifier, parameters, ValidationResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const GOLDEN_PHASE: &str = "GOLDEN_PHASE";
/// Template-derived handoff without seed/restore phase environment overrides.
pub const TEMPLATE_HANDOFF_VERSION: &str = "3";

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
        if self.handoff_contract_version == TEMPLATE_HANDOFF_VERSION && !self.restore_env.is_empty()
        {
            return Err("phase-free Golden handoff does not accept restore-only variables".into());
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

/// Automatic results with the template-derived contract are selected by version.
pub fn template_golden(template: &crate::TemplateVersion) -> Option<&PrewarmRecipe> {
    template.prewarm_recipe.as_ref().filter(|recipe| {
        template.auto_prepare_snapshot
            && template.snapshot_preparation == Some(SnapshotPreparation::Ready)
            && recipe.handoff_contract_version == TEMPLATE_HANDOFF_VERSION
            && recipe.restore_env.is_empty()
    })
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

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct GoldenRetirement {
    pub tenant: String,
    pub snapshot_id: String,
}
