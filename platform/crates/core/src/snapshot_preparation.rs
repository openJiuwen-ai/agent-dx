//! Durable registration and preparation metadata for reusable snapshot templates.
use crate::{EnvironmentSpec, Error, Resources, Result};
use serde::{Deserialize, Serialize};

pub const MAX_SNAPSHOT_TEMPLATE_NAME_BYTES: usize = 128;
pub const MAX_PREPARATION_ERROR_BYTES: usize = 2048;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationState {
    Registered,
    Creating,
    Snapshotting,
    Cleaning,
    Ready,
    Failed,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotPreparation {
    pub id: String,
    pub created_at_unix_ms: u64,
    #[serde(default)]
    pub updated_at_unix_ms: u64,
    pub resources: Resources,
    pub state: PreparationState,
    pub spec: Option<EnvironmentSpec>,
    pub snapshot_id: Option<String>,
    pub expected_revision: Option<u64>,
    pub error: Option<String>,
    pub cleanup_pending: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTemplate {
    pub id: String,
    pub tenant_id: String,
    pub name: String,
    pub created_at_unix_ms: u64,
    #[serde(default)]
    pub updated_at_unix_ms: u64,
    pub revision: u64,
    pub deleted: bool,
    pub preparation: SnapshotPreparation,
    pub latest_successful: Option<SuccessfulPreparation>,
    /// None denotes history written before native usage accounting existed.
    #[serde(default)]
    pub usage: Option<SnapshotTemplateUsage>,
}

/// Cumulative initial creations from this family, retained across rebuilds and deletions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SnapshotTemplateUsage {
    pub spawn_count: u64,
    pub last_spawned_at_unix_ms: Option<u64>,
}

/// The most recent completed build points to an independently retained native Golden snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SuccessfulPreparation {
    pub preparation_id: String,
    pub snapshot_id: String,
    #[serde(default)]
    pub created_at_unix_ms: u64,
    #[serde(default)]
    pub updated_at_unix_ms: u64,
}

pub fn valid_preparation_id(value: &str) -> bool {
    value.len() == 36
        && value.bytes().enumerate().all(|(index, byte)| {
            if matches!(index, 8 | 13 | 18 | 23) {
                byte == b'-'
            } else {
                byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte)
            }
        })
}

/// Canonicalize a user-facing family name. Unsupported characters and oversized names fail.
pub fn normalize_name(value: &str) -> Result<String> {
    let name = value.trim().to_ascii_lowercase();
    if name.is_empty()
        || name.len() > MAX_SNAPSHOT_TEMPLATE_NAME_BYTES
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || b"-_".contains(&byte))
    {
        return Err(Error::Invalid("invalid snapshot template name".into()));
    }
    Ok(name)
}

pub fn validate_template_tenant(tenant: &str) -> Result<()> {
    if tenant.is_empty()
        || tenant.len() > 128
        || tenant.trim() != tenant
        || tenant.chars().any(char::is_control)
    {
        return Err(Error::Invalid("invalid snapshot template tenant".into()));
    }
    Ok(())
}

impl SnapshotPreparation {
    pub fn validate(&self) -> Result<()> {
        self.resources.validate()?;
        if let Some(spec) = &self.spec {
            spec.validate()?;
        }
        if !valid_preparation_id(&self.id)
            || self.created_at_unix_ms == 0
            || self.expected_revision == Some(0)
            || self.snapshot_id.as_ref().is_some_and(|id| {
                id.is_empty() || id.len() > 256 || id.chars().any(char::is_control)
            })
            || self.error.as_ref().is_some_and(|error| {
                error.len() > MAX_PREPARATION_ERROR_BYTES || error.contains('\0')
            })
            || (matches!(
                self.state,
                PreparationState::Creating
                    | PreparationState::Snapshotting
                    | PreparationState::Cleaning
            ) && self.spec.is_none())
            || (self.state == PreparationState::Snapshotting && self.expected_revision.is_none())
            || (self.state == PreparationState::Cleaning && self.snapshot_id.is_none())
            || (self.state == PreparationState::Ready
                && (self.snapshot_id.is_none() || self.cleanup_pending))
        {
            return Err(Error::Invalid("invalid snapshot preparation".into()));
        }
        Ok(())
    }
}

impl SnapshotTemplate {
    pub fn validate(&self) -> Result<()> {
        validate_template_tenant(&self.tenant_id)?;
        self.preparation.validate()?;
        if !valid_preparation_id(&self.id)
            || normalize_name(&self.name)? != self.name
            || self.created_at_unix_ms == 0
            || self.revision == 0
            || self
                .preparation
                .spec
                .as_ref()
                .is_some_and(|spec| spec.tenant_id != self.tenant_id)
            || self.latest_successful.as_ref().is_some_and(|build| {
                !valid_preparation_id(&build.preparation_id)
                    || build.snapshot_id.is_empty()
                    || build.snapshot_id.len() > 256
                    || build.snapshot_id.chars().any(char::is_control)
                    || (build.preparation_id == self.preparation.id
                        && (self.preparation.state != PreparationState::Ready
                            || self.preparation.snapshot_id.as_ref() != Some(&build.snapshot_id)))
            })
            || (self.preparation.state == PreparationState::Ready
                && self
                    .latest_successful
                    .as_ref()
                    .is_none_or(|build| build.preparation_id != self.preparation.id))
        {
            return Err(Error::Invalid("invalid snapshot template".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn names_are_normalized_without_selector_syntax() {
        assert_eq!(normalize_name("  My-Template  ").unwrap(), "my-template");
        for name in [
            "",
            "../template",
            "template/name",
            "a b",
            "template.v1",
            "demo:default",
        ] {
            assert!(normalize_name(name).is_err(), "{name}");
        }
    }

    #[test]
    fn ready_preparation_requires_capture_and_completed_cleanup() {
        let mut build = SnapshotPreparation {
            id: "00000000-0000-4000-8000-000000000002".into(),
            created_at_unix_ms: 1,
            updated_at_unix_ms: 1,
            resources: Resources {
                cpu_millis: 1000,
                memory_bytes: 1024,
                disk_bytes: 0,
            },
            state: PreparationState::Ready,
            spec: None,
            snapshot_id: Some("capture".into()),
            expected_revision: None,
            error: None,
            cleanup_pending: false,
        };
        build.validate().unwrap();
        build.snapshot_id = None;
        assert!(build.validate().is_err());
        build.snapshot_id = Some("capture".into());
        build.cleanup_pending = true;
        assert!(build.validate().is_err());
    }
}
