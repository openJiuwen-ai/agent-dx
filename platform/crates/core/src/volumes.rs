//! Stable user-storage definitions and Coordinator-owned execution occupancy.
use crate::{
    execution::{CreateOptions, MountSource},
    Assignment, EnvironmentSpec, Error, Result,
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

impl EnvironmentSpec {
    /// Fix every Volume mount to its saved UID. Missing or mismatched identities conflict.
    pub fn fix_volume_references(&mut self, fixed: &[VolumeRef]) -> Result<()> {
        for mount in &mut self.execution.mounts {
            let MountSource::Volume(reference) = &mut mount.source else {
                continue;
            };
            let saved = fixed
                .iter()
                .find(|saved| saved.id == reference.id)
                .ok_or(Error::Conflict)?;
            if saved.uid.is_none()
                || reference
                    .uid
                    .as_ref()
                    .is_some_and(|uid| Some(uid) != saved.uid.as_ref())
            {
                return Err(Error::Conflict);
            }
            reference.uid.clone_from(&saved.uid);
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    #[default]
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum VolumeSource {
    Directory {
        storage_domain: String,
        path: String,
    },
}

/// Trusted deployment mapping; paths are relative to the same backend on every node.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageDomain {
    pub backend_id: String,
    /// Accept roots verified and reported by current node sessions.
    #[serde(default)]
    pub dynamic_nodes: bool,
    pub node_roots: BTreeMap<String, String>,
    pub tenant_paths: BTreeMap<String, String>,
}

impl StorageDomain {
    /// Reject incomplete backend/topology mappings and unsafe configured paths.
    pub fn validate(&self) -> Result<()> {
        identifier(&self.backend_id)?;
        if !self.dynamic_nodes && self.node_roots.is_empty() {
            return Err(Error::Invalid(
                "volume domain requires accessible nodes".into(),
            ));
        }
        for (node, root) in &self.node_roots {
            identifier(node)?;
            StorageMount {
                backend_id: self.backend_id.clone(),
                root: root.clone(),
            }
            .validate()?;
        }
        for (tenant, path) in &self.tenant_paths {
            identifier(tenant)?;
            relative_directory(path)?;
        }
        Ok(())
    }

    /// Reject a source outside this domain's configured tenant prefix.
    pub fn authorize(&self, spec: &VolumeSpec) -> Result<()> {
        self.validate()?;
        spec.validate()?;
        let prefix = self.tenant_paths.get(&spec.tenant_id).ok_or_else(|| {
            Error::Invalid("tenant has no volume directory in this domain".into())
        })?;
        let VolumeSource::Directory { path, .. } = &spec.source;
        if path != prefix
            && !path
                .strip_prefix(prefix)
                .is_some_and(|tail| tail.starts_with('/'))
        {
            return Err(Error::Invalid(
                "volume directory is outside the tenant prefix".into(),
            ));
        }
        Ok(())
    }
}

/// Node-local root of a configured storage backend, verified before reporting.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StorageMount {
    pub backend_id: String,
    pub root: String,
}

impl StorageMount {
    /// Reject missing backend identities and ambiguous local roots.
    pub fn validate(&self) -> Result<()> {
        identifier(&self.backend_id)?;
        if !self.root.starts_with('/')
            || self.root.chars().any(char::is_control)
            || self.root.split('/').any(|part| matches!(part, "." | ".."))
        {
            return Err(Error::Invalid(
                "volume storage requires an absolute root without traversal".into(),
            ));
        }
        Ok(())
    }
}

impl VolumeSource {
    /// Reject missing storage identities and non-relative, ambiguous directory paths.
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Directory {
                storage_domain,
                path,
            } => {
                identifier(storage_domain)?;
                relative_directory(path)
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeSpec {
    pub tenant_id: String,
    pub id: String,
    pub source: VolumeSource,
    #[serde(default)]
    pub access_mode: AccessMode,
    #[serde(default)]
    pub create_if_missing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_options: Option<CreateOptions>,
}

impl VolumeSpec {
    /// Validate identity, source, and explicit initialization rules without touching storage.
    pub fn validate(&self) -> Result<()> {
        identifier(&self.tenant_id)?;
        validate_volume_id(&self.id)?;
        self.source.validate()?;
        if let Some(options) = &self.create_options {
            if !self.create_if_missing {
                return Err(Error::Invalid(
                    "volume create_options requires create_if_missing".into(),
                ));
            }
            options.mode_bits()?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeRef {
    #[serde(rename = "volume_id")]
    pub id: String,
    #[serde(
        default,
        rename = "volume_uid",
        skip_serializing_if = "Option::is_none"
    )]
    pub uid: Option<String>,
}

impl VolumeRef {
    /// Reject an empty identity; a supplied UID must also be nonempty.
    pub fn validate(&self) -> Result<()> {
        validate_volume_id(&self.id)?;
        if let Some(uid) = &self.uid {
            identifier(uid)?;
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Volume {
    pub uid: String,
    pub spec: VolumeSpec,
    pub revision: u64,
}

impl Volume {
    /// Validate a persisted definition, including its server-generated identity.
    pub fn validate(&self) -> Result<()> {
        self.spec.validate()?;
        identifier(&self.uid)?;
        if self.revision == 0 {
            return Err(Error::Invalid("volume revision must be positive".into()));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OccupancyPhase {
    Reserved,
    Held,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Occupancy {
    pub volume_uid: String,
    pub assignment: Assignment,
    pub runtime_id: String,
    pub node_session_id: String,
    pub phase: OccupancyPhase,
}

impl Occupancy {
    /// Reject an incomplete owner or a runtime from a different execution generation.
    pub fn validate(&self) -> Result<()> {
        identifier(&self.volume_uid)?;
        identifier(&self.assignment.environment_id)?;
        identifier(&self.assignment.node_id)?;
        identifier(&self.node_session_id)?;
        if self.assignment.generation == 0
            || !crate::valid_runtime_id(
                &self.assignment.environment_id,
                self.assignment.generation,
                &self.runtime_id,
            )
        {
            return Err(Error::Invalid(
                "volume occupancy requires a matching execution identity".into(),
            ));
        }
        Ok(())
    }
}

/// Volume IDs are 1–256 ASCII letters, digits, dots, underscores or hyphens.
/// A single or double dot is rejected because URL clients normalize those segments.
pub fn validate_volume_id(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err(Error::Invalid(
            "invalid Volume ID: expected 1–256 URL-safe ASCII characters".into(),
        ));
    }
    Ok(())
}

fn identifier(value: &str) -> Result<()> {
    if value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(Error::Invalid("invalid volume identity".into()));
    }
    Ok(())
}

fn relative_directory(path: &str) -> Result<()> {
    if path.is_empty()
        || path.contains('\\')
        || path.chars().any(char::is_control)
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(Error::Invalid(
            "volume path must be a concrete relative directory".into(),
        ));
    }
    Ok(())
}
