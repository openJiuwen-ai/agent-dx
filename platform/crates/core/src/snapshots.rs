//! Reusable snapshot metadata. References are durable identities, not counters.
use crate::{CheckpointArtifact, EnvironmentSpec, Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeSet,
    time::{SystemTime, UNIX_EPOCH},
};

/// Stable capture identity scoped to tenant, Environment and operation.
/// Names are excluded so retries with changed arguments conflict instead of duplicating captures.
pub fn snapshot_id(tenant: &str, environment: &str, operation: &str) -> Result<String> {
    let identity = serde_json::to_vec(&(tenant, environment, operation))
        .map_err(|_| Error::Invalid("invalid snapshot capture identity".into()))?;
    Ok(format!("snapshot-{:x}", Sha256::digest(identity)))
}

/// Reusable captures share storage and restore semantics regardless of preparation.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SnapshotKind {
    #[default]
    Regular,
    Golden,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum SnapshotState {
    Ready,
    Deleting,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Reference {
    Restore {
        environment_id: String,
    },
    Template {
        node_id: String,
        template_id: String,
    },
}
impl Reference {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Restore { environment_id } if valid_id(environment_id) => Ok(()),
            Self::Template {
                node_id,
                template_id,
            } if valid_id(node_id) && valid_id(template_id) => Ok(()),
            _ => Err(Error::Invalid("invalid snapshot reference".into())),
        }
    }
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Snapshot {
    pub id: String,
    pub names: Vec<String>,
    #[serde(default)]
    pub kind: SnapshotKind,
    /// Capture creation time, or zero when reading metadata written before it was recorded.
    #[serde(default)]
    pub created_at_unix_ms: u64,
    /// Coordinator-owned family provenance; omitted by node wire protocols.
    #[serde(default)]
    pub preparation_template_id: Option<String>,
    pub template: EnvironmentSpec,
    pub source_node_id: String,
    pub source_runtime_id: String,
    pub artifact: CheckpointArtifact,
    pub revision: u64,
    pub state: SnapshotState,
    pub references: BTreeSet<Reference>,
}
impl Snapshot {
    pub fn new(
        id: String,
        names: Vec<String>,
        template: EnvironmentSpec,
        source_node_id: String,
        source_runtime_id: String,
        artifact: CheckpointArtifact,
    ) -> Result<Self> {
        Self::new_with_kind(
            SnapshotKind::Regular,
            id,
            names,
            template,
            source_node_id,
            source_runtime_id,
            artifact,
        )
    }

    /// Create a reusable capture with its trusted preparation kind and fixed creation time.
    /// Returns an invalid-input error for invalid metadata, or unavailable if the clock
    /// cannot be represented as UNIX milliseconds.
    pub fn new_with_kind(
        kind: SnapshotKind,
        id: String,
        names: Vec<String>,
        mut template: EnvironmentSpec,
        source_node_id: String,
        source_runtime_id: String,
        artifact: CheckpointArtifact,
    ) -> Result<Self> {
        let created_at_unix_ms = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .ok()
            .and_then(|duration| u64::try_from(duration.as_millis()).ok())
            .ok_or_else(|| Error::Unavailable("snapshot capture clock unavailable".into()))?;
        // A Golden is a reusable base, not the temporary source's SDK identity.
        // Ordinary captures and records read from storage retain their fixed spec.
        if kind == SnapshotKind::Golden {
            template.compatibility = None;
        }
        let s = Self {
            id,
            names,
            kind,
            created_at_unix_ms,
            preparation_template_id: None,
            template,
            source_node_id,
            source_runtime_id,
            artifact,
            revision: 1,
            state: SnapshotState::Ready,
            references: BTreeSet::new(),
        };
        s.validate()?;
        Ok(s)
    }
    pub fn validate(&self) -> Result<()> {
        self.template.validate()?;
        if !valid_id(&self.id)
            || !valid_id(&self.source_node_id)
            || !valid_id(&self.source_runtime_id)
            || self.preparation_template_id.as_ref().is_some_and(|id| {
                !crate::snapshot_preparation::valid_preparation_id(id)
                    || self.kind != SnapshotKind::Golden
            })
            || self.revision == 0
            || self.artifact.storage.is_empty()
            || self.artifact.location.is_empty()
            || self.artifact.size_bytes == 0
            || self.names.len() > 16
            || self.names.iter().any(|n| !valid_id(n))
            || self.names.iter().collect::<BTreeSet<_>>().len() != self.names.len()
            || (self.state == SnapshotState::Deleted && !self.references.is_empty())
        {
            return Err(Error::Invalid("invalid reusable snapshot".into()));
        }
        for r in &self.references {
            r.validate()?;
        }
        Ok(())
    }
    pub fn origin(&self) -> Result<crate::runtime::RuntimeIdentity> {
        let suffix = self
            .source_runtime_id
            .strip_prefix(&format!("{}-", self.template.id))
            .ok_or(Error::Conflict)?;
        let generation = suffix
            .split("-r")
            .next()
            .and_then(|s| s.parse::<u64>().ok())
            .filter(|g| *g > 0)
            .ok_or(Error::Conflict)?;
        if !crate::valid_runtime_id(&self.template.id, generation, &self.source_runtime_id) {
            return Err(Error::Conflict);
        }
        Ok(crate::runtime::RuntimeIdentity {
            environment_id: self.template.id.clone(),
            runtime_id: self.source_runtime_id.clone(),
            ownership_generation: generation,
        })
    }
    pub fn collectable(&self) -> bool {
        self.state == SnapshotState::Deleting && self.references.is_empty()
    }
    fn advance(&mut self) -> Result<()> {
        self.revision = self.revision.checked_add(1).ok_or(Error::Conflict)?;
        Ok(())
    }
    pub fn acquire(&mut self, tenant: &str, reference: Reference) -> Result<()> {
        self.authorize(tenant)?;
        reference.validate()?;
        if self.references.contains(&reference) {
            return Ok(());
        }
        if self.state != SnapshotState::Ready {
            return Err(Error::Conflict);
        }
        self.advance()?;
        self.references.insert(reference);
        Ok(())
    }
    pub fn release(&mut self, reference: &Reference) -> Result<()> {
        reference.validate()?;
        if self.references.contains(reference) {
            self.advance()?;
            self.references.remove(reference);
        }
        Ok(())
    }
    pub fn delete(&mut self, tenant: &str) -> Result<()> {
        self.authorize(tenant)?;
        if self.state == SnapshotState::Ready {
            if !self.references.is_empty() {
                return Err(Error::SnapshotInUse {
                    sandbox_ids: self
                        .references
                        .iter()
                        .filter_map(|r| match r {
                            Reference::Restore { environment_id } => Some(environment_id.clone()),
                            _ => None,
                        })
                        .collect(),
                    template_ids: self
                        .references
                        .iter()
                        .filter_map(|r| match r {
                            Reference::Template {
                                node_id,
                                template_id,
                            } => Some(format!("{node_id}/{template_id}")),
                            _ => None,
                        })
                        .collect(),
                });
            }
            self.advance()?;
            self.state = SnapshotState::Deleting;
        }
        Ok(())
    }
    pub fn deleted(&mut self, expected: u64) -> Result<()> {
        if self.state == SnapshotState::Deleted {
            return Ok(());
        }
        if self.revision != expected || !self.collectable() {
            return Err(Error::Conflict);
        }
        self.advance()?;
        self.state = SnapshotState::Deleted;
        Ok(())
    }
    pub fn authorize(&self, tenant: &str) -> Result<()> {
        if self.template.tenant_id != tenant {
            return Err(Error::Invalid("snapshot belongs to another tenant".into()));
        }
        Ok(())
    }
    pub fn same_content(&self, other: &Self) -> bool {
        self.id == other.id
            && self.names == other.names
            && self.kind == other.kind
            && self.created_at_unix_ms == other.created_at_unix_ms
            && self.template.compatibility == other.template.compatibility
            && self.matches_template(&other.template)
            && self.source_node_id == other.source_node_id
            && self.source_runtime_id == other.source_runtime_id
            && self.artifact == other.artifact
    }

    /// Match fixed Volume identities, omitting the source's SDK route identity for
    /// credential-free Golden captures. Legacy captures retain strict matching.
    pub fn matches_template(&self, spec: &EnvironmentSpec) -> bool {
        if self.kind == SnapshotKind::Golden && self.template.compatibility.is_none() {
            let mut source = spec.clone();
            source.compatibility = None;
            source.matches_fixed_spec(&self.template)
        } else {
            spec.matches_fixed_spec(&self.template)
        }
    }
}
