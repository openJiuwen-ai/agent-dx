//! Reusable snapshot metadata. References are durable identities, not counters.
use crate::{CapsuleSpec, CheckpointArtifact, Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::BTreeSet;
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
        capsule_id: String,
    },
    Template {
        node_id: String,
        template_id: String,
    },
}
impl Reference {
    pub fn validate(&self) -> Result<()> {
        match self {
            Self::Restore { capsule_id } if valid_id(capsule_id) => Ok(()),
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
    pub template: CapsuleSpec,
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
        template: CapsuleSpec,
        source_node_id: String,
        source_runtime_id: String,
        artifact: CheckpointArtifact,
    ) -> Result<Self> {
        let s = Self {
            id,
            names,
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
            capsule_id: self.template.id.clone(),
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
            && self.template == other.template
            && self.source_node_id == other.source_node_id
            && self.source_runtime_id == other.source_runtime_id
            && self.artifact == other.artifact
    }
}
