//! Managed image ownership is independent of an SDK process or Environment.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::{BTreeMap, BTreeSet};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "mode", rename_all = "snake_case", deny_unknown_fields)]
pub enum ImageRetention {
    Retained,
    Ephemeral { grace_seconds: u64 },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PublicationState {
    Preparing,
    Publishing,
    Ready,
    Deleting,
    Deleted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ManagedImage {
    pub id: String,
    pub tenant_id: String,
    pub request_id: String,
    pub revision: u64,
    pub created_at: u64,
    pub state: PublicationState,
    pub retention: ImageRetention,
    pub initial_retention: ImageRetention,
    pub retain_until: u64,
    pub plan: BTreeMap<String, String>,
    pub outputs: BTreeMap<String, String>,
    pub leases: BTreeSet<String>,
    pub released_leases: BTreeSet<String>,
    pub references: BTreeSet<String>,
    pub unreferenced_since: Option<u64>,
    pub deletion_manifests: BTreeSet<String>,
    pub execution_id: String,
    pub deleted_manifest_count: usize,
}

pub fn valid_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
fn label(value: &str) -> Result<()> {
    if value.is_empty() || value.len() > 256 || value.chars().any(char::is_control) {
        return Err(Error::Invalid("bounded image identity required".into()));
    }
    Ok(())
}
fn digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(valid_id)
}

impl ManagedImage {
    /// A tenant-scoped request identity survives lost creation responses.
    pub fn new(tenant: &str, request: &str, retention: ImageRetention, now: u64) -> Result<Self> {
        label(tenant)?;
        label(request)?;
        if matches!(retention, ImageRetention::Ephemeral { grace_seconds } if grace_seconds > 31_536_000)
        {
            return Err(Error::Invalid("image grace exceeds one year".into()));
        }
        let identity = serde_json::to_vec(&(tenant, request))
            .map_err(|_| Error::Invalid("invalid image publication identity".into()))?;
        Ok(Self {
            id: format!("{:x}", Sha256::digest(identity)),
            tenant_id: tenant.into(),
            request_id: request.into(),
            revision: 1,
            created_at: now,
            state: PublicationState::Preparing,
            initial_retention: retention.clone(),
            retention,
            retain_until: 0,
            plan: BTreeMap::new(),
            outputs: BTreeMap::new(),
            leases: BTreeSet::from(["publisher".into()]),
            released_leases: BTreeSet::new(),
            references: BTreeSet::new(),
            unreferenced_since: None,
            deletion_manifests: BTreeSet::new(),
            execution_id: String::new(),
            deleted_manifest_count: 0,
        })
    }

    fn available(&self) -> Result<()> {
        if matches!(
            self.state,
            PublicationState::Deleting | PublicationState::Deleted
        ) {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Register every output before executing the native publisher. Each new
    /// publication uses its own repository namespace; legacy repositories cannot be adopted.
    pub fn plan(&mut self, outputs: BTreeMap<String, String>, _now: u64) -> Result<()> {
        self.available()?;
        if !self.plan.is_empty() {
            return if self.plan == outputs {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        if self.state != PublicationState::Preparing || outputs.is_empty() || outputs.len() > 128 {
            return Err(Error::Invalid(
                "one to 128 planned image outputs required".into(),
            ));
        }
        let marker = format!("/publications/{}/", self.id);
        let mut unique = BTreeSet::new();
        for (name, tagged) in &outputs {
            label(name)?;
            let (repository, tag) = tagged
                .rsplit_once(':')
                .ok_or_else(|| Error::Invalid("tagged publication output required".into()))?;
            let (host, path) = repository
                .split_once('/')
                .ok_or_else(|| Error::Invalid("Registry repository required".into()))?;
            if host.is_empty()
                || !host
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"[]:.-".contains(&b))
                || !path
                    .bytes()
                    .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"/._-".contains(&b))
                || host.contains('@')
                || repository.contains("://")
                || tagged.len() > 1024
                || tagged.chars().any(char::is_whitespace)
                || !repository.contains(&marker)
                || path
                    .split('/')
                    .any(|part| part.is_empty() || part == "." || part == "..")
                || tag.is_empty()
                || !tag
                    .bytes()
                    .all(|b| b.is_ascii_alphanumeric() || b"_.-".contains(&b))
                || !unique.insert(repository)
            {
                return Err(Error::Invalid(
                    "output requires a unique managed publication repository".into(),
                ));
            }
        }
        self.plan = outputs;
        self.state = PublicationState::Publishing;
        Ok(())
    }

    /// Completion is all-or-nothing; partial publication remains protected and queryable.
    pub fn publish(&mut self, digests: BTreeMap<String, String>, _now: u64) -> Result<()> {
        self.available()?;
        if self.plan.is_empty()
            || self.plan.keys().ne(digests.keys())
            || digests.values().any(|v| !digest(v))
        {
            return Err(Error::Invalid(
                "complete immutable publication receipt required".into(),
            ));
        }
        let outputs: BTreeMap<_, _> = digests
            .into_iter()
            .map(|(name, digest)| {
                let tagged = &self.plan[&name];
                let repository = tagged
                    .rsplit_once(':')
                    .map_or(tagged.as_str(), |(repo, _)| repo);
                (name, format!("{repository}@{digest}"))
            })
            .collect();
        if self.state == PublicationState::Ready {
            return if self.outputs == outputs {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        if self.state != PublicationState::Publishing {
            return Err(Error::Conflict);
        }
        self.outputs = outputs;
        self.state = PublicationState::Ready;
        Ok(())
    }

    pub fn retain(&mut self, lease: &str) -> Result<()> {
        self.available()?;
        label(lease)?;
        // A late release must never release a new acquisition under the same ID.
        if self.released_leases.contains(lease)
            || (!self.leases.contains(lease)
                && self.leases.len() + self.released_leases.len() >= 4096)
        {
            return Err(Error::Conflict);
        }
        self.leases.insert(lease.into());
        self.unreferenced_since = None;
        Ok(())
    }
    pub fn release(&mut self, lease: &str) -> Result<()> {
        label(lease)?;
        if self.state == PublicationState::Deleted {
            return Ok(());
        }
        if !self.leases.contains(lease) && !self.released_leases.contains(lease) {
            return Err(Error::NotFound);
        }
        self.leases.remove(lease);
        self.released_leases.insert(lease.into());
        Ok(())
    }
    pub fn extend(&mut self, until: u64) -> Result<()> {
        self.available()?;
        self.retain_until = self.retain_until.max(until);
        Ok(())
    }
    pub fn pin(&mut self) -> Result<()> {
        self.available()?;
        self.retention = ImageRetention::Retained;
        self.unreferenced_since = None;
        Ok(())
    }
    pub fn set_retention(
        &mut self,
        retention: ImageRetention,
        expected_revision: u64,
    ) -> Result<()> {
        self.available()?;
        if self.revision != expected_revision {
            return Err(Error::Conflict);
        }
        if matches!(retention, ImageRetention::Ephemeral { grace_seconds } if grace_seconds > 31_536_000)
        {
            return Err(Error::Invalid("image grace exceeds one year".into()));
        }
        self.retention = retention;
        self.unreferenced_since = None;
        Ok(())
    }
    /// Reconciler observations never substitute for the fresh inventory at deletion claim.
    pub fn observe(&mut self, references: &[String], now: u64) -> Result<()> {
        if !references.is_empty() {
            self.available()?;
        }
        self.references = references.iter().cloned().collect();
        if self.state == PublicationState::Ready && self.leases.is_empty() && references.is_empty()
        {
            self.unreferenced_since.get_or_insert(now);
        } else {
            self.unreferenced_since = None;
        }
        Ok(())
    }
    /// Called by admission before exposing a new consumer, including a queued create.
    pub fn acquire(&mut self) -> Result<()> {
        self.available()?;
        self.unreferenced_since = None;
        Ok(())
    }
    pub fn reclaimable(&self, now: u64) -> bool {
        let ImageRetention::Ephemeral { grace_seconds } = self.retention else {
            return false;
        };
        self.state == PublicationState::Ready
            && self.leases.is_empty()
            && self.references.is_empty()
            && now >= self.retain_until
            && self
                .unreferenced_since
                .and_then(|t| t.checked_add(grace_seconds))
                .is_some_and(|deadline| now >= deadline)
    }
    pub fn claim_deletion(&mut self, now: u64) -> Result<()> {
        if self.state == PublicationState::Deleting {
            return Ok(());
        }
        if !self.reclaimable(now) {
            return Err(Error::Conflict);
        }
        self.state = PublicationState::Deleting;
        Ok(())
    }
    /// Persist the complete manifest worklist before any remote deletion.
    /// Only this publication's repositories are accepted; shared blobs are never targets.
    pub fn claim_manifests(&mut self, manifests: &[String], now: u64) -> Result<()> {
        let manifests: BTreeSet<_> = manifests.iter().cloned().collect();
        let repositories: BTreeSet<_> = self
            .outputs
            .values()
            .filter_map(|v| v.split_once('@').map(|(repo, _)| repo))
            .collect();
        if manifests.is_empty()
            || manifests.len() > 4096
            || self.outputs.values().any(|v| !manifests.contains(v))
            || manifests.iter().any(|v| {
                v.split_once('@')
                    .is_none_or(|(repo, hash)| !repositories.contains(repo) || !digest(hash))
            })
        {
            return Err(Error::Invalid(
                "complete publication-scoped manifest worklist required".into(),
            ));
        }
        if self.state == PublicationState::Deleting {
            return if self.deletion_manifests == manifests {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        self.claim_deletion(now)?;
        self.deletion_manifests = manifests;
        Ok(())
    }

    pub fn finish_deletion(&mut self) -> Result<()> {
        if self.state == PublicationState::Deleted {
            return Ok(());
        }
        if !matches!(
            self.state,
            PublicationState::Deleting | PublicationState::Deleted
        ) {
            return Err(Error::Conflict);
        }
        self.state = PublicationState::Deleted;
        self.deleted_manifest_count = self.deletion_manifests.len();
        self.deletion_manifests.clear();
        self.released_leases.clear();
        self.plan.clear();
        Ok(())
    }
}

/// Public image mutations are tenant scoped; deletion operations additionally require an administrator.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "operation", rename_all = "snake_case", deny_unknown_fields)]
pub enum ImageMutation {
    Begin {
        request_id: String,
        retention: ImageRetention,
    },
    Plan {
        outputs: BTreeMap<String, String>,
        execution_id: String,
    },
    Publish {
        digests: BTreeMap<String, String>,
    },
    Retain {
        lease_id: String,
    },
    Release {
        lease_id: String,
    },
    Extend {
        retain_until: u64,
    },
    Pin,
    SetRetention {
        retention: ImageRetention,
        expected_revision: u64,
    },
    Observe,
    ClaimDeletion {
        manifests: Vec<String>,
    },
    FinishDeletion,
}

/// Extract reserved managed publication identities without scanning the image catalog.
pub fn reference_id(reference: &str) -> Option<&str> {
    let (_, rest) = reference.split_once("/publications/")?;
    let (id, _) = rest.split_once('/')?;
    valid_id(id).then_some(id)
}

/// Rootfs, image mounts and managed Compose publications share one dependency list.
pub fn dependencies(spec: &crate::EnvironmentSpec) -> Result<BTreeSet<String>> {
    use crate::execution::{MountSource, StorageSource};
    let mut references = vec![spec.image.as_str()];
    if let Some(root) = &spec.execution.rootfs {
        if let StorageSource::Image(image) = &root.source {
            references.push(image);
        }
    }
    for mount in &spec.execution.mounts {
        if let MountSource::Inline(StorageSource::Image(image)) = &mount.source {
            references.push(image);
        }
    }
    let mut ids: BTreeSet<_> = references
        .into_iter()
        .filter_map(reference_id)
        .map(str::to_string)
        .collect();
    ids.extend(explicit_dependencies(spec)?);
    Ok(ids)
}

pub fn explicit_dependencies(spec: &crate::EnvironmentSpec) -> Result<BTreeSet<String>> {
    let mut ids = BTreeSet::new();
    if spec
        .execution
        .extra_config
        .contains("managedImagePublications")
    {
        let value: serde_json::Value = serde_json::from_str(&spec.execution.extra_config)
            .map_err(|_| Error::Invalid("invalid runtime extra_config".into()))?;
        if let Some(images) = value.get("managedImagePublications") {
            let images: Vec<String> = serde_json::from_value(images.clone())
                .map_err(|_| Error::Invalid("invalid managed image dependencies".into()))?;
            if images.len() > 128 || images.iter().any(|id| !valid_id(id)) {
                return Err(Error::Invalid("invalid managed image dependencies".into()));
            }
            ids.extend(images);
        }
    }
    Ok(ids)
}
