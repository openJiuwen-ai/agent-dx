//! Managed image ownership is independent of an SDK process or Environment.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub const MANAGED_IMAGE_MUTATION_BYTES: usize = 512 * 1024;

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
    Abandoned,
    Deleting,
    Deleted,
}

/// The operator reserves this repository namespace for this installation's
/// managed writers and scopes Registry deletion credentials to it.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RegistryScope {
    pub authority: String,
    pub repository_prefix: String,
}

impl RegistryScope {
    pub fn validate(&self) -> Result<()> {
        if self.authority.is_empty()
            || self.authority.len() > 255
            || !self
                .authority
                .bytes()
                .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"[]:.-".contains(&b))
            || self.repository_prefix.len() > 128
            || !self
                .repository_prefix
                .split('/')
                .all(|part| repository_component(part) && !valid_id(part))
        {
            return Err(Error::Invalid("invalid managed Registry scope".into()));
        }
        Ok(())
    }

    pub fn contains_publication_reference(&self, id: &str, reference: &str) -> bool {
        let Some((authority, path)) = repository(reference).and_then(|repo| repo.split_once('/'))
        else {
            return false;
        };
        authority.eq_ignore_ascii_case(&self.authority)
            && path
                .strip_prefix(&self.repository_prefix)
                .and_then(|suffix| suffix.strip_prefix('/'))
                .is_some_and(|path| {
                    path.split('/').all(repository_component)
                        && path.rsplit_once('/').is_some_and(|(parent, _)| {
                            parent.split('/').any(|component| component == id)
                        })
                })
    }
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
    pub registry: RegistryScope,
    pub retain_until: u64,
    pub registered_outputs: BTreeMap<String, String>,
    pub outputs: BTreeMap<String, String>,
    pub leases: BTreeSet<String>,
    pub released_leases: BTreeSet<String>,
    pub references: BTreeSet<String>,
    pub unreferenced_since: Option<u64>,
    #[serde(default)]
    pub observed_admissions: u64,
    pub deletion_manifests: BTreeSet<String>,
    pub attempt_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub writer_fence: Option<String>,
    pub deleted_manifest_count: usize,
}

/// Bounded inventory entry. Full receipts and runtime reference observations
/// are retrieved separately.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ManagedImageSummary {
    pub id: String,
    pub tenant_id: String,
    pub state: PublicationState,
    pub retention: ImageRetention,
    pub revision: u64,
    pub created_at: u64,
    pub retain_until: u64,
    pub lease_count: usize,
    pub output_count: usize,
}

pub fn valid_id(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}

fn repository_component(value: &str) -> bool {
    value
        .bytes()
        .next()
        .is_some_and(|b| b.is_ascii_lowercase() || b.is_ascii_digit())
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b"._-".contains(&b))
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
    pub fn summary(&self) -> ManagedImageSummary {
        ManagedImageSummary {
            id: self.id.clone(),
            tenant_id: self.tenant_id.clone(),
            state: self.state,
            retention: self.retention.clone(),
            revision: self.revision,
            created_at: self.created_at,
            retain_until: self.retain_until,
            lease_count: self.leases.len(),
            output_count: self.outputs.len(),
        }
    }

    /// Allocate a fresh repository identity and durably index the request separately.
    pub fn new(
        id: String,
        tenant: &str,
        request: &str,
        retention: ImageRetention,
        registry: RegistryScope,
        now: u64,
    ) -> Result<Self> {
        label(tenant)?;
        label(request)?;
        if matches!(retention, ImageRetention::Ephemeral { grace_seconds } if grace_seconds > 31_536_000)
        {
            return Err(Error::Invalid("image grace exceeds one year".into()));
        }
        registry.validate()?;
        if !valid_id(&id) {
            return Err(Error::Invalid("invalid image publication identity".into()));
        }
        Ok(Self {
            id,
            tenant_id: tenant.into(),
            request_id: request.into(),
            revision: 1,
            created_at: now,
            state: PublicationState::Preparing,
            initial_retention: retention.clone(),
            retention,
            registry,
            retain_until: 0,
            registered_outputs: BTreeMap::new(),
            outputs: BTreeMap::new(),
            leases: BTreeSet::from(["publisher".into()]),
            released_leases: BTreeSet::new(),
            references: BTreeSet::new(),
            unreferenced_since: None,
            observed_admissions: 0,
            deletion_manifests: BTreeSet::new(),
            attempt_id: None,
            writer_fence: None,
            deleted_manifest_count: 0,
        })
    }

    fn available(&self) -> Result<()> {
        if !self.admission_open() || self.archived() {
            return Err(Error::Conflict);
        }
        Ok(())
    }

    /// Reserve the exact repositories before the caller writes. The attempt
    /// identity correlates retries within the owning tenant.
    pub fn register_outputs(
        &mut self,
        outputs: BTreeMap<String, String>,
        attempt_id: &str,
    ) -> Result<()> {
        self.available()?;
        label(attempt_id)?;
        if self.state == PublicationState::Abandoned {
            return Err(Error::Conflict);
        }
        if !self.registered_outputs.is_empty() {
            return if self.registered_outputs == outputs
                && self.attempt_id.as_deref() == Some(attempt_id)
            {
                Ok(())
            } else {
                Err(Error::Conflict)
            };
        }
        if self.state != PublicationState::Preparing || outputs.is_empty() || outputs.len() > 128 {
            return Err(Error::Invalid("one to 128 image outputs required".into()));
        }
        let mut unique = BTreeSet::new();
        for (name, tagged) in &outputs {
            label(name)?;
            let (repository, tag) = tagged
                .rsplit_once(':')
                .ok_or_else(|| Error::Invalid("tagged publication output required".into()))?;
            if !self
                .registry
                .contains_publication_reference(&self.id, tagged)
                || publication_ids(repository).count() != 1
                || repository
                    .split_once('/')
                    .is_none_or(|(authority, _)| authority != self.registry.authority)
            {
                return Err(Error::Invalid(
                    "output is outside the reserved Registry namespace".into(),
                ));
            }
            if tagged.contains('@')
                || tagged.len() > 512
                || tag.is_empty()
                || tag.len() > 128
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
        self.registered_outputs = outputs;
        self.attempt_id = Some(attempt_id.to_owned());
        self.state = PublicationState::Publishing;
        Ok(())
    }

    /// The registered writer confirms completion with a digest for every output.
    pub fn complete(&mut self, digests: BTreeMap<String, String>, attempt_id: &str) -> Result<()> {
        self.available()?;
        if self.attempt_id.as_deref() != Some(attempt_id) {
            return Err(Error::Conflict);
        }
        if self.registered_outputs.keys().ne(digests.keys()) {
            return Err(Error::Invalid(
                "complete immutable publication receipt required".into(),
            ));
        }
        let outputs = self.resolve_receipt(digests)?;
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

    fn resolve_receipt(
        &self,
        digests: BTreeMap<String, String>,
    ) -> Result<BTreeMap<String, String>> {
        digests
            .into_iter()
            .map(|(name, hash)| {
                let repository = self
                    .registered_outputs
                    .get(&name)
                    .and_then(|tag| repository(tag))
                    .filter(|_| digest(&hash))
                    .ok_or_else(|| {
                        Error::Invalid("receipt does not match the registered outputs".into())
                    })?;
                Ok((name, format!("{repository}@{hash}")))
            })
            .collect()
    }

    pub fn cancel_preparation(&mut self) -> Result<()> {
        if self.state == PublicationState::Abandoned && self.writer_fence.is_none() {
            return Ok(());
        }
        if self.state != PublicationState::Preparing {
            return Err(Error::Conflict);
        }
        self.abandon();
        Ok(())
    }

    /// The operator has fenced the writer and observed every planned tag.
    /// The exact subset that exists can enter normal grace and reclamation.
    pub fn reconcile_abandoned(
        &mut self,
        digests: BTreeMap<String, String>,
        writer_fence: &str,
        expected_revision: u64,
    ) -> Result<()> {
        label(writer_fence)?;
        let outputs = self.resolve_receipt(digests)?;
        if self.state == PublicationState::Abandoned
            && self.writer_fence.as_deref() == Some(writer_fence)
            && self.outputs == outputs
        {
            return Ok(());
        }
        if self.state != PublicationState::Publishing || self.revision != expected_revision {
            return Err(Error::Conflict);
        }
        self.outputs = outputs;
        self.writer_fence = Some(writer_fence.to_owned());
        self.abandon();
        Ok(())
    }

    fn abandon(&mut self) {
        self.state = PublicationState::Abandoned;
        if self.leases.remove("publisher") {
            self.released_leases.insert("publisher".into());
        }
        self.unreferenced_since = None;
        self.compact_if_archived();
    }

    pub fn publication_pending(&self) -> bool {
        matches!(
            self.state,
            PublicationState::Preparing | PublicationState::Publishing
        )
    }

    /// Archived identities remain queryable and replayable by ID. They no
    /// longer occupy active inventory or accept new caller leases.
    pub fn archived(&self) -> bool {
        self.state == PublicationState::Deleted
            || (self.state == PublicationState::Abandoned
                && self.outputs.is_empty()
                && self.leases.is_empty())
    }

    fn compact_if_archived(&mut self) {
        if self.archived() {
            self.registered_outputs.clear();
            self.outputs.clear();
            self.attempt_id = None;
            self.released_leases.clear();
            self.references.clear();
        }
    }

    fn has_reclaimable_receipt(&self) -> bool {
        matches!(
            self.state,
            PublicationState::Ready | PublicationState::Abandoned
        ) && !self.outputs.is_empty()
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
        if self.archived() {
            return Ok(());
        }
        if !self.leases.contains(lease) && !self.released_leases.contains(lease) {
            return Err(Error::NotFound);
        }
        self.leases.remove(lease);
        self.released_leases.insert(lease.into());
        self.compact_if_archived();
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
        self.observe_after_admission(references, now, 0)
    }

    pub fn observe_after_admission(
        &mut self,
        references: &[String],
        now: u64,
        admissions: u64,
    ) -> Result<()> {
        if !references.is_empty()
            && matches!(
                self.state,
                PublicationState::Deleting | PublicationState::Deleted
            )
        {
            self.available()?;
        }
        if self.observed_admissions != admissions {
            self.unreferenced_since = None;
        }
        self.observed_admissions = admissions;
        self.references = references.iter().cloned().collect();
        if self.has_reclaimable_receipt() && self.leases.is_empty() && references.is_empty() {
            self.unreferenced_since.get_or_insert(now);
        } else {
            self.unreferenced_since = None;
        }
        Ok(())
    }

    pub fn admission_open(&self) -> bool {
        !matches!(
            self.state,
            PublicationState::Deleting | PublicationState::Deleted
        )
    }

    pub fn matches_repository(&self, reference: &str) -> bool {
        repository(reference).is_some_and(|candidate| {
            self.registered_outputs
                .values()
                .filter_map(|output| repository(output))
                .any(
                    |output| match (candidate.split_once('/'), output.split_once('/')) {
                        (Some((host, path)), Some((expected_host, expected_path))) => {
                            host.eq_ignore_ascii_case(expected_host) && path == expected_path
                        }
                        _ => false,
                    },
                )
        })
    }
    pub fn reclaimable(&self, now: u64) -> bool {
        let ImageRetention::Ephemeral { grace_seconds } = self.retention else {
            return false;
        };
        self.has_reclaimable_receipt()
            && self.leases.is_empty()
            && self.references.is_empty()
            && now >= self.retain_until
            && self
                .unreferenced_since
                .and_then(|t| t.checked_add(grace_seconds))
                .is_some_and(|deadline| now >= deadline)
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
            || manifests.iter().map(|value| value.len() + 3).sum::<usize>()
                > MANAGED_IMAGE_MUTATION_BYTES - 1024
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
        if !self.reclaimable(now) {
            return Err(Error::Conflict);
        }
        self.state = PublicationState::Deleting;
        self.deletion_manifests = manifests;
        Ok(())
    }

    pub fn finish_deletion(&mut self) -> Result<()> {
        if self.state == PublicationState::Deleted {
            return Ok(());
        }
        if self.state != PublicationState::Deleting {
            return Err(Error::Conflict);
        }
        self.state = PublicationState::Deleted;
        self.deleted_manifest_count = self.deletion_manifests.len();
        self.deletion_manifests.clear();
        self.compact_if_archived();
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
    RegisterOutputs {
        outputs: BTreeMap<String, String>,
        attempt_id: String,
    },
    Complete {
        digests: BTreeMap<String, String>,
        attempt_id: String,
    },
    CancelPreparation,
    ReconcileAbandoned {
        digests: BTreeMap<String, String>,
        writer_fence: String,
        expected_revision: u64,
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

fn publication_ids(repository: &str) -> impl Iterator<Item = &str> {
    let parent = repository
        .split_once('/')
        .and_then(|(_, path)| path.rsplit_once('/'))
        .map_or("", |(parent, _)| parent);
    parent.split('/').filter(|part| valid_id(part))
}

/// Candidate references are resolved against registered repositories at admission.
pub fn image_references(spec: &crate::EnvironmentSpec) -> Vec<(&str, &str)> {
    use crate::execution::{MountSource, StorageSource};
    let mut references = vec![spec.image.as_str()];
    if let Some(profile) = &spec.runtime_profile {
        references.push(&profile.rootfs.image);
    }
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
    references
        .into_iter()
        .flat_map(|reference| {
            repository(reference)
                .into_iter()
                .flat_map(move |repository| {
                    publication_ids(repository).map(move |id| (id, reference))
                })
        })
        .collect()
}

pub fn validate_dependencies(ids: &BTreeSet<String>) -> Result<()> {
    if ids.len() > 128 || ids.iter().any(|id| !valid_id(id)) {
        return Err(Error::Invalid("invalid managed image dependencies".into()));
    }
    Ok(())
}

fn repository(reference: &str) -> Option<&str> {
    let reference = reference.trim();
    // Image fetchers may accept a scheme on a reference to the same repository.
    let reference = reference
        .strip_prefix("https://")
        .or_else(|| reference.strip_prefix("http://"))
        .unwrap_or(reference);
    let repository_with_tag = reference
        .split_once('@')
        .map_or(reference, |(repo, _)| repo);
    let (path, leaf) = repository_with_tag.rsplit_once('/')?;
    if path.is_empty() || leaf.is_empty() {
        return None;
    }
    Some(
        repository_with_tag
            .rsplit_once(':')
            .filter(|_| leaf.contains(':'))
            .map_or(repository_with_tag, |(repository, _)| repository),
    )
}
