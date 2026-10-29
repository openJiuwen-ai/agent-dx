//! Platform-owned preparation contracts. Slots are inert, single-consumer resources;
//! they are never anonymous Environments or reusable user executions.
pub mod management;

use crate::{Error, Resources, Result};
use serde::{Deserialize, Serialize};

pub fn fingerprint(value: &impl Serialize) -> Result<String> {
    use sha2::{Digest, Sha256};
    let bytes = serde_json::to_vec(value)
        .map_err(|_| Error::Invalid("preparation serialization failed".into()))?;
    Ok(format!("sha256:{:x}", Sha256::digest(bytes)))
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationArtifact {
    pub id: String,
    pub node_id: String,
    pub node_session: String,
    pub profile_digest: String,
    pub state: SlotState,
    pub size_bytes: u64,
    pub created_at: u64,
    pub checked_at: u64,
    pub expires_at: u64,
    pub retry_at: u64,
    pub failures: u32,
}
impl PreparationArtifact {
    pub fn claimable(&self, now: u64, policy: &PoolPolicy) -> bool {
        self.state == SlotState::Ready
            && now < self.expires_at
            && now >= self.checked_at
            && now - self.checked_at <= policy.health.max_staleness_seconds
    }
    pub fn counts_as_live(&self) -> bool {
        self.state != SlotState::Deleted
    }
}

/// Node side effects are only callable by the Coordinator. Prepare never accepts
/// Environment/user configuration. Pin happens separately after real assignment.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "action", rename_all = "snake_case", deny_unknown_fields)]
pub enum ArtifactCommand {
    Qualify {
        profile: Box<PreparationProfile>,
    },
    Prepare {
        id: String,
        profile: Box<PreparationProfile>,
        max_bytes: u64,
        expected_recipe_digest: String,
    },
    Observe {
        id: String,
    },
    Destroy {
        id: String,
    },
}
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ArtifactObservation {
    pub recipe_digest: String,
    pub size_bytes: u64,
    pub ready: bool,
    pub in_use: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SourceRef {
    pub tenant: String,
    pub template: String,
    pub version: String,
    pub template_digest: String,
}

pub fn valid_digest(value: &str) -> bool {
    value.strip_prefix("sha256:").is_some_and(|hex| {
        hex.len() == 64
            && hex
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    })
}

pub fn validate_identifier(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 512
        || matches!(value, "." | "..")
        || value
            .chars()
            .any(|c| c.is_control() || matches!(c, '/' | '\\'))
    {
        return Err(Error::Invalid("invalid preparation identifier".into()));
    }
    Ok(())
}

impl SourceRef {
    pub fn validate(&self, tenant: &str) -> Result<()> {
        for value in [&self.tenant, &self.template, &self.version] {
            validate_identifier(value)?;
        }
        if self.tenant != tenant || !valid_digest(&self.template_digest) {
            return Err(Error::Invalid(
                "preparation source tenant or digest mismatch".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PreparationPreference {
    Prefer,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationContext {
    pub source_ref: SourceRef,
    pub preference: PreparationPreference,
    pub contract_version: u32,
}
impl PreparationContext {
    pub fn validate(&self, tenant: &str) -> Result<()> {
        self.source_ref.validate(tenant)?;
        if self.contract_version != 1 {
            return Err(Error::Invalid(
                "unsupported preparation contract version".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum PoolMode {
    ImageRootfs,
    RuntimeSlot,
}
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CapacityUnit {
    Slots,
    ArtifactNodeReplicas,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolCapacity {
    pub unit: CapacityUnit,
    pub min_ready: u32,
    pub target_ready: u32,
    pub max_live: u32,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RefillPolicy {
    pub prepare_concurrency: u32,
    pub initial_backoff_seconds: u64,
    pub max_backoff_seconds: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct HealthPolicy {
    pub interval_seconds: u64,
    pub max_staleness_seconds: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct IdleBudget {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu_millis: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u64>,
    pub disk_bytes: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolPolicy {
    pub mode: PoolMode,
    pub enabled: bool,
    pub capacity: PoolCapacity,
    pub refill: RefillPolicy,
    pub health: HealthPolicy,
    pub ready_ttl_seconds: u64,
    #[serde(default)]
    pub acquire_wait_ms: u32,
    pub idle_budget: IdleBudget,
}

impl PoolPolicy {
    /// Check units, bounded work and worst-case idle reservations, including
    /// quarantined and retiring resources. Only immediate acquisition is supported.
    pub fn validate(&self, resources: Resources) -> Result<()> {
        let c = &self.capacity;
        let r = &self.refill;
        if c.max_live == 0
            || c.max_live > 10_000
            || c.min_ready > c.target_ready
            || c.target_ready > c.max_live
            || r.prepare_concurrency == 0
            || r.prepare_concurrency > 64
            || r.prepare_concurrency > c.max_live
            || r.initial_backoff_seconds == 0
            || r.max_backoff_seconds > 86_400
            || r.initial_backoff_seconds > r.max_backoff_seconds
            || self.health.interval_seconds == 0
            || self.health.max_staleness_seconds < self.health.interval_seconds
            || self.health.max_staleness_seconds > 86_400
            || self.ready_ttl_seconds == 0
            || self.ready_ttl_seconds > 604_800
            || self.acquire_wait_ms != 0
            || self.idle_budget.disk_bytes == 0
        {
            return Err(Error::Invalid(
                "invalid prewarm capacity, timing or concurrency policy".into(),
            ));
        }
        match self.mode {
            PoolMode::ImageRootfs
                if c.unit == CapacityUnit::ArtifactNodeReplicas
                    && self.idle_budget.cpu_millis.is_none()
                    && self.idle_budget.memory_mib.is_none() =>
            {
                Ok(())
            }
            PoolMode::RuntimeSlot if c.unit == CapacityUnit::Slots => {
                resources.validate()?;
                let cpu = resources.cpu_millis.checked_mul(u64::from(c.max_live));
                let memory = resources.memory_bytes.checked_mul(u64::from(c.max_live));
                let disk = resources.disk_bytes.checked_mul(u64::from(c.max_live));
                let budget_memory = self
                    .idle_budget
                    .memory_mib
                    .and_then(|m| m.checked_mul(1_048_576));
                if cpu
                    .zip(self.idle_budget.cpu_millis)
                    .is_none_or(|(used, budget)| used > budget)
                    || memory
                        .zip(budget_memory)
                        .is_none_or(|(used, budget)| used > budget)
                    || disk.is_none_or(|used| used > self.idle_budget.disk_bytes)
                {
                    return Err(Error::Invalid(
                        "idle budget cannot cover max_live or overflows".into(),
                    ));
                }
                Ok(())
            }
            _ => Err(Error::Invalid(
                "prewarm mode and capacity units do not match".into(),
            )),
        }
    }
}

/// Contains only static, non-user material. A digest is not a qualification
/// certificate: the backend must separately accept the exact contract.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparationProfile {
    pub source_ref: SourceRef,
    pub image: String,
    pub runtime_class: String,
    pub resources: Resources,
    pub architecture: String,
    pub runtime_recipe_digest: String,
    pub security_digest: String,
    pub late_binding_contract_digest: String,
    pub contract_version: u32,
}
impl PreparationProfile {
    pub fn validate(&self) -> Result<()> {
        self.source_ref.validate(&self.source_ref.tenant)?;
        let pinned = self
            .image
            .rsplit_once('@')
            .is_some_and(|(repository, digest)| {
                !repository.is_empty()
                    && !repository.contains([' ', '\t', '\n', '\r', '@'])
                    && valid_digest(digest)
            });
        if !pinned
            || self.runtime_class.trim().is_empty()
            || !matches!(self.architecture.as_str(), "amd64" | "arm64")
            || !valid_digest(&self.runtime_recipe_digest)
            || !valid_digest(&self.security_digest)
            || !valid_digest(&self.late_binding_contract_digest)
            || self.contract_version != 1
        {
            return Err(Error::Invalid(
                "preparation requires a pinned image and immutable qualified profile".into(),
            ));
        }
        self.resources.validate()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotState {
    Preparing,
    Ready,
    Reserved,
    Attaching,
    Consumed,
    Quarantined,
    Retiring,
    Deleted,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotOwner {
    pub tenant: String,
    pub environment_id: String,
    pub ownership_generation: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedSlot {
    pub id: String,
    pub pool_id: String,
    pub node_id: String,
    pub node_session: String,
    pub preparation_generation: u64,
    pub state: SlotState,
    pub owner: Option<SlotOwner>,
    pub created_at: u64,
    pub checked_at: Option<u64>,
    pub expires_at: Option<u64>,
    pub diagnostic: Option<String>,
}
impl PreparedSlot {
    pub fn preparing(
        id: String,
        pool_id: String,
        node_id: String,
        node_session: String,
        preparation_generation: u64,
        now: u64,
    ) -> Result<Self> {
        for value in [&id, &pool_id, &node_id, &node_session] {
            validate_identifier(value)?;
        }
        if preparation_generation == 0 {
            return Err(Error::Conflict);
        }
        Ok(Self {
            id,
            pool_id,
            node_id,
            node_session,
            preparation_generation,
            state: SlotState::Preparing,
            owner: None,
            created_at: now,
            checked_at: None,
            expires_at: None,
            diagnostic: None,
        })
    }
    pub fn mark_ready(&mut self, now: u64, ttl: u64) -> Result<()> {
        if self.state != SlotState::Preparing
            || self.owner.is_some()
            || ttl == 0
            || now < self.created_at
        {
            return Err(Error::Conflict);
        }
        let expires_at = now.checked_add(ttl).ok_or(Error::Conflict)?;
        self.checked_at = Some(now);
        self.expires_at = Some(expires_at);
        self.state = SlotState::Ready;
        Ok(())
    }
    pub fn claimable(&self, now: u64, policy: &PoolPolicy) -> bool {
        self.state == SlotState::Ready
            && self.owner.is_none()
            && self.expires_at.is_some_and(|until| now < until)
            && self
                .checked_at
                .is_some_and(|at| now >= at && now - at <= policy.health.max_staleness_seconds)
    }
    pub fn reserve(&mut self, owner: SlotOwner, now: u64, policy: &PoolPolicy) -> Result<()> {
        validate_identifier(&owner.tenant)?;
        validate_identifier(&owner.environment_id)?;
        if !self.claimable(now, policy) || owner.ownership_generation == 0 {
            return Err(Error::Conflict);
        }
        self.owner = Some(owner);
        self.state = SlotState::Reserved;
        Ok(())
    }
    pub fn attach(&mut self, owner: &SlotOwner) -> Result<()> {
        if self.owner.as_ref() != Some(owner)
            || !matches!(
                self.state,
                SlotState::Reserved | SlotState::Quarantined | SlotState::Attaching
            )
        {
            return Err(Error::Conflict);
        }
        self.state = SlotState::Attaching;
        Ok(())
    }
    pub fn consume(&mut self, owner: &SlotOwner) -> Result<()> {
        if self.owner.as_ref() != Some(owner)
            || !matches!(self.state, SlotState::Attaching | SlotState::Consumed)
        {
            return Err(Error::Conflict);
        }
        self.state = SlotState::Consumed;
        Ok(())
    }
    pub fn quarantine(&mut self, diagnostic: String) -> Result<()> {
        if matches!(
            self.state,
            SlotState::Consumed | SlotState::Deleted | SlotState::Retiring
        ) {
            return Err(Error::Conflict);
        }
        self.state = SlotState::Quarantined;
        // Diagnostics are codes/messages supplied by trusted controllers, never user values.
        self.diagnostic = Some(diagnostic.chars().take(256).collect());
        Ok(())
    }
    pub fn retire(&mut self) -> Result<()> {
        if matches!(self.state, SlotState::Consumed | SlotState::Deleted) {
            return Err(Error::Conflict);
        }
        self.state = SlotState::Retiring;
        Ok(())
    }
    /// Call only after physical cleanup (or confirmed transfer to the original
    /// Environment) and its admission-ledger handoff have been acknowledged.
    pub fn confirm_deleted(&mut self) -> Result<()> {
        if self.state != SlotState::Retiring {
            return Err(Error::Conflict);
        }
        self.state = SlotState::Deleted;
        Ok(())
    }
    pub fn counts_as_idle(&self) -> bool {
        !matches!(self.state, SlotState::Consumed | SlotState::Deleted)
    }
}

pub fn unsupported() -> Error {
    Error::BackendFailure {
        code: crate::BackendFailureCode::Unimplemented,
        message: "backend has no qualified artifact/slot preparation contract".into(),
    }
}
