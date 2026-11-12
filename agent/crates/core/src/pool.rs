//! Prewarmed DigitalMate pool contracts.
//! Pool slots own an unclaimed key; claims are the only path that transfers it to a user.
use crate::{mounts::Parameters, AgentBinding, Scope, ValidationResult};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;

pub const DEFAULT_POOL_PAGE: usize = 20;

fn enabled() -> bool {
    true
}
fn page_limit() -> usize {
    DEFAULT_POOL_PAGE
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolConfig {
    pub pool_id: String,
    pub tenant: String,
    pub template: String,
    pub version: String,
    #[serde(default = "enabled")]
    pub enabled: bool,
    pub min_running: u64,
    pub min_ready: u64,
    pub max_slots: u64,
    pub max_provisioning: u64,
    pub key_confirm_timeout_seconds: u64,
    pub provision_timeout_seconds: u64,
    pub claim_timeout_seconds: u64,
    pub max_ready_age_seconds: u64,
}
impl PoolConfig {
    pub fn validate(&self) -> ValidationResult {
        for (label, value) in [
            ("pool_id", self.pool_id.as_str()),
            ("tenant", self.tenant.as_str()),
            ("template", self.template.as_str()),
            ("version", self.version.as_str()),
        ] {
            crate::identifier(value, label)?;
        }
        crate::version_number(&self.version)?;
        if self.min_running > self.min_ready
            || self.min_ready > self.max_slots
            || self.max_slots == 0
            || self.max_provisioning == 0
            || self.max_provisioning > self.max_slots
        {
            return Err(
                "pool requires min_running <= min_ready <= max_slots and 0 < max_provisioning <= max_slots"
                    .into(),
            );
        }
        for (label, value) in [
            (
                "key_confirm_timeout_seconds",
                self.key_confirm_timeout_seconds,
            ),
            ("provision_timeout_seconds", self.provision_timeout_seconds),
            ("claim_timeout_seconds", self.claim_timeout_seconds),
            ("max_ready_age_seconds", self.max_ready_age_seconds),
        ] {
            if value == 0 || value > 2_592_000 {
                return Err(format!("{label} must be between 1 and 2592000"));
            }
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotState {
    KeyPending,
    Provisioning,
    Ready,
    Claiming,
    Quarantined,
    Retiring,
}
impl SlotState {
    pub fn active(self) -> bool {
        matches!(
            self,
            Self::KeyPending | Self::Provisioning | Self::Ready | Self::Claiming
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SlotPhase {
    Running,
    Paused,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSlot {
    pub creation_attempted: bool,
    pub slot_id: String,
    pub state: SlotState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<SlotPhase>,
    pub version: String,
    pub environment_id: String,
    pub workspace: String,
    pub model_owner: String,
    pub key_digest: String,
    pub parameters: Parameters,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub volumes: BTreeMap<String, String>,
    pub created_at_unix_ms: u64,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub deadline_unix_ms: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub claim_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub quarantine_reason: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ClaimState {
    Granted,
    Committed,
    Aborted,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolClaim {
    pub claim_id: String,
    pub pool_id: String,
    pub state: ClaimState,
    pub slot_id: String,
    pub environment_id: String,
    pub scope: Scope,
    pub deadline_unix_ms: u64,
    pub released: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolRetired {
    pub preserve_key: bool,
    pub slot_id: String,
    pub model_owner: String,
    pub key_digest: String,
    pub model_api_key: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolRequest {
    pub tenant: String,
    pub pool_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolPutRequest {
    pub config: PoolConfig,
    #[serde(default)]
    pub expected_revision: Option<String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolSnapshot {
    pub config: PoolConfig,
    pub revision: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolStatusCounts {
    pub key_pending: u64,
    pub provisioning: u64,
    pub ready_running: u64,
    pub ready_paused: u64,
    pub claiming: u64,
    pub quarantined: u64,
    pub retiring: u64,
    pub claims_granted: u64,
    pub claims_committed: u64,
    pub claims_aborted: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PoolStatus {
    pub pool_id: String,
    pub version: String,
    pub enabled: bool,
    pub counts: PoolStatusCounts,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReserveRequest {
    pub tenant: String,
    pub pool_id: String,
    pub model_api_base: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ReservedSlot {
    pub slot_id: String,
    pub state: SlotState,
    pub environment_id: String,
    pub version: String,
    pub workspace: String,
    pub model_owner: String,
    pub key_digest: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingKeysRequest {
    pub tenant: String,
    pub pool_id: String,
    #[serde(default = "page_limit")]
    pub limit: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingKey {
    pub slot_id: String,
    pub model_owner: String,
    pub model_api_key: String,
    pub key_digest: String,
    pub deadline_unix_ms: u64,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PendingKeyPage {
    pub slots: Vec<PendingKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct KeyConfirmedRequest {
    pub tenant: String,
    pub pool_id: String,
    pub slot_id: String,
    pub key_digest: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SlotView {
    pub slot_id: String,
    pub state: SlotState,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub phase: Option<SlotPhase>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimRequest {
    pub scope: Scope,
    pub pool_id: String,
    pub claim_id: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ClaimLookupRequest {
    pub tenant: String,
    pub claim_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "state", rename_all = "snake_case")]
pub enum ClaimOutcome {
    Granted {
        claim_id: String,
        slot_id: String,
        environment_id: String,
        version: String,
        phase: SlotPhase,
        parameters: Parameters,
        model_owner: String,
        claim_deadline_unix_ms: u64,
    },
    Committed {
        claim_id: String,
        binding: AgentBinding,
    },
    Aborted {
        claim_id: String,
        released: bool,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CommitRequest {
    pub tenant: String,
    pub pool_id: String,
    pub claim_id: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AbortRequest {
    pub tenant: String,
    pub pool_id: String,
    pub claim_id: String,
    #[serde(default)]
    pub reason: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredRequest {
    pub tenant: String,
    pub pool_id: String,
    #[serde(default = "page_limit")]
    pub limit: usize,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredSlot {
    pub preserve_key: bool,
    pub slot_id: String,
    pub model_owner: String,
    pub key_digest: String,
    pub model_api_key: String,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RetiredPage {
    pub slots: Vec<RetiredSlot>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct AckRetiredRequest {
    pub tenant: String,
    pub pool_id: String,
    pub slot_id: String,
}
