//! Durable provider receipt, fixed-member scheduling and fixed-generation
//! delivery in the existing Gateway process. Identity and connection authority
//! remain with their published owners; JSON projections alone grant no access.
use super::composio::VerifiedWebhook;
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;

mod delivery;
mod relay_delivery;
mod runtime;
mod startup;
mod store;
mod webhook_http;
mod worker;
pub use delivery::{DeliveryBatch, DeliveryEvent, DurableAck};
pub use relay_delivery::{DeliveryAuthorization, RelayDelivery};
pub use runtime::IngestionRuntime;
pub(crate) use startup::IngestionConfig;
pub use store::{Inbox, InboxLimits, Lease, ReceiveReceipt};
pub use webhook_http::{WebhookHttp, WEBHOOK_PATH};
pub use worker::{DeliveryFailure, DeliveryPort, Worker, WorkerOutcome};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("ingestion_scope_or_generation_refused")]
    Refused,
    #[error("ingestion_invalid_limit")]
    InvalidLimit,
    #[error("ingestion_conflicting_input")]
    ConflictingInput,
    #[error("ingestion_invalid_manifest")]
    InvalidManifest,
    #[error("ingestion_store_unavailable")]
    Unavailable,
    #[error("ingestion_capacity_exhausted")]
    Capacity,
    #[error("ingestion_lease_lost")]
    LeaseLost,
    #[error("ingestion_delivery_unknown")]
    DeliveryUnknown,
}

/// Current owner-issued projection, not request JSON and not an authentication
/// credential. Real owner reads are required at receive/claim/execute/ACK.
/// A revoked/replaced projection must never authorize a stale queued input.
#[derive(Clone, Eq, PartialEq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SubscriptionBinding {
    pub tenant: String,
    pub user_id: String,
    pub connection_id: String,
    pub template: String,
    pub template_ref: String,
    pub subscription_id: String,
    pub subscription_generation: u64,
    pub connection_generation: u64,
    pub workspace: String,
    pub binding_id: String,
    pub binding_generation: String,
    pub service_port: u16,
    pub trigger_id: String,
    pub trigger_slug: String,
    pub account_id: String,
    pub auth_config_id: String,
    pub composio_user_id: String,
    pub enabled: bool,
}

impl SubscriptionBinding {
    /// Wire validation only; this does not authorize subscription registration.
    pub fn validate(&self) -> Result<(), Error> {
        if self.subscription_generation == 0
            || self.connection_generation == 0
            || self.subscription_generation > i64::MAX as u64
            || self.connection_generation > i64::MAX as u64
            || self.service_port == 0
            || [
                &self.subscription_id,
                &self.tenant,
                &self.user_id,
                &self.connection_id,
                &self.template,
                &self.template_ref,
                &self.workspace,
                &self.binding_id,
                &self.binding_generation,
            ]
            .iter()
            .any(|value| {
                value.trim().is_empty() || value.len() > 256 || value.chars().any(char::is_control)
            })
            || [
                &self.trigger_id,
                &self.trigger_slug,
                &self.account_id,
                &self.auth_config_id,
                &self.composio_user_id,
            ]
            .iter()
            .any(|value| {
                value.is_empty()
                    || value.len() > 256
                    || !value
                        .bytes()
                        .all(|byte| byte.is_ascii_alphanumeric() || b"_-:".contains(&byte))
            })
            || self
                .template_ref
                .parse::<adx_agent_core::TemplateRef>()
                .is_err()
        {
            return Err(Error::Refused);
        }
        Ok(())
    }
}

/// No Debug/Serialize: the body may contain private mail. Persist through the
/// ingestion store owner before any successful webhook acknowledgement.
pub struct PendingInput {
    binding: SubscriptionBinding,
    source_id: String,
    digest: String,
    raw: Vec<u8>,
}

impl PendingInput {
    /// Caller resolves the subscription through its real owner. Signed user_id
    /// by itself does not select a workspace, binding or account permission.
    pub fn bind(event: VerifiedWebhook, current: &SubscriptionBinding) -> Result<Self, Error> {
        current.validate()?;
        if !current.enabled
            || event.trigger_id() != current.trigger_id
            || event.trigger_slug() != current.trigger_slug
            || event.account_id() != current.account_id
            || event.auth_config_id() != current.auth_config_id
            || event.user_id() != current.composio_user_id
        {
            return Err(Error::Refused);
        }
        Ok(Self {
            binding: current.clone(),
            source_id: event.source_event_id().into(),
            digest: event.digest().into(),
            raw: event.raw_body().to_vec(),
        })
    }

    pub fn source_event_id(&self) -> &str {
        &self.source_id
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
    pub fn raw_body(&self) -> &[u8] {
        &self.raw
    }
    pub fn assert_current(&self, current: &SubscriptionBinding) -> Result<(), Error> {
        if current.enabled && current == &self.binding {
            Ok(())
        } else {
            Err(Error::Refused)
        }
    }
}

/// Preparation only. The store must atomically claim/persist exact members and
/// this digest before dispatch; retries reuse that claim, never build a new one.
pub struct BatchPlan<'a> {
    pub batch_id: String,
    pub digest: String,
    pub members: Vec<&'a PendingInput>,
    pub remaining: usize,
}

pub fn prepare_batch<'a>(
    inputs: &'a [PendingInput],
    current: &SubscriptionBinding,
    limit: usize,
) -> Result<BatchPlan<'a>, Error> {
    current.validate()?;
    if !(1..=50).contains(&limit) || inputs.is_empty() || inputs.len() > 1000 {
        return Err(Error::InvalidLimit);
    }
    let mut distinct: BTreeMap<&str, &PendingInput> = BTreeMap::new();
    for input in inputs {
        input.assert_current(current)?;
        if let Some(previous) = distinct.insert(input.source_event_id(), input) {
            if previous.digest() != input.digest() {
                return Err(Error::ConflictingInput);
            }
        }
    }
    let total = distinct.len();
    let mut bytes: usize = 0;
    let members: Vec<_> = distinct
        .values()
        .copied()
        .take(limit)
        .take_while(|input| {
            let next = bytes.saturating_add(input.raw_body().len());
            if next > 1_048_576 {
                false
            } else {
                bytes = next;
                true
            }
        })
        .collect();
    if members.is_empty() {
        return Err(Error::InvalidLimit);
    }
    // Full pending set is considered; no sequence high-water mark can lose a
    // late-committing lower-sequence row. Persistent scheduling remains pending.
    let member_ids: Vec<_> = members
        .iter()
        .map(|input| (input.source_event_id(), input.digest()))
        .collect();
    let manifest = serde_json::to_vec(&(
        "digitalmate.ingestion.batch.v1",
        (
            &current.tenant,
            &current.user_id,
            &current.connection_id,
            &current.template,
            &current.template_ref,
        ),
        &current.subscription_id,
        current.subscription_generation,
        current.connection_generation,
        &current.workspace,
        &current.binding_id,
        &current.binding_generation,
        current.service_port,
        &current.account_id,
        &current.auth_config_id,
        &current.composio_user_id,
        &current.trigger_id,
        &current.trigger_slug,
        member_ids,
    ))
    .map_err(|_| Error::InvalidManifest)?;
    let digest = hex::encode(Sha256::digest(manifest));
    Ok(BatchPlan {
        batch_id: format!("batch_{digest}"),
        digest,
        remaining: total - members.len(),
        members,
    })
}

#[cfg(test)]
mod persistence_tests;
#[cfg(all(test, feature = "mock-e2e"))]
mod relay_tests;
#[cfg(test)]
mod tests;
