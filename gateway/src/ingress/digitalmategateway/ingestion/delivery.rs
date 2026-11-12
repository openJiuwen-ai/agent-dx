use super::{prepare_batch, Error, PendingInput, SubscriptionBinding};
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

/// Values carried over authenticated Gateway->sandbox transport. These fields
/// confer no identity or tool authority: the receiver compares trusted context
/// and queries the real connection owner before durable acceptance.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryBatch {
    pub version: u8,
    pub batch_id: String,
    pub digest: String,
    pub binding: SubscriptionBinding,
    pub events: Vec<DeliveryEvent>,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DeliveryEvent {
    pub source_event_id: String,
    pub digest: String,
    pub raw_base64: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DurableAck {
    pub version: u8,
    pub batch_id: String,
    pub digest: String,
    pub state: String,
}

impl DeliveryBatch {
    pub(super) fn from_inputs(
        inputs: &[PendingInput],
        binding: &SubscriptionBinding,
    ) -> Result<Self, Error> {
        let plan = prepare_batch(inputs, binding, 50)?;
        Ok(Self {
            version: 1,
            batch_id: plan.batch_id,
            digest: plan.digest,
            binding: binding.clone(),
            events: plan
                .members
                .iter()
                .map(|input| DeliveryEvent {
                    source_event_id: input.source_event_id().into(),
                    digest: input.digest().into(),
                    raw_base64: STANDARD.encode(input.raw_body()),
                })
                .collect(),
        })
    }

    pub fn validate(&self) -> Result<(), Error> {
        if self.version != 1 || self.events.is_empty() || self.events.len() > 50 {
            return Err(Error::InvalidManifest);
        }
        let mut inputs = Vec::new();
        for event in &self.events {
            if event.raw_base64.len() > 1_398_104 {
                return Err(Error::InvalidManifest);
            }
            let raw = STANDARD
                .decode(&event.raw_base64)
                .map_err(|_| Error::InvalidManifest)?;
            if hex::encode(Sha256::digest(&raw)) != event.digest {
                return Err(Error::InvalidManifest);
            }
            inputs.push(PendingInput {
                binding: self.binding.clone(),
                source_id: event.source_event_id.clone(),
                digest: event.digest.clone(),
                raw,
            });
        }
        let plan = prepare_batch(&inputs, &self.binding, 50)?;
        if plan.batch_id != self.batch_id
            || plan.digest != self.digest
            || plan.remaining != 0
            || plan.members.len() != self.events.len()
        {
            return Err(Error::InvalidManifest);
        }
        Ok(())
    }
}
