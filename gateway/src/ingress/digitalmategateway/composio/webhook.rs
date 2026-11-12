//! Composio V3 raw-byte authentication. Signing secrets stay with this adapter.
//! Verification is not durable acceptance or authorization to a workspace.
use base64::{engine::general_purpose::STANDARD, Engine};
use http::HeaderMap;
use ring::hmac;
use serde::Deserialize;
use sha2::{Digest, Sha256};

#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum WebhookError {
    #[error("webhook_not_configured")]
    NotConfigured,
    #[error("webhook_invalid_headers")]
    InvalidHeaders,
    #[error("webhook_timestamp_outside_window")]
    Timestamp,
    #[error("webhook_signature_invalid")]
    Signature,
    #[error("webhook_response_too_large")]
    TooLarge,
    #[error("webhook_unsupported_envelope")]
    Envelope,
}

pub struct WebhookVerifier {
    keys: Vec<hmac::Key>,
}

#[derive(Deserialize)]
struct Envelope {
    id: String,
    #[serde(rename = "type")]
    kind: String,
    metadata: Metadata,
    data: serde_json::Value,
}

#[derive(Deserialize)]
struct Metadata {
    trigger_id: String,
    trigger_slug: String,
    connected_account_id: String,
    auth_config_id: String,
    user_id: String,
}

/// Only this verifier constructs a proof. Metadata remains provider assertions;
/// ingestion must resolve its own current subscription before routing.
pub struct VerifiedWebhook {
    envelope: Envelope,
    raw: Vec<u8>,
    digest: String,
}

impl VerifiedWebhook {
    pub fn source_event_id(&self) -> &str {
        &self.envelope.id
    }
    pub fn trigger_id(&self) -> &str {
        &self.envelope.metadata.trigger_id
    }
    pub fn trigger_slug(&self) -> &str {
        &self.envelope.metadata.trigger_slug
    }
    pub fn account_id(&self) -> &str {
        &self.envelope.metadata.connected_account_id
    }
    pub fn auth_config_id(&self) -> &str {
        &self.envelope.metadata.auth_config_id
    }
    pub fn user_id(&self) -> &str {
        &self.envelope.metadata.user_id
    }
    pub fn raw_body(&self) -> &[u8] {
        &self.raw
    }
    pub fn digest(&self) -> &str {
        &self.digest
    }
}

impl WebhookVerifier {
    /// Raw UTF-8 secret bytes, as documented by Composio; not Svix whsec decoding.
    /// Up to two trusted secrets support a bounded rotation overlap.
    pub fn new(secrets: &[&[u8]]) -> Result<Self, WebhookError> {
        if secrets.is_empty()
            || secrets.len() > 2
            || secrets.iter().any(|s| s.is_empty() || s.len() > 4096)
        {
            return Err(WebhookError::NotConfigured);
        }
        Ok(Self {
            keys: secrets
                .iter()
                .map(|s| hmac::Key::new(hmac::HMAC_SHA256, s))
                .collect(),
        })
    }

    /// Call on unmodified request bytes, before parsing/re-serializing JSON.
    /// Fixed 300-second past/future tolerance; no option to disable replay checks.
    /// Duplicate delivery suppression additionally requires the durable inbox.
    pub fn verify(
        &self,
        headers: &HeaderMap,
        raw: &[u8],
        now_epoch_seconds: u64,
    ) -> Result<VerifiedWebhook, WebhookError> {
        if raw.is_empty() || raw.len() > 1_048_576 {
            return Err(WebhookError::TooLarge);
        }
        let id = one_header(headers, "webhook-id", 256)?;
        let timestamp = one_header(headers, "webhook-timestamp", 20)?;
        let signatures = one_header(headers, "webhook-signature", 2048)?;
        if !identifier(id, 256) || !timestamp.bytes().all(|b| b.is_ascii_digit()) {
            return Err(WebhookError::InvalidHeaders);
        }
        let seconds: u64 = timestamp
            .parse()
            .map_err(|_| WebhookError::InvalidHeaders)?;
        if seconds.abs_diff(now_epoch_seconds) > 300 {
            return Err(WebhookError::Timestamp);
        }
        let mut signed = Vec::with_capacity(raw.len() + 280);
        signed.extend_from_slice(id.as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(timestamp.as_bytes());
        signed.push(b'.');
        signed.extend_from_slice(raw);
        let parts: Vec<_> = signatures.split_ascii_whitespace().collect();
        if parts.is_empty() || parts.len() > 8 {
            return Err(WebhookError::InvalidHeaders);
        }
        let valid = parts
            .iter()
            .filter_map(|value| value.strip_prefix("v1,"))
            .filter_map(|value| STANDARD.decode(value).ok())
            .any(|signature| {
                self.keys
                    .iter()
                    .any(|key| hmac::verify(key, &signed, &signature).is_ok())
            });
        if !valid {
            return Err(WebhookError::Signature);
        }
        let envelope: Envelope = serde_json::from_slice(raw).map_err(|_| WebhookError::Envelope)?;
        if envelope.kind != "composio.trigger.message"
            || !envelope.data.is_object()
            || !identifier(&envelope.id, 256)
            || !identifier(&envelope.metadata.trigger_id, 256)
            || !identifier(&envelope.metadata.trigger_slug, 128)
            || !identifier(&envelope.metadata.connected_account_id, 256)
            || !identifier(&envelope.metadata.auth_config_id, 256)
            || !identifier(&envelope.metadata.user_id, 256)
        {
            return Err(WebhookError::Envelope);
        }
        Ok(VerifiedWebhook {
            envelope,
            raw: raw.to_vec(),
            digest: hex::encode(Sha256::digest(raw)),
        })
    }
}

fn one_header<'a>(headers: &'a HeaderMap, name: &str, max: usize) -> Result<&'a str, WebhookError> {
    if headers.get_all(name).iter().count() != 1 {
        return Err(WebhookError::InvalidHeaders);
    }
    let value = headers
        .get(name)
        .and_then(|v| v.to_str().ok())
        .ok_or(WebhookError::InvalidHeaders)?;
    if value.is_empty() || value.len() > max {
        return Err(WebhookError::InvalidHeaders);
    }
    Ok(value)
}

fn identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-:".contains(&b))
}
