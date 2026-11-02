//! Workspace-scoped envelope encryption. Provider credentials are never persisted here.
pub mod access;
pub mod service;
pub mod storage;

use base64::{engine::general_purpose::STANDARD, Engine};
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use zeroize::Zeroizing;

pub const MAX_PLAINTEXT: usize = 131_072;

#[derive(Debug, thiserror::Error)]
pub enum CryptoError {
    #[error("invalid credential context or payload")]
    InvalidInput,
    #[error("invalid or unauthenticated credential envelope")]
    InvalidEnvelope,
    #[error("credential encryption unavailable")]
    Unavailable,
}

pub use adx_agent_core::service_grants::{
    valid_service_identifier as valid_identifier, ServiceAccess as Context,
};

fn aad(context: &Context, layer: &str, key_id: Option<&str>) -> Result<Vec<u8>, CryptoError> {
    let mut values = vec![
        layer,
        "1",
        &context.tenant,
        &context.workspace,
        &context.purpose,
    ];
    if let Some(key) = key_id {
        values.push(key);
    }
    serde_json::to_vec(&values).map_err(|_| CryptoError::Unavailable)
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Envelope {
    pub version: u8,
    pub algorithm: String,
    pub key_backend: String,
    pub key_id: String,
    pub context: Context,
    pub wrapped_key: String,
    pub ciphertext: String,
}

/// The dedicated KEK keyring stays in the service, never in a user image.
pub struct Keyring {
    active: String,
    keys: BTreeMap<String, Zeroizing<[u8; 32]>>,
}

impl Keyring {
    /// Rejects an absent active key and invalid key identifiers.
    pub fn new(active: String, keys: BTreeMap<String, [u8; 32]>) -> Result<Self, CryptoError> {
        if !keys.contains_key(&active) || !keys.keys().all(|s| valid_identifier(s)) {
            return Err(CryptoError::InvalidInput);
        }
        Ok(Self {
            active,
            keys: keys
                .into_iter()
                .map(|(k, v)| (k, Zeroizing::new(v)))
                .collect(),
        })
    }

    pub fn seal(&self, context: &Context, plaintext: &[u8]) -> Result<Envelope, CryptoError> {
        context.validate().map_err(|_| CryptoError::InvalidInput)?;
        if plaintext.len() > MAX_PLAINTEXT {
            return Err(CryptoError::InvalidInput);
        }
        let mut dek = Zeroizing::new([0u8; 32]);
        SystemRandom::new()
            .fill(dek.as_mut())
            .map_err(|_| CryptoError::Unavailable)?;
        let kek = self
            .keys
            .get(&self.active)
            .ok_or(CryptoError::Unavailable)?;
        Ok(Envelope {
            version: 1,
            algorithm: "AES-256-GCM".into(),
            key_backend: "local".into(),
            key_id: self.active.clone(),
            context: context.clone(),
            wrapped_key: encrypt(
                kek.as_ref(),
                &aad(context, "adx.credentials.dek", Some(&self.active))?,
                dek.as_ref(),
            )?,
            ciphertext: encrypt(
                dek.as_ref(),
                &aad(context, "adx.credentials.data", None)?,
                plaintext,
            )?,
        })
    }

    fn unwrap_key(
        &self,
        context: &Context,
        envelope: &Envelope,
    ) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        context.validate().map_err(|_| CryptoError::InvalidInput)?;
        if envelope.version != 1
            || envelope.algorithm != "AES-256-GCM"
            || envelope.key_backend != "local"
            || &envelope.context != context
        {
            return Err(CryptoError::InvalidEnvelope);
        }
        let kek = self
            .keys
            .get(&envelope.key_id)
            .ok_or(CryptoError::InvalidEnvelope)?;
        let dek = decrypt(
            kek.as_ref(),
            &aad(context, "adx.credentials.dek", Some(&envelope.key_id))?,
            &envelope.wrapped_key,
            32,
        )?;
        if dek.len() != 32 {
            return Err(CryptoError::InvalidEnvelope);
        }
        Ok(dek)
    }

    pub fn open(
        &self,
        context: &Context,
        envelope: &Envelope,
    ) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
        let dek = self.unwrap_key(context, envelope)?;
        decrypt(
            &dek,
            &aad(context, "adx.credentials.data", None)?,
            &envelope.ciphertext,
            MAX_PLAINTEXT,
        )
    }

    /// Administrative operation: rewraps the DEK without returning it or changing data ciphertext.
    pub fn rewrap(&self, context: &Context, envelope: &Envelope) -> Result<Envelope, CryptoError> {
        let dek = self.unwrap_key(context, envelope)?;
        self.open(context, envelope)?;
        let kek = self
            .keys
            .get(&self.active)
            .ok_or(CryptoError::Unavailable)?;
        let mut updated = envelope.clone();
        updated.key_id = self.active.clone();
        updated.wrapped_key = encrypt(
            kek.as_ref(),
            &aad(context, "adx.credentials.dek", Some(&self.active))?,
            &dek,
        )?;
        Ok(updated)
    }
}

fn encrypt(key: &[u8], aad: &[u8], plaintext: &[u8]) -> Result<String, CryptoError> {
    let key = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, key).map_err(|_| CryptoError::Unavailable)?,
    );
    let mut nonce = [0u8; 12];
    SystemRandom::new()
        .fill(&mut nonce)
        .map_err(|_| CryptoError::Unavailable)?;
    let mut buffer = Zeroizing::new(plaintext.to_vec());
    key.seal_in_place_append_tag(
        aead::Nonce::assume_unique_for_key(nonce),
        aead::Aad::from(aad),
        &mut *buffer,
    )
    .map_err(|_| CryptoError::Unavailable)?;
    let mut packed = nonce.to_vec();
    packed.extend_from_slice(&buffer);
    Ok(STANDARD.encode(packed))
}

fn decrypt(
    key: &[u8],
    aad: &[u8],
    encoded: &str,
    maximum: usize,
) -> Result<Zeroizing<Vec<u8>>, CryptoError> {
    if encoded.len() > (maximum + 28).div_ceil(3) * 4 {
        return Err(CryptoError::InvalidEnvelope);
    }
    let mut packed = Zeroizing::new(
        STANDARD
            .decode(encoded)
            .map_err(|_| CryptoError::InvalidEnvelope)?,
    );
    if packed.len() < 28 || packed.len() > maximum + 28 {
        return Err(CryptoError::InvalidEnvelope);
    }
    let nonce: [u8; 12] = packed[..12]
        .try_into()
        .map_err(|_| CryptoError::InvalidEnvelope)?;
    let key = aead::LessSafeKey::new(
        aead::UnboundKey::new(&aead::AES_256_GCM, key).map_err(|_| CryptoError::Unavailable)?,
    );
    let plaintext = key
        .open_in_place(
            aead::Nonce::assume_unique_for_key(nonce),
            aead::Aad::from(aad),
            &mut packed[12..],
        )
        .map_err(|_| CryptoError::InvalidEnvelope)?;
    Ok(Zeroizing::new(plaintext.to_vec()))
}
