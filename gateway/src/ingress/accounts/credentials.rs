//! Existing account-database credential protection; unrelated to Binding initialization.
use ring::{
    aead,
    rand::{SecureRandom, SystemRandom},
};

/// AES-256-GCM envelope; associated data binds ciphertext to its purpose and owner.
pub struct CredentialCipher {
    key: aead::LessSafeKey,
    // The gateway's fixed namespace binds model identity and encryption.
    namespace: String,
}
impl CredentialCipher {
    /// Uses the gateway's fixed namespace for model identity and encryption derivation.
    /// Returns Configuration if the supplied namespace is not a valid identifier.
    pub fn new(key: [u8; 32], model_namespace: &str) -> super::Result<Self> {
        adx_agent_core::identifier(model_namespace, "model credential namespace")
            .map_err(|_| super::Error::Configuration)?;
        // AES_256_GCM accepts exactly 32 bytes; the array type enforces this invariant.
        let key = aead::UnboundKey::new(&aead::AES_256_GCM, &key).expect("32-byte AES key");
        Ok(Self {
            key: aead::LessSafeKey::new(key),
            namespace: model_namespace.into(),
        })
    }
    pub(crate) fn associated_data(&self, user_id: &str) -> Result<Vec<u8>, serde_json::Error> {
        serde_json::to_vec(&("litellm", &self.namespace, user_id))
    }
    pub(crate) fn model_owner(&self, user_id: &str) -> String {
        format!(
            "adx-{}",
            super::digest(&serde_json::json!([self.namespace, user_id]).to_string())
        )
    }
    pub fn seal(&self, aad: &[u8], plaintext: &[u8]) -> Result<Vec<u8>, &'static str> {
        let mut nonce = [0; 12];
        SystemRandom::new()
            .fill(&mut nonce)
            .map_err(|_| "credential randomness unavailable")?;
        let mut encrypted = plaintext.to_vec();
        self.key
            .seal_in_place_append_tag(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(aad),
                &mut encrypted,
            )
            .map_err(|_| "credential encryption failed")?;
        let mut result = nonce.to_vec();
        result.extend(encrypted);
        Ok(result)
    }
    pub fn open(&self, aad: &[u8], ciphertext: &[u8]) -> Result<Vec<u8>, &'static str> {
        if ciphertext.len() < 28 {
            return Err("invalid encrypted credential");
        }
        let nonce: [u8; 12] = ciphertext[..12].try_into().map_err(|_| "invalid nonce")?;
        let mut encrypted = ciphertext[12..].to_vec();
        self.key
            .open_in_place(
                aead::Nonce::assume_unique_for_key(nonce),
                aead::Aad::from(aad),
                &mut encrypted,
            )
            .map(|v| v.to_vec())
            .map_err(|_| "credential decryption failed")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn credentials_are_bound_to_owner_and_reject_tampering() {
        let cipher = CredentialCipher::new([9; 32], "jiuwen").unwrap();
        let aad = cipher.associated_data("alice").unwrap();
        assert_eq!(aad, br#"["litellm","jiuwen","alice"]"#);
        assert_eq!(
            cipher.model_owner("alice"),
            format!("adx-{}", super::super::digest(r#"["jiuwen","alice"]"#))
        );
        let sealed = cipher
            .seal(br#"["litellm","jiuwen","alice"]"#, b"sk-private")
            .unwrap();
        assert_eq!(cipher.open(&aad, &sealed).unwrap(), b"sk-private");
        assert!(cipher
            .open(&cipher.associated_data("bob").unwrap(), &sealed)
            .is_err());
        assert!(CredentialCipher::new([8; 32], "jiuwen")
            .unwrap()
            .open(&aad, &sealed)
            .is_err());
        let mut damaged = sealed;
        damaged[12] ^= 1;
        assert!(cipher.open(&aad, &damaged).is_err());
    }
}
