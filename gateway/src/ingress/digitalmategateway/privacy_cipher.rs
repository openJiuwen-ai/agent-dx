//! Workspace-bound protection for DigitalMate configuration secrets.

use crate::ingress::accounts::CredentialCipher;
use base64::{engine::general_purpose::STANDARD, Engine};
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::io::Read;
use std::path::Path;
use zeroize::{Zeroize, Zeroizing};

const MAX_SECRET_BYTES: usize = 131_072;
const AEAD_OVERHEAD: usize = 28;

#[derive(Debug, thiserror::Error)]
pub(crate) enum PrivacyCipherError {
    #[error("invalid privacy encryption configuration")]
    Configuration,
    #[error("invalid secret context or payload")]
    Invalid,
    #[error("invalid or unauthenticated secret ciphertext")]
    InvalidCiphertext,
    #[error("secret encryption unavailable")]
    Unavailable,
}

pub(crate) struct SecretContext<'a> {
    pub tenant: &'a str,
    pub workspace: &'a str,
    pub user: &'a str,
    pub name: &'a str,
}

/// Server-only namespace, deliberately disjoint from every user's configuration.
pub(crate) struct ServiceSecretContext<'a> {
    pub tenant: &'a str,
    pub service: &'a str,
    pub name: &'a str,
}

impl ServiceSecretContext<'_> {
    fn aad(&self, key_id: &str) -> Result<Vec<u8>, PrivacyCipherError> {
        adx_agent_core::identifier(self.tenant, "service secret tenant")
            .map_err(|_| PrivacyCipherError::Invalid)?;
        if self.tenant.len() > 128
            || !valid_name(self.service)
            || !valid_name(self.name)
            || !valid_name(key_id)
        {
            return Err(PrivacyCipherError::Invalid);
        }
        serde_json::to_vec(&(
            "digitalmate_service_secret",
            "v1",
            self.tenant,
            self.service,
            self.name,
            key_id,
        ))
        .map_err(|_| PrivacyCipherError::Unavailable)
    }
}

impl SecretContext<'_> {
    fn aad(&self, key_id: &str) -> Result<Vec<u8>, PrivacyCipherError> {
        for (value, label) in [
            (self.tenant, "tenant"),
            (self.workspace, "workspace"),
            (self.user, "user"),
        ] {
            adx_agent_core::identifier(value, label).map_err(|_| PrivacyCipherError::Invalid)?;
            if value.len() > 128 {
                return Err(PrivacyCipherError::Invalid);
            }
        }
        if !valid_name(self.name) || !valid_name(key_id) {
            return Err(PrivacyCipherError::Invalid);
        }
        serde_json::to_vec(&(
            "digitalmate_secret",
            "v1",
            self.tenant,
            self.workspace,
            self.user,
            self.name,
            key_id,
        ))
        .map_err(|_| PrivacyCipherError::Unavailable)
    }
}

fn valid_name(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'))
}

pub(crate) struct EncryptedSecret {
    pub key_id: String,
    pub ciphertext: Vec<u8>,
}

/// AEAD protection using the Gateway credential root and a separate secret AAD domain.
pub(crate) struct PrivacyCipher {
    active: String,
    keys: BTreeMap<String, CredentialCipher>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct KeyFile {
    active_key_id: String,
    keys: BTreeMap<String, String>,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegacyEnvelope {
    version: u8,
    algorithm: String,
    key_backend: String,
    key_id: String,
    context: LegacyContext,
    wrapped_key: String,
    ciphertext: String,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct LegacyContext {
    tenant: String,
    workspace: String,
    purpose: String,
}

impl Drop for KeyFile {
    fn drop(&mut self) {
        for encoded in self.keys.values_mut() {
            encoded.zeroize();
        }
    }
}

pub(super) fn private_key_file(path: &Path) -> Result<Zeroizing<Vec<u8>>, PrivacyCipherError> {
    if !path.is_absolute() {
        return Err(PrivacyCipherError::Configuration);
    }
    let before = std::fs::symlink_metadata(path).map_err(|_| PrivacyCipherError::Configuration)?;
    if !before.is_file() || before.file_type().is_symlink() {
        return Err(PrivacyCipherError::Configuration);
    }
    let mut options = std::fs::OpenOptions::new();
    options.read(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.custom_flags(libc::O_NOFOLLOW | libc::O_NONBLOCK | libc::O_CLOEXEC);
    }
    let file = options
        .open(path)
        .map_err(|_| PrivacyCipherError::Configuration)?;
    let metadata = file
        .metadata()
        .map_err(|_| PrivacyCipherError::Configuration)?;
    if !metadata.is_file() || metadata.len() > 65_536 {
        return Err(PrivacyCipherError::Configuration);
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::MetadataExt;
        if metadata.mode() & 0o077 != 0
            || before.dev() != metadata.dev()
            || before.ino() != metadata.ino()
        {
            return Err(PrivacyCipherError::Configuration);
        }
    }
    let mut bytes = Zeroizing::new(Vec::new());
    file.take(65_537)
        .read_to_end(&mut bytes)
        .map_err(|_| PrivacyCipherError::Configuration)?;
    if bytes.len() > 65_536 {
        return Err(PrivacyCipherError::Configuration);
    }
    Ok(bytes)
}

impl PrivacyCipher {
    /// Export an authenticated old-format envelope for a controlled rollback.
    pub fn seal_legacy(
        &self,
        tenant: &str,
        workspace: &str,
        plaintext: &[u8],
    ) -> Result<LegacyEnvelope, PrivacyCipherError> {
        use ring::rand::{SecureRandom, SystemRandom};
        let safe = |value: &str| {
            !value.is_empty()
                && value.len() <= 128
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
        };
        if !safe(tenant)
            || !safe(workspace)
            || !safe(&self.active)
            || plaintext.len() > MAX_SECRET_BYTES
        {
            return Err(PrivacyCipherError::Invalid);
        }
        let mut dek = Zeroizing::new([0u8; 32]);
        SystemRandom::new()
            .fill(dek.as_mut())
            .map_err(|_| PrivacyCipherError::Unavailable)?;
        let key_aad = serde_json::to_vec(&[
            "adx.credentials.dek",
            "1",
            tenant,
            workspace,
            "orbit-config",
            &self.active,
        ])
        .map_err(|_| PrivacyCipherError::Unavailable)?;
        let data_aad = serde_json::to_vec(&[
            "adx.credentials.data",
            "1",
            tenant,
            workspace,
            "orbit-config",
        ])
        .map_err(|_| PrivacyCipherError::Unavailable)?;
        let old = self
            .keys
            .get(&self.active)
            .ok_or(PrivacyCipherError::Unavailable)?;
        let wrapped_key = old
            .seal(&key_aad, dek.as_ref())
            .map_err(|_| PrivacyCipherError::Unavailable)?;
        let data_cipher = CredentialCipher::new(*dek, "digitalmate-legacy")
            .map_err(|_| PrivacyCipherError::Unavailable)?;
        let ciphertext = data_cipher
            .seal(&data_aad, plaintext)
            .map_err(|_| PrivacyCipherError::Unavailable)?;
        Ok(LegacyEnvelope {
            version: 1,
            algorithm: "AES-256-GCM".into(),
            key_backend: "local".into(),
            key_id: self.active.clone(),
            context: LegacyContext {
                tenant: tenant.into(),
                workspace: workspace.into(),
                purpose: "orbit-config".into(),
            },
            wrapped_key: STANDARD.encode(wrapped_key),
            ciphertext: STANDARD.encode(ciphertext),
        })
    }

    /// Open the former service's envelope only for its original tenant and workspace.
    pub fn open_legacy(
        &self,
        tenant: &str,
        workspace: &str,
        envelope: &LegacyEnvelope,
    ) -> Result<Zeroizing<Vec<u8>>, PrivacyCipherError> {
        if envelope.version != 1
            || envelope.algorithm != "AES-256-GCM"
            || envelope.key_backend != "local"
            || envelope.context.tenant != tenant
            || envelope.context.workspace != workspace
            || envelope.context.purpose != "orbit-config"
            || !valid_name(&envelope.key_id)
            || envelope.wrapped_key.len() > 80
            || envelope.ciphertext.len() > (MAX_SECRET_BYTES + AEAD_OVERHEAD).div_ceil(3) * 4
        {
            return Err(PrivacyCipherError::InvalidCiphertext);
        }
        let wrapped = STANDARD
            .decode(&envelope.wrapped_key)
            .map_err(|_| PrivacyCipherError::InvalidCiphertext)?;
        let data = STANDARD
            .decode(&envelope.ciphertext)
            .map_err(|_| PrivacyCipherError::InvalidCiphertext)?;
        if wrapped.len() != 32 + AEAD_OVERHEAD
            || !(AEAD_OVERHEAD..=MAX_SECRET_BYTES + AEAD_OVERHEAD).contains(&data.len())
        {
            return Err(PrivacyCipherError::InvalidCiphertext);
        }
        let key_aad = serde_json::to_vec(&[
            "adx.credentials.dek",
            "1",
            tenant,
            workspace,
            "orbit-config",
            &envelope.key_id,
        ])
        .map_err(|_| PrivacyCipherError::Unavailable)?;
        let data_aad = serde_json::to_vec(&[
            "adx.credentials.data",
            "1",
            tenant,
            workspace,
            "orbit-config",
        ])
        .map_err(|_| PrivacyCipherError::Unavailable)?;
        let old = self
            .keys
            .get(&envelope.key_id)
            .ok_or(PrivacyCipherError::InvalidCiphertext)?;
        let unwrapped = Zeroizing::new(
            old.open(&key_aad, &wrapped)
                .map_err(|_| PrivacyCipherError::InvalidCiphertext)?,
        );
        let key = Zeroizing::new(
            <[u8; 32]>::try_from(unwrapped.as_slice())
                .map_err(|_| PrivacyCipherError::InvalidCiphertext)?,
        );
        let data_cipher = CredentialCipher::new(*key, "digitalmate-legacy")
            .map_err(|_| PrivacyCipherError::Unavailable)?;
        Ok(Zeroizing::new(
            data_cipher
                .open(&data_aad, &data)
                .map_err(|_| PrivacyCipherError::InvalidCiphertext)?,
        ))
    }

    pub fn load(path: &Path) -> Result<Self, PrivacyCipherError> {
        let mut file: KeyFile = serde_json::from_slice(&private_key_file(path)?)
            .map_err(|_| PrivacyCipherError::Configuration)?;
        let mut keys = BTreeMap::new();
        for (id, encoded) in &mut file.keys {
            let decoded = Zeroizing::new(
                STANDARD
                    .decode(encoded.as_bytes())
                    .map_err(|_| PrivacyCipherError::Configuration)?,
            );
            let key: [u8; 32] = decoded
                .as_slice()
                .try_into()
                .map_err(|_| PrivacyCipherError::Configuration)?;
            keys.insert(id.clone(), key);
            encoded.zeroize();
        }
        Self::new(file.active_key_id.clone(), keys)
    }

    pub fn new(
        active: String,
        keys: BTreeMap<String, [u8; 32]>,
    ) -> Result<Self, PrivacyCipherError> {
        if keys.is_empty()
            || keys.len() > 32
            || !valid_name(&active)
            || !keys.contains_key(&active)
            || !keys.keys().all(|key| valid_name(key))
        {
            return Err(PrivacyCipherError::Configuration);
        }
        let keys = keys
            .into_iter()
            .map(|(id, mut key)| {
                let result = CredentialCipher::new(key, "digitalmate-secrets")
                    .map(|cipher| (id, cipher))
                    .map_err(|_| PrivacyCipherError::Configuration);
                key.zeroize();
                result
            })
            .collect::<Result<_, _>>()?;
        Ok(Self { active, keys })
    }

    pub fn seal(
        &self,
        context: &SecretContext<'_>,
        plaintext: &[u8],
    ) -> Result<EncryptedSecret, PrivacyCipherError> {
        let aad = context.aad(&self.active)?;
        self.seal_aad(&aad, plaintext)
    }

    pub fn seal_service(
        &self,
        context: &ServiceSecretContext<'_>,
        plaintext: &[u8],
    ) -> Result<EncryptedSecret, PrivacyCipherError> {
        self.seal_aad(&context.aad(&self.active)?, plaintext)
    }

    fn seal_aad(
        &self,
        aad: &[u8],
        plaintext: &[u8],
    ) -> Result<EncryptedSecret, PrivacyCipherError> {
        if plaintext.len() > MAX_SECRET_BYTES {
            return Err(PrivacyCipherError::Invalid);
        }
        let cipher = self
            .keys
            .get(&self.active)
            .ok_or(PrivacyCipherError::Unavailable)?;
        let ciphertext = cipher
            .seal(aad, plaintext)
            .map_err(|_| PrivacyCipherError::Unavailable)?;
        Ok(EncryptedSecret {
            key_id: self.active.clone(),
            ciphertext,
        })
    }

    pub fn open(
        &self,
        context: &SecretContext<'_>,
        secret: &EncryptedSecret,
    ) -> Result<Vec<u8>, PrivacyCipherError> {
        self.open_aad(&context.aad(&secret.key_id)?, secret)
    }

    pub fn open_service(
        &self,
        context: &ServiceSecretContext<'_>,
        secret: &EncryptedSecret,
    ) -> Result<Zeroizing<Vec<u8>>, PrivacyCipherError> {
        self.open_aad(&context.aad(&secret.key_id)?, secret)
            .map(Zeroizing::new)
    }

    fn open_aad(
        &self,
        aad: &[u8],
        secret: &EncryptedSecret,
    ) -> Result<Vec<u8>, PrivacyCipherError> {
        if secret.ciphertext.len() < AEAD_OVERHEAD
            || secret.ciphertext.len() > MAX_SECRET_BYTES + AEAD_OVERHEAD
        {
            return Err(PrivacyCipherError::InvalidCiphertext);
        }
        let cipher = self
            .keys
            .get(&secret.key_id)
            .ok_or(PrivacyCipherError::InvalidCiphertext)?;
        cipher
            .open(aad, &secret.ciphertext)
            .map_err(|_| PrivacyCipherError::InvalidCiphertext)
    }
}

#[cfg(test)]
mod tests {
    use super::{PrivacyCipher, SecretContext, ServiceSecretContext};
    use std::collections::BTreeMap;

    fn keys(active: &str) -> PrivacyCipher {
        PrivacyCipher::new(
            active.into(),
            BTreeMap::from([("old".into(), [1; 32]), ("new".into(), [2; 32])]),
        )
        .unwrap()
    }

    fn context<'a>() -> SecretContext<'a> {
        SecretContext {
            tenant: "tenant-a",
            workspace: "workspace-a",
            user: "user-a",
            name: "FEISHU_TOKEN",
        }
    }

    #[test]
    fn service_secrets_cannot_be_opened_as_user_configuration() {
        let cipher = keys("new");
        let context = ServiceSecretContext {
            tenant: "tenant-a",
            service: "composio",
            name: "PROJECT_API_KEY",
        };
        let sealed = cipher
            .seal_service(&context, b"synthetic-project-key")
            .unwrap();
        assert_eq!(
            cipher.open_service(&context, &sealed).unwrap().as_slice(),
            b"synthetic-project-key"
        );
        assert!(cipher
            .open(
                &SecretContext {
                    tenant: "tenant-a",
                    workspace: "composio",
                    user: "composio",
                    name: "PROJECT_API_KEY",
                },
                &sealed
            )
            .is_err());
        assert!(cipher
            .open_service(
                &ServiceSecretContext {
                    tenant: "other",
                    ..context
                },
                &sealed
            )
            .is_err());
        assert!(cipher
            .open_service(
                &ServiceSecretContext {
                    service: "other",
                    ..context
                },
                &sealed
            )
            .is_err());
        assert!(cipher
            .open_service(
                &ServiceSecretContext {
                    name: "OTHER_KEY",
                    ..context
                },
                &sealed
            )
            .is_err());
        let user = cipher
            .seal(&super::tests::context(), b"user-secret")
            .unwrap();
        assert!(cipher.open_service(&context, &user).is_err());
    }

    #[test]
    fn old_workspace_envelope_can_only_be_opened_in_its_original_context() {
        let old =
            PrivacyCipher::new("v1".into(), BTreeMap::from([("v1".into(), [7; 32])])).unwrap();
        let envelope = serde_json::from_str(r#"{"version":1,"algorithm":"AES-256-GCM","key_backend":"local","key_id":"v1","context":{"tenant":"digitalmate","workspace":"alice-workspace","purpose":"orbit-config"},"wrapped_key":"AQEBAQEBAQEBAQEBc+SMspW66QvU0dkkdTJ1NK3+EfEdkj4WXTRGZ2OxM77zhpWEwExA4LXGDYhQ79Nu","ciphertext":"AgICAgICAgICAgICrj6Lz7//T4WAifF7/FexfSVNWyG6Pzba5TrCKAGavkySzf9l8qfr6g=="}"#).unwrap();
        assert_eq!(
            old.open_legacy("digitalmate", "alice-workspace", &envelope)
                .unwrap()
                .as_slice(),
            b"persisted-provider-token"
        );
        assert!(old
            .open_legacy("digitalmate", "other-workspace", &envelope)
            .is_err());
        assert!(old
            .open_legacy("other-tenant", "alice-workspace", &envelope)
            .is_err());
        let returned = old
            .seal_legacy("digitalmate", "alice-workspace", b"refreshed-token")
            .unwrap();
        assert_eq!(
            old.open_legacy("digitalmate", "alice-workspace", &returned)
                .unwrap()
                .as_slice(),
            b"refreshed-token"
        );
        assert!(old
            .open_legacy("digitalmate", "other-workspace", &returned)
            .is_err());
    }

    #[test]
    fn ciphertext_is_randomized_and_bound_to_its_owner_and_name() {
        let cipher = keys("old");
        let input = b"synthetic-test-secret";
        let first = cipher.seal(&context(), input).unwrap();
        let second = cipher.seal(&context(), input).unwrap();
        assert_ne!(first.ciphertext, second.ciphertext);
        assert_eq!(cipher.open(&context(), &first).unwrap(), input);
        for altered in [
            SecretContext {
                tenant: "tenant-b",
                ..context()
            },
            SecretContext {
                workspace: "workspace-b",
                ..context()
            },
            SecretContext {
                user: "user-b",
                ..context()
            },
            SecretContext {
                name: "OTHER_TOKEN",
                ..context()
            },
        ] {
            assert!(cipher.open(&altered, &first).is_err());
        }
    }

    #[test]
    fn authentication_rejects_changed_data_and_wrong_key() {
        let cipher = keys("old");
        let mut sealed = cipher.seal(&context(), b"synthetic").unwrap();
        sealed.ciphertext[12] ^= 1;
        assert!(cipher.open(&context(), &sealed).is_err());
        let mut sealed = cipher.seal(&context(), b"synthetic").unwrap();
        sealed.key_id = "new".into();
        assert!(cipher.open(&context(), &sealed).is_err());
        sealed.key_id = "missing".into();
        assert!(cipher.open(&context(), &sealed).is_err());
    }

    #[test]
    fn rotation_preserves_old_decryption_and_uses_new_key() {
        let old = keys("old").seal(&context(), b"synthetic").unwrap();
        let rotated = keys("new");
        assert_eq!(rotated.open(&context(), &old).unwrap(), b"synthetic");
        assert_eq!(
            rotated.seal(&context(), b"synthetic").unwrap().key_id,
            "new"
        );
    }

    #[test]
    fn invalid_context_keys_and_payloads_fail_closed() {
        assert!(PrivacyCipher::new("missing".into(), BTreeMap::new()).is_err());
        assert!(PrivacyCipher::new(
            "bad/name".into(),
            BTreeMap::from([("bad/name".into(), [1; 32])])
        )
        .is_err());
        let cipher = keys("old");
        assert!(cipher
            .seal(
                &SecretContext {
                    user: "",
                    ..context()
                },
                b"secret"
            )
            .is_err());
        assert!(cipher
            .seal(
                &SecretContext {
                    name: "../secret",
                    ..context()
                },
                b"secret"
            )
            .is_err());
        assert!(cipher.seal(&context(), &vec![0; 131_073]).is_err());
    }

    #[test]
    fn reusing_gateway_root_does_not_allow_cross_purpose_ciphertext() {
        let root = [7; 32];
        let privacy = PrivacyCipher::new(
            "gateway-v1".into(),
            BTreeMap::from([("gateway-v1".into(), root)]),
        )
        .unwrap();
        let model = crate::ingress::accounts::CredentialCipher::new(root, "tenant-a").unwrap();
        let aad = model.associated_data("user-a").unwrap();
        let sealed = privacy.seal(&context(), b"private").unwrap();
        assert!(model.open(&aad, &sealed.ciphertext).is_err());
        let model_secret = model.seal(&aad, b"model-key").unwrap();
        assert!(privacy
            .open(
                &context(),
                &super::EncryptedSecret {
                    key_id: "gateway-v1".into(),
                    ciphertext: model_secret
                }
            )
            .is_err());
    }

    #[cfg(unix)]
    #[test]
    fn legacy_import_keyring_is_private_and_accepts_the_existing_root() {
        use base64::{engine::general_purpose::STANDARD, Engine};
        use std::os::unix::fs::{symlink, PermissionsExt};

        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("privacy-keys.json");
        let encoded = serde_json::json!({
            "active_key_id":"primary",
            "keys":{"primary": STANDARD.encode([7u8; 32])}
        });
        std::fs::write(&path, encoded.to_string()).unwrap();
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        assert!(PrivacyCipher::load(&path).is_ok());
        let link = directory.path().join("link.json");
        symlink(&path, &link).unwrap();
        assert!(PrivacyCipher::load(&link).is_err());
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o644)).unwrap();
        assert!(PrivacyCipher::load(&path).is_err());
    }
}
