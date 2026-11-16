//! Revocable, signed business credentials delivered only over an admitted Gateway handshake.
use super::privacy_center::BindingReader;
use crate::ingress::accounts::{AccountStore, Error, PrivacyOwner, StoredPrivacyGrant};
use adx_agent_core::{BindingPhase, Scope};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use ring::{
    hmac,
    rand::{SecureRandom, SystemRandom},
};
use serde::{Deserialize, Serialize};
use std::{
    sync::Arc,
    time::{SystemTime, UNIX_EPOCH},
};
use zeroize::Zeroizing;

pub(crate) const ACCESS_SECONDS: i64 = 300;
const RECOVERY_SECONDS: i64 = 30 * 24 * 3600;
const CLOCK_SKEW_SECONDS: i64 = 60;

pub(crate) fn now() -> Result<i64, Error> {
    i64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable)?
            .as_secs(),
    )
    .map_err(|_| Error::Unavailable)
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Claims {
    pub id: String,
    tenant: String,
    workspace: String,
    user: String,
    exp: i64,
    recovery_until: i64,
}

pub(crate) struct TokenSigner {
    key: hmac::Key,
    tenant: String,
}
impl TokenSigner {
    pub fn new(root: &[u8; 32], tenant: &str) -> Self {
        let key = hmac::Key::new(hmac::HMAC_SHA256, root);
        let derived = hmac::sign(&key, b"digitalmate_secret_token_v1");
        Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, derived.as_ref()),
            tenant: tenant.into(),
        }
    }
    fn sign(&self, claims: &Claims) -> Result<Zeroizing<String>, Error> {
        let payload =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).map_err(|_| Error::Unavailable)?);
        let signed = format!("dm-privacy-v1.{payload}");
        let signature = URL_SAFE_NO_PAD.encode(hmac::sign(&self.key, signed.as_bytes()).as_ref());
        Ok(Zeroizing::new(format!("{signed}.{signature}")))
    }
    pub fn verify(&self, token: &str, renewal: bool) -> Result<Claims, Error> {
        if token.len() > 2048 {
            return Err(Error::Unauthorized);
        }
        let (signed, signature) = token.rsplit_once('.').ok_or(Error::Unauthorized)?;
        let payload = signed
            .strip_prefix("dm-privacy-v1.")
            .ok_or(Error::Unauthorized)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| Error::Unauthorized)?;
        hmac::verify(&self.key, signed.as_bytes(), &signature).map_err(|_| Error::Unauthorized)?;
        let bytes = URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| Error::Unauthorized)?;
        let claims: Claims = serde_json::from_slice(&bytes).map_err(|_| Error::Unauthorized)?;
        let current = now()?;
        if claims.tenant != self.tenant
            || claims.id.len() != 64
            || !claims.id.bytes().all(|v| v.is_ascii_hexdigit())
            || claims.recovery_until <= current
            || (!renewal && claims.exp <= current)
            || claims.exp > current.saturating_add(ACCESS_SECONDS + CLOCK_SKEW_SECONDS)
            || claims.exp > claims.recovery_until
        {
            return Err(Error::Unauthorized);
        }
        Ok(claims)
    }
    pub fn matches(claims: &Claims, grant: &StoredPrivacyGrant) -> bool {
        !grant.revoked
            && claims.tenant == grant.scope.tenant
            && claims.workspace == grant.workspace
            && claims.user == grant.user
            && claims.recovery_until <= grant.expires_at
    }
}

/// ADX binding access is an admission/renewal adapter; secret operations use only business state.
pub(crate) struct PrivacyIdentity {
    template: String,
    store: AccountStore,
    bindings: Arc<dyn BindingReader>,
    signer: Arc<TokenSigner>,
}
impl PrivacyIdentity {
    pub fn new(
        template: String,
        store: AccountStore,
        bindings: Arc<dyn BindingReader>,
        signer: Arc<TokenSigner>,
    ) -> Self {
        Self {
            template,
            store,
            bindings,
            signer,
        }
    }
    pub fn targets(&self, template: &str) -> bool {
        self.template == template
    }
    pub async fn admit(
        &self,
        scope: &Scope,
        workspace: &str,
        user: &str,
    ) -> Result<Zeroizing<String>, Error> {
        let binding = self.bindings.binding(scope).await?;
        if !self.targets(&scope.template)
            || binding.scope != *scope
            || binding.phase != BindingPhase::Active
            || binding.generation.is_empty()
            || binding.parameters.get("workspace").map(String::as_str) != Some(workspace)
        {
            return Err(Error::Unauthorized);
        }
        let mut random = Zeroizing::new([0u8; 32]);
        SystemRandom::new()
            .fill(random.as_mut())
            .map_err(|_| Error::Unavailable)?;
        let id = hex::encode(random.as_ref());
        let current = now()?;
        let mut claims = Claims {
            id,
            tenant: scope.tenant.clone(),
            workspace: workspace.into(),
            user: user.into(),
            exp: current + ACCESS_SECONDS,
            recovery_until: current + RECOVERY_SECONDS,
        };
        claims.id = self
            .store
            .privacy_issue_grant(
                &claims.id,
                &PrivacyOwner {
                    tenant: &scope.tenant,
                    workspace,
                    user,
                },
                scope,
                &binding.generation,
                claims.recovery_until,
                || async {
                    let current = self.bindings.binding(scope).await?;
                    if current.scope != *scope
                        || current.phase != BindingPhase::Active
                        || current.generation != binding.generation
                        || current.parameters.get("workspace").map(String::as_str)
                            != Some(workspace)
                    {
                        return Err(Error::Unauthorized);
                    }
                    Ok(())
                },
            )
            .await?;
        self.signer.sign(&claims)
    }
    pub async fn renew(&self, token: &str) -> Result<serde_json::Value, Error> {
        let mut claims = self.signer.verify(token, true)?;
        let grant = self
            .store
            .privacy_get_grant(&claims.id)
            .await?
            .ok_or(Error::Unauthorized)?;
        if !self.targets(&grant.scope.template) || !TokenSigner::matches(&claims, &grant) {
            return Err(Error::Unauthorized);
        }
        let binding = match self.bindings.binding(&grant.scope).await {
            Ok(binding) => binding,
            Err(Error::Unauthorized) => {
                self.store.privacy_revoke_grant(&claims.id).await?;
                return Err(Error::Unauthorized);
            }
            Err(error) => return Err(error),
        };
        if binding.scope != grant.scope
            || binding.phase != BindingPhase::Active
            || binding.generation != grant.generation
            || binding.parameters.get("workspace").map(String::as_str)
                != Some(grant.workspace.as_str())
        {
            self.store.privacy_revoke_grant(&claims.id).await?;
            return Err(Error::Unauthorized);
        }
        let current = now()?;
        claims.exp = current + ACCESS_SECONDS;
        claims.recovery_until = current + RECOVERY_SECONDS;
        self.store
            .privacy_extend_grant(&claims.id, claims.recovery_until)
            .await?;
        let token = self.signer.sign(&claims)?;
        Ok(serde_json::json!({"token":token.as_str(),"expires_at":claims.exp}))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn accepts_clock_skew_but_does_not_extend_expired_access() {
        let signer = TokenSigner::new(&[7; 32], "tenant");
        let current = now().unwrap();
        let mut claims = Claims {
            id: "a".repeat(64),
            tenant: "tenant".into(),
            workspace: "workspace".into(),
            user: "user".into(),
            exp: current + ACCESS_SECONDS + 30,
            recovery_until: current + 3600,
        };
        assert!(signer.verify(&signer.sign(&claims).unwrap(), false).is_ok());
        claims.exp = current + ACCESS_SECONDS + 90;
        assert!(signer
            .verify(&signer.sign(&claims).unwrap(), false)
            .is_err());
        claims.exp = current - 1;
        assert!(signer
            .verify(&signer.sign(&claims).unwrap(), false)
            .is_err());
    }

    #[test]
    fn signed_identity_rejects_forgery_wrong_key_and_expired_reads_but_allows_recovery() {
        let signer = TokenSigner::new(&[7; 32], "tenant");
        let current = now().unwrap();
        let mut claims = Claims {
            id: "a".repeat(64),
            tenant: "tenant".into(),
            workspace: "workspace".into(),
            user: "user".into(),
            exp: current + 60,
            recovery_until: current + 3600,
        };
        let token = signer.sign(&claims).unwrap();
        assert!(signer.verify(&token, false).is_ok());
        assert!(TokenSigner::new(&[7; 32], "other-tenant")
            .verify(&token, false)
            .is_err());
        assert!(TokenSigner::new(&[8; 32], "tenant")
            .verify(&token, false)
            .is_err());
        assert!(signer
            .verify(&format!("{}X", token.as_str()), false)
            .is_err());
        claims.exp = current - 1;
        let old = signer.sign(&claims).unwrap();
        assert!(signer.verify(&old, false).is_err());
        assert!(signer.verify(&old, true).is_ok());
        claims.recovery_until = current - 1;
        assert!(signer.verify(&signer.sign(&claims).unwrap(), true).is_err());
    }
}
