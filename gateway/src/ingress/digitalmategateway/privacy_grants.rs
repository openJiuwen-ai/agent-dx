//! Revocable, signed business credentials delivered only over an admitted Gateway handshake.
use super::ingestion::{
    DeliveryAuthorization, DeliveryBatch, DeliveryFailure, Inbox, SubscriptionBinding,
};
use super::privacy_center::BindingReader;
use crate::ingress::accounts::{AccountStore, Error, PrivacyOwner, StoredPrivacyGrant};
use crate::ingress::agent_service::{ExistingServiceAdmission, ServiceSelector};
use adx_agent_core::{BindingPhase, Protocol, Scope};
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
const DELIVERY_ACCESS_SECONDS: i64 = 60;

#[cfg(test)]
#[path = "privacy_purpose_tests.rs"]
mod purpose_tests;

#[cfg(test)]
#[path = "privacy_delivery_tests.rs"]
mod delivery_tests;

#[cfg(all(test, unix))]
#[path = "composio/runtime_identity_tests.rs"]
mod runtime_identity_tests;

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
    /// Delivery-derived connector access cannot acquire a renewable lifetime.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bootstrap_until: Option<i64>,
}

/// A delivery credential identifies a persisted batch, never a JSON-provided scope.
/// The identity owner must resolve its current binding and subscription from Inbox.
#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DeliveryClaims {
    pub grant_id: String,
    pub tenant: String,
    pub batch_id: String,
    pub digest: String,
    pub wire_bytes: usize,
    pub exp: i64,
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
        self.sign_payload("dm-privacy-v1", claims)
    }
    fn sign_connector(&self, claims: &Claims) -> Result<Zeroizing<String>, Error> {
        self.sign_payload("dm-connector-v1", claims)
    }
    fn sign_delivery(&self, claims: &DeliveryClaims) -> Result<Zeroizing<String>, Error> {
        self.sign_payload("dm-ingestion-v1", claims)
    }

    pub(super) fn sign_owner(
        &self,
        claims: &super::connector_owner::OwnerClaims,
    ) -> Result<Zeroizing<String>, Error> {
        self.sign_payload(super::connector_owner::PREFIX, claims)
    }

    pub(super) fn verify_owner(
        &self,
        token: &str,
    ) -> Result<super::connector_owner::OwnerClaims, Error> {
        let bytes = self.verify_payload(token, super::connector_owner::PREFIX)?;
        let claims: super::connector_owner::OwnerClaims =
            serde_json::from_slice(&bytes).map_err(|_| Error::Unauthorized)?;
        claims.validate(&self.tenant)?;
        Ok(claims)
    }
    fn sign_payload<T: Serialize>(
        &self,
        prefix: &str,
        claims: &T,
    ) -> Result<Zeroizing<String>, Error> {
        let payload =
            URL_SAFE_NO_PAD.encode(serde_json::to_vec(claims).map_err(|_| Error::Unavailable)?);
        let signed = format!("{prefix}.{payload}");
        let signature = URL_SAFE_NO_PAD.encode(hmac::sign(&self.key, signed.as_bytes()).as_ref());
        Ok(Zeroizing::new(format!("{signed}.{signature}")))
    }
    pub fn verify(&self, token: &str, renewal: bool) -> Result<Claims, Error> {
        self.verify_scope(token, "dm-privacy-v1", renewal)
    }
    fn verify_connector(&self, token: &str, renewal: bool) -> Result<Claims, Error> {
        self.verify_scope(token, "dm-connector-v1", renewal)
    }
    fn verify_delivery(&self, token: &str) -> Result<DeliveryClaims, Error> {
        let bytes = self.verify_payload(token, "dm-ingestion-v1")?;
        let claims: DeliveryClaims =
            serde_json::from_slice(&bytes).map_err(|_| Error::Unauthorized)?;
        let current = now()?;
        let digest = |value: &str| {
            value.len() == 64
                && value
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        };
        if claims.tenant != self.tenant
            || !digest(&claims.grant_id)
            || !digest(&claims.digest)
            || claims.batch_id != format!("batch_{}", claims.digest)
            || !(1..=2_097_152).contains(&claims.wire_bytes)
            || claims.exp <= current
            || claims.exp > current.saturating_add(DELIVERY_ACCESS_SECONDS + CLOCK_SKEW_SECONDS)
        {
            return Err(Error::Unauthorized);
        }
        Ok(claims)
    }
    fn verify_payload(&self, token: &str, prefix: &str) -> Result<Vec<u8>, Error> {
        if token.len() > 2048 {
            return Err(Error::Unauthorized);
        }
        let (signed, signature) = token.rsplit_once('.').ok_or(Error::Unauthorized)?;
        let payload = signed
            .strip_prefix(&format!("{prefix}."))
            .ok_or(Error::Unauthorized)?;
        let signature = URL_SAFE_NO_PAD
            .decode(signature)
            .map_err(|_| Error::Unauthorized)?;
        hmac::verify(&self.key, signed.as_bytes(), &signature).map_err(|_| Error::Unauthorized)?;
        URL_SAFE_NO_PAD
            .decode(payload)
            .map_err(|_| Error::Unauthorized)
    }
    fn verify_scope(&self, token: &str, prefix: &str, renewal: bool) -> Result<Claims, Error> {
        let bytes = self.verify_payload(token, prefix)?;
        let claims: Claims = serde_json::from_slice(&bytes).map_err(|_| Error::Unauthorized)?;
        let current = now()?;
        if claims.tenant != self.tenant
            || claims.id.len() != 64
            || !claims.id.bytes().all(|v| v.is_ascii_hexdigit())
            || claims.recovery_until <= current
            || (!renewal && claims.exp <= current)
            || claims.exp > current.saturating_add(ACCESS_SECONDS + CLOCK_SKEW_SECONDS)
            || claims.exp > claims.recovery_until
            || claims.bootstrap_until.is_some_and(|until| {
                prefix != "dm-connector-v1"
                    || until != claims.exp
                    || until != claims.recovery_until
                    || until <= current
                    || until > current.saturating_add(DELIVERY_ACCESS_SECONDS + CLOCK_SKEW_SECONDS)
            })
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

/// Separate-purpose credentials from one trusted admission and one stored grant.
/// Neither connector identity nor privacy identity grants a connection read.
pub(crate) struct WorkloadCredentials {
    pub privacy: Zeroizing<String>,
    pub connector: Zeroizing<String>,
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
    #[cfg(test)]
    pub async fn admit(
        &self,
        scope: &Scope,
        workspace: &str,
        user: &str,
    ) -> Result<Zeroizing<String>, Error> {
        self.signer
            .sign(&self.admission_claims(scope, workspace, user).await?)
    }

    pub async fn admit_workload(
        &self,
        scope: &Scope,
        workspace: &str,
        user: &str,
    ) -> Result<WorkloadCredentials, Error> {
        let claims = self.admission_claims(scope, workspace, user).await?;
        Ok(WorkloadCredentials {
            privacy: self.signer.sign(&claims)?,
            connector: self.signer.sign_connector(&claims)?,
        })
    }

    async fn admission_claims(
        &self,
        scope: &Scope,
        workspace: &str,
        user: &str,
    ) -> Result<Claims, Error> {
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
            bootstrap_until: None,
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
        Ok(claims)
    }

    async fn connector_grant(
        &self,
        token: &str,
        renewal: bool,
    ) -> Result<(Claims, StoredPrivacyGrant), Error> {
        let claims = self.signer.verify_connector(token, renewal)?;
        let grant = self
            .store
            .privacy_get_grant(&claims.id)
            .await?
            .ok_or(Error::Unauthorized)?;
        if !self.targets(&grant.scope.template) || !TokenSigner::matches(&claims, &grant) {
            return Err(Error::Unauthorized);
        }
        self.current_binding(&claims.id, &grant).await?;
        Ok((claims, grant))
    }

    pub(super) async fn owner_connector_grant(
        &self,
        token: &str,
    ) -> Result<(StoredPrivacyGrant, String, i64), Error> {
        let (claims, grant) = self.connector_grant(token, false).await?;
        Ok((grant, claims.id, claims.exp))
    }

    pub(super) async fn owner_grant(&self, id: &str) -> Result<StoredPrivacyGrant, Error> {
        let grant = self
            .store
            .privacy_get_grant(id)
            .await?
            .ok_or(Error::Unauthorized)?;
        self.current_binding(id, &grant).await?;
        Ok(grant)
    }

    async fn current_binding(&self, id: &str, grant: &StoredPrivacyGrant) -> Result<(), Error> {
        if grant.revoked || !self.targets(&grant.scope.template) {
            return Err(Error::Unauthorized);
        }
        let binding = match self.bindings.binding(&grant.scope).await {
            Ok(binding) => binding,
            Err(Error::Unauthorized) => {
                self.store.privacy_revoke_grant(id).await?;
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
            self.store.privacy_revoke_grant(id).await?;
            return Err(Error::Unauthorized);
        }
        Ok(())
    }

    /// Verified current workload identity. Connection/session permissions must
    /// still come from their actual owners before constructing a tool scope.
    pub async fn connector_identity(&self, token: &str) -> Result<serde_json::Value, Error> {
        let (claims, grant) = self.connector_grant(token, false).await?;
        Ok(serde_json::json!({
            "purpose":"connector.tool", "tenant":grant.scope.tenant,
            "user_id":grant.user, "workspace":grant.workspace,
            "template":grant.scope.template, "template_ref":grant.scope.template_ref.name(),
            "binding_id":grant.scope.binding_id, "binding_generation":grant.generation,
            "expires_at":claims.exp,
            "renewable":claims.bootstrap_until.is_none(),
            "recovery_expires_at":claims.recovery_until,
            "bootstrap_until":claims.bootstrap_until,
        }))
    }

    pub async fn renew_connector(&self, token: &str) -> Result<serde_json::Value, Error> {
        let (mut claims, _) = self.connector_grant(token, true).await?;
        if claims.bootstrap_until.is_some() {
            return Err(Error::Forbidden);
        }
        let current = now()?;
        claims.exp = current + ACCESS_SECONDS;
        claims.recovery_until = current + RECOVERY_SECONDS;
        self.store
            .privacy_extend_grant(&claims.id, claims.recovery_until)
            .await?;
        let token = self.signer.sign_connector(&claims)?;
        Ok(serde_json::json!({
            "token":token.as_str(), "expires_at":claims.exp,
            "renewable":claims.bootstrap_until.is_none(),
            "recovery_expires_at":claims.recovery_until,
            "bootstrap_until":claims.bootstrap_until,
        }))
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

/// Uses the existing identity owner and the sole ingestion persistence owner.
/// No second signing key, grant table, connection writer or scope-from-JSON path.
pub(crate) struct DeliveryIdentity {
    identity: Arc<PrivacyIdentity>,
    inbox: Inbox,
}

pub(crate) struct VerifiedDelivery {
    claims: DeliveryClaims,
    binding: SubscriptionBinding,
    admission: ExistingServiceAdmission,
}

impl VerifiedDelivery {
    pub(super) fn json(&self) -> serde_json::Value {
        serde_json::json!({
            "purpose":"ingestion.delivery", "tenant":self.binding.tenant,
            "user_id":self.binding.user_id, "workspace":self.binding.workspace,
            "template":self.binding.template, "template_ref":self.binding.template_ref,
            "binding_id":self.binding.binding_id, "binding_generation":self.binding.binding_generation,
            "service_port":self.binding.service_port, "batch_id":self.claims.batch_id,
            "digest":self.claims.digest, "wire_bytes":self.claims.wire_bytes,
            "expires_at":self.claims.exp,
        })
    }
}

fn ingestion_error(error: super::ingestion::Error) -> Error {
    match error {
        super::ingestion::Error::Unavailable => Error::Unavailable,
        _ => Error::Unauthorized,
    }
}

pub(super) fn binding_scope(binding: &SubscriptionBinding) -> Result<Scope, Error> {
    Ok(Scope {
        tenant: binding.tenant.clone(),
        template: binding.template.clone(),
        template_ref: binding
            .template_ref
            .parse()
            .map_err(|_| Error::Unauthorized)?,
        binding_id: binding.binding_id.clone(),
    })
}

impl DeliveryIdentity {
    pub(crate) fn new(identity: Arc<PrivacyIdentity>, inbox: Inbox) -> Self {
        Self { identity, inbox }
    }

    /// Exchange only a currently persisted delivery for bounded connector access.
    /// Reuses its existing grant and signer; no privacy credential is issued.
    pub(crate) async fn bootstrap(
        &self,
        token: &str,
        owner: &super::connector_owner::OwnerAuthority,
    ) -> Result<serde_json::Value, Error> {
        let admission = self.verify(token).await?;
        let target = owner.delivery_target(&admission.binding).await?;
        let expires_at = admission.claims.exp.min(now()? + DELIVERY_ACCESS_SECONDS);
        let claims = Claims {
            id: admission.claims.grant_id.clone(),
            tenant: admission.binding.tenant.clone(),
            workspace: admission.binding.workspace.clone(),
            user: admission.binding.user_id.clone(),
            exp: expires_at,
            recovery_until: expires_at,
            bootstrap_until: Some(expires_at),
        };
        let connector = self.identity.signer.sign_connector(&claims)?;
        self.identity.connector_grant(&connector, false).await?;
        if owner.delivery_target(&admission.binding).await? != target {
            return Err(Error::Unauthorized);
        }
        // Last owner I/O rechecks subscription, persisted batch, grant and Binding.
        let current = self.verify(token).await?;
        if current.binding != admission.binding {
            return Err(Error::Unauthorized);
        }
        self.identity.signer.verify_connector(&connector, false)?;
        Ok(serde_json::json!({
            "purpose":"ingestion.connector-bootstrap", "target":target,
            "connector_token":connector.as_str(), "expires_at":expires_at,
        }))
    }

    async fn issue(&self, batch: &DeliveryBatch) -> Result<Zeroizing<String>, Error> {
        batch.validate().map_err(ingestion_error)?;
        let size = serde_json::to_vec(batch).map_err(|_| Error::Invalid)?.len();
        let binding = self
            .inbox
            .delivery_binding(&batch.batch_id, &batch.digest, size)
            .await
            .map_err(ingestion_error)?;
        if binding != batch.binding {
            return Err(Error::Unauthorized);
        }
        let scope = binding_scope(&binding)?;
        let (id, grant) = self
            .identity
            .store
            .privacy_current_grant(
                &PrivacyOwner {
                    tenant: &binding.tenant,
                    workspace: &binding.workspace,
                    user: &binding.user_id,
                },
                &scope,
                &binding.binding_generation,
            )
            .await?
            .ok_or(Error::Unauthorized)?;
        self.identity.current_binding(&id, &grant).await?;
        let claims = DeliveryClaims {
            grant_id: id,
            tenant: binding.tenant,
            batch_id: batch.batch_id.clone(),
            digest: batch.digest.clone(),
            wire_bytes: size,
            exp: (now()? + DELIVERY_ACCESS_SECONDS).min(grant.expires_at),
        };
        let token = self.identity.signer.sign_delivery(&claims)?;
        // Re-read current owned facts before returning an external credential.
        self.verify(&token).await?;
        Ok(token)
    }

    pub(crate) async fn verify(&self, token: &str) -> Result<VerifiedDelivery, Error> {
        let claims = self.identity.signer.verify_delivery(token)?;
        let binding = self
            .inbox
            .delivery_binding(&claims.batch_id, &claims.digest, claims.wire_bytes)
            .await
            .map_err(ingestion_error)?;
        let grant = self
            .identity
            .store
            .privacy_get_grant(&claims.grant_id)
            .await?
            .ok_or(Error::Unauthorized)?;
        if claims.tenant != binding.tenant
            || grant.scope != binding_scope(&binding)?
            || grant.workspace != binding.workspace
            || grant.user != binding.user_id
            || grant.generation != binding.binding_generation
            || claims.exp > grant.expires_at
        {
            return Err(Error::Unauthorized);
        }
        self.identity
            .current_binding(&claims.grant_id, &grant)
            .await?;
        let admission = ExistingServiceAdmission::new(
            grant.scope.clone(),
            grant.generation,
            ServiceSelector {
                protocol: Protocol::Http,
                port: Some(binding.service_port),
            },
        )
        .map_err(|_| Error::Unauthorized)?;
        Ok(VerifiedDelivery {
            claims,
            binding,
            admission,
        })
    }
}

#[async_trait::async_trait]
impl DeliveryAuthorization for DeliveryIdentity {
    async fn admission(
        &self,
        batch: &DeliveryBatch,
    ) -> Result<ExistingServiceAdmission, DeliveryFailure> {
        let token = self.issue(batch).await.map_err(|error| match error {
            Error::Unavailable => DeliveryFailure::Unknown,
            _ => DeliveryFailure::Rejected,
        })?;
        self.verify(&token)
            .await
            .map(|verified| verified.admission)
            .map_err(|error| match error {
                Error::Unavailable => DeliveryFailure::Unknown,
                _ => DeliveryFailure::Rejected,
            })
    }
    async fn headers(&self, batch: &DeliveryBatch) -> Result<http::HeaderMap, DeliveryFailure> {
        let token = self.issue(batch).await.map_err(|error| match error {
            Error::Unavailable => DeliveryFailure::Unknown,
            _ => DeliveryFailure::Rejected,
        })?;
        let mut value: http::HeaderValue = format!("Bearer {}", token.as_str())
            .parse()
            .map_err(|_| DeliveryFailure::Rejected)?;
        value.set_sensitive(true);
        Ok(http::HeaderMap::from_iter([(
            http::header::AUTHORIZATION,
            value,
        )]))
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
            bootstrap_until: None,
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
            bootstrap_until: None,
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
