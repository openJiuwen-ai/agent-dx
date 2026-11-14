//! Fixed Gateway-to-M44 owner requests using the existing identity signer and owners.
use super::application_ws::{Application, ApplicationWsConfig};
use super::foreground_admission::ForegroundIdentity;
use super::privacy_center::BindingReader;
use super::privacy_grants::{now, PrivacyIdentity, TokenSigner};
use crate::ingress::accounts::{self, BusinessAuth, Principal, SessionGuard};
use crate::ingress::agent_service::ServiceSelector;
use adx_agent_core::{AgentBinding, BindingPhase, Protocol, Scope};
use http::{header, HeaderMap, HeaderValue};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::sync::Arc;
use zeroize::Zeroizing;

pub const VERIFY_PATH: &str = "/v1/connector/authorization-owner";
pub const OWNER_PATH: &str = "/internal/digitalmate/connectors/v1/authorization-owner";
pub(super) const PREFIX: &str = "dm-authorization-owner-v1";
const ACCESS_SECONDS: i64 = 30;
const MAX_BODY: usize = 16_384;

pub(crate) struct OwnerRuntimeConfig {
    pub application: ApplicationWsConfig,
    pub auth: Arc<dyn BusinessAuth>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum OwnerOperation {
    Current,
    Effect,
    Claim,
    Record,
    Cancel,
    Connection,
    Metadata,
    Intent,
    /// Query the real M44 task/consent owner; this is not a provider mail read.
    Read,
}

impl OwnerOperation {
    fn workload_allowed(self) -> bool {
        matches!(
            self,
            Self::Connection | Self::Metadata | Self::Intent | Self::Read
        )
    }
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub(super) enum Actor {
    Foreground {
        token_digest: String,
        expires_at: i64,
    },
    ConnectorWorkload {
        grant_id: String,
    },
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(super) struct OwnerClaims {
    pub scope: Scope,
    pub user_id: String,
    pub workspace: String,
    pub binding_generation: String,
    pub operation: OwnerOperation,
    pub body_sha256: String,
    pub audience: String,
    pub method: String,
    pub expires_at: i64,
    pub actor: Actor,
}

fn bounded(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

pub(super) fn digest_valid(value: &str) -> bool {
    value.len() == 64
        && value
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}

impl OwnerClaims {
    pub(super) fn validate(&self, tenant: &str) -> accounts::Result<()> {
        let current = now()?;
        if self.scope.validate().is_err()
            || self.scope.tenant != tenant
            || !bounded(&self.user_id)
            || !bounded(&self.workspace)
            || !bounded(&self.binding_generation)
            || !digest_valid(&self.body_sha256)
            || self.audience != OWNER_PATH
            || self.method != "POST"
            || self.expires_at <= current
            || self.expires_at > current.saturating_add(ACCESS_SECONDS + 5)
        {
            return Err(accounts::Error::Unauthorized);
        }
        match &self.actor {
            Actor::Foreground {
                token_digest,
                expires_at,
            } => {
                if !bounded(token_digest) || self.expires_at > *expires_at {
                    return Err(accounts::Error::Unauthorized);
                }
            }
            Actor::ConnectorWorkload { grant_id } => {
                if !digest_valid(grant_id) {
                    return Err(accounts::Error::Unauthorized);
                }
                if !self.operation.workload_allowed() {
                    return Err(accounts::Error::Forbidden);
                }
            }
        }
        Ok(())
    }
}

/// Identity-owned operation context. Business callers can only consume headers;
/// freshness checks preserve the original actor, operation and logical authority.
pub struct OwnerAuthorization {
    headers: HeaderMap,
    claims: OwnerClaims,
    workload: Option<Arc<PrivacyIdentity>>,
    refresh_until: i64,
}
impl OwnerAuthorization {
    pub(super) fn scope(&self) -> &Scope {
        &self.claims.scope
    }
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
}

/// Existing connector-purpose grant, not a phone session or a mail permission.
/// Real connection intent/capability checks are still the M44 owner's job.
pub struct ConnectorWorkloadIdentity {
    identity: Arc<PrivacyIdentity>,
    token: Zeroizing<String>,
    scope: Scope,
    user_id: String,
    workspace: String,
    generation: String,
    grant_id: String,
    expires_at: i64,
}
impl ConnectorWorkloadIdentity {
    pub(super) async fn authenticate(
        identity: Arc<PrivacyIdentity>,
        headers: &HeaderMap,
    ) -> accounts::Result<Self> {
        let token = Zeroizing::new(bearer(headers)?.to_owned());
        let (grant, grant_id, expires_at) = identity.owner_connector_grant(&token).await?;
        Ok(Self {
            identity,
            token,
            scope: grant.scope,
            user_id: grant.user,
            workspace: grant.workspace,
            generation: grant.generation,
            grant_id,
            expires_at,
        })
    }
    pub fn tenant(&self) -> &str {
        &self.scope.tenant
    }
    pub fn user_id(&self) -> &str {
        &self.user_id
    }
    pub fn workspace(&self) -> &str {
        &self.workspace
    }
    pub fn binding_scope(&self) -> &Scope {
        &self.scope
    }
    pub async fn check(&self) -> accounts::Result<()> {
        let (grant, id, _) = self.identity.owner_connector_grant(&self.token).await?;
        if id != self.grant_id
            || grant.scope != self.scope
            || grant.user != self.user_id
            || grant.workspace != self.workspace
            || grant.generation != self.generation
        {
            return Err(accounts::Error::Unauthorized);
        }
        Ok(())
    }
}

/// Reuses the actual TokenSigner. No key, session or intent store is introduced.
pub struct OwnerAuthority {
    tenant: String,
    application: ApplicationWsConfig,
    signer: Arc<TokenSigner>,
    bindings: Arc<dyn BindingReader>,
    auth: Arc<dyn BusinessAuth>,
    identity: Option<Arc<PrivacyIdentity>>,
}
impl OwnerAuthority {
    pub(super) fn new(
        tenant: String,
        application: ApplicationWsConfig,
        signer: Arc<TokenSigner>,
        bindings: Arc<dyn BindingReader>,
        auth: Arc<dyn BusinessAuth>,
    ) -> accounts::Result<Self> {
        application
            .validate(&tenant, Application::DigitalMate)
            .map_err(|_| accounts::Error::Configuration)?;
        Ok(Self {
            tenant,
            application,
            signer,
            bindings,
            auth,
            identity: None,
        })
    }

    pub(super) fn with_identity(mut self, identity: Arc<PrivacyIdentity>) -> Self {
        self.identity = Some(identity);
        self
    }

    pub(super) fn service_selector(&self) -> ServiceSelector {
        ServiceSelector {
            protocol: Protocol::Http,
            port: Some(self.application.service_port),
        }
    }

    /// Refresh the original operation, never select a newer authority by Scope.
    pub(super) async fn refresh(
        &self,
        original: &OwnerAuthorization,
    ) -> accounts::Result<OwnerAuthorization> {
        let mut claims = original.claims.clone();
        claims.expires_at = (now()? + ACCESS_SECONDS).min(original.refresh_until);
        self.issue(claims, original.workload.clone(), original.refresh_until)
            .await
    }

    pub(super) async fn subscription_snapshot(
        &self,
        original: &OwnerAuthorization,
        facts: super::ingestion::SubscriptionFacts,
    ) -> accounts::Result<super::ingestion::SubscriptionSnapshot> {
        let current = self.refresh(original).await?;
        let claims = &current.claims;
        let Actor::ConnectorWorkload { grant_id } = &claims.actor else {
            return Err(accounts::Error::Forbidden);
        };
        let grant = self
            .identity
            .as_ref()
            .ok_or(accounts::Error::Unavailable)?
            .owner_grant(grant_id)
            .await?;
        if grant.scope != claims.scope
            || grant.user != claims.user_id
            || grant.workspace != claims.workspace
            || grant.generation != claims.binding_generation
        {
            return Err(accounts::Error::Unauthorized);
        }
        let authority_ref = grant_id.clone();
        Ok(super::ingestion::SubscriptionSnapshot {
            binding_scope: claims.scope.clone(),
            user_id: claims.user_id.clone(),
            workspace: claims.workspace.clone(),
            authority_ref,
            connection_id: facts.connection_id,
            provider: facts.provider,
            trigger_slug: facts.trigger_slug,
            account_id: facts.account_id,
            auth_config_id: facts.auth_config_id,
            composio_user_id: facts.composio_user_id,
            connection_generation: facts.connection_generation,
            record_revision: facts.record_revision,
            intent_revision: facts.intent_revision,
            enabled: facts.enabled,
            usable: facts.usable,
        })
    }

    async fn binding(
        &self,
        scope: &Scope,
        user: &str,
        workspace: &str,
    ) -> accounts::Result<AgentBinding> {
        let expected = self
            .application
            .scope(&self.tenant, user, Application::DigitalMate)
            .map_err(|_| accounts::Error::Unauthorized)?;
        if scope != &expected {
            return Err(accounts::Error::Unauthorized);
        }
        let binding = tokio::time::timeout(accounts::IO_TIMEOUT, self.bindings.binding(scope))
            .await
            .map_err(|_| accounts::Error::Unavailable)??;
        if binding.scope != *scope
            || binding.phase != BindingPhase::Active
            || binding.parameters.get("workspace").map(String::as_str) != Some(workspace)
            || !bounded(&binding.generation)
        {
            return Err(accounts::Error::Unauthorized);
        }
        Ok(binding)
    }

    pub(super) async fn delivery_target(
        &self,
        delivery: &super::ingestion::SubscriptionBinding,
    ) -> accounts::Result<serde_json::Value> {
        delivery
            .validate()
            .map_err(|_| accounts::Error::Unauthorized)?;
        let identity = self.identity.as_ref().ok_or(accounts::Error::Unavailable)?;
        let grant = identity.owner_grant(&delivery.authority_ref).await?;
        if !delivery.enabled
            || grant.scope != super::privacy_grants::binding_scope(delivery)?
            || grant.user != delivery.user_id
            || grant.workspace != delivery.workspace
        {
            return Err(accounts::Error::Unauthorized);
        }
        let binding = self
            .binding(&grant.scope, &grant.user, &grant.workspace)
            .await?;
        Ok(serde_json::json!({
            "tenant":binding.scope.tenant, "user_id":delivery.user_id,
            "workspace":delivery.workspace, "template":binding.scope.template,
            "template_ref":binding.scope.template_ref.name(), "binding_id":binding.scope.binding_id,
            "binding_generation":binding.generation,
        }))
    }

    fn claims(
        &self,
        binding: AgentBinding,
        identity: (&str, &str),
        operation: OwnerOperation,
        body: &[u8],
        actor: Actor,
        expires_at: i64,
    ) -> accounts::Result<OwnerClaims> {
        if body.is_empty() || body.len() > MAX_BODY {
            return Err(accounts::Error::Invalid);
        }
        Ok(OwnerClaims {
            scope: binding.scope,
            user_id: identity.0.into(),
            workspace: identity.1.into(),
            binding_generation: binding.generation,
            operation,
            body_sha256: hex::encode(Sha256::digest(body)),
            audience: OWNER_PATH.into(),
            method: "POST".into(),
            expires_at,
            actor,
        })
    }

    /// Rechecks the original real session and current Binding before issuing one
    /// operation/body credential. Raw body bytes are hashed without JSON rewriting.
    pub async fn foreground_headers(
        &self,
        identity: &ForegroundIdentity,
        operation: OwnerOperation,
        raw_body: &[u8],
    ) -> accounts::Result<OwnerAuthorization> {
        identity.check().await?;
        let binding = self
            .binding(
                identity.binding_scope(),
                identity.user_id(),
                identity.workspace(),
            )
            .await?;
        let principal = identity.principal();
        let claims = self.claims(
            binding,
            (identity.user_id(), identity.workspace()),
            operation,
            raw_body,
            Actor::Foreground {
                token_digest: principal.token_digest.clone(),
                expires_at: principal.expires_at,
            },
            (now()? + ACCESS_SECONDS).min(principal.expires_at),
        )?;
        self.issue(claims, None, principal.expires_at).await
    }

    /// Workload identity authorizes only same-workspace owner queries, never
    /// OAuth effects or provider mail reads. Read queries require actual M44
    /// task/consent proof; background reads require actual enabled intent. The
    /// exact connection generation/revision/capability remain caller checks.
    pub async fn workload_headers(
        &self,
        identity: &ConnectorWorkloadIdentity,
        operation: OwnerOperation,
        raw_body: &[u8],
    ) -> accounts::Result<OwnerAuthorization> {
        if !operation.workload_allowed() {
            return Err(accounts::Error::Forbidden);
        }
        identity.check().await?;
        let binding = self
            .binding(
                identity.binding_scope(),
                identity.user_id(),
                identity.workspace(),
            )
            .await?;
        let claims = self.claims(
            binding,
            (identity.user_id(), identity.workspace()),
            operation,
            raw_body,
            Actor::ConnectorWorkload {
                grant_id: identity.grant_id.clone(),
            },
            (now()? + ACCESS_SECONDS).min(identity.expires_at),
        )?;
        self.issue(claims, Some(identity.identity.clone()), identity.expires_at)
            .await
    }

    async fn issue(
        &self,
        claims: OwnerClaims,
        workload: Option<Arc<PrivacyIdentity>>,
        refresh_until: i64,
    ) -> accounts::Result<OwnerAuthorization> {
        claims.validate(&self.tenant)?;
        let token = self.signer.sign_owner(&claims)?;
        self.verify(
            &token,
            claims.operation,
            &claims.body_sha256,
            workload.as_deref(),
        )
        .await?;
        let mut header = HeaderValue::from_str(&format!("Bearer {}", token.as_str()))
            .map_err(|_| accounts::Error::Unavailable)?;
        header.set_sensitive(true);
        Ok(OwnerAuthorization {
            headers: HeaderMap::from_iter([(header::AUTHORIZATION, header)]),
            workload,
            refresh_until,
            claims,
        })
    }

    pub(super) async fn verify(
        &self,
        token: &str,
        operation: OwnerOperation,
        body_sha256: &str,
        workload: Option<&PrivacyIdentity>,
    ) -> accounts::Result<serde_json::Value> {
        let claims = self.signer.verify_owner(token)?;
        claims.validate(&self.tenant)?;
        if claims.operation != operation || claims.body_sha256 != body_sha256 {
            return Err(accounts::Error::Unauthorized);
        }
        let actor = self.check_actor(&claims, workload).await?;
        let binding = self
            .binding(&claims.scope, &claims.user_id, &claims.workspace)
            .await?;
        if binding.generation != claims.binding_generation {
            return Err(accounts::Error::Unauthorized);
        }
        self.check_actor(&claims, workload).await?;
        // Recheck expiry after owner I/O; a slow lookup cannot extend a credential.
        claims.validate(&self.tenant)?;
        Ok(
            serde_json::json!({ "purpose":"connector.authorization-owner", "actor":actor,
            "tenant":claims.scope.tenant, "user_id":claims.user_id, "workspace":claims.workspace,
            "template":claims.scope.template, "template_ref":claims.scope.template_ref.name(),
            "binding_id":claims.scope.binding_id, "binding_generation":claims.binding_generation,
            "operation":claims.operation, "body_sha256":claims.body_sha256, "expires_at":claims.expires_at,
            "method":"POST", "path":OWNER_PATH }),
        )
    }

    async fn check_actor(
        &self,
        claims: &OwnerClaims,
        workload: Option<&PrivacyIdentity>,
    ) -> accounts::Result<&'static str> {
        Ok(match &claims.actor {
            Actor::Foreground {
                token_digest,
                expires_at,
            } => {
                SessionGuard {
                    auth: self.auth.clone(),
                    principal: Principal {
                        user_id: claims.user_id.clone(),
                        token_digest: token_digest.clone(),
                        expires_at: *expires_at,
                    },
                }
                .check()
                .await?;
                "foreground"
            }
            Actor::ConnectorWorkload { grant_id } => {
                let grant = workload
                    .ok_or(accounts::Error::Unavailable)?
                    .owner_grant(grant_id)
                    .await?;
                if grant.scope != claims.scope
                    || grant.user != claims.user_id
                    || grant.workspace != claims.workspace
                    || grant.generation != claims.binding_generation
                    || grant.expires_at < claims.expires_at
                {
                    return Err(accounts::Error::Unauthorized);
                }
                "connector_workload"
            }
        })
    }
}

pub(super) fn bearer(headers: &HeaderMap) -> accounts::Result<&str> {
    let mut values = headers.get_all(header::AUTHORIZATION).iter();
    let value = values.next().ok_or(accounts::Error::Unauthorized)?;
    if values.next().is_some() || headers.contains_key("x-auth") {
        return Err(accounts::Error::Unauthorized);
    }
    let value = value.to_str().map_err(|_| accounts::Error::Unauthorized)?;
    let (scheme, token) = value.split_once(' ').ok_or(accounts::Error::Unauthorized)?;
    if !scheme.eq_ignore_ascii_case("Bearer")
        || token.is_empty()
        || token.len() > 2048
        || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(accounts::Error::Unauthorized);
    }
    Ok(token)
}
