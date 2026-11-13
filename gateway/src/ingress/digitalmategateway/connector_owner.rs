//! Fixed Gateway-to-M44 owner requests using the existing identity signer and owners.
use super::application_ws::{Application, ApplicationWsConfig};
use super::foreground_admission::ForegroundIdentity;
use super::privacy_center::BindingReader;
use super::privacy_grants::{now, PrivacyIdentity, TokenSigner};
use crate::ingress::accounts::{self, BusinessAuth, Principal, SessionGuard};
use adx_agent_core::{AgentBinding, BindingPhase, Scope};
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
}

impl OwnerOperation {
    fn workload_allowed(self) -> bool {
        matches!(self, Self::Connection | Self::Metadata | Self::Intent)
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
    pub environment_id: String,
    pub service_port: u16,
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
            || !bounded(&self.environment_id)
            || self.service_port == 0
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

/// Routing and headers are one snapshot. Send only to this exact admitted Binding
/// generation, Environment, port and OWNER_PATH; never replay a sent owner write.
pub struct OwnerAuthorization {
    headers: HeaderMap,
    claims: OwnerClaims,
}
impl OwnerAuthorization {
    pub fn headers(&self) -> &HeaderMap {
        &self.headers
    }
    pub fn binding_scope(&self) -> &Scope {
        &self.claims.scope
    }
    pub fn generation(&self) -> &str {
        &self.claims.binding_generation
    }
    pub fn environment_id(&self) -> &str {
        &self.claims.environment_id
    }
    pub fn service_port(&self) -> u16 {
        self.claims.service_port
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
            || !bounded(&binding.current.environment_id)
        {
            return Err(accounts::Error::Unauthorized);
        }
        Ok(binding)
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
            environment_id: binding.current.environment_id,
            service_port: self.application.service_port,
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
        self.issue(claims, None).await
    }

    /// Workload identity authorizes only same-workspace owner queries, never
    /// OAuth effects or provider mail reads. The real enabled intent and exact
    /// connection generation/revision/capability remain mandatory at that caller.
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
        self.issue(claims, Some(&identity.identity)).await
    }

    async fn issue(
        &self,
        claims: OwnerClaims,
        workload: Option<&PrivacyIdentity>,
    ) -> accounts::Result<OwnerAuthorization> {
        claims.validate(&self.tenant)?;
        let token = self.signer.sign_owner(&claims)?;
        self.verify(&token, claims.operation, &claims.body_sha256, workload)
            .await?;
        let mut header = HeaderValue::from_str(&format!("Bearer {}", token.as_str()))
            .map_err(|_| accounts::Error::Unavailable)?;
        header.set_sensitive(true);
        Ok(OwnerAuthorization {
            headers: HeaderMap::from_iter([(header::AUTHORIZATION, header)]),
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
        if binding.generation != claims.binding_generation
            || binding.current.environment_id != claims.environment_id
            || claims.service_port != self.application.service_port
        {
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
            "environment_id":claims.environment_id, "service_port":claims.service_port,
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
