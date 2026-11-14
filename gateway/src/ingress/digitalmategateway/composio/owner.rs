//! Actual M44 public operations through the existing fixed-generation ADX Relay.
use super::super::connector_owner::{
    OwnerAuthority, OwnerAuthorization, OwnerOperation, OWNER_PATH,
};
use super::super::fixed_service::FixedServiceAccess;
use super::{
    Attempt, AttemptPhase, Authority, ConnectionGrant, ConnectionMetadata, Error, MailAuthConfigs,
    MailProvider, Permissions, Scope, Started, VerifiedAccount,
};
use crate::ingress::server::Ingress;
use adx_agent_api::{managed::ManagedService, request::RequestContext};
use async_trait::async_trait;
use bytes::Bytes;
use http::{Method, Request, StatusCode};
use http_body_util::{BodyExt, Full, Limited};
use hyper_util::rt::TokioIo;
use serde::Deserialize;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

pub(super) struct M44Authority {
    issuer: Arc<OwnerAuthority>,
    access: FixedServiceAccess,
    ingress: Arc<Ingress>,
    configs: MailAuthConfigs,
}

struct ConnectionTask(tokio::task::JoinHandle<()>);
impl Drop for ConnectionTask {
    fn drop(&mut self) {
        self.0.abort();
    }
}

impl M44Authority {
    pub(super) fn new(
        issuer: Arc<OwnerAuthority>,
        managed: Arc<ManagedService>,
        ingress: Arc<Ingress>,
        configs: MailAuthConfigs,
    ) -> Self {
        Self {
            issuer,
            access: FixedServiceAccess::new(managed),
            ingress,
            configs,
        }
    }

    pub(super) async fn authorization(
        &self,
        scope: &Scope,
        operation: OwnerOperation,
        raw: &[u8],
    ) -> Result<OwnerAuthorization, Error> {
        if let Some(identity) = scope.workload_identity() {
            self.issuer
                .workload_headers(identity, operation, raw)
                .await
                .map_err(identity_error)
        } else {
            self.issuer
                .foreground_headers(scope.foreground_identity()?, operation, raw)
                .await
                .map_err(identity_error)
        }
    }

    pub(super) async fn send(
        &self,
        scope: &Scope,
        operation: OwnerOperation,
        body: Value,
    ) -> Result<Value, Error> {
        scope.check_bound_identity().await?;
        let raw = serde_json::to_vec(&body).map_err(|_| Error::InvalidRequest)?;
        if raw.len() > 16_384 {
            return Err(Error::InvalidRequest);
        }
        let admitted = self.authorization(scope, operation, &raw).await?;
        let context = Arc::new(RequestContext::new(Duration::from_secs(30)));
        let selection = self
            .access
            .select(
                context.clone(),
                admitted.binding_scope().clone(),
                admitted.service_port(),
                admitted.generation(),
            )
            .await
            .map_err(|_| Error::UpstreamUnavailable)?;
        if !selection.matches_target(admitted.environment_id(), admitted.service_port()) {
            return Err(Error::Forbidden);
        }
        let stream = self
            .access
            .connect(&self.ingress, &selection)
            .await
            .map_err(|_| Error::UpstreamUnavailable)?;
        self.access
            .revalidate(&selection)
            .await
            .map_err(|_| Error::Forbidden)?;
        let (mut sender, connection) = hyper::client::conn::http1::handshake(TokioIo::new(stream))
            .await
            .map_err(|_| Error::UpstreamUnavailable)?;
        let _connection = ConnectionTask(tokio::spawn(async move {
            let _ = connection.await;
        }));
        // Readiness can outlast the original short credential. Reissue only for
        // the exact same target and raw bytes; never reselect a rebuilt Binding.
        let current = self.authorization(scope, operation, &raw).await?;
        if current.binding_scope() != admitted.binding_scope()
            || current.generation() != admitted.generation()
            || current.environment_id() != admitted.environment_id()
            || current.service_port() != admitted.service_port()
        {
            return Err(Error::Forbidden);
        }
        let mut request = Request::builder()
            .method(Method::POST)
            .uri(OWNER_PATH)
            .header("host", "digitalmate-owner")
            .header("content-type", "application/json")
            .body(Full::new(Bytes::from(raw)))
            .map_err(|_| Error::InvalidRequest)?;
        for (name, value) in current.headers() {
            request.headers_mut().insert(name.clone(), value.clone());
        }
        let write = matches!(
            operation,
            OwnerOperation::Claim | OwnerOperation::Record | OwnerOperation::Cancel
        );
        let unknown = || {
            if write {
                Error::OutcomeUnknown
            } else {
                Error::UpstreamUnavailable
            }
        };
        // Exactly one physical send. A sent write is never retried by transport.
        let response = tokio::time::timeout_at(context.deadline(), sender.send_request(request))
            .await
            .map_err(|_| unknown())?
            .map_err(|_| unknown())?;
        let status = response.status();
        let bytes = tokio::time::timeout_at(
            context.deadline(),
            Limited::new(response.into_body(), 16_384).collect(),
        )
        .await
        .map_err(|_| unknown())?
        .map_err(|_| unknown())?
        .to_bytes();
        scope.check_bound_identity().await?;
        let value: Value = serde_json::from_slice(&bytes).map_err(|_| unknown())?;
        if status != StatusCode::OK {
            return Err(match value.get("error").and_then(Value::as_str) {
                Some("authorization_inactive") => Error::AuthorizationInactive,
                Some("outcome_unknown" | "conflict") => Error::OutcomeUnknown,
                _ if status == StatusCode::FORBIDDEN || status == StatusCode::UNAUTHORIZED => {
                    Error::Forbidden
                }
                _ if status == StatusCode::BAD_REQUEST => Error::InvalidRequest,
                _ => unknown(),
            });
        }
        if !value.is_object() {
            return Err(unknown());
        }
        Ok(value)
    }

    fn digest(&self, scope: &Scope, attempt: &Attempt) -> Result<String, Error> {
        let config = match attempt.provider {
            MailProvider::Google => &self.configs.gmail,
            MailProvider::Microsoft => &self.configs.outlook,
        }
        .as_ref()
        .ok_or(Error::NotConfigured)?;
        super::types::link_digest(scope, attempt, &config.id)
    }

    async fn effect(&self, scope: &Scope, attempt: &Attempt) -> Result<Option<Effect>, Error> {
        let value = self
            .send(
                scope,
                OwnerOperation::Effect,
                json!({"operation":"effect", "registration":attempt.registration, "kind":"link"}),
            )
            .await?;
        let effect: EffectResult = decode(value)?;
        if let Some(effect) = &effect.effect {
            if effect.registration != attempt.registration
                || effect.kind != "link"
                || effect.request_digest != self.digest(scope, attempt)?
            {
                return Err(Error::AuthorizationInactive);
            }
        }
        Ok(effect.effect)
    }

    async fn record(
        &self,
        scope: &Scope,
        attempt: &Attempt,
        account_id: &str,
        subject_id: &str,
        email: &str,
        outcome: &str,
    ) -> Result<(), Error> {
        let value = self.send(scope, OwnerOperation::Record, json!({"operation":"record", "registration":attempt.registration, "revision":attempt.revision,
            "kind":"link", "request_digest":self.digest(scope, attempt)?, "outcome":outcome, "account_id":account_id, "subject_id":subject_id, "email":email})).await?;
        let effect: Effect = decode(value)?;
        if effect.registration != attempt.registration
            || effect.kind != "link"
            || effect.request_digest != self.digest(scope, attempt)?
            || effect.account_id != account_id
            || effect.outcome != outcome
            || effect.subject_id != subject_id
            || effect.email != email
        {
            return Err(Error::OutcomeUnknown);
        }
        Ok(())
    }
}

fn identity_error(error: crate::ingress::accounts::Error) -> Error {
    match error {
        crate::ingress::accounts::Error::Unauthorized
        | crate::ingress::accounts::Error::Forbidden => Error::Forbidden,
        _ => Error::UpstreamUnavailable,
    }
}
fn decode<T: serde::de::DeserializeOwned>(value: Value) -> Result<T, Error> {
    serde_json::from_value(value).map_err(|_| Error::InvalidResponse)
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Current {
    registration: String,
    revision: u64,
    provider: String,
    backend: String,
    method: String,
    connection_id: String,
    deadline_epoch_seconds: u64,
    phase: String,
    account_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Effect {
    registration: String,
    kind: String,
    request_digest: String,
    outcome: String,
    account_id: String,
    subject_id: String,
    email: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct EffectResult {
    effect: Option<Effect>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Snapshot {
    connection_id: String,
    provider: String,
    backend: String,
    credential_custody: String,
    external_account_id: String,
    external_subject_id: String,
    authorization_generation: u64,
    state: String,
    usable: bool,
    granted_capabilities: Vec<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct MetadataResult {
    metadata: ConnectionMetadata,
}
fn provider(value: &str) -> Result<MailProvider, Error> {
    match value {
        "google" => Ok(MailProvider::Google),
        "microsoft" => Ok(MailProvider::Microsoft),
        _ => Err(Error::Forbidden),
    }
}

pub(super) fn read_proof(
    value: Value,
    scope: &Scope,
    connection_id: &str,
    grant: &ConnectionGrant,
    now: u64,
) -> Result<bool, Error> {
    #[derive(Deserialize)]
    #[serde(deny_unknown_fields)]
    struct Proof {
        purpose: String,
        connection_id: String,
        request_digest: String,
        access: super::types::ReadAccess,
        allowed: bool,
        read_body: bool,
        expires_at: u64,
    }
    let (access, digest) = scope.read_access(grant)?;
    let proof: Proof = decode(value)?;
    if proof.purpose != "connector.mail.read"
        || proof.connection_id != connection_id
        || proof.request_digest != digest
        || &proof.access != access
        || !proof.allowed
        || proof.expires_at <= now
        || proof.expires_at > now.saturating_add(15)
        || (proof.read_body && matches!(access, super::types::ReadAccess::Subscription { .. }))
    {
        return Err(Error::Forbidden);
    }
    Ok(proof.read_body)
}

#[async_trait]
impl Authority for M44Authority {
    async fn authorize_read(
        &self,
        scope: &Scope,
        connection_id: &str,
        grant: &ConnectionGrant,
    ) -> Result<bool, Error> {
        let (access, digest) = scope.read_access(grant)?;
        let proof = self.send(scope, OwnerOperation::Read, json!({
            "operation":"read", "connection_id":connection_id, "request_digest":digest, "access":access
        })).await?;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_err(|_| Error::Forbidden)?
            .as_secs();
        read_proof(proof, scope, connection_id, grant, now)
    }
    async fn metadata(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<ConnectionMetadata, Error> {
        // Pure owner snapshots, including an account waiting for its first live
        // validation. Neither operation invokes ensure_ready or implies consent.
        let snapshot: Snapshot = decode(
            self.send(
                scope,
                OwnerOperation::Connection,
                json!({"operation":"connection", "connection_id":connection_id}),
            )
            .await?,
        )?;
        if snapshot.connection_id != connection_id
            || snapshot.backend != "composio"
            || snapshot.credential_custody != "external"
            || !matches!(snapshot.state.as_str(), "current" | "unavailable")
        {
            return Err(Error::Forbidden);
        }
        let metadata: MetadataResult = decode(
            self.send(
                scope,
                OwnerOperation::Metadata,
                json!({"operation":"metadata", "connection_id":connection_id}),
            )
            .await?,
        )?;
        let metadata = metadata.metadata;
        metadata.check(connection_id)?;
        if metadata.provider != snapshot.provider
            || metadata.backend != snapshot.backend
            || metadata.external_account_id != snapshot.external_account_id
            || metadata.external_subject_id != snapshot.external_subject_id
            || metadata.authorization_generation != snapshot.authorization_generation
        {
            return Err(Error::Forbidden);
        }
        Ok(metadata)
    }

    async fn attempt(&self, scope: &Scope, registration: &str) -> Result<Attempt, Error> {
        let current: Current = decode(
            self.send(
                scope,
                OwnerOperation::Current,
                json!({"operation":"current", "registration":registration}),
            )
            .await?,
        )?;
        if current.registration != registration
            || current.backend != "composio"
            || current.method != "connect"
            || current.revision == 0
            || !super::types::bounded(&current.connection_id, 256)
        {
            return Err(Error::AuthorizationInactive);
        }
        let phase = match current.phase.as_str() {
            "ready" => AttemptPhase::Ready,
            "starting" => AttemptPhase::Starting,
            "waiting" => AttemptPhase::Waiting,
            "complete" => AttemptPhase::Complete,
            _ => return Err(Error::AuthorizationInactive),
        };
        Ok(Attempt {
            provider: provider(&current.provider)?,
            registration: current.registration,
            revision: current.revision,
            deadline_epoch_seconds: current.deadline_epoch_seconds,
            phase,
            account_id: current.account_id,
            expected_email: None,
        })
    }
    async fn cancel_attempt(&self, scope: &Scope, registration: &str) -> Result<(), Error> {
        let value = self
            .send(
                scope,
                OwnerOperation::Cancel,
                json!({"operation":"cancel", "registration":registration}),
            )
            .await?;
        if value != json!({"cancelled":true}) {
            return Err(Error::OutcomeUnknown);
        }
        Ok(())
    }
    async fn verified_receipt(
        &self,
        scope: &Scope,
        attempt: &Attempt,
    ) -> Result<Option<VerifiedAccount>, Error> {
        let current = self.attempt(scope, &attempt.registration).await?;
        if current.phase != AttemptPhase::Complete
            || current.revision != attempt.revision
            || current.provider != attempt.provider
            || current.account_id != attempt.account_id
        {
            return Err(Error::AuthorizationInactive);
        }
        let effect = self.effect(scope, attempt).await?;
        Ok(effect
            .filter(|effect| {
                effect.outcome == "verified" && effect.account_id == attempt.account_id
            })
            .map(|effect| VerifiedAccount {
                provider: attempt.provider,
                subject_id: effect.subject_id,
                account_id: effect.account_id,
                email: effect.email,
            }))
    }
    async fn claim_start(&self, scope: &Scope, attempt: &Attempt) -> Result<(), Error> {
        let value = self.send(scope, OwnerOperation::Claim, json!({"operation":"claim", "registration":attempt.registration, "revision":attempt.revision, "kind":"link", "request_digest":self.digest(scope, attempt)?})).await?;
        if value != json!({"claimed":true}) {
            return Err(Error::OutcomeUnknown);
        }
        Ok(())
    }
    async fn claim_confirmation(
        &self,
        scope: &Scope,
        attempt: &Attempt,
        digest: &str,
    ) -> Result<(), Error> {
        let value = self.send(scope, OwnerOperation::Claim, json!({"operation":"claim", "registration":attempt.registration, "revision":attempt.revision, "kind":"confirmation", "request_digest":digest})).await?;
        if value != json!({"claimed":true}) {
            return Err(Error::OutcomeUnknown);
        }
        Ok(())
    }
    async fn bind_started(
        &self,
        scope: &Scope,
        attempt: &Attempt,
        started: &Started,
    ) -> Result<(), Error> {
        self.record(scope, attempt, &started.account_id, "", "", "started")
            .await
    }
    async fn accept_verified(
        &self,
        scope: &Scope,
        attempt: &Attempt,
        account: &VerifiedAccount,
    ) -> Result<(), Error> {
        if account.provider != attempt.provider || account.account_id != attempt.account_id {
            return Err(Error::AccountMismatch);
        }
        self.record(
            scope,
            attempt,
            &account.account_id,
            &account.subject_id,
            &account.email,
            "verified",
        )
        .await
    }
    async fn connection(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<ConnectionGrant, Error> {
        let snapshot: Snapshot = decode(
            self.send(
                scope,
                OwnerOperation::Connection,
                json!({"operation":"connection", "connection_id":connection_id}),
            )
            .await?,
        )?;
        if snapshot.connection_id != connection_id
            || snapshot.backend != "composio"
            || snapshot.credential_custody != "external"
            || !snapshot.usable
            || snapshot.state != "current"
            || snapshot.authorization_generation == 0
        {
            return Err(Error::Forbidden);
        }
        let metadata: MetadataResult = decode(
            self.send(
                scope,
                OwnerOperation::Metadata,
                json!({"operation":"metadata", "connection_id":connection_id}),
            )
            .await?,
        )?;
        let metadata = metadata.metadata;
        if metadata.connection_id != snapshot.connection_id
            || metadata.provider != snapshot.provider
            || metadata.backend != snapshot.backend
            || metadata.external_account_id != snapshot.external_account_id
            || metadata.external_subject_id != snapshot.external_subject_id
            || metadata.authorization_generation != snapshot.authorization_generation
            || metadata.record_revision == 0
            || !metadata.created_at.is_finite()
            || metadata.created_at <= 0.0
        {
            return Err(Error::Forbidden);
        }
        let provider = provider(&snapshot.provider)?;
        let headers = snapshot
            .granted_capabilities
            .iter()
            .any(|value| value == "read_history");
        Ok(ConnectionGrant {
            provider,
            subject_id: snapshot.external_subject_id,
            account_id: snapshot.external_account_id,
            email: if provider == MailProvider::Google {
                metadata.identity_user_id
            } else {
                String::new()
            },
            generation: snapshot.authorization_generation,
            record_revision: metadata.record_revision,
            permissions: Permissions {
                search: headers,
                read_headers: headers,
                read_body: false,
            },
        })
    }
    async fn assert_connection(
        &self,
        scope: &Scope,
        connection_id: &str,
        expected: &ConnectionGrant,
    ) -> Result<(), Error> {
        let current = self.connection(scope, connection_id).await?;
        if current.provider != expected.provider
            || current.subject_id != expected.subject_id
            || current.account_id != expected.account_id
            || current.email != expected.email
            || current.generation != expected.generation
            || current.record_revision != expected.record_revision
            || current.permissions != expected.permissions
        {
            return Err(Error::Forbidden);
        }
        Ok(())
    }
    async fn fence_connection(&self, _: &Scope, _: &str) -> Result<ConnectionGrant, Error> {
        // The frozen owner contract has no remove/cleanup-receipt operation.
        // Never imitate that writer or revoke first without its local fence.
        Err(Error::NotConfigured)
    }
}
