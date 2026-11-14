//! Restricted Composio authorization and mail reads outside user sandboxes.
//!
//! The fixed entry can retain actual foreground sessions and call M44 through
//! its purpose issuer and existing ADX Relay. Explicit process configuration
//! prepares the shared client and budget. Normal workloads can validate account
//! references or request mail using a fresh exact-request M44 task/intent proof.
//! Background subscriptions and delivery retain their existing real owners.
//! A crypto bearer or JSON scope grants no connector or mail permission.

mod budget;
mod client;
pub mod http;
mod outlook;
mod owner;
#[cfg(unix)]
mod startup;
#[cfg(test)]
mod subscription_tests;
mod subscriptions;
mod types;
mod webhook;

pub use budget::{PostgresBudget, RequestBudgetLimits};
#[cfg(unix)]
pub(crate) use startup::{ComposioConfig, PreparedComposio};
pub use webhook::{VerifiedWebhook, WebhookError, WebhookVerifier};

pub use types::{
    Attempt, AttemptPhase, Authority, Budget, ConnectionGrant, ConnectionMetadata, Error,
    GmailRequest, MailAuthConfig, MailAuthConfigs, MailProvider, MailRequest, OAuthAppOwnership,
    OutlookRequest, Permissions, Scope, Started, VerifiedAccount,
};

use client::Client;
use std::sync::Arc;

/// Trusted configuration only; never construct from a user request or template.
#[derive(Clone)]
pub struct Composio {
    client: Client,
    authority: Arc<dyn Authority>,
    subscriptions: Option<Arc<super::ingestion::SubscriptionCoordinator>>,
}

/// Kept inside the transport: an account capability never substitutes for the
/// actual task/subscription owner after a budget wait or a late provider reply.
pub(super) struct MailReadGuard<'a> {
    gateway: &'a Composio,
    scope: &'a Scope,
    connection_id: &'a str,
    grant: &'a ConnectionGrant,
    body: bool,
}

impl MailReadGuard<'_> {
    pub(super) async fn check(&self) -> Result<(), Error> {
        self.scope.read_access(self.grant)?;
        self.gateway
            .authority
            .assert_connection(self.scope, self.connection_id, self.grant)
            .await?;
        let body = self
            .gateway
            .authority
            .authorize_read(self.scope, self.connection_id, self.grant)
            .await?;
        if self.body && !body {
            return Err(Error::Forbidden);
        }
        self.scope.check_bound_identity().await
    }
}

impl Composio {
    /// Fixed account/profile and one-ID read probe for the real M44 validator.
    /// Returns no mail, creates no connection/consent and does not require an
    /// already usable snapshot: the Registry owns the subsequent validation CAS.
    pub async fn validate_account(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<serde_json::Value, Error> {
        let current = self.authority.metadata(scope, connection_id).await?;
        let provider = current.check(connection_id)?;
        self.client
            .account(scope, &current.external_account_id, None, provider)
            .await?;
        let profile = self
            .client
            .profile(scope, &current.external_account_id, provider)
            .await?;
        if profile.subject_id != current.external_subject_id
            || (provider == MailProvider::Google
                && !current.identity_user_id.is_empty()
                && !current
                    .identity_user_id
                    .eq_ignore_ascii_case(&profile.email))
        {
            return Err(Error::AccountMismatch);
        }
        let request = match provider {
            MailProvider::Google => MailRequest::Gmail(GmailRequest::Search {
                query: String::new(),
                limit: 1,
                page: String::new(),
            }),
            MailProvider::Microsoft => MailRequest::Outlook(OutlookRequest::Search {
                query: String::new(),
                sender: String::new(),
                since: String::new(),
                until: String::new(),
                limit: 1,
                page: String::new(),
            }),
        };
        // A fixed minimal permission probe, never a caller-selected mail request.
        // Its IDs are discarded and cannot be exposed by this account endpoint.
        let probe = ConnectionGrant {
            provider,
            subject_id: profile.subject_id.clone(),
            account_id: current.external_account_id.clone(),
            email: profile.email.clone(),
            generation: current.authorization_generation,
            record_revision: current.record_revision,
            permissions: Permissions {
                search: true,
                read_headers: false,
                read_body: false,
            },
        };
        if self.authority.metadata(scope, connection_id).await? != current {
            return Err(Error::Forbidden);
        }
        let _ = self.client.mail(scope, &probe, request, None).await?;
        if self.authority.metadata(scope, connection_id).await? != current {
            return Err(Error::Forbidden);
        }
        Ok(serde_json::json!({
            "connection_id":current.connection_id, "provider":current.provider, "backend":"composio",
            "account_id":current.external_account_id, "subject_id":current.external_subject_id,
            "authorization_generation":current.authorization_generation, "record_revision":current.record_revision,
            "email":profile.email, "granted_capabilities":["read_history"]
        }))
    }
    async fn synchronize_subscription(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<super::ingestion::SubscriptionOutcome, Error> {
        use super::ingestion::{Error as IngestionError, SubscriptionError};
        self.subscriptions
            .as_ref()
            .ok_or(Error::NotConfigured)?
            .synchronize(scope, connection_id)
            .await
            .map_err(|failure| match failure {
                SubscriptionError::Provider(error) => error,
                SubscriptionError::Ingestion(error) => match error {
                    IngestionError::Refused => Error::Forbidden,
                    IngestionError::ConflictingInput
                    | IngestionError::LeaseLost
                    | IngestionError::DeliveryUnknown => Error::OutcomeUnknown,
                    IngestionError::Capacity => Error::RateLimited { retry_after: None },
                    IngestionError::InvalidLimit | IngestionError::InvalidManifest => {
                        Error::InvalidResponse
                    }
                    IngestionError::Unavailable => Error::UpstreamUnavailable,
                },
            })
    }

    /// Claim before the POST: a lost result stays with the flow owner for
    /// reconciliation, rather than repeating account creation on a retry.
    pub async fn start(&self, scope: &Scope, registration: &str) -> Result<Started, Error> {
        let attempt = self.authority.attempt(scope, registration).await?;
        attempt.check()?;
        self.client.config(attempt.provider)?;
        if attempt.phase != AttemptPhase::Ready {
            return Err(Error::OutcomeUnknown);
        }
        self.authority.claim_start(scope, &attempt).await?;
        let started = self.client.link(scope, &attempt).await?;
        // The owner atomically rejects cancel/expiry/replacement and binds only
        // the account returned by this physical creation, not a callback ID.
        self.authority
            .bind_started(scope, &attempt, &started)
            .await?;
        Ok(started)
    }

    /// session_uri is opaque data sent to Composio, never fetched as a URL.
    /// The caller must be reauthenticated by the business identity owner.
    pub async fn confirm(
        &self,
        scope: &Scope,
        registration: &str,
        session_uri: &str,
    ) -> Result<VerifiedAccount, Error> {
        let attempt = self.authority.attempt(scope, registration).await?;
        if attempt.phase == AttemptPhase::Complete {
            return self.completed(scope, &attempt).await;
        }
        attempt.check()?;
        if attempt.phase != AttemptPhase::Waiting || attempt.account_id.is_empty() {
            return Err(Error::AuthorizationInactive);
        }
        if session_uri.is_empty() || !types::bounded(session_uri, 8192) {
            return Err(Error::InvalidRequest);
        }
        let digest = attempt.confirmation_digest(scope, session_uri);
        self.authority
            .claim_confirmation(scope, &attempt, &digest)
            .await?;
        let account_id = self
            .client
            .complete(scope, session_uri, attempt.provider)
            .await?;
        if account_id != attempt.account_id {
            return Err(Error::AccountMismatch);
        }
        self.finish_authorization(scope, &attempt).await
    }

    /// Recover a lost single-use complete_auth reply by querying only the
    /// account already bound by the flow owner. Never replay complete_auth.
    pub async fn reconcile_authorization(
        &self,
        scope: &Scope,
        registration: &str,
    ) -> Result<VerifiedAccount, Error> {
        let attempt = self.authority.attempt(scope, registration).await?;
        if attempt.phase == AttemptPhase::Complete {
            return self.completed(scope, &attempt).await;
        }
        attempt.check()?;
        if attempt.phase != AttemptPhase::Waiting || attempt.account_id.is_empty() {
            return Err(Error::AuthorizationInactive);
        }
        self.finish_authorization(scope, &attempt).await
    }

    /// Local cancellation is authoritative and must fence late completion.
    pub async fn cancel(&self, scope: &Scope, registration: &str) -> Result<(), Error> {
        self.authority.cancel_attempt(scope, registration).await
    }

    async fn completed(&self, scope: &Scope, attempt: &Attempt) -> Result<VerifiedAccount, Error> {
        let receipt = self
            .authority
            .verified_receipt(scope, attempt)
            .await?
            .ok_or(Error::OutcomeUnknown)?;
        if receipt.provider != attempt.provider
            || (receipt.provider == MailProvider::Microsoft
                && !types::identifier(&receipt.subject_id, 256))
            || receipt.account_id != attempt.account_id
            || !types::account_id(&receipt.account_id)
            || !types::email_address(&receipt.email)
        {
            return Err(Error::AccountMismatch);
        }
        Ok(receipt)
    }

    async fn finish_authorization(
        &self,
        scope: &Scope,
        attempt: &Attempt,
    ) -> Result<VerifiedAccount, Error> {
        let account_id = attempt.account_id.clone();
        self.client
            .account(scope, &account_id, Some(&attempt.alias()), attempt.provider)
            .await?;
        let verified = self
            .client
            .profile(scope, &account_id, attempt.provider)
            .await?;
        self.authority
            .accept_verified(scope, attempt, &verified)
            .await?;
        Ok(verified)
    }

    /// Shared foreground read entrance. Request provider never grants authority.
    /// Returns current-owner/permission failures and bounded upstream errors.
    pub async fn mail(
        &self,
        scope: &Scope,
        connection_id: &str,
        request: MailRequest,
    ) -> Result<serde_json::Value, Error> {
        request.validate()?;
        let grant = self.authority.connection(scope, connection_id).await?;
        scope.read_access(&grant)?;
        let read_body = self
            .authority
            .authorize_read(scope, connection_id, &grant)
            .await?;
        let mut effective = grant.clone();
        effective.permissions.read_body = read_body && grant.permissions.read_headers;
        effective.check(&request)?;
        let body = matches!(
            &request,
            MailRequest::Gmail(GmailRequest::Read { body: true, .. })
                | MailRequest::Outlook(OutlookRequest::Read { body: true, .. })
        );
        let guard = MailReadGuard {
            gateway: self,
            scope,
            connection_id,
            grant: &grant,
            body,
        };
        self.client.config(grant.provider)?;
        if let MailRequest::Outlook(request) = &request {
            request.endpoint(&grant.subject_id)?;
        }
        self.client
            .account(scope, &grant.account_id, None, grant.provider)
            .await?;
        let profile = self
            .client
            .profile(scope, &grant.account_id, grant.provider)
            .await?;
        if (!grant.email.is_empty() && !grant.email.eq_ignore_ascii_case(&profile.email))
            || grant.subject_id != profile.subject_id
        {
            return Err(Error::AccountMismatch);
        }
        self.authority
            .assert_connection(scope, connection_id, &grant)
            .await?;
        let result = self
            .client
            .mail(scope, &effective, request, Some(&guard))
            .await?;
        // Revocation cannot stop an already dispatched read, but its late
        // content must not be delivered after the local authority is fenced.
        guard.check().await?;
        Ok(result)
    }
}

#[cfg(test)]
mod budget_tests;
#[cfg(test)]
pub(crate) mod tests;
