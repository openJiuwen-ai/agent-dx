//! Restricted Composio authorization and mail reads outside user sandboxes.
//!
//! This is an unwired integration candidate. Authentication, purpose grants,
//! connection/attempt authority and shared request budget must be supplied by
//! their real owners. A crypto bearer or a JSON scope is not such a grant.
//! No connection database, token store, ingestion scheduler or HTTP route lives here.

mod budget;
mod client;
mod outlook;
mod types;
mod webhook;

pub use budget::{PostgresBudget, RequestBudgetLimits};
pub use webhook::{VerifiedWebhook, WebhookError, WebhookVerifier};

pub use types::{
    Attempt, AttemptPhase, Authority, Budget, ConnectionGrant, Error, GmailRequest, MailAuthConfig,
    MailAuthConfigs, MailProvider, MailRequest, OAuthAppOwnership, OutlookRequest, Permissions,
    Scope, Started, VerifiedAccount,
};

use client::Client;
use std::sync::Arc;

/// Trusted configuration only; never construct from a user request or template.
pub struct Composio {
    client: Client,
    authority: Arc<dyn Authority>,
}

impl Composio {
    /// Requires custom Gmail OAuth auth config and project callback verification.
    /// Fails closed on empty configuration, invalid key header or HTTP setup.
    pub fn new(
        api_key: String,
        gmail_auth_config: String,
        authority: Arc<dyn Authority>,
        budget: Arc<dyn Budget>,
    ) -> Result<Self, Error> {
        Self::with_configs(
            api_key,
            MailAuthConfigs {
                gmail: Some(MailAuthConfig::custom(gmail_auth_config)),
                outlook: None,
            },
            authority,
            budget,
        )
    }

    /// Exact per-provider app ownership and auth config come from deployment.
    /// Both managed and custom OAuth still require current real owner authority.
    pub fn with_configs(
        api_key: String,
        configs: MailAuthConfigs,
        authority: Arc<dyn Authority>,
        budget: Arc<dyn Budget>,
    ) -> Result<Self, Error> {
        Ok(Self {
            client: Client::new(api_key, configs, budget)?,
            authority,
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
            || attempt
                .expected_email
                .as_ref()
                .is_some_and(|v| !v.eq_ignore_ascii_case(&receipt.email))
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
        if attempt
            .expected_email
            .as_ref()
            .is_some_and(|value| !value.eq_ignore_ascii_case(&verified.email))
        {
            return Err(Error::AccountMismatch);
        }
        self.authority
            .accept_verified(scope, attempt, &verified)
            .await?;
        Ok(verified)
    }

    /// Only declared read operations; the owner supplies current generation,
    /// mailbox identity and existing tool permissions. ACTIVE grants no tool.
    pub async fn gmail(
        &self,
        scope: &Scope,
        connection_id: &str,
        request: GmailRequest,
    ) -> Result<serde_json::Value, Error> {
        self.mail(scope, connection_id, MailRequest::Gmail(request))
            .await
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
        grant.check(&request)?;
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
        if !grant.email.eq_ignore_ascii_case(&profile.email)
            || (grant.provider == MailProvider::Microsoft && grant.subject_id != profile.subject_id)
        {
            return Err(Error::AccountMismatch);
        }
        self.authority
            .assert_connection(scope, connection_id, &grant)
            .await?;
        let result = self.client.mail(scope, &grant, request).await?;
        // Revocation cannot stop an already dispatched read, but its late
        // content must not be delivered after the local authority is fenced.
        self.authority
            .assert_connection(scope, connection_id, &grant)
            .await?;
        Ok(result)
    }

    /// Fence local access first. Report remote revoke independently; a network
    /// failure never re-enables this connection or means revocation succeeded.
    pub async fn disconnect(&self, scope: &Scope, connection_id: &str) -> Result<(), Error> {
        let grant = self
            .authority
            .fence_connection(scope, connection_id)
            .await?;
        self.client
            .revoke(scope, &grant.account_id, grant.provider)
            .await
    }
}

#[cfg(test)]
mod budget_tests;
#[cfg(test)]
mod tests;
