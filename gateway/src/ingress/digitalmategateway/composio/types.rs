use async_trait::async_trait;
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// Stable errors contain neither upstream descriptions nor account content.
#[derive(Clone, Copy, Debug, Eq, PartialEq, thiserror::Error)]
pub enum Error {
    #[error("not_configured")]
    NotConfigured,
    #[error("forbidden")]
    Forbidden,
    #[error("authorization_inactive")]
    AuthorizationInactive,
    #[error("authorization_pending")]
    AuthorizationPending,
    #[error("account_mismatch")]
    AccountMismatch,
    #[error("reauth_required")]
    ReauthRequired,
    #[error("consent_required")]
    ConsentRequired,
    #[error("not_found")]
    NotFound,
    #[error("invalid_response")]
    InvalidResponse,
    #[error("invalid_request")]
    InvalidRequest,
    #[error("response_too_large")]
    ResponseTooLarge,
    #[error("rate_limited")]
    RateLimited { retry_after: Option<u64> },
    #[error("upstream_unavailable")]
    UpstreamUnavailable,
    #[error("outcome_unknown")]
    OutcomeUnknown,
}

/// Canonical connection-owner providers; toolkit names are transport details.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MailProvider {
    Google,
    Microsoft,
}

impl MailProvider {
    pub(super) fn toolkit(self) -> &'static str {
        match self {
            Self::Google => "gmail",
            Self::Microsoft => "outlook",
        }
    }
}

/// App ownership is trusted deployment policy, never a user request option.
#[derive(Clone, Copy, Eq, PartialEq)]
pub enum OAuthAppOwnership {
    Custom,
    ComposioManaged,
}

impl OAuthAppOwnership {
    pub(super) fn is_managed(self) -> bool {
        self == Self::ComposioManaged
    }
}

pub struct MailAuthConfig {
    pub id: String,
    pub ownership: OAuthAppOwnership,
}

impl MailAuthConfig {
    pub fn custom(id: String) -> Self {
        Self {
            id,
            ownership: OAuthAppOwnership::Custom,
        }
    }

    pub fn managed(id: String) -> Self {
        Self {
            id,
            ownership: OAuthAppOwnership::ComposioManaged,
        }
    }
}

/// Configure only providers enabled by deployment. Missing providers fail closed.
#[derive(Default)]
pub struct MailAuthConfigs {
    pub gmail: Option<MailAuthConfig>,
    pub outlook: Option<MailAuthConfig>,
}

/// Construct only from authenticated, foreground-purpose business authority.
/// Deliberately not Deserialize: request JSON must never construct identity.
/// No background purpose is accepted in this first integration candidate.
#[derive(Clone, Eq, PartialEq)]
pub struct Scope {
    tenant: String,
    user: String,
    workspace: String,
}

impl Scope {
    pub fn from_authenticated_foreground(
        tenant: String,
        user: String,
        workspace: String,
    ) -> Result<Self, Error> {
        if [&tenant, &user, &workspace]
            .iter()
            .any(|v| !bounded(v, 256) || v.is_empty())
        {
            return Err(Error::Forbidden);
        }
        Ok(Self {
            tenant,
            user,
            workspace,
        })
    }

    pub fn tenant(&self) -> &str {
        &self.tenant
    }

    pub fn user_id(&self) -> &str {
        &self.user
    }

    pub fn workspace(&self) -> &str {
        &self.workspace
    }

    /// Non-email, namespace-separated mapping, independent of OAuth credentials.
    pub fn composio_user_id(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"digitalmate.composio.user.v1\0");
        for part in [&self.tenant, &self.user, &self.workspace] {
            hash.update(part.as_bytes());
            hash.update([0]);
        }
        format!("dm_{}", hex::encode(hash.finalize()))
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub enum AttemptPhase {
    Ready,
    Starting,
    Waiting,
    Cancelled,
    Expired,
    Complete,
}

/// Read-only snapshot from the existing authorization owner; not another table.
#[derive(Clone)]
pub struct Attempt {
    pub provider: MailProvider,
    pub registration: String,
    pub revision: u64,
    pub deadline_epoch_seconds: u64,
    pub phase: AttemptPhase,
    pub account_id: String,
    pub expected_email: Option<String>,
}

impl Attempt {
    pub(super) fn check(&self) -> Result<(), Error> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::AuthorizationInactive)?
            .as_secs();
        if !bounded(&self.registration, 256)
            || self.registration.is_empty()
            || now >= self.deadline_epoch_seconds
            || matches!(
                self.phase,
                AttemptPhase::Cancelled | AttemptPhase::Expired | AttemptPhase::Complete
            )
        {
            return Err(Error::AuthorizationInactive);
        }
        Ok(())
    }

    pub(super) fn alias(&self) -> String {
        let mut hash = Sha256::new();
        hash.update(b"digitalmate.composio.attempt.v1\0");
        hash.update(self.registration.as_bytes());
        hash.update(self.revision.to_be_bytes());
        format!("dm_{}", hex::encode(hash.finalize()))
    }

    pub(super) fn confirmation_digest(&self, scope: &Scope, session_uri: &str) -> String {
        let mut hash = Sha256::new();
        hash.update(b"digitalmate.composio.confirm.v1\0");
        let user = scope.composio_user_id();
        for part in [
            user.as_str(),
            self.registration.as_str(),
            self.provider.toolkit(),
            self.account_id.as_str(),
        ] {
            hash.update(part.as_bytes());
            hash.update([0]);
        }
        hash.update(self.revision.to_be_bytes());
        hash.update(session_uri.as_bytes());
        hex::encode(hash.finalize())
    }
}

/// Safe projection: credentials and link_token are intentionally absent.
pub struct Started {
    pub account_id: String,
    pub redirect_url: String,
    pub expires_at: String,
}

/// Account proof conveys identity only. It never grants read/write capabilities.
#[derive(Clone)]
pub struct VerifiedAccount {
    pub provider: MailProvider,
    /// Online immutable Graph subject for Microsoft; not an email or caller ID.
    /// Gmail profile does not supply this proof, so its value remains empty.
    pub subject_id: String,
    pub account_id: String,
    pub email: String,
}

#[derive(Clone, Copy, Default, Eq, PartialEq)]
pub struct Permissions {
    pub search: bool,
    pub read_headers: bool,
    pub read_body: bool,
}

#[derive(Clone)]
pub struct ConnectionGrant {
    pub provider: MailProvider,
    pub subject_id: String,
    pub account_id: String,
    pub email: String,
    pub generation: u64,
    pub permissions: Permissions,
}

impl ConnectionGrant {
    pub(super) fn check(&self, request: &MailRequest) -> Result<(), Error> {
        if self.provider != request.provider()
            || !account_id(&self.account_id)
            || !email_address(&self.email)
            || (self.provider == MailProvider::Microsoft && !identifier(&self.subject_id, 256))
        {
            return Err(Error::Forbidden);
        }
        let allowed = match request {
            MailRequest::Gmail(GmailRequest::Search { .. })
            | MailRequest::Outlook(OutlookRequest::Search { .. }) => self.permissions.search,
            MailRequest::Gmail(GmailRequest::Read { body: false, .. })
            | MailRequest::Outlook(OutlookRequest::Read { body: false, .. }) => {
                self.permissions.read_headers
            }
            MailRequest::Gmail(GmailRequest::Read { body: true, .. })
            | MailRequest::Outlook(OutlookRequest::Read { body: true, .. }) => {
                self.permissions.read_body
            }
        };
        if allowed {
            Ok(())
        } else {
            Err(Error::Forbidden)
        }
    }
}

/// No arbitrary method, URL, toolkit, user ID, account ID, tool or headers.
pub enum GmailRequest {
    Search {
        query: String,
        limit: u16,
        page: String,
    },
    Read {
        message_id: String,
        body: bool,
    },
}

impl GmailRequest {
    pub(super) fn validate(&self) -> Result<(), Error> {
        let valid = match self {
            Self::Search { query, limit, page } => {
                bounded(query, 4096) && (1..=100).contains(limit) && bounded(page, 4096)
            }
            Self::Read { message_id, .. } => identifier(message_id, 128),
        };
        if valid {
            Ok(())
        } else {
            Err(Error::InvalidRequest)
        }
    }
}

/// Personal Microsoft mailbox only; no free-form OData, tool, URL or headers.
pub enum OutlookRequest {
    Search {
        sender: String,
        since: String,
        until: String,
        limit: u16,
        page: String,
    },
    Read {
        message_id: String,
        body: bool,
    },
}

/// Shared read entrance. Provider must match the current owner connection grant.
pub enum MailRequest {
    Gmail(GmailRequest),
    Outlook(OutlookRequest),
}

impl MailRequest {
    pub(super) fn provider(&self) -> MailProvider {
        match self {
            Self::Gmail(_) => MailProvider::Google,
            Self::Outlook(_) => MailProvider::Microsoft,
        }
    }

    pub(super) fn validate(&self) -> Result<(), Error> {
        match self {
            Self::Gmail(request) => request.validate(),
            Self::Outlook(request) => request.validate(),
        }
    }
}

pub(super) fn bounded(value: &str, max: usize) -> bool {
    value.len() <= max && !value.chars().any(char::is_control)
}

pub(super) fn identifier(value: &str, max: usize) -> bool {
    !value.is_empty()
        && value.len() <= max
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'_' || c == b'-')
}

pub(super) fn account_id(value: &str) -> bool {
    value.starts_with("ca_") && identifier(value, 256)
}

pub(super) fn email_address(value: &str) -> bool {
    let Some((local, domain)) = value.split_once('@') else {
        return false;
    };
    bounded(value, 254)
        && !local.is_empty()
        && domain.contains('.')
        && local
            .bytes()
            .all(|v| v.is_ascii_alphanumeric() || b"._%+-".contains(&v))
        && domain.split('.').all(|label| {
            !label.is_empty()
                && !label.starts_with('-')
                && !label.ends_with('-')
                && label
                    .bytes()
                    .all(|v| v.is_ascii_alphanumeric() || v == b'-')
        })
}

/// Candidate integration port. Production adapters must use the real connection
/// and authorization owners' published proxies, CAS/generation and persistence.
/// No implementation/default or private-table access is supplied here.
#[async_trait]
pub trait Authority: Send + Sync {
    async fn attempt(&self, scope: &Scope, registration: &str) -> Result<Attempt, Error>;
    async fn cancel_attempt(&self, scope: &Scope, registration: &str) -> Result<(), Error>;
    /// Read an already committed proof without replaying complete_auth or a
    /// write. Owner atomically checks scope/current revision, invalidation and
    /// receipt retention; Complete alone is never proof of a still-valid result.
    async fn verified_receipt(
        &self,
        scope: &Scope,
        attempt: &Attempt,
    ) -> Result<Option<VerifiedAccount>, Error>;
    /// Durably mark Starting before external creation. Repeated/unknown starts
    /// require reconciliation by this owner, never another blind POST.
    async fn claim_start(&self, scope: &Scope, attempt: &Attempt) -> Result<(), Error>;
    /// The real M44 owner durably claims the single confirmation before I/O,
    /// atomically fencing scope/provider/account/revision, expiry and replacement.
    /// Store only this request digest, never session_uri. An already claimed
    /// unknown outcome (including a different session) cannot dispatch again;
    /// reconcile the original account instead. Completed retries read the receipt.
    async fn claim_confirmation(
        &self,
        scope: &Scope,
        attempt: &Attempt,
        session_digest: &str,
    ) -> Result<(), Error>;
    async fn bind_started(
        &self,
        scope: &Scope,
        attempt: &Attempt,
        started: &Started,
    ) -> Result<(), Error>;
    /// Recheck expiry/cancel/replacement, expected account and generation in the
    /// flow owner's atomic receipt. This publishes proof, not a connection write;
    /// the sandbox consumer uses ScanRegistrations.keep_external, which delegates
    /// the sole connection write to ConnectionRegistry.save_external.
    async fn accept_verified(
        &self,
        scope: &Scope,
        attempt: &Attempt,
        account: &VerifiedAccount,
    ) -> Result<(), Error>;
    async fn connection(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<ConnectionGrant, Error>;
    /// Recheck provider/backend, subject, generation and current operation permissions.
    async fn assert_connection(
        &self,
        scope: &Scope,
        connection_id: &str,
        grant: &ConnectionGrant,
    ) -> Result<(), Error>;
    /// Disable access before returning; repeated calls retain a cleanup handle
    /// so an upstream revoke failure can be retried without re-enabling access.
    async fn fence_connection(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<ConnectionGrant, Error>;
}

/// Shared organization/per-user budget owner, charged for every physical call.
/// No unlimited default and no process-local imitation of cluster enforcement.
#[async_trait]
pub trait Budget: Send + Sync {
    async fn acquire(&self, scope: &Scope) -> Result<(), Error>;
}

pub(super) fn status_error(status: u16, retry_after: Option<u64>) -> Error {
    match status {
        401 => Error::ReauthRequired,
        403 => Error::ConsentRequired,
        404 => Error::NotFound,
        429 => Error::RateLimited { retry_after },
        _ => Error::UpstreamUnavailable,
    }
}

pub(super) fn json_object(value: Value) -> Result<Value, Error> {
    if value.is_object() {
        Ok(value)
    } else {
        Err(Error::InvalidResponse)
    }
}
