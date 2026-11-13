use super::types::{account_id, bounded, email_address, json_object, status_error};
use super::{
    Attempt, Budget, ConnectionGrant, Error, GmailRequest, MailAuthConfig, MailAuthConfigs,
    MailProvider, MailRequest, Scope, Started, VerifiedAccount,
};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Method, Url,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

const MAX_RESPONSE: usize = 1_048_576;
const GMAIL: &str = "https://gmail.googleapis.com/gmail/v1/users/me";
const GMAIL_HEADERS: [&str; 5] = ["Subject", "From", "To", "Date", "Message-ID"];

#[derive(Clone)]
pub(super) struct Client {
    http: reqwest::Client,
    pub(super) origin: String,
    auth_configs: MailAuthConfigs,
    pub(super) budget: Arc<dyn Budget>,
}

// Only safe account metadata is deserialized. Unknown fields (including token
// material on custom auth configs) are ignored, never logged or returned.
#[derive(Deserialize)]
struct Account {
    id: String,
    user_id: String,
    status: String,
    #[serde(default)]
    alias: String,
    toolkit: Toolkit,
    auth_config: AuthConfig,
    #[serde(default)]
    is_disabled: bool,
    experimental: Option<AccountType>,
}
#[derive(Deserialize)]
struct Toolkit {
    slug: String,
}
#[derive(Deserialize)]
struct AuthConfig {
    id: String,
    auth_scheme: String,
    is_composio_managed: bool,
    #[serde(default)]
    is_disabled: bool,
}
#[derive(Deserialize)]
struct AccountType {
    account_type: String,
}
#[derive(Deserialize)]
struct Link {
    connected_account_id: String,
    redirect_url: String,
    expires_at: String,
}
#[derive(Deserialize)]
struct Complete {
    connected_account_id: String,
    toolkit_slug: String,
}

impl Client {
    pub(super) fn new(
        api_key: String,
        auth_configs: MailAuthConfigs,
        budget: Arc<dyn Budget>,
    ) -> Result<Self, Error> {
        let api_key = zeroize::Zeroizing::new(api_key);
        if api_key.is_empty() {
            return Err(Error::NotConfigured);
        }
        auth_configs.validate()?;
        let mut headers = HeaderMap::new();
        let mut key = HeaderValue::from_str(&api_key).map_err(|_| Error::NotConfigured)?;
        key.set_sensitive(true);
        headers.insert("x-api-key", key);
        let http = reqwest::Client::builder()
            .default_headers(headers)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(30))
            .connect_timeout(Duration::from_secs(10))
            .build()
            .map_err(|_| Error::NotConfigured)?;
        Ok(Self {
            http,
            origin: "https://backend.composio.dev".into(),
            auth_configs,
            budget,
        })
    }

    pub(super) fn config(&self, provider: MailProvider) -> Result<&MailAuthConfig, Error> {
        match provider {
            MailProvider::Google => self.auth_configs.gmail.as_ref(),
            MailProvider::Microsoft => self.auth_configs.outlook.as_ref(),
        }
        .ok_or(Error::NotConfigured)
    }

    pub(super) async fn request(
        &self,
        scope: &Scope,
        method: Method,
        path: &str,
        body: Option<Value>,
        effect: bool,
    ) -> Result<Value, Error> {
        self.request_guarded(scope, method, path, body, effect, None)
            .await
    }

    async fn request_guarded(
        &self,
        scope: &Scope,
        method: Method,
        path: &str,
        body: Option<Value>,
        effect: bool,
        guard: Option<&super::MailReadGuard<'_>>,
    ) -> Result<Value, Error> {
        scope.check_bound_identity().await?;
        self.budget.acquire(scope).await?;
        scope.check_bound_identity().await?;
        if let Some(guard) = guard {
            guard.check().await?;
        }
        let mut request = self.http.request(method, format!("{}{path}", self.origin));
        if let Some(value) = body {
            request = request.json(&value);
        }
        let mut response = request.send().await.map_err(|_| {
            if effect {
                Error::OutcomeUnknown
            } else {
                Error::UpstreamUnavailable
            }
        })?;
        let status = response.status().as_u16();
        let retry_after = response
            .headers()
            .get("retry-after")
            .and_then(|v| v.to_str().ok())
            .and_then(|v| v.parse::<u64>().ok())
            .map(|v| v.min(3600));
        if !(200..300).contains(&status) {
            return Err(match status {
                // The project key, not the user's provider token, was refused.
                401 => Error::NotConfigured,
                403 => Error::Forbidden,
                404 => Error::NotFound,
                429 => Error::RateLimited { retry_after },
                500..=599 if effect => Error::OutcomeUnknown,
                _ => Error::UpstreamUnavailable,
            });
        }
        if response
            .content_length()
            .is_some_and(|v| v > MAX_RESPONSE as u64)
        {
            return Err(if effect {
                Error::OutcomeUnknown
            } else {
                Error::ResponseTooLarge
            });
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response.chunk().await.map_err(|_| {
            if effect {
                Error::OutcomeUnknown
            } else {
                Error::UpstreamUnavailable
            }
        })? {
            if bytes.len().saturating_add(chunk.len()) > MAX_RESPONSE {
                return Err(if effect {
                    Error::OutcomeUnknown
                } else {
                    Error::ResponseTooLarge
                });
            }
            bytes.extend_from_slice(&chunk);
        }
        let result: Value = serde_json::from_slice(&bytes).map_err(|_| {
            if effect {
                Error::OutcomeUnknown
            } else {
                Error::InvalidResponse
            }
        })?;
        if effect && !result.is_object() {
            return Err(Error::OutcomeUnknown);
        }
        scope.check_bound_identity().await?;
        json_object(result)
    }

    pub(super) async fn link(&self, scope: &Scope, attempt: &Attempt) -> Result<Started, Error> {
        let config = self.config(attempt.provider)?;
        let response = self
            .request(
                scope,
                Method::POST,
                "/api/v3.1/connected_accounts/link",
                Some(super::types::link_payload(scope, attempt, &config.id)),
                true,
            )
            .await?;
        let link: Link = serde_json::from_value(response).map_err(|_| Error::OutcomeUnknown)?;
        let url = Url::parse(&link.redirect_url).map_err(|_| Error::OutcomeUnknown)?;
        // No browser secrets are copied or reimplemented. Composio owns the
        // provider OAuth protocol and token exchange; callback verifier is required.
        if !account_id(&link.connected_account_id)
            || url.scheme() != "https"
            || url.host_str() != Some("connect.composio.dev")
            || !url.username().is_empty()
            || url.password().is_some()
            || url.port().is_some()
            || !bounded(&link.redirect_url, 8192)
            || !bounded(&link.expires_at, 64)
            || link.expires_at.is_empty()
        {
            return Err(Error::OutcomeUnknown);
        }
        Ok(Started {
            account_id: link.connected_account_id,
            redirect_url: link.redirect_url,
            expires_at: link.expires_at,
        })
    }

    pub(super) async fn complete(
        &self,
        scope: &Scope,
        session_uri: &str,
        provider: MailProvider,
    ) -> Result<String, Error> {
        self.config(provider)?;
        if session_uri.is_empty() || !bounded(session_uri, 8192) {
            return Err(Error::InvalidRequest);
        }
        let value = self
            .request(
                scope,
                Method::POST,
                "/api/v3.1/connected_accounts/complete_auth",
                Some(json!({
                    "session_uri": session_uri, "user_id":scope.composio_user_id(),
                })),
                true,
            )
            .await?;
        let complete: Complete =
            serde_json::from_value(value).map_err(|_| Error::OutcomeUnknown)?;
        if !account_id(&complete.connected_account_id)
            || complete.toolkit_slug != provider.toolkit()
        {
            return Err(Error::AccountMismatch);
        }
        Ok(complete.connected_account_id)
    }

    pub(super) async fn account(
        &self,
        scope: &Scope,
        id: &str,
        alias: Option<&str>,
        provider: MailProvider,
    ) -> Result<(), Error> {
        let account = self.owned_account(scope, id, alias, provider).await?;
        if alias.is_some()
            && !account.is_disabled
            && !account.auth_config.is_disabled
            && matches!(account.status.as_str(), "INITIALIZING" | "INITIATED")
        {
            return Err(Error::AuthorizationPending);
        }
        if account.is_disabled || account.auth_config.is_disabled || account.status != "ACTIVE" {
            return Err(Error::ReauthRequired);
        }
        Ok(())
    }

    async fn owned_account(
        &self,
        scope: &Scope,
        id: &str,
        alias: Option<&str>,
        provider: MailProvider,
    ) -> Result<Account, Error> {
        let config = self.config(provider)?;
        if !account_id(id) {
            return Err(Error::Forbidden);
        }
        let value = self
            .request(
                scope,
                Method::GET,
                &format!("/api/v3.1/connected_accounts/{id}"),
                None,
                false,
            )
            .await?;
        let account: Account = serde_json::from_value(value).map_err(|_| Error::InvalidResponse)?;
        if account.id != id
            || account.user_id != scope.composio_user_id()
            || account.toolkit.slug != provider.toolkit()
            || account.auth_config.id != config.id
            || account.auth_config.is_composio_managed != config.ownership.is_managed()
            || account.auth_config.auth_scheme != "OAUTH2"
            || alias.is_some_and(|v| v != account.alias)
            || account
                .experimental
                .as_ref()
                .is_some_and(|v| v.account_type != "PRIVATE")
        {
            return Err(Error::AccountMismatch);
        }
        Ok(account)
    }

    async fn proxy(
        &self,
        scope: &Scope,
        account_id: &str,
        endpoint: String,
    ) -> Result<Value, Error> {
        self.proxy_guarded(scope, account_id, endpoint, None).await
    }

    async fn proxy_guarded(
        &self,
        scope: &Scope,
        account_id: &str,
        endpoint: String,
        guard: Option<&super::MailReadGuard<'_>>,
    ) -> Result<Value, Error> {
        let result = self.request_guarded(scope, Method::POST, "/api/v3.1/tools/execute/proxy", Some(json!({
            "connected_account_id":account_id, "user_id":scope.composio_user_id(), "method":"GET", "endpoint":endpoint,
        })), false, guard).await?;
        let status = result
            .get("status")
            .and_then(Value::as_u64)
            .and_then(|v| u16::try_from(v).ok())
            .ok_or(Error::InvalidResponse)?;
        if !(200..300).contains(&status) {
            let retry = result
                .get("headers")
                .and_then(|v| v.get("retry-after"))
                .and_then(Value::as_str)
                .and_then(|v| v.parse::<u64>().ok())
                .map(|v| v.min(3600));
            return Err(status_error(status, retry));
        }
        json_object(result.get("data").cloned().ok_or(Error::InvalidResponse)?)
    }

    pub(super) async fn profile(
        &self,
        scope: &Scope,
        id: &str,
        provider: MailProvider,
    ) -> Result<VerifiedAccount, Error> {
        let endpoint = match provider {
            MailProvider::Google => format!("{GMAIL}/profile?fields=emailAddress"),
            MailProvider::Microsoft => {
                "https://graph.microsoft.com/v1.0/me?$select=id,mail,userPrincipalName".into()
            }
        };
        let profile = self.proxy(scope, id, endpoint).await?;
        let (email, subject_id) = match provider {
            MailProvider::Google => {
                let email = profile
                    .get("emailAddress")
                    .and_then(Value::as_str)
                    .filter(|value| email_address(value))
                    .ok_or(Error::InvalidResponse)?;
                // Gmail profile owns the mailbox address. OAuth2 UserInfo owns
                // the immutable Google ID; neither email nor Composio user_id
                // may stand in for that subject in the existing M44 receipt.
                let identity = self.proxy(scope, id, "https://www.googleapis.com/oauth2/v2/userinfo?fields=id,email,verified_email".into()).await?;
                let subject = identity
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| super::types::identifier(value, 256))
                    .ok_or(Error::InvalidResponse)?;
                if let Some(other) = identity.get("email") {
                    if !other
                        .as_str()
                        .is_some_and(|other| email.eq_ignore_ascii_case(other))
                        || identity.get("verified_email").and_then(Value::as_bool) != Some(true)
                    {
                        return Err(Error::AccountMismatch);
                    }
                }
                (Some(email), subject.to_string())
            }
            MailProvider::Microsoft => {
                let subject = profile
                    .get("id")
                    .and_then(Value::as_str)
                    .filter(|value| super::types::identifier(value, 256))
                    .ok_or(Error::InvalidResponse)?;
                let email = profile
                    .get("mail")
                    .and_then(Value::as_str)
                    .filter(|value| !value.is_empty())
                    .or_else(|| profile.get("userPrincipalName").and_then(Value::as_str));
                (email, subject.to_string())
            }
        };
        let email = email
            .filter(|value| email_address(value))
            .ok_or(Error::InvalidResponse)?
            .to_string();
        Ok(VerifiedAccount {
            provider,
            subject_id,
            account_id: id.into(),
            email,
        })
    }

    pub(super) async fn mail(
        &self,
        scope: &Scope,
        grant: &ConnectionGrant,
        request: MailRequest,
        guard: Option<&super::MailReadGuard<'_>>,
    ) -> Result<Value, Error> {
        match request {
            MailRequest::Gmail(request) => {
                self.gmail(scope, &grant.account_id, &grant.email, request, guard)
                    .await
            }
            MailRequest::Outlook(request) => {
                let endpoint = request.endpoint(&grant.subject_id)?;
                let result = self
                    .proxy_guarded(scope, &grant.account_id, endpoint.to_string(), guard)
                    .await?;
                request.project(result, &grant.subject_id)
            }
        }
    }

    pub(super) async fn gmail(
        &self,
        scope: &Scope,
        id: &str,
        email: &str,
        request: GmailRequest,
        guard: Option<&super::MailReadGuard<'_>>,
    ) -> Result<Value, Error> {
        let metadata_id = match &request {
            GmailRequest::Read {
                message_id,
                body: false,
            } => Some(message_id.clone()),
            _ => None,
        };
        // Pin the verified mailbox as Google's userId. A token rebound to a
        // different mailbox between profile and dispatch cannot select `me`.
        let mut endpoint = Url::parse("https://gmail.googleapis.com/gmail/v1/users")
            .map_err(|_| Error::NotConfigured)?;
        endpoint
            .path_segments_mut()
            .map_err(|_| Error::NotConfigured)?
            .push(email)
            .push("messages");
        match request {
            GmailRequest::Search { query, limit, page } => {
                endpoint
                    .query_pairs_mut()
                    .append_pair("maxResults", &limit.to_string())
                    .append_pair("q", &query)
                    .append_pair("includeSpamTrash", "false");
                if !page.is_empty() {
                    endpoint.query_pairs_mut().append_pair("pageToken", &page);
                }
                endpoint.query_pairs_mut().append_pair(
                    "fields",
                    "messages(id,threadId),nextPageToken,resultSizeEstimate",
                );
            }
            GmailRequest::Read { message_id, body } => {
                endpoint
                    .path_segments_mut()
                    .map_err(|_| Error::NotConfigured)?
                    .push(&message_id);
                endpoint
                    .query_pairs_mut()
                    .append_pair("format", if body { "full" } else { "metadata" });
                if !body {
                    endpoint
                        .query_pairs_mut()
                        .append_pair("fields", "id,threadId,internalDate,payload(headers)");
                    for name in GMAIL_HEADERS {
                        endpoint
                            .query_pairs_mut()
                            .append_pair("metadataHeaders", name);
                    }
                }
            }
        }
        let result = self
            .proxy_guarded(scope, id, endpoint.to_string(), guard)
            .await?;
        match metadata_id {
            Some(message_id) => gmail_metadata(result, &message_id),
            None => Ok(result),
        }
    }

    pub(super) async fn revoke(
        &self,
        scope: &Scope,
        id: &str,
        provider: MailProvider,
    ) -> Result<(), Error> {
        let current = self.owned_account(scope, id, None, provider).await?;
        if current.status == "REVOKED" {
            return Ok(());
        }
        let result = self
            .request(
                scope,
                Method::POST,
                &format!("/api/v3.1/connected_accounts/{id}/revoke"),
                None,
                true,
            )
            .await?;
        let account = result
            .get("connected_account")
            .ok_or(Error::OutcomeUnknown)?;
        if account.get("id").and_then(Value::as_str) != Some(id)
            || account.get("status").and_then(Value::as_str) != Some("REVOKED")
        {
            return Err(Error::OutcomeUnknown);
        }
        Ok(())
    }
}

// Gmail's metadata representation includes snippet by default. Select and
// project headers explicitly so read_headers cannot disclose a body preview.
fn gmail_metadata(value: Value, message_id: &str) -> Result<Value, Error> {
    if value.get("id").and_then(Value::as_str) != Some(message_id) {
        return Err(Error::InvalidResponse);
    }
    let mut result = json!({});
    for field in ["id", "threadId", "internalDate"] {
        if let Some(value) = value.get(field) {
            result[field] = value.clone();
        }
    }
    let mut headers = Vec::new();
    if let Some(payload) = value.get("payload") {
        let payload = payload.as_object().ok_or(Error::InvalidResponse)?;
        if let Some(values) = payload.get("headers") {
            let values = values
                .as_array()
                .filter(|values| values.len() <= 200)
                .ok_or(Error::InvalidResponse)?;
            for header in values {
                let name = header
                    .get("name")
                    .and_then(Value::as_str)
                    .filter(|name| bounded(name, 256))
                    .ok_or(Error::InvalidResponse)?;
                if GMAIL_HEADERS
                    .iter()
                    .any(|allowed| name.eq_ignore_ascii_case(allowed))
                {
                    let value = header
                        .get("value")
                        .and_then(Value::as_str)
                        .filter(|value| value.len() <= 16384)
                        .ok_or(Error::InvalidResponse)?;
                    headers.push(json!({"name":name,"value":value}));
                }
            }
        }
    }
    result["payload"] = json!({"headers":headers});
    Ok(result)
}
