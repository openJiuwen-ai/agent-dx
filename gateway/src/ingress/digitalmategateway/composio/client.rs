use super::types::{account_id, bounded, email_address, json_object, status_error};
use super::{Attempt, Budget, Error, GmailRequest, Scope, Started};
use reqwest::{
    header::{HeaderMap, HeaderValue},
    Method, Url,
};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{sync::Arc, time::Duration};

const MAX_RESPONSE: usize = 1_048_576;
const GMAIL: &str = "https://gmail.googleapis.com/gmail/v1/users/me";

pub(super) struct Client {
    http: reqwest::Client,
    pub(super) origin: String,
    auth_config: String,
    budget: Arc<dyn Budget>,
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
        auth_config: String,
        budget: Arc<dyn Budget>,
    ) -> Result<Self, Error> {
        if api_key.is_empty()
            || !super::types::identifier(&auth_config, 256)
            || !auth_config.starts_with("ac_")
        {
            return Err(Error::NotConfigured);
        }
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
            auth_config,
            budget,
        })
    }

    async fn request(
        &self,
        scope: &Scope,
        method: Method,
        path: &str,
        body: Option<Value>,
        effect: bool,
    ) -> Result<Value, Error> {
        self.budget.acquire(scope).await?;
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
        json_object(result)
    }

    pub(super) async fn link(&self, scope: &Scope, attempt: &Attempt) -> Result<Started, Error> {
        let response = self.request(scope, Method::POST, "/api/v3.1/connected_accounts/link", Some(json!({
            "auth_config_id": self.auth_config, "user_id":scope.composio_user_id(), "alias":attempt.alias(),
        })), true).await?;
        let link: Link = serde_json::from_value(response).map_err(|_| Error::OutcomeUnknown)?;
        let url = Url::parse(&link.redirect_url).map_err(|_| Error::OutcomeUnknown)?;
        // No browser secrets are copied or reimplemented. Composio owns the
        // Google OAuth protocol and token exchange; callback verifier is required.
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

    pub(super) async fn complete(&self, scope: &Scope, session_uri: &str) -> Result<String, Error> {
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
        if !account_id(&complete.connected_account_id) || complete.toolkit_slug != "gmail" {
            return Err(Error::AccountMismatch);
        }
        Ok(complete.connected_account_id)
    }

    pub(super) async fn account(
        &self,
        scope: &Scope,
        id: &str,
        alias: Option<&str>,
    ) -> Result<(), Error> {
        let account = self.owned_account(scope, id, alias).await?;
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
    ) -> Result<Account, Error> {
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
            || account.toolkit.slug != "gmail"
            || account.auth_config.id != self.auth_config
            || account.auth_config.is_composio_managed
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
        let result = self.request(scope, Method::POST, "/api/v3.1/tools/execute/proxy", Some(json!({
            "connected_account_id":account_id, "user_id":scope.composio_user_id(), "method":"GET", "endpoint":endpoint,
        })), false).await?;
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

    pub(super) async fn profile(&self, scope: &Scope, id: &str) -> Result<String, Error> {
        let profile = self
            .proxy(scope, id, format!("{GMAIL}/profile?fields=emailAddress"))
            .await?;
        let email = profile
            .get("emailAddress")
            .and_then(Value::as_str)
            .filter(|v| email_address(v))
            .ok_or(Error::InvalidResponse)?;
        Ok(email.to_string())
    }

    pub(super) async fn gmail(
        &self,
        scope: &Scope,
        id: &str,
        email: &str,
        request: GmailRequest,
    ) -> Result<Value, Error> {
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
                    for name in ["Subject", "From", "To", "Date", "Message-ID"] {
                        endpoint
                            .query_pairs_mut()
                            .append_pair("metadataHeaders", name);
                    }
                }
            }
        }
        self.proxy(scope, id, endpoint.to_string()).await
    }

    pub(super) async fn revoke(&self, scope: &Scope, id: &str) -> Result<(), Error> {
        let current = self.owned_account(scope, id, None).await?;
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
