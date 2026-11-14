//! The existing M44 owner and project client implement the ingestion ports.
use super::super::connector_owner::OwnerOperation;
use super::super::ingestion::{
    Inbox, IntentPort, ProviderTrigger, SubscriptionBinding, SubscriptionCoordinator,
    SubscriptionFacts, SubscriptionSnapshot, TriggerPort,
};
use super::client::Client;
use super::owner::M44Authority;
use super::types::identifier;
use super::{Authority, Error, MailProvider, Scope};
use async_trait::async_trait;
use reqwest::{Method, Url};
use serde::Deserialize;
use serde_json::{json, Value};
use std::{collections::BTreeSet, sync::Arc};

/// The prepared client and exact same typed M44 proxy can be shared by assembly.
pub(super) fn coordinator(
    client: Client,
    owner: Arc<M44Authority>,
    inbox: Inbox,
) -> SubscriptionCoordinator {
    SubscriptionCoordinator::new(
        inbox,
        Arc::new(OwnerIntent {
            owner,
            client: client.clone(),
        }),
        Arc::new(ProjectTriggers::new(client)),
    )
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Intent {
    connection_id: String,
    provider: String,
    backend: String,
    external_account_id: String,
    external_subject_id: String,
    authorization_generation: u64,
    enabled: bool,
    intent_revision: u64,
    usable: bool,
    state: String,
}

struct OwnerIntent {
    owner: Arc<M44Authority>,
    client: Client,
}

#[async_trait]
impl IntentPort for OwnerIntent {
    async fn current(
        &self,
        scope: &Scope,
        id: &str,
    ) -> Result<Option<SubscriptionSnapshot>, Error> {
        let body = json!({"operation":"intent", "connection_id":id});
        let raw = serde_json::to_vec(&body).map_err(|_| Error::InvalidRequest)?;
        let authority = self
            .owner
            .authorization(scope, OwnerOperation::Intent, &raw)
            .await?;
        // The M44 owner HTTP operation returns the snapshot directly; the
        // client-facing connector.subscription.get envelope is not used here.
        let intent: Intent =
            serde_json::from_value(self.owner.send(scope, OwnerOperation::Intent, body).await?)
                .map_err(|_| Error::InvalidResponse)?;
        if intent.connection_id != id {
            return Err(Error::AccountMismatch);
        }
        if intent.state == "deleted" {
            if intent.enabled || intent.usable {
                return Err(Error::InvalidResponse);
            }
            return Ok(None);
        }
        if intent.state == "unavailable" {
            return Err(Error::UpstreamUnavailable);
        }
        if !matches!(intent.state.as_str(), "current" | "stale") || intent.backend != "composio" {
            return Err(Error::Forbidden);
        }
        // This published adapter already compares the actual Connection and
        // Metadata responses, without refresh/ensure_ready recursion.
        let metadata = self.owner.metadata(scope, id).await?;
        let provider = metadata.check(id)?;
        if intent.provider != metadata.provider
            || intent.backend != metadata.backend
            || intent.external_account_id != metadata.external_account_id
            || intent.external_subject_id != metadata.external_subject_id
            || intent.authorization_generation != metadata.authorization_generation
        {
            return Err(Error::AccountMismatch);
        }
        let slug = match provider {
            MailProvider::Google => "GMAIL_NEW_GMAIL_MESSAGE",
            MailProvider::Microsoft => "OUTLOOK_MESSAGE_TRIGGER",
        };
        let config = self.client.config(provider)?;
        Ok(Some(
            self.owner
                .issuer
                .subscription_snapshot(
                    &authority,
                    SubscriptionFacts {
                        connection_id: id.into(),
                        provider: intent.provider,
                        trigger_slug: slug.into(),
                        account_id: intent.external_account_id,
                        auth_config_id: config.id.clone(),
                        composio_user_id: scope.composio_user_id(),
                        connection_generation: intent.authorization_generation,
                        record_revision: metadata.record_revision,
                        intent_revision: intent.intent_revision,
                        enabled: intent.enabled,
                        usable: intent.usable && intent.state == "current",
                    },
                )
                .await
                .map_err(|_| Error::Forbidden)?,
        ))
    }
}

#[cfg(test)]
mod intent_contract_tests {
    use super::*;

    #[test]
    fn m44_owner_intent_response_is_the_direct_snapshot() {
        // owner_http._dispatch("intent") returns SubscriptionIntent.model_dump;
        // connector.subscription.get is a different, client-facing envelope.
        let wire = json!({
            "connection_id":"connection-fixture", "provider":"microsoft", "backend":"composio",
            "external_account_id":"ca_fixture", "external_subject_id":"subject-fixture",
            "authorization_generation":1, "enabled":true, "intent_revision":1,
            "usable":true, "state":"current"
        });
        let reply: Intent = serde_json::from_value(wire.clone()).unwrap();
        assert_eq!(reply.connection_id, "connection-fixture");
        assert!(reply.enabled);
        assert!(serde_json::from_value::<Intent>(json!({"subscription_intent":wire})).is_err());
        let mut extra = wire.clone();
        extra["untrusted"] = json!(true);
        assert!(serde_json::from_value::<Intent>(extra).is_err());
        let mut wrong_type = wire;
        wrong_type["enabled"] = json!("true");
        assert!(serde_json::from_value::<Intent>(wrong_type).is_err());
    }
}

pub(super) struct ProjectTriggers {
    client: Client,
}
impl ProjectTriggers {
    pub(super) fn new(client: Client) -> Self {
        Self { client }
    }

    fn provider(&self, slug: &str, config: &str) -> Result<MailProvider, Error> {
        let provider = match slug {
            "GMAIL_NEW_GMAIL_MESSAGE" => MailProvider::Google,
            "OUTLOOK_MESSAGE_TRIGGER" => MailProvider::Microsoft,
            _ => return Err(Error::Forbidden),
        };
        if self.client.config(provider)?.id != config {
            return Err(Error::Forbidden);
        }
        Ok(provider)
    }

    async fn find(
        &self,
        scope: &Scope,
        filter: &TriggerFilter<'_>,
        trigger_id: Option<&str>,
    ) -> Result<Option<ProviderTrigger>, Error> {
        let mut cursor = String::new();
        let mut seen = BTreeSet::new();
        let mut found = None;
        for _ in 0..8 {
            let mut url =
                Url::parse("https://backend.composio.dev/api/v3.1/trigger_instances/active")
                    .map_err(|_| Error::NotConfigured)?;
            {
                let mut query = url.query_pairs_mut();
                query.append_pair("connected_account_ids", filter.account);
                query.append_pair("auth_config_ids", filter.config);
                query.append_pair("user_ids", filter.user);
                query.append_pair("trigger_names", filter.slug);
                query.append_pair("show_disabled", "true");
                query.append_pair("limit", "50");
                if let Some(id) = trigger_id {
                    query.append_pair("trigger_ids", id);
                }
                if !cursor.is_empty() {
                    query.append_pair("cursor", &cursor);
                }
            }
            let path = format!(
                "{}?{}",
                url.path(),
                url.query().ok_or(Error::InvalidRequest)?
            );
            let page: TriggerPage = serde_json::from_value(
                self.client
                    .request(scope, Method::GET, &path, None, false)
                    .await?,
            )
            .map_err(|_| Error::InvalidResponse)?;
            if page.items.len() > 50 {
                return Err(Error::InvalidResponse);
            }
            for item in page.items {
                if item.connected_account_id != filter.account
                    || item.user_id != filter.user
                    || item.trigger_name != filter.slug
                    || trigger_id.is_some_and(|id| id != item.id)
                {
                    return Err(Error::AccountMismatch);
                }
                // This basic subscription intentionally has no sender/subject
                // filtering. Do not reuse another configured/filtered instance.
                if !basic_config(filter.slug, &item.trigger_config) {
                    continue;
                }
                if !identifier(&item.id, 256) || !item.id.starts_with("ti_") || found.is_some() {
                    return Err(Error::InvalidResponse);
                }
                found = Some(ProviderTrigger {
                    trigger_id: item.id,
                    trigger_slug: item.trigger_name,
                    account_id: item.connected_account_id,
                    composio_user_id: item.user_id,
                    enabled: item.disabled_at.is_none(),
                });
            }
            let Some(next) = page.next_cursor.filter(|value| !value.is_empty()) else {
                return Ok(found);
            };
            if next.len() > 2048 || next.chars().any(char::is_control) || !seen.insert(next.clone())
            {
                return Err(Error::InvalidResponse);
            }
            cursor = next;
        }
        Err(Error::InvalidResponse)
    }
}

struct TriggerFilter<'a> {
    account: &'a str,
    config: &'a str,
    user: &'a str,
    slug: &'a str,
}
#[derive(Deserialize)]
struct TriggerPage {
    items: Vec<TriggerRecord>,
    next_cursor: Option<String>,
}
#[derive(Deserialize)]
struct TriggerRecord {
    id: String,
    connected_account_id: String,
    trigger_name: String,
    user_id: String,
    trigger_config: Value,
    disabled_at: Option<String>,
}
#[derive(Deserialize)]
struct Upsert {
    trigger_id: String,
}

fn basic_config(slug: &str, value: &Value) -> bool {
    let Some(config) = value.as_object() else {
        return false;
    };
    // Outlook may materialize its documented default inbox/optional nulls.
    // Non-default sender, subject, recipient, importance or attachment filters
    // must not suppress messages from this unfiltered subscription.
    config
        .iter()
        .all(|(key, value)| match (slug, key.as_str()) {
            ("OUTLOOK_MESSAGE_TRIGGER", "folder_id") => {
                value.is_null() || value.as_str() == Some("inbox")
            }
            (
                "OUTLOOK_MESSAGE_TRIGGER",
                "from_email" | "to_email" | "subject_contains" | "importance" | "has_attachments",
            ) => value.is_null(),
            _ => false,
        })
}

#[async_trait]
impl TriggerPort for ProjectTriggers {
    async fn ensure(
        &self,
        scope: &Scope,
        expected: &SubscriptionSnapshot,
    ) -> Result<ProviderTrigger, Error> {
        if !expected.enabled
            || !expected.usable
            || expected.composio_user_id != scope.composio_user_id()
        {
            return Err(Error::Forbidden);
        }
        let provider = self.provider(&expected.trigger_slug, &expected.auth_config_id)?;
        if (provider == MailProvider::Google && expected.provider != "google")
            || (provider == MailProvider::Microsoft && expected.provider != "microsoft")
        {
            return Err(Error::AccountMismatch);
        }
        self.client
            .account(scope, &expected.account_id, None, provider)
            .await?;
        let filter = TriggerFilter {
            account: &expected.account_id,
            config: &expected.auth_config_id,
            user: &expected.composio_user_id,
            slug: &expected.trigger_slug,
        };
        if let Some(active) = self
            .find(scope, &filter, None)
            .await?
            .filter(|value| value.enabled)
        {
            return Ok(active);
        }
        // Official v3.1 same-configuration upsert is idempotent and re-enables a
        // disabled matching instance. Pin the exact account, never first-by-user.
        let reply: Upsert = serde_json::from_value(self.client.request(
            scope, Method::POST, &format!("/api/v3.1/trigger_instances/{}/upsert", filter.slug),
            Some(json!({"connected_account_id":filter.account, "user_id":filter.user, "trigger_config":{}})), true,
        ).await?).map_err(|_| Error::OutcomeUnknown)?;
        if !identifier(&reply.trigger_id, 256) || !reply.trigger_id.starts_with("ti_") {
            return Err(Error::InvalidResponse);
        }
        let actual = self
            .find(scope, &filter, Some(&reply.trigger_id))
            .await?
            .ok_or(Error::OutcomeUnknown)?;
        if !actual.enabled {
            return Err(Error::OutcomeUnknown);
        }
        Ok(actual)
    }

    async fn disable(&self, scope: &Scope, binding: &SubscriptionBinding) -> Result<(), Error> {
        binding.validate().map_err(|_| Error::Forbidden)?;
        if binding.tenant != scope.tenant()
            || binding.user_id != scope.user_id()
            || binding.workspace != scope.workspace()
            || binding.composio_user_id != scope.composio_user_id()
        {
            return Err(Error::Forbidden);
        }
        self.provider(&binding.trigger_slug, &binding.auth_config_id)?;
        let filter = TriggerFilter {
            account: &binding.account_id,
            config: &binding.auth_config_id,
            user: &binding.composio_user_id,
            slug: &binding.trigger_slug,
        };
        let Some(actual) = self.find(scope, &filter, Some(&binding.trigger_id)).await? else {
            return Ok(());
        };
        if !actual.enabled {
            return Ok(());
        }
        let result = self
            .client
            .request(
                scope,
                Method::PATCH,
                &format!("/api/v3.1/trigger_instances/manage/{}", actual.trigger_id),
                Some(json!({"status":"disable", "user_id":filter.user})),
                true,
            )
            .await?;
        if result.get("status").and_then(Value::as_str) != Some("success") {
            return Err(Error::OutcomeUnknown);
        }
        Ok(())
    }
}
