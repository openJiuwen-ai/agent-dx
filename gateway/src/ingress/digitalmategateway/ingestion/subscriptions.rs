//! Reconcile existing owner intent with provider triggers and the sole Inbox.
//! No mail rules, credential store, intent writer or additional scheduler lives here.
use super::super::composio::{Error as ProviderError, Scope};
use super::{Error, Inbox, SubscriptionBinding};
use async_trait::async_trait;
use sha2::{Digest, Sha256};
use std::sync::Arc;

/// An authenticated owner's current facts, never a request JSON authority.
/// The production adapter must match Connection/Metadata/Intent and obtain the
/// routing tuple from the actual issuer/Binding, not a sandbox-supplied target.
#[derive(Clone, Eq, PartialEq)]
pub struct SubscriptionSnapshot {
    pub binding_scope: adx_agent_core::Scope,
    pub user_id: String,
    pub workspace: String,
    pub binding_generation: String,
    pub service_port: u16,
    pub connection_id: String,
    pub provider: String,
    /// Selected by trusted provider configuration, never the request body.
    pub trigger_slug: String,
    pub account_id: String,
    pub auth_config_id: String,
    pub composio_user_id: String,
    pub connection_generation: u64,
    pub record_revision: u64,
    pub intent_revision: u64,
    pub enabled: bool,
    pub usable: bool,
}

/// Provider/account facts supplied to the identity owner's v1 projection.
pub(crate) struct SubscriptionFacts {
    pub connection_id: String,
    pub provider: String,
    pub trigger_slug: String,
    pub account_id: String,
    pub auth_config_id: String,
    pub composio_user_id: String,
    pub connection_generation: u64,
    pub record_revision: u64,
    pub intent_revision: u64,
    pub enabled: bool,
    pub usable: bool,
}

/// Metadata returned only after the project client verifies the exact account,
/// configured auth config, immutable mapped user and configured trigger type.
pub struct ProviderTrigger {
    pub trigger_id: String,
    pub trigger_slug: String,
    pub account_id: String,
    pub composio_user_id: String,
    pub enabled: bool,
}

#[async_trait]
pub trait IntentPort: Send + Sync {
    /// None means explicitly deleted. Transport or permission failures are errors.
    async fn current(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<Option<SubscriptionSnapshot>, ProviderError>;
}

#[async_trait]
pub trait TriggerPort: Send + Sync {
    /// Use the existing project client/budget and exact connected account.
    /// Reconcile unknown outcomes via the provider's same-config upsert/list;
    /// never create a new account, select a first account or accept a user URL.
    async fn ensure(
        &self,
        scope: &Scope,
        expected: &SubscriptionSnapshot,
    ) -> Result<ProviderTrigger, ProviderError>;
    /// The binding is the previously verified Inbox record, not client JSON.
    /// Retry the same disable after unknown completion; do not erase its record.
    async fn disable(
        &self,
        scope: &Scope,
        binding: &SubscriptionBinding,
    ) -> Result<(), ProviderError>;
}

#[derive(Debug, thiserror::Error)]
pub enum SubscriptionError {
    #[error(transparent)]
    Ingestion(#[from] Error),
    #[error(transparent)]
    Provider(#[from] ProviderError),
}

pub enum SubscriptionOutcome {
    Enabled(Box<SubscriptionBinding>),
    Disabled,
}

pub struct SubscriptionCoordinator {
    inbox: Inbox,
    intents: Arc<dyn IntentPort>,
    triggers: Arc<dyn TriggerPort>,
}

impl SubscriptionCoordinator {
    pub fn new(inbox: Inbox, intents: Arc<dyn IntentPort>, triggers: Arc<dyn TriggerPort>) -> Self {
        Self {
            inbox,
            intents,
            triggers,
        }
    }

    /// Synchronize one explicitly selected connection using current owner facts.
    /// This consumes a saved intent; it never enables or changes that intent.
    /// Durable provider receipt and domain processing keep their existing owners.
    #[tracing::instrument(skip_all, name = "digitalmate.subscription.sync")]
    pub async fn synchronize(
        &self,
        scope: &Scope,
        connection_id: &str,
    ) -> Result<SubscriptionOutcome, SubscriptionError> {
        if !bounded(connection_id) {
            return Err(ProviderError::InvalidRequest.into());
        }
        let current = self.current(scope, connection_id).await?;
        let id = subscription_id(scope, connection_id);
        let previous = self.inbox.subscription(&id).await?;
        if let Some(binding) = &previous {
            if binding.tenant != scope.tenant()
                || binding.user_id != scope.user_id()
                || binding.workspace != scope.workspace()
                || binding.connection_id != connection_id
            {
                return Err(ProviderError::Forbidden.into());
            }
        }
        let Some(expected) = current.as_ref().filter(|value| value.enabled) else {
            if let Some(binding) = &previous {
                // Fence locally before the external API, retaining the cleanup
                // target even when the upstream response is unavailable/unknown.
                self.inbox.disable_subscription(binding).await?;
                self.assert_current(scope, connection_id, &current).await?;
                self.triggers.disable(scope, binding).await?;
                self.assert_current(scope, connection_id, &current).await?;
            }
            tracing::debug!(outcome = "disabled", "connector subscription synchronized");
            return Ok(SubscriptionOutcome::Disabled);
        };
        if !expected.usable {
            // A temporary validation/read-permission failure is not user opt-out.
            return Err(ProviderError::ConsentRequired.into());
        }
        if previous.as_ref().is_some_and(|binding| {
            binding.subscription_generation > expected.intent_revision
                || binding.connection_generation > expected.connection_generation
                || (!binding.enabled && binding.subscription_generation == expected.intent_revision)
        }) {
            return Err(Error::LeaseLost.into());
        }
        self.assert_current(scope, connection_id, &current).await?;
        let trigger = self.triggers.ensure(scope, expected).await?;
        if !trigger.enabled
            || !identifier(&trigger.trigger_id)
            || !trigger.trigger_id.starts_with("ti_")
            || trigger.trigger_slug != expected.trigger_slug
            || trigger.account_id != expected.account_id
            || trigger.composio_user_id != expected.composio_user_id
        {
            return Err(ProviderError::AccountMismatch.into());
        }
        self.assert_current(scope, connection_id, &current).await?;
        let binding = expected.binding(id, trigger.trigger_id);
        binding.validate()?;
        self.inbox.register(&binding).await?;
        // A revocation may race the short cross-owner commit. The receiver also
        // reads live owner intent; a stale registration alone cannot authorize it.
        let after = self.current(scope, connection_id).await?;
        if after != current {
            if after.as_ref().is_none_or(|value| {
                !value.enabled
                    || value.intent_revision != expected.intent_revision
                    || value.connection_generation != expected.connection_generation
                    || value.binding_generation != expected.binding_generation
            }) {
                // CAS never stops a replacement that another synchronizer committed.
                match self.inbox.disable_subscription(&binding).await {
                    Ok(()) | Err(Error::LeaseLost) => {}
                    Err(error) => return Err(error.into()),
                }
            }
            return Err(Error::LeaseLost.into());
        }
        tracing::debug!(outcome = "enabled", "connector subscription synchronized");
        Ok(SubscriptionOutcome::Enabled(Box::new(binding)))
    }

    async fn current(
        &self,
        scope: &Scope,
        id: &str,
    ) -> Result<Option<SubscriptionSnapshot>, SubscriptionError> {
        let result = self.intents.current(scope, id).await?;
        if let Some(current) = &result {
            current.validate(scope, id)?;
        }
        Ok(result)
    }

    async fn assert_current(
        &self,
        scope: &Scope,
        id: &str,
        expected: &Option<SubscriptionSnapshot>,
    ) -> Result<(), SubscriptionError> {
        if self.current(scope, id).await? == *expected {
            Ok(())
        } else {
            Err(Error::LeaseLost.into())
        }
    }
}

impl SubscriptionSnapshot {
    fn validate(&self, scope: &Scope, connection_id: &str) -> Result<(), ProviderError> {
        if self.connection_id != connection_id
            || self.binding_scope.tenant != scope.tenant()
            || self.user_id != scope.user_id()
            || self.workspace != scope.workspace()
            || self.binding_scope.validate().is_err()
            || !bounded(&self.binding_generation)
            || self.service_port == 0
            || !identifier(&self.provider)
            || !identifier(&self.trigger_slug)
        {
            return Err(ProviderError::Forbidden);
        }
        if self.enabled
            && (!identifier(&self.account_id)
                || !self.account_id.starts_with("ca_")
                || !identifier(&self.auth_config_id)
                || !self.auth_config_id.starts_with("ac_")
                || self.composio_user_id != scope.composio_user_id()
                || self.connection_generation == 0
                || self.connection_generation > i64::MAX as u64
                || self.intent_revision == 0
                || self.intent_revision > i64::MAX as u64
                || self.record_revision == 0)
        {
            return Err(ProviderError::Forbidden);
        }
        Ok(())
    }

    fn binding(&self, id: String, trigger_id: String) -> SubscriptionBinding {
        SubscriptionBinding {
            tenant: self.binding_scope.tenant.clone(),
            user_id: self.user_id.clone(),
            connection_id: self.connection_id.clone(),
            template: self.binding_scope.template.clone(),
            template_ref: self.binding_scope.template_ref.name().to_owned(),
            subscription_id: id,
            subscription_generation: self.intent_revision,
            connection_generation: self.connection_generation,
            workspace: self.workspace.clone(),
            binding_id: self.binding_scope.binding_id.clone(),
            binding_generation: self.binding_generation.clone(),
            service_port: self.service_port,
            trigger_id,
            trigger_slug: self.trigger_slug.clone(),
            account_id: self.account_id.clone(),
            auth_config_id: self.auth_config_id.clone(),
            composio_user_id: self.composio_user_id.clone(),
            enabled: true,
        }
    }
}

fn subscription_id(scope: &Scope, connection_id: &str) -> String {
    let mut digest = Sha256::new();
    digest.update(b"digitalmate.subscription.v1\0");
    for part in [
        scope.tenant(),
        scope.user_id(),
        scope.workspace(),
        connection_id,
    ] {
        digest.update(part.as_bytes());
        digest.update([0]);
    }
    format!("sub_{}", hex::encode(digest.finalize()))
}

fn bounded(value: &str) -> bool {
    !value.is_empty() && value.len() <= 256 && !value.chars().any(char::is_control)
}

fn identifier(value: &str) -> bool {
    bounded(value)
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"_-:".contains(&byte))
}
