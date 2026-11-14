//! Actual Inbox/PG; the intent and provider ports below are named controlled substitutes.
use super::super::composio::{Error as ProviderError, Scope};
use super::persistence_tests::store;
use super::subscriptions::*;
use super::{Inbox, SubscriptionBinding};
use async_trait::async_trait;
use std::{
    collections::BTreeMap,
    sync::{
        atomic::{AtomicBool, AtomicUsize, Ordering},
        Arc, Mutex,
    },
};

#[derive(Default)]
struct ControlledIntent {
    values: Mutex<BTreeMap<String, SubscriptionSnapshot>>,
    unavailable: AtomicBool,
}

#[async_trait]
impl IntentPort for ControlledIntent {
    async fn current(
        &self,
        _: &Scope,
        id: &str,
    ) -> Result<Option<SubscriptionSnapshot>, ProviderError> {
        if self.unavailable.load(Ordering::SeqCst) {
            return Err(ProviderError::UpstreamUnavailable);
        }
        Ok(self.values.lock().unwrap().get(id).cloned())
    }
}

struct ControlledTriggers {
    owner: Arc<ControlledIntent>,
    enables: AtomicUsize,
    disables: AtomicUsize,
    replace_during_enable: AtomicBool,
    wrong_account: AtomicBool,
    disable_unavailable: AtomicBool,
}

#[async_trait]
impl TriggerPort for ControlledTriggers {
    async fn ensure(
        &self,
        _: &Scope,
        current: &SubscriptionSnapshot,
    ) -> Result<ProviderTrigger, ProviderError> {
        self.enables.fetch_add(1, Ordering::SeqCst);
        if self.replace_during_enable.load(Ordering::SeqCst) {
            let mut next = current.clone();
            next.enabled = false;
            next.intent_revision += 1;
            self.owner
                .values
                .lock()
                .unwrap()
                .insert(current.connection_id.clone(), next);
        }
        Ok(ProviderTrigger {
            trigger_id: format!("ti_{}", current.account_id),
            trigger_slug: current.trigger_slug.clone(),
            account_id: if self.wrong_account.load(Ordering::SeqCst) {
                "ca_foreign".into()
            } else {
                current.account_id.clone()
            },
            composio_user_id: current.composio_user_id.clone(),
            enabled: true,
        })
    }

    async fn disable(&self, _: &Scope, _: &SubscriptionBinding) -> Result<(), ProviderError> {
        self.disables.fetch_add(1, Ordering::SeqCst);
        if self.disable_unavailable.load(Ordering::SeqCst) {
            return Err(ProviderError::UpstreamUnavailable);
        }
        Ok(())
    }
}

fn fixtures() -> (
    Scope,
    SubscriptionSnapshot,
    Arc<ControlledIntent>,
    Arc<ControlledTriggers>,
) {
    let binding = super::tests::binding();
    let scope = Scope::from_authenticated_foreground(
        binding.tenant.clone(),
        binding.user_id.clone(),
        binding.workspace.clone(),
    )
    .unwrap();
    let snapshot = SubscriptionSnapshot {
        binding_scope: adx_agent_core::Scope {
            tenant: binding.tenant,
            template: binding.template,
            template_ref: binding.template_ref.parse().unwrap(),
            binding_id: binding.binding_id,
        },
        user_id: binding.user_id,
        workspace: binding.workspace,
        authority_ref: binding.authority_ref,
        connection_id: binding.connection_id,
        provider: "microsoft".into(),
        trigger_slug: "OUTLOOK_MESSAGE_TRIGGER".into(),
        account_id: binding.account_id,
        auth_config_id: binding.auth_config_id,
        composio_user_id: scope.composio_user_id(),
        connection_generation: binding.connection_generation,
        record_revision: 1,
        intent_revision: binding.subscription_generation,
        enabled: true,
        usable: true,
    };
    let owner = Arc::new(ControlledIntent::default());
    owner
        .values
        .lock()
        .unwrap()
        .insert(snapshot.connection_id.clone(), snapshot.clone());
    let triggers = Arc::new(ControlledTriggers {
        owner: owner.clone(),
        enables: AtomicUsize::new(0),
        disables: AtomicUsize::new(0),
        replace_during_enable: AtomicBool::new(false),
        wrong_account: AtomicBool::new(false),
        disable_unavailable: AtomicBool::new(false),
    });
    (scope, snapshot, owner, triggers)
}

fn registered(outcome: SubscriptionOutcome) -> SubscriptionBinding {
    match outcome {
        SubscriptionOutcome::Enabled(binding) => *binding,
        SubscriptionOutcome::Disabled => panic!("the enabled fixture must register a subscription"),
    }
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn subscription_sync_multiple_accounts_and_non_mail_profile_share_the_real_inbox() {
    let inbox = store().await;
    let (scope, first, owner, triggers) = fixtures();
    let coordinator = SubscriptionCoordinator::new(inbox.clone(), owner.clone(), triggers.clone());
    let a = registered(
        coordinator
            .synchronize(&scope, &first.connection_id)
            .await
            .unwrap(),
    );
    let mut second = first.clone();
    second.connection_id = "github_connection".into();
    second.provider = "github".into();
    second.trigger_slug = "GITHUB_COMMIT_EVENT".into();
    second.account_id = "ca_second".into();
    owner
        .values
        .lock()
        .unwrap()
        .insert(second.connection_id.clone(), second.clone());
    let b = registered(
        coordinator
            .synchronize(&scope, &second.connection_id)
            .await
            .unwrap(),
    );
    assert_ne!(a.subscription_id, b.subscription_id);
    assert_eq!(a.trigger_slug, "OUTLOOK_MESSAGE_TRIGGER");
    assert_eq!(b.trigger_slug, "GITHUB_COMMIT_EVENT");
    assert_eq!(b.subscription_generation, second.intent_revision);
    assert!(inbox.subscription(&a.subscription_id).await.unwrap() == Some(a.clone()));
    assert!(inbox.subscription(&b.subscription_id).await.unwrap() == Some(b.clone()));
    let mut stopped = first.clone();
    stopped.enabled = false;
    stopped.intent_revision += 1;
    owner
        .values
        .lock()
        .unwrap()
        .insert(first.connection_id.clone(), stopped);
    assert!(matches!(
        coordinator
            .synchronize(&scope, &first.connection_id)
            .await
            .unwrap(),
        SubscriptionOutcome::Disabled
    ));
    assert!(
        !inbox
            .subscription(&a.subscription_id)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    assert!(inbox.subscription(&b.subscription_id).await.unwrap() == Some(b));
    assert_eq!(triggers.disables.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn subscription_sync_replaced_intent_or_foreign_provider_response_never_registers() {
    let inbox = store().await;
    let (scope, snapshot, owner, triggers) = fixtures();
    let coordinator = SubscriptionCoordinator::new(inbox.clone(), owner.clone(), triggers.clone());
    triggers.wrong_account.store(true, Ordering::SeqCst);
    assert!(coordinator
        .synchronize(&scope, &snapshot.connection_id)
        .await
        .is_err());
    let client = inbox.pool().get().await.unwrap();
    assert_eq!(
        client
            .query_one(
                "SELECT count(*)::bigint FROM digitalmate_ingestion.subscriptions",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    drop(client);
    triggers.wrong_account.store(false, Ordering::SeqCst);
    triggers.replace_during_enable.store(true, Ordering::SeqCst);
    assert!(coordinator
        .synchronize(&scope, &snapshot.connection_id)
        .await
        .is_err());
    let client = inbox.pool().get().await.unwrap();
    assert_eq!(
        client
            .query_one(
                "SELECT count(*)::bigint FROM digitalmate_ingestion.subscriptions",
                &[]
            )
            .await
            .unwrap()
            .get::<_, i64>(0),
        0
    );
    // A stale call cannot disable a possibly newer owner's provider registration.
    assert_eq!(triggers.disables.load(Ordering::SeqCst), 0);
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn subscription_sync_network_failure_keeps_intent_and_deleted_account_stops_with_retry() {
    let inbox = store().await;
    let (scope, snapshot, owner, triggers) = fixtures();
    let coordinator = SubscriptionCoordinator::new(inbox.clone(), owner.clone(), triggers.clone());
    let binding = registered(
        coordinator
            .synchronize(&scope, &snapshot.connection_id)
            .await
            .unwrap(),
    );
    owner.unavailable.store(true, Ordering::SeqCst);
    assert!(coordinator
        .synchronize(&scope, &snapshot.connection_id)
        .await
        .is_err());
    assert!(inbox.subscription(&binding.subscription_id).await.unwrap() == Some(binding.clone()));
    assert_eq!(triggers.disables.load(Ordering::SeqCst), 0);
    owner.unavailable.store(false, Ordering::SeqCst);
    owner.values.lock().unwrap().remove(&snapshot.connection_id);
    triggers.disable_unavailable.store(true, Ordering::SeqCst);
    assert!(coordinator
        .synchronize(&scope, &snapshot.connection_id)
        .await
        .is_err());
    assert!(
        !inbox
            .subscription(&binding.subscription_id)
            .await
            .unwrap()
            .unwrap()
            .enabled
    );
    triggers.disable_unavailable.store(false, Ordering::SeqCst);
    let reopened = Inbox::from_pool(inbox.pool().clone(), inbox.limits().clone())
        .await
        .unwrap();
    let restarted = SubscriptionCoordinator::new(reopened, owner.clone(), triggers.clone());
    assert!(matches!(
        restarted
            .synchronize(&scope, &snapshot.connection_id)
            .await
            .unwrap(),
        SubscriptionOutcome::Disabled
    ));
    assert_eq!(triggers.disables.load(Ordering::SeqCst), 2);
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn subscription_sync_wrong_workspace_and_unusable_permission_cannot_create_a_trigger() {
    let inbox = store().await;
    let (scope, snapshot, owner, triggers) = fixtures();
    let coordinator = SubscriptionCoordinator::new(inbox, owner.clone(), triggers.clone());
    let mut foreign = snapshot.clone();
    foreign.workspace = "someone_else".into();
    owner
        .values
        .lock()
        .unwrap()
        .insert(snapshot.connection_id.clone(), foreign);
    assert!(coordinator
        .synchronize(&scope, &snapshot.connection_id)
        .await
        .is_err());
    let mut unavailable = snapshot.clone();
    unavailable.usable = false;
    owner
        .values
        .lock()
        .unwrap()
        .insert(snapshot.connection_id.clone(), unavailable);
    assert!(coordinator
        .synchronize(&scope, &snapshot.connection_id)
        .await
        .is_err());
    assert_eq!(triggers.enables.load(Ordering::SeqCst), 0);
    assert_eq!(triggers.disables.load(Ordering::SeqCst), 0);
}
