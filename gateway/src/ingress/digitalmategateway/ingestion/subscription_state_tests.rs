//! The actual ingestion owner/PG boundary; provider and mail data are synthetic.
use super::super::composio::WebhookVerifier;
use super::persistence_tests::store;
use super::tests::{binding, headers, NOW, SECRET};
use super::*;
use serde_json::json;
use std::time::Duration;

async fn receive_for(inbox: &Inbox, current: &SubscriptionBinding, id: &str) {
    let raw = serde_json::to_vec(&json!({
        "id":id, "type":"composio.trigger.message", "metadata":{
            "trigger_id":current.trigger_id, "trigger_slug":current.trigger_slug,
            "connected_account_id":current.account_id, "auth_config_id":current.auth_config_id,
            "user_id":current.composio_user_id
        }, "data":{}
    }))
    .unwrap();
    let event = WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap();
    inbox.receive(event).await.unwrap();
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn subscription_state_survives_reopen_and_stale_disable_cannot_fence_a_new_epoch() {
    let inbox = store().await;
    let old = binding();
    assert!(inbox
        .subscription(&old.subscription_id)
        .await
        .unwrap()
        .is_none());
    inbox.register(&old).await.unwrap();
    let reopened = Inbox::from_pool(inbox.pool().clone(), inbox.limits().clone())
        .await
        .unwrap();
    assert!(reopened.subscription(&old.subscription_id).await.unwrap() == Some(old.clone()));

    let mut next = old.clone();
    next.subscription_generation += 1;
    next.connection_generation += 1;
    inbox.register(&next).await.unwrap();
    assert_eq!(
        reopened.disable_subscription(&old).await,
        Err(Error::LeaseLost)
    );
    assert!(inbox.subscription(&old.subscription_id).await.unwrap() == Some(next.clone()));
    reopened.disable_subscription(&next).await.unwrap();
    let mut stopped = next.clone();
    stopped.enabled = false;
    assert!(inbox.subscription(&old.subscription_id).await.unwrap() == Some(stopped));
    // A retry reconciles the same stop without making a new authorization epoch.
    reopened.disable_subscription(&next).await.unwrap();
    // The old enabled projection must not resurrect a locally fenced epoch.
    assert_eq!(inbox.register(&next).await, Err(Error::ConflictingInput));
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn subscription_state_stop_only_retires_its_account_and_preserves_another_active_lease() {
    let inbox = store().await;
    let first = binding();
    let mut other = first.clone();
    other.subscription_id = "sub_other".into();
    other.connection_id = "connection_other".into();
    other.account_id = "ca_other".into();
    other.trigger_id = "ti_other".into();
    inbox.register(&first).await.unwrap();
    inbox.register(&other).await.unwrap();
    receive_for(&inbox, &first, "first_event").await;
    let active = inbox.claim(Duration::from_secs(30)).await.unwrap().unwrap();
    receive_for(&inbox, &other, "other_event").await;
    inbox.disable_subscription(&other).await.unwrap();

    // Stopping another account must neither cancel nor release this lease.
    inbox.renew(&active, Duration::from_secs(30)).await.unwrap();
    inbox
        .accept(
            &active,
            &DurableAck {
                version: 2,
                batch_id: active.batch.batch_id.clone(),
                digest: active.batch.digest.clone(),
                state: "accepted".into(),
            },
        )
        .await
        .unwrap();
    assert!(inbox
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .is_none());
    let client = inbox.pool().get().await.unwrap();
    let row = client
        .query_one(
            "SELECT pending_events,pending_bytes FROM digitalmate_ingestion.capacity WHERE singleton=true",
            &[],
        )
        .await
        .unwrap();
    assert_eq!((row.get::<_, i64>(0), row.get::<_, i64>(1)), (0, 0));
    drop(client);
    // The sole stopped epoch's previously unknown input cannot re-enter.
    let mut enabled = other.clone();
    enabled.subscription_generation += 1;
    inbox.register(&enabled).await.unwrap();
    receive_for(&inbox, &enabled, "after_reenable").await;
    assert!(inbox
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .is_some());
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn subscription_state_stop_fences_its_in_flight_batch_and_checks_the_entire_expected_target()
{
    let inbox = store().await;
    let current = binding();
    inbox.register(&current).await.unwrap();
    receive_for(&inbox, &current, "in_flight").await;
    let active = inbox.claim(Duration::from_secs(30)).await.unwrap().unwrap();
    let mut foreign = current.clone();
    foreign.user_id = "another_user".into();
    assert_eq!(
        inbox.disable_subscription(&foreign).await,
        Err(Error::LeaseLost)
    );
    inbox.renew(&active, Duration::from_secs(30)).await.unwrap();
    inbox.disable_subscription(&current).await.unwrap();
    assert_eq!(
        inbox.renew(&active, Duration::from_secs(30)).await,
        Err(Error::LeaseLost)
    );
    assert!(inbox
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .is_none());
}
