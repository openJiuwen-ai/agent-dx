//! Actual PostgreSQL owner paths; provider events/policy owner are controlled.
use super::super::composio::WebhookVerifier;
use super::persistence_tests::store;
use super::tests::{binding, body, headers, NOW, SECRET};
use super::*;
use serde_json::json;
use std::time::Duration;

fn policy(action: &str) -> ScreeningPolicy {
    serde_json::from_value(json!({"version":1,"rules":[{
        "id":"explicit-scope","attribute":"label","value":"SPAM","action":action
    }]}))
    .unwrap()
}

fn proof(id: &str, labels: serde_json::Value) -> super::super::composio::VerifiedWebhook {
    let mut wire: serde_json::Value =
        serde_json::from_slice(&body(id, "same sender, subject and thread")).unwrap();
    wire["data"]["label_ids"] = labels;
    wire["data"]["subject"] = json!("same subject");
    wire["data"]["sender"] = json!("same@example.test");
    wire["data"]["thread_id"] = json!("same-thread");
    let raw = serde_json::to_vec(&wire).unwrap();
    WebhookVerifier::new(&[SECRET])
        .unwrap()
        .verify(&headers(&raw, NOW, SECRET), &raw, NOW)
        .unwrap()
}

async fn receive(inbox: &Inbox, id: &str, labels: serde_json::Value) -> ReceiveReceipt {
    inbox.receive(proof(id, labels)).await.unwrap()
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn screening_persists_decisions_and_only_accepted_events_are_scheduled() {
    let inbox = store().await;
    let binding = binding();
    inbox
        .register_policy(&binding, &policy("suppress"), 7)
        .await
        .unwrap();
    let excluded = receive(&inbox, "excluded", json!(["SPAM"])).await;
    assert_eq!(excluded.screening.action, ScreeningAction::Suppress);
    assert_eq!(excluded.screening.policy_revision, 7);
    assert!(inbox
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .is_none());
    let replay = receive(&inbox, "excluded", json!(["SPAM"])).await;
    assert!(replay.duplicate);
    assert!(replay.screening == excluded.screening);
    assert!(matches!(
        inbox.receive(proof("excluded", json!([]))).await,
        Err(Error::ConflictingInput)
    ));
    // Missing/malformed fields and provider marketing categories do not prove
    // this explicit exclusion. Similar content is not an input identity.
    for (id, labels) in [
        ("unknown", json!(null)),
        ("promotion", json!(["CATEGORY_PROMOTIONS"])),
        ("ordinary-a", json!([])),
        ("ordinary-b", json!([])),
    ] {
        assert_eq!(
            receive(&inbox, id, labels).await.screening.action,
            ScreeningAction::Accept
        );
    }
    let reopened = Inbox::from_pool(inbox.pool().clone(), inbox.limits().clone())
        .await
        .unwrap();
    let lease = reopened
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(lease.batch.events.len(), 4);
    assert!(!lease
        .batch
        .events
        .iter()
        .any(|e| e.source_event_id == "excluded"));
    let client = inbox.pool().get().await.unwrap();
    let raw: Vec<u8> = client
        .query_one(
            "SELECT raw FROM digitalmate_ingestion.events WHERE event_id='excluded'",
            &[],
        )
        .await
        .unwrap()
        .get(0);
    assert!(raw.is_empty());
    let row = client
        .query_one(
            "SELECT pending_events,retained_events FROM digitalmate_ingestion.capacity",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(row.get::<_, i64>(0), 4);
    assert_eq!(row.get::<_, i64>(1), 5);
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn rules_only_updates_preserve_inflight_identity_and_refuse_policy_rollback() {
    let inbox = store().await;
    let binding = binding();
    inbox
        .register_policy(&binding, &ScreeningPolicy::default(), 1)
        .await
        .unwrap();
    receive(&inbox, "before-choice", json!(["SPAM"])).await;
    let lease = inbox.claim(Duration::from_secs(30)).await.unwrap().unwrap();
    inbox
        .register_policy(&binding, &policy("suppress"), 2)
        .await
        .unwrap();
    let bytes = serde_json::to_vec(&lease.batch).unwrap().len();
    assert!(inbox
        .delivery_binding(&lease.batch.batch_id, &lease.batch.digest, bytes)
        .await
        .is_ok());
    assert_eq!(
        receive(&inbox, "after-choice", json!(["SPAM"]))
            .await
            .screening
            .action,
        ScreeningAction::Suppress
    );
    assert!(inbox
        .register_policy(&binding, &ScreeningPolicy::default(), 1)
        .await
        .is_err());
    assert!(inbox
        .register_policy(&binding, &policy("quarantine"), 2)
        .await
        .is_err());
    inbox
        .accept(
            &lease,
            &DurableAck {
                version: 2,
                batch_id: lease.batch.batch_id.clone(),
                digest: lease.batch.digest.clone(),
                state: "accepted".into(),
            },
        )
        .await
        .unwrap();
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn quarantine_is_bounded_and_expires_through_the_store_owner() {
    let inbox = store().await;
    let mut limits = inbox.limits().clone();
    limits.per_subscription_events = 1;
    let inbox = Inbox::from_pool(inbox.pool().clone(), limits)
        .await
        .unwrap();
    let binding = binding();
    inbox
        .register_policy(&binding, &policy("quarantine"), 1)
        .await
        .unwrap();
    assert_eq!(
        receive(&inbox, "hold", json!(["SPAM"]))
            .await
            .screening
            .action,
        ScreeningAction::Quarantine
    );
    assert!(inbox
        .claim(Duration::from_secs(30))
        .await
        .unwrap()
        .is_none());
    assert!(matches!(
        inbox.receive(proof("over-capacity", json!(["SPAM"]))).await,
        Err(Error::Capacity)
    ));
    let client = inbox.pool().get().await.unwrap();
    let before = client
        .query_one(
            "SELECT pending_events,pending_bytes FROM digitalmate_ingestion.capacity",
            &[],
        )
        .await
        .unwrap();
    assert_eq!(before.get::<_, i64>(0), 1);
    assert!(before.get::<_, i64>(1) > 0);
    client.execute("UPDATE digitalmate_ingestion.events SET finished_at=clock_timestamp()-interval '8 days' WHERE event_id='hold'", &[]).await.unwrap();
    drop(client);
    assert_eq!(inbox.purge_terminal(10).await.unwrap(), 1);
    let client = inbox.pool().get().await.unwrap();
    let after = client.query_one("SELECT pending_events,pending_bytes,retained_events FROM digitalmate_ingestion.capacity", &[]).await.unwrap();
    assert_eq!(
        (
            after.get::<_, i64>(0),
            after.get::<_, i64>(1),
            after.get::<_, i64>(2)
        ),
        (0, 0, 0)
    );
}

#[tokio::test]
#[ignore = "requires an explicit task-owned PostgreSQL database"]
async fn screening_schema_upgrade_requires_a_quiesced_settled_owner() {
    let inbox = store().await;
    let client = inbox.pool().get().await.unwrap();
    // Reconstruct the previous columns/constraints from this test-owned schema.
    client.batch_execute("ALTER TABLE digitalmate_ingestion.subscriptions DROP COLUMN screening_json,DROP COLUMN screening_revision;
        ALTER TABLE digitalmate_ingestion.events DROP COLUMN decision_json;
        ALTER TABLE digitalmate_ingestion.events DROP CONSTRAINT events_state_check;
        ALTER TABLE digitalmate_ingestion.events ADD CHECK(state IN ('pending','batched','delivered','revoked'));
        ALTER TABLE digitalmate_ingestion.capacity DROP COLUMN retained_events;
        ALTER TABLE digitalmate_ingestion.version DROP CONSTRAINT version_version_check;
        UPDATE digitalmate_ingestion.version SET version=1;
        ALTER TABLE digitalmate_ingestion.version ADD CHECK(version=1);
        UPDATE digitalmate_ingestion.capacity SET pending_events=1;").await.unwrap();
    let upgrade = include_str!("upgrade_screening_v2.sql");
    assert!(client.batch_execute(upgrade).await.is_err());
    client.batch_execute("ROLLBACK").await.unwrap();
    assert_eq!(
        client
            .query_one("SELECT version FROM digitalmate_ingestion.version", &[])
            .await
            .unwrap()
            .get::<_, i32>(0),
        1
    );
    client
        .execute(
            "UPDATE digitalmate_ingestion.capacity SET pending_events=0",
            &[],
        )
        .await
        .unwrap();
    client.batch_execute(upgrade).await.unwrap();
    drop(client);
    assert!(
        Inbox::from_pool(inbox.pool().clone(), inbox.limits().clone())
            .await
            .is_ok()
    );
}
