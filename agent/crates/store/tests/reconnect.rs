use adx_agent_store::*;
use std::time::Duration;
static TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

#[tokio::test]
#[ignore = "requires a disposable ADX_AGENT_TEST_REDIS_URL; kills normal test connections"]
async fn reconnect_without_replaying_unknown_write() {
    let _guard = TEST_LOCK.lock().await;
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").expect("set disposable Redis URL");
    let store = RedisRepository::connect(
        &url,
        &format!("reconnect-{}", uuid::Uuid::new_v4()),
        Duration::from_secs(3),
    )
    .await
    .unwrap();
    let key = Key::new("instance", &["tenant", "id"]).unwrap();
    let mut admin = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: u64 = redis::cmd("CLIENT")
        .arg("KILL")
        .arg("TYPE")
        .arg("normal")
        .arg("SKIPME")
        .arg("yes")
        .query_async(&mut admin)
        .await
        .unwrap();
    let record = Record::new(&serde_json::json!({"phase":"creating"})).unwrap();
    let tx = Transaction::new(
        vec![Check {
            key: key.clone(),
            expected: None,
        }],
        vec![Put {
            key: key.clone(),
            record,
        }],
    )
    .unwrap();
    assert!(matches!(
        store.commit(&tx).await,
        Err(Error::OutcomeUnknown(_))
    ));
    // The next read reconnects, and confirms no hidden automatic write replay occurred.
    assert!(store.get(&key).await.unwrap().is_none());
    assert!(store.commit(&tx).await.unwrap());
    assert!(store.get(&key).await.unwrap().is_some());
}
