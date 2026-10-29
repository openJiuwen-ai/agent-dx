#![cfg(feature = "test-memory")]
use adx_agent_core::activator::PublishRequest;
use adx_agent_store::{AgentState, MemoryRepository};
use std::sync::Arc;

fn request(tenant: &str, name: &str) -> PublishRequest {
    serde_json::from_value(serde_json::json!({
        "tenant":tenant,
        "template":{"name":name,"image":"app:1","isolation_runtime":"runc",
        "resources":{"cpu_millis":100,"memory_mib":128}}
    }))
    .unwrap()
}

#[tokio::test]
async fn publication_allocates_versions_per_tenant_and_template() {
    let repo = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repo.clone());
    let input = request("tenant", "agent");
    state.publish(&input.tenant, &input.template).await.unwrap();
    state.publish(&input.tenant, &input.template).await.unwrap();
    for version in ["1", "2"] {
        assert_eq!(
            state
                .template("tenant", "agent", version)
                .await
                .unwrap()
                .unwrap()
                .version,
            version
        );
    }
    let restarted = AgentState::new(repo);
    restarted
        .publish(&input.tenant, &input.template)
        .await
        .unwrap();
    assert!(restarted
        .template("tenant", "agent", "3")
        .await
        .unwrap()
        .is_some());
    for (tenant, name) in [("other", "agent"), ("tenant", "other")] {
        let input = request(tenant, name);
        restarted.publish(tenant, &input.template).await.unwrap();
        assert!(restarted
            .template(tenant, name, "1")
            .await
            .unwrap()
            .is_some());
        assert!(restarted
            .template(tenant, name, "2")
            .await
            .unwrap()
            .is_none());
    }
}

#[tokio::test]
async fn concurrent_publishers_cannot_overwrite_or_share_a_version() {
    let repo = Arc::new(MemoryRepository::default());
    let first = AgentState::new(repo.clone());
    let second = AgentState::new(repo);
    let a = request("tenant", "agent");
    let mut b = request("tenant", "agent");
    b.template.image = "app:2".into();
    let (a, b) = tokio::join!(
        first.publish("tenant", &a.template),
        second.publish("tenant", &b.template)
    );
    a.unwrap();
    b.unwrap();
    let one = first
        .template("tenant", "agent", "1")
        .await
        .unwrap()
        .unwrap();
    let two = first
        .template("tenant", "agent", "2")
        .await
        .unwrap()
        .unwrap();
    assert_ne!(one.image, two.image);
    assert!(first
        .template("tenant", "agent", "3")
        .await
        .unwrap()
        .is_none());
}

#[test]
fn publish_input_rejects_caller_assigned_versions() {
    let mut input = serde_json::to_value(request("tenant", "agent")).unwrap();
    input["template"]["version"] = "12".into();
    assert!(serde_json::from_value::<PublishRequest>(input).is_err());
}

#[tokio::test]
async fn missing_or_exhausted_sequence_never_overwrites_existing_versions() {
    use adx_agent_store::{Check, Error, Key, Put, Record, Repository, Transaction};
    let repo = Arc::new(MemoryRepository::default());
    let state = AgentState::new(repo.clone());
    let input = request("tenant", "agent");
    let original = input.template.with_version("1".into());
    let key = Key::new("template", &["tenant", "agent", "1"]).unwrap();
    assert!(repo
        .commit(
            &Transaction::new(
                vec![Check {
                    key: key.clone(),
                    expected: None
                }],
                vec![Put {
                    key: key.clone(),
                    record: Record::new(&original).unwrap()
                }]
            )
            .unwrap()
        )
        .await
        .unwrap());
    assert!(matches!(
        state.publish("tenant", &input.template).await,
        Err(Error::Conflict(_))
    ));
    assert_eq!(
        state
            .template("tenant", "agent", "1")
            .await
            .unwrap()
            .unwrap(),
        original
    );
    let sequence = Key::new("template-sequence", &["tenant", "agent"]).unwrap();
    assert!(repo.get(&sequence).await.unwrap().is_none());
    assert!(repo
        .commit(
            &Transaction::new(
                vec![Check {
                    key: sequence.clone(),
                    expected: None
                }],
                vec![Put {
                    key: sequence,
                    record: Record::new(&u64::MAX).unwrap()
                }]
            )
            .unwrap()
        )
        .await
        .unwrap());
    assert!(matches!(
        state.publish("tenant", &input.template).await,
        Err(Error::Conflict(_))
    ));
    assert!(state
        .template("tenant", "agent", "2")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires a disposable ADX_AGENT_TEST_REDIS_URL"]
async fn redis_independent_publishers_preserve_large_sequences_exactly() {
    use adx_agent_store::{Check, Key, Put, Record, RedisRepository, Repository, Transaction};
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("publication-{}", uuid::Uuid::new_v4());
    let a = Arc::new(
        RedisRepository::connect(&url, &namespace, std::time::Duration::from_secs(3))
            .await
            .unwrap(),
    );
    let b = Arc::new(
        RedisRepository::connect(&url, &namespace, std::time::Duration::from_secs(3))
            .await
            .unwrap(),
    );
    let key = Key::new("template-sequence", &["tenant", "agent"]).unwrap();
    let high = 9_007_199_254_740_992_u64;
    a.commit(
        &Transaction::new(
            vec![Check {
                key: key.clone(),
                expected: None,
            }],
            vec![Put {
                key,
                record: Record::new(&high).unwrap(),
            }],
        )
        .unwrap(),
    )
    .await
    .unwrap();
    let first = AgentState::new(a);
    let second = AgentState::new(b);
    let input = request("tenant", "agent");
    let (one, two) = tokio::join!(
        first.publish("tenant", &input.template),
        second.publish("tenant", &input.template)
    );
    let mut versions = [
        one.unwrap().version.parse::<u64>().unwrap(),
        two.unwrap().version.parse::<u64>().unwrap(),
    ];
    versions.sort();
    assert_eq!(versions, [high + 1, high + 2]);
    let last = first.publish("tenant", &input.template).await.unwrap();
    assert_eq!(last.version, (high + 3).to_string());
}

#[tokio::test]
async fn publication_does_not_replay_an_ambiguous_commit() {
    use adx_agent_store::{Error, Index, Key, Record, Repository, Transaction};
    use std::sync::atomic::{AtomicUsize, Ordering};
    #[derive(Default)]
    struct LostAck {
        inner: MemoryRepository,
        writes: AtomicUsize,
    }
    #[async_trait::async_trait]
    impl Repository for LostAck {
        async fn get(&self, key: &Key) -> Result<Option<Record>, Error> {
            self.inner.get(key).await
        }
        async fn page(
            &self,
            index: &Index,
            after: Option<&str>,
            limit: usize,
        ) -> Result<Vec<(String, Record)>, Error> {
            self.inner.page(index, after, limit).await
        }
        async fn commit(&self, transaction: &Transaction) -> Result<bool, Error> {
            self.writes.fetch_add(1, Ordering::SeqCst);
            self.inner.commit(transaction).await?;
            Err(Error::OutcomeUnknown("lost publication reply".into()))
        }
    }
    let repo = Arc::new(LostAck::default());
    let state = AgentState::new(repo.clone());
    let input = request("tenant", "agent");
    assert!(matches!(
        state.publish("tenant", &input.template).await,
        Err(Error::OutcomeUnknown(_))
    ));
    assert_eq!(repo.writes.load(Ordering::SeqCst), 1);
    assert!(state
        .template("tenant", "agent", "1")
        .await
        .unwrap()
        .is_some());
    assert!(state
        .template("tenant", "agent", "2")
        .await
        .unwrap()
        .is_none());
}

#[tokio::test]
#[ignore = "requires a disposable ADX_AGENT_TEST_REDIS_URL"]
async fn redis_old_schema_is_rejected_without_migration() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("old-publication-{}", uuid::Uuid::new_v4());
    let mut connection = redis::Client::open(url.as_str())
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let key = format!("adx:v2:{namespace}:schema");
    redis::cmd("SET")
        .arg(&key)
        .arg("binding-alias-current-v2")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        adx_agent_store::RedisRepository::connect(
            &url,
            &namespace,
            std::time::Duration::from_secs(3)
        )
        .await,
        Err(adx_agent_store::Error::Invalid(_))
    ));
    let marker: String = redis::cmd("GET")
        .arg(&key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(marker, "binding-alias-current-v2");
}
