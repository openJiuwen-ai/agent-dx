use adx_agent_store::*;
use serde_json::json;

fn create(key: Key, value: serde_json::Value) -> Transaction {
    Transaction::new(
        vec![Check {
            key: key.clone(),
            expected: None,
        }],
        vec![Put {
            key,
            record: Record::new(&value).unwrap(),
        }],
    )
    .unwrap()
}

async fn contracts(a: &dyn Repository, b: &dyn Repository) {
    let template = Key::new("template", &["tenant", "i"]).unwrap();
    let environment = Key::new("environment", &["tenant", "ctx", "s"]).unwrap();
    let another_environment = Key::new("environment", &["tenant", "ctx"]).unwrap();
    assert!(a
        .commit(&create(
            template.clone(),
            json!({"phase":"ready","large":u64::MAX})
        ))
        .await
        .unwrap());
    let before = a.get(&template).await.unwrap().unwrap();
    let insert = |id: &str| {
        Transaction::new(
            vec![
                Check {
                    key: template.clone(),
                    expected: Some(before.revision.clone()),
                },
                Check {
                    key: environment.clone(),
                    expected: None,
                },
            ],
            vec![Put {
                key: environment.clone(),
                record: Record::new(&json!({"template":id})).unwrap(),
            }],
        )
        .unwrap()
    };
    let left = insert("left");
    let right = insert("right");
    let (x, y) = tokio::join!(a.commit(&left), b.commit(&right));
    assert_ne!(x.unwrap(), y.unwrap());
    assert_eq!(
        a.get(&environment).await.unwrap(),
        b.get(&environment).await.unwrap()
    );
    assert_eq!(
        a.get(&template).await.unwrap().unwrap().value["large"],
        json!(u64::MAX)
    );

    let deleting = Record::new(&json!({"phase":"deleting"})).unwrap();
    let tx = Transaction::new(
        vec![Check {
            key: template.clone(),
            expected: Some(before.revision.clone()),
        }],
        vec![Put {
            key: template.clone(),
            record: deleting,
        }],
    )
    .unwrap();
    assert!(a.commit(&tx).await.unwrap());
    // A stale candidate revision must prevent ALL writes, including a new Environment record.
    let stale = Transaction::new(
        vec![
            Check {
                key: template,
                expected: Some(before.revision),
            },
            Check {
                key: another_environment.clone(),
                expected: None,
            },
        ],
        vec![Put {
            key: another_environment.clone(),
            record: Record::new(&json!({"phase":"active"})).unwrap(),
        }],
    )
    .unwrap();
    assert!(!b.commit(&stale).await.unwrap());
    assert!(a.get(&another_environment).await.unwrap().is_none());
    let prior = a.get(&environment).await.unwrap().unwrap();
    let removal = Transaction::with_deletes(
        vec![Check {
            key: environment.clone(),
            expected: Some(prior.revision.clone()),
        }],
        vec![],
        vec![environment.clone()],
    )
    .unwrap();
    assert!(b.commit(&removal).await.unwrap());
    assert!(a.get(&environment).await.unwrap().is_none());
    assert!(a
        .commit(&create(
            environment.clone(),
            json!({"template":"new-lifecycle"})
        ))
        .await
        .unwrap());
    assert!(!b.commit(&removal).await.unwrap());
    assert_eq!(
        a.get(&environment).await.unwrap().unwrap().value["template"],
        "new-lifecycle"
    );
    assert!(Transaction::with_deletes(
        vec![Check {
            key: environment.clone(),
            expected: None
        }],
        vec![],
        vec![environment]
    )
    .is_err());
}

#[cfg(feature = "test-memory")]
#[tokio::test]
async fn memory_contract() {
    let store = MemoryRepository::default();
    contracts(&store, &store).await;
}

#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for a disposable real Redis"]
async fn two_independent_redis_clients() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").expect("set disposable Redis URL");
    let namespace = format!("test-{}", uuid::Uuid::new_v4());
    let timeout = std::time::Duration::from_secs(3);
    let a = RedisRepository::connect(&url, &namespace, timeout)
        .await
        .unwrap();
    let b = RedisRepository::connect(&url, &namespace, timeout)
        .await
        .unwrap();
    contracts(&a, &b).await;
    let other = RedisRepository::connect(&url, &format!("{namespace}-other"), timeout)
        .await
        .unwrap();
    assert!(other
        .get(&Key::new("template", &["tenant", "i"]).unwrap())
        .await
        .unwrap()
        .is_none());
}

#[test]
fn writes_require_unique_checks_and_fresh_revisions() {
    let key = Key::new("template", &["t", "i"]).unwrap();
    let record = Record::new(&json!({})).unwrap();
    assert!(Transaction::new(
        vec![Check {
            key: key.clone(),
            expected: Some(record.revision.clone())
        }],
        vec![Put { key, record }]
    )
    .is_err());
    assert!(Transaction::new(vec![], vec![]).is_err());
}

#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for a disposable real Redis"]
async fn conflicting_schema_marker_is_preserved() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let ns = format!("tagged-{}", uuid::Uuid::new_v4());
    let client = redis::Client::open(url.clone()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let key = format!("adx:v2:{ns}:schema");
    redis::cmd("SET")
        .arg(&key)
        .arg("unrelated-state-format")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    assert!(matches!(
        RedisRepository::connect(&url, &ns, std::time::Duration::from_secs(3)).await,
        Err(Error::Invalid(_))
    ));
    let preserved: String = redis::cmd("GET")
        .arg(key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(preserved, "unrelated-state-format");
}

#[tokio::test]
#[ignore = "requires ADX_AGENT_TEST_REDIS_URL for a disposable real Redis"]
async fn concurrent_namespace_initialization_preserves_current_state() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let ns = format!("initialize-{}", uuid::Uuid::new_v4());
    let client = redis::Client::open(url.clone()).unwrap();
    let mut connection = client.get_multiplexed_async_connection().await.unwrap();
    let key = format!("adx:v2:{ns}:template:seed");
    redis::cmd("SET")
        .arg(&key)
        .arg("preserved")
        .query_async::<()>(&mut connection)
        .await
        .unwrap();
    let (a, b) = tokio::join!(
        RedisRepository::connect(&url, &ns, std::time::Duration::from_secs(3)),
        RedisRepository::connect(&url, &ns, std::time::Duration::from_secs(3))
    );
    a.unwrap();
    b.unwrap();
    let value: String = redis::cmd("GET")
        .arg(key)
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(value, "preserved");
    let marker: String = redis::cmd("GET")
        .arg(format!("adx:v2:{ns}:schema"))
        .query_async(&mut connection)
        .await
        .unwrap();
    assert_eq!(marker, "environment-v1");
}
