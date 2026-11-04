use adx_agent_core::discovery::{ActivatorEndpoint, Member, MemberCondition};
use adx_agent_store::{discovery::RedisRegistry, Error};
use std::time::Duration;
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL pointing to a disposable Redis"]
async fn registry_leases_expire_and_old_incarnations_cannot_remove_replacements() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("discovery-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let endpoint = ActivatorEndpoint {
        id: "a".into(),
        url: "http://127.0.0.1:8091".into(),
    };
    let member = |endpoint: &ActivatorEndpoint| Member {
        endpoint: endpoint.clone(),
        condition: MemberCondition::default(),
    };
    registry
        .renew(
            &endpoint,
            "old",
            Duration::from_millis(100),
            MemberCondition::default(),
        )
        .await
        .unwrap();
    assert_eq!(registry.members().await.unwrap(), vec![member(&endpoint)]);
    assert!(matches!(
        registry
            .renew(
                &endpoint,
                "new",
                Duration::from_secs(1),
                MemberCondition::default()
            )
            .await,
        Err(Error::Conflict(_))
    ));
    tokio::time::sleep(Duration::from_millis(150)).await;
    assert!(registry.members().await.unwrap().is_empty());
    registry
        .renew(
            &endpoint,
            "new",
            Duration::from_secs(1),
            MemberCondition::default(),
        )
        .await
        .unwrap();
    registry.unregister(&endpoint.id, "old").await.unwrap();
    assert_eq!(registry.members().await.unwrap(), vec![member(&endpoint)]);
    assert!(matches!(
        registry
            .renew(
                &endpoint,
                "old",
                Duration::from_secs(1),
                MemberCondition::default()
            )
            .await,
        Err(Error::Conflict(_))
    ));
    registry.unregister(&endpoint.id, "new").await.unwrap();
    assert!(registry.members().await.unwrap().is_empty());
}

/// AC-4: the member record round-trips its condition, so Gateway-side
/// selection can filter not-ready and draining members; records written by
/// older Activators (no condition fields) are still readable.
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL pointing to a disposable Redis"]
async fn registry_records_carry_condition_and_tolerate_legacy_records() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("discovery-condition-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let ready = ActivatorEndpoint {
        id: "ready".into(),
        url: "http://127.0.0.1:8091".into(),
    };
    let draining = ActivatorEndpoint {
        id: "draining".into(),
        url: "http://127.0.0.1:8092".into(),
    };
    let unready = ActivatorEndpoint {
        id: "unready".into(),
        url: "http://127.0.0.1:8093".into(),
    };
    registry
        .renew(
            &ready,
            "inc-1",
            Duration::from_secs(60),
            MemberCondition::default(),
        )
        .await
        .unwrap();
    registry
        .renew(
            &draining,
            "inc-2",
            Duration::from_secs(60),
            MemberCondition {
                ready: true,
                draining: true,
            },
        )
        .await
        .unwrap();
    registry
        .renew(
            &unready,
            "inc-3",
            Duration::from_secs(60),
            MemberCondition {
                ready: false,
                draining: false,
            },
        )
        .await
        .unwrap();

    let mut members = registry.members().await.unwrap();
    members.sort_by(|a, b| a.endpoint.id.cmp(&b.endpoint.id));
    assert_eq!(
        members,
        vec![
            Member {
                endpoint: draining,
                condition: MemberCondition {
                    ready: true,
                    draining: true,
                },
            },
            Member {
                endpoint: ready,
                condition: MemberCondition::default(),
            },
            Member {
                endpoint: unready,
                condition: MemberCondition {
                    ready: false,
                    draining: false,
                },
            },
        ]
    );

    // A record written by an older Activator (endpoint+incarnation only) is
    // still readable and counts as ready-and-not-draining.
    let legacy_record =
        r#"{"endpoint":{"id":"legacy","url":"http://127.0.0.1:8094"},"incarnation":"inc-4"}"#;
    let mut admin = redis::Client::open(url)
        .unwrap()
        .get_multiplexed_async_connection()
        .await
        .unwrap();
    let _: () = redis::cmd("HSET")
        .arg(format!("adx:v2:{namespace}:activators:records"))
        .arg("legacy")
        .arg(legacy_record)
        .query_async(&mut admin)
        .await
        .unwrap();
    let _: () = redis::cmd("ZADD")
        .arg(format!("adx:v2:{namespace}:activators:leases"))
        .arg("99999999999999")
        .arg("legacy")
        .query_async(&mut admin)
        .await
        .unwrap();
    let legacy = registry
        .members()
        .await
        .unwrap()
        .into_iter()
        .find(|member| member.endpoint.id == "legacy")
        .expect("legacy record is readable");
    assert!(legacy.condition.selectable());
}

/// AC-5: a plain lease renewal (heartbeat, no membership change) must not
/// publish a change notification — only register/unregister do. Publishing
/// on every renewal would flood Ingress with re-reads; lease expiry itself
/// is covered by the periodic refresh.
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL"]
async fn plain_renewal_does_not_publish_a_change_notification() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("discovery-silent-renew-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let subscriber = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let mut events = subscriber.events();

    // Drain the connect-time re-read signal so the next signal would have to
    // be a real publish.
    let _ = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("initial connect-time re-read signal");

    let endpoint = ActivatorEndpoint {
        id: "a".into(),
        url: "http://127.0.0.1:8091".into(),
    };
    registry
        .renew(
            &endpoint,
            "inc-a",
            Duration::from_secs(60),
            MemberCondition::default(),
        )
        .await
        .unwrap();
    // The register itself did not publish either (publish_changed is a
    // separate explicit call); heartbeats must stay silent.
    match tokio::time::timeout(Duration::from_millis(500), events.recv()).await {
        Err(_silent) => {}
        Ok(signal) => panic!("plain renewal published a notification: {signal:?}"),
    }
}

/// AC1: register/unregister publish a change notification, and a subscriber
/// via `events()` receives it (plus an immediate re-read on (re)connect, so
/// changes missed while disconnected are caught up). This covers the V2 gap:
/// the membership-change notification path had no test.
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL"]
async fn publish_changed_notifies_subscribers_on_register_and_unregister() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("discovery-notify-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();

    // A second registry on the same namespace subscribes to change events.
    let subscriber = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let mut events = subscriber.events();

    // The first event arrives immediately on subscription connect (re-read on
    // connect). Drain it so the next signal is a real publish, not the
    // connect-time re-read.
    let _ = tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("initial connect-time re-read signal");

    // Registering member A publishes a change notification.
    let endpoint_a = ActivatorEndpoint {
        id: "a".into(),
        url: "http://127.0.0.1:8091".into(),
    };
    registry
        .renew(
            &endpoint_a,
            "inc-a",
            Duration::from_secs(60),
            MemberCondition::default(),
        )
        .await
        .unwrap();
    registry.publish_changed().await.unwrap();

    // The subscriber receives the notification.
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("register change notification")
        .expect("notification channel open");

    // After the notification, a fresh members() read reflects the new member.
    assert_eq!(
        registry.members().await.unwrap(),
        vec![Member {
            endpoint: endpoint_a.clone(),
            condition: MemberCondition::default(),
        }]
    );

    // Unregistering publishes another change notification.
    registry.unregister(&endpoint_a.id, "inc-a").await.unwrap();
    registry.publish_changed().await.unwrap();
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("unregister change notification")
        .expect("notification channel open");

    assert!(registry.members().await.unwrap().is_empty());
}

/// AC1: a lost notification is harmless because the periodic refresh is the
/// fallback. The subscriber's connect-time re-read (and periodic refresh in
/// the Ingress-side discovery loop) catches changes published while no
/// subscription was active. Here we verify the connect-time re-read signal
/// arrives even when no publish happened — the mechanism that catches up
/// missed notifications.
#[tokio::test]
#[ignore = "requires disposable ADX_AGENT_TEST_REDIS_URL"]
async fn events_emits_re_read_on_connect_to_catch_missed_changes() {
    let url = std::env::var("ADX_AGENT_TEST_REDIS_URL").unwrap();
    let namespace = format!("discovery-reconnect-{}", uuid::Uuid::new_v4());
    let registry = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();

    // Add a member BEFORE subscribing — it was published while no subscriber
    // was connected.
    let endpoint = ActivatorEndpoint {
        id: "pre".into(),
        url: "http://127.0.0.1:8092".into(),
    };
    registry
        .renew(
            &endpoint,
            "inc-pre",
            Duration::from_secs(60),
            MemberCondition::default(),
        )
        .await
        .unwrap();
    registry.publish_changed().await.unwrap(); // lost (no subscriber yet)

    // Subscribing now: the connect-time re-read signal arrives so the caller
    // re-reads members() and discovers the pre-existing member.
    let subscriber = RedisRegistry::new(&url, &namespace, Duration::from_secs(2)).unwrap();
    let mut events = subscriber.events();
    tokio::time::timeout(Duration::from_secs(3), events.recv())
        .await
        .expect("connect-time re-read signal")
        .expect("notification channel open");

    // The re-read reflects the member that existed before subscribing.
    assert_eq!(
        subscriber.members().await.unwrap(),
        vec![Member {
            endpoint,
            condition: MemberCondition::default(),
        }]
    );
}
