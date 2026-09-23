use super::*;
use adx_coordinator::auth::{digest, Credential};

fn admin() -> Credential {
    Credential {
        tenant_id: "admin".into(),
        administrator: true,
        expires_at_unix_seconds: 0,
    }
}
fn key(name: &str) -> String {
    format!("admin-rotation-{name}-012345678901234567890123456789")
}

#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn administrator_rotation_is_atomic_persistent_and_preserves_tenant_keys() {
    let redis = common::Redis::new().await;
    let store = redis.store().await;
    let first = store.begin(1).await.unwrap();
    let old = key("old");
    let new = key("new");
    let tenant = key("tenant");
    first.bootstrap_credential(&old, &admin()).await.unwrap();
    first
        .bootstrap_credential(
            &tenant,
            &Credential {
                tenant_id: "tenant".into(),
                administrator: false,
                expires_at_unix_seconds: 0,
            },
        )
        .await
        .unwrap();
    let desired = vec![(new.clone(), admin())];
    first.reconcile_administrators(&desired).await.unwrap();
    // A lost reply can be retried without rotating again or touching tenant keys.
    first.reconcile_administrators(&desired).await.unwrap();
    assert!(matches!(
        first.credential(&digest(&old).unwrap()).await,
        Err(Error::NotFound)
    ));
    assert!(first.credential(&digest(&tenant).unwrap()).await.is_ok());
    assert!(
        first
            .credential(&digest(&new).unwrap())
            .await
            .unwrap()
            .administrator
    );
    let second = store.begin(1).await.unwrap();
    assert!(matches!(
        first.reconcile_administrators(&desired).await,
        Err(Error::Conflict)
    ));
    second.reconcile_administrators(&desired).await.unwrap();
    assert!(second
        .reconcile_administrators(&[(old.clone(), admin())])
        .await
        .is_err());
    second.bootstrap_credential(&old, &admin()).await.unwrap();
    assert!(matches!(
        second.credential(&digest(&old).unwrap()).await,
        Err(Error::NotFound)
    ));
    assert!(second.credential(&digest(&new).unwrap()).await.is_ok());
}

#[tokio::test]
#[ignore = "requires isolated Redis"]
async fn administrator_rotation_rejects_invalid_set_without_removing_current_key() {
    let redis = common::Redis::new().await;
    let session = redis.store().await.begin(1).await.unwrap();
    let current = key("current");
    session
        .reconcile_administrators(&[(current.clone(), admin())])
        .await
        .unwrap();
    let mut expired = admin();
    expired.expires_at_unix_seconds = 1;
    for bad in [
        vec![],
        vec![("short".into(), admin())],
        vec![(key("expired"), expired)],
        vec![(current.clone(), admin()), (current.clone(), admin())],
    ] {
        assert!(session.reconcile_administrators(&bad).await.is_err());
        assert!(session.credential(&digest(&current).unwrap()).await.is_ok());
    }
    let tenant = key("tenant");
    let mut tenant_credential = admin();
    tenant_credential.administrator = false;
    session
        .bootstrap_credential(&tenant, &tenant_credential)
        .await
        .unwrap();
    assert!(session
        .reconcile_administrators(&[(tenant.clone(), admin())])
        .await
        .is_err());
    assert!(session.credential(&digest(&current).unwrap()).await.is_ok());
    assert!(
        !session
            .credential(&digest(&tenant).unwrap())
            .await
            .unwrap()
            .administrator
    );
    let extra = key("extra");
    session
        .reconcile_administrators(&[(current.clone(), admin()), (extra.clone(), admin())])
        .await
        .unwrap();
    assert!(session.credential(&digest(&current).unwrap()).await.is_ok());
    assert!(session.credential(&digest(&extra).unwrap()).await.is_ok());
}
