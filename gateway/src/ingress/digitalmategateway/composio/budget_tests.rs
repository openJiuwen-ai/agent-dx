//! Shared PostgreSQL quota behavior, independent of provider/network credentials.
use super::{Budget, Error, PostgresBudget, RequestBudgetLimits, Scope};
use deadpool_postgres::{Manager, Pool};

fn scope(user: &str, workspace: &str) -> Scope {
    Scope::from_authenticated_foreground("tenant".into(), user.into(), workspace.into()).unwrap()
}

#[test]
fn request_budget_configuration_is_explicit_and_bounded() {
    for (organization, user) in [(0, 1), (1, 0), (-1, 1), (3, 4)] {
        assert!(RequestBudgetLimits {
            organization_per_minute: organization,
            user_per_minute: user,
        }
        .validate()
        .is_err());
    }
    assert!(RequestBudgetLimits {
        organization_per_minute: 10,
        user_per_minute: 2,
    }
    .validate()
    .is_ok());
}

#[tokio::test]
#[ignore = "requires the explicit task-owned PostgreSQL database"]
async fn shared_request_budget_survives_reopen_and_refuses_atomic_overrun() {
    let dsn = std::env::var("ADX_INGESTION_TEST_DATABASE_URL")
        .expect("explicit task-owned PostgreSQL required");
    let config: tokio_postgres::Config = dsn.parse().unwrap();
    assert!(config
        .get_dbname()
        .unwrap_or("")
        .starts_with("dm_ingestion_test_"));
    let pool = Pool::builder(Manager::new(config, tokio_postgres::NoTls))
        .max_size(8)
        .build()
        .unwrap();
    let client = pool.get().await.unwrap();
    client
        .batch_execute("DROP SCHEMA IF EXISTS digitalmate_composio_budget CASCADE")
        .await
        .unwrap();
    drop(client);
    let limits = RequestBudgetLimits {
        organization_per_minute: 4,
        user_per_minute: 2,
    };
    let one = PostgresBudget::initialize(pool.clone(), "organization_one".into(), limits.clone())
        .await
        .unwrap();
    let reopened =
        PostgresBudget::from_pool(pool.clone(), "organization_one".into(), limits.clone())
            .await
            .unwrap();
    one.acquire(&scope("alice", "work_a")).await.unwrap();
    // Another workspace does not give this same user another request allowance.
    reopened.acquire(&scope("alice", "work_b")).await.unwrap();
    assert!(matches!(
        one.acquire(&scope("alice", "work_c")).await,
        Err(Error::RateLimited {
            retry_after: Some(1..=60)
        })
    ));
    // Refusing the user must roll back the organization charge as well.
    one.acquire(&scope("bob", "work_a")).await.unwrap();
    reopened.acquire(&scope("bob", "work_b")).await.unwrap();
    assert!(matches!(
        reopened.acquire(&scope("carol", "work_a")).await,
        Err(Error::RateLimited { .. })
    ));
    let incompatible = RequestBudgetLimits {
        organization_per_minute: 100,
        user_per_minute: 100,
    };
    assert!(matches!(
        PostgresBudget::from_pool(pool.clone(), "organization_one".into(), incompatible).await,
        Err(Error::NotConfigured)
    ));
    let concurrent = PostgresBudget::initialize(
        pool.clone(),
        "organization_two".into(),
        RequestBudgetLimits {
            organization_per_minute: 1,
            user_per_minute: 1,
        },
    )
    .await
    .unwrap();
    let user = scope("alice", "work_a");
    let results = tokio::join!(
        concurrent.acquire(&user),
        concurrent.acquire(&user),
        concurrent.acquire(&user)
    );
    assert_eq!(
        [results.0, results.1, results.2]
            .iter()
            .filter(|result| result.is_ok())
            .count(),
        1
    );
    // A failed database operation cannot fall back to an unlimited allowance.
    pool.close();
    assert!(matches!(
        one.acquire(&user).await,
        Err(Error::UpstreamUnavailable)
    ));
}
