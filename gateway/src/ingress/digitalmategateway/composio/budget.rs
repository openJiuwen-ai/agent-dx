//! Shared physical-request accounting. This schema owns no connection or token.
use super::{Budget, Error, Scope};
use async_trait::async_trait;
use deadpool_postgres::Pool;
use sha2::{Digest, Sha256};

#[derive(Clone, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RequestBudgetLimits {
    pub organization_per_minute: i64,
    pub user_per_minute: i64,
}

impl RequestBudgetLimits {
    /// Invalid or unbounded policies fail before any provider request.
    pub fn validate(&self) -> Result<(), Error> {
        if self.organization_per_minute <= 0
            || self.user_per_minute <= 0
            || self.user_per_minute > self.organization_per_minute
        {
            return Err(Error::NotConfigured);
        }
        Ok(())
    }
}

/// Uses database-clock fixed minute windows shared by all Gateway replicas.
/// The trusted organization identifier must stay stable across project keys.
/// A replica with a different policy is refused rather than widening the quota.
#[derive(Clone)]
pub struct PostgresBudget {
    pool: Pool,
    organization: String,
    limits: RequestBudgetLimits,
}

fn unavailable(_: impl std::fmt::Debug) -> Error {
    Error::UpstreamUnavailable
}

fn identity(namespace: &[u8], parts: &[&str]) -> String {
    let mut hash = Sha256::new();
    hash.update(namespace);
    for part in parts {
        hash.update([0]);
        hash.update(part.as_bytes());
    }
    hex::encode(hash.finalize())
}

impl PostgresBudget {
    /// Test-only schema fixture; formal startup uses the operator-applied schema.
    #[cfg(test)]
    pub(super) async fn initialize(
        pool: Pool,
        organization: String,
        limits: RequestBudgetLimits,
    ) -> Result<Self, Error> {
        limits.validate()?;
        let client = pool.get().await.map_err(unavailable)?;
        client
            .batch_execute(include_str!("budget.sql"))
            .await
            .map_err(unavailable)?;
        drop(client);
        Self::from_pool(pool, organization, limits).await
    }

    /// Opens an initialized schema and pins the configured policy atomically.
    /// Existing policies require an explicit coordinated operator change.
    pub async fn from_pool(
        pool: Pool,
        organization: String,
        limits: RequestBudgetLimits,
    ) -> Result<Self, Error> {
        limits.validate()?;
        if organization.is_empty()
            || organization.len() > 256
            || organization.chars().any(char::is_control)
        {
            return Err(Error::NotConfigured);
        }
        let organization = identity(b"digitalmate.composio.organization.v1", &[&organization]);
        let mut client = pool.get().await.map_err(unavailable)?;
        let transaction = client.transaction().await.map_err(unavailable)?;
        transaction
            .batch_execute("SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '5s'")
            .await
            .map_err(unavailable)?;
        let versions = transaction
            .query(
                "SELECT version FROM digitalmate_composio_budget.version",
                &[],
            )
            .await
            .map_err(unavailable)?;
        if versions.len() != 1 || versions[0].get::<_, i32>(0) != 1 {
            return Err(Error::NotConfigured);
        }
        transaction.execute(
            "INSERT INTO digitalmate_composio_budget.organizations (organization, organization_limit, user_limit) VALUES ($1, $2, $3) ON CONFLICT DO NOTHING",
            &[&organization, &limits.organization_per_minute, &limits.user_per_minute],
        ).await.map_err(unavailable)?;
        let policy = transaction.query_one(
            "SELECT organization_limit, user_limit FROM digitalmate_composio_budget.organizations WHERE organization = $1 FOR UPDATE", &[&organization],
        ).await.map_err(unavailable)?;
        if policy.get::<_, i64>(0) != limits.organization_per_minute
            || policy.get::<_, i64>(1) != limits.user_per_minute
        {
            return Err(Error::NotConfigured);
        }
        transaction.commit().await.map_err(unavailable)?;
        Ok(Self {
            pool,
            organization,
            limits,
        })
    }
}

#[async_trait]
impl Budget for PostgresBudget {
    async fn acquire(&self, scope: &Scope) -> Result<(), Error> {
        // Workspace is intentionally excluded: one user has one allowance.
        let user = identity(
            b"digitalmate.composio.budget.user.v1",
            &[scope.tenant(), scope.user_id()],
        );
        let mut client = self.pool.get().await.map_err(unavailable)?;
        let transaction = client.transaction().await.map_err(unavailable)?;
        transaction
            .batch_execute("SET LOCAL lock_timeout = '5s'; SET LOCAL statement_timeout = '5s'")
            .await
            .map_err(unavailable)?;
        // The organization lock orders all charges, including user-row creation.
        let policy = transaction.query_one(
            "SELECT organization_limit, user_limit, window_id, used FROM digitalmate_composio_budget.organizations WHERE organization = $1 FOR UPDATE", &[&self.organization],
        ).await.map_err(unavailable)?;
        if policy.get::<_, i64>(0) != self.limits.organization_per_minute
            || policy.get::<_, i64>(1) != self.limits.user_per_minute
        {
            return Err(Error::NotConfigured);
        }
        // Read after acquiring the lock; transaction start time can be stale.
        let clock = transaction
            .query_one(
                "SELECT floor(extract(epoch FROM clock_timestamp()))::bigint",
                &[],
            )
            .await
            .map_err(unavailable)?;
        let seconds: i64 = clock.get(0);
        let window_id = seconds.div_euclid(60);
        let retry_after =
            u64::try_from(60 - seconds.rem_euclid(60)).map_err(|_| Error::UpstreamUnavailable)?;
        let organization_used = if policy.get::<_, i64>(2) == window_id {
            policy.get::<_, i64>(3)
        } else {
            0
        };
        let row = transaction.query_opt(
            "SELECT used FROM digitalmate_composio_budget.users WHERE organization = $1 AND identity = $2 AND window_id = $3", &[&self.organization, &user, &window_id],
        ).await.map_err(unavailable)?;
        let user_used = row.map(|row| row.get::<_, i64>(0)).unwrap_or(0);
        if organization_used >= self.limits.organization_per_minute
            || user_used >= self.limits.user_per_minute
        {
            return Err(Error::RateLimited {
                retry_after: Some(retry_after),
            });
        }
        transaction.execute(
            "UPDATE digitalmate_composio_budget.organizations SET window_id = $2, used = $3 WHERE organization = $1", &[&self.organization, &window_id, &(organization_used + 1)],
        ).await.map_err(unavailable)?;
        // Remove stale users so the table tracks only this minute's identities.
        transaction.execute("DELETE FROM digitalmate_composio_budget.users WHERE organization = $1 AND window_id <> $2", &[&self.organization, &window_id]).await.map_err(unavailable)?;
        transaction.execute(
            "INSERT INTO digitalmate_composio_budget.users (organization, identity, window_id, used) VALUES ($1, $2, $3, $4) ON CONFLICT (organization, identity) DO UPDATE SET window_id = EXCLUDED.window_id, used = EXCLUDED.used", &[&self.organization, &user, &window_id, &(user_used + 1)],
        ).await.map_err(unavailable)?;
        transaction.commit().await.map_err(unavailable)?;
        Ok(())
    }
}
