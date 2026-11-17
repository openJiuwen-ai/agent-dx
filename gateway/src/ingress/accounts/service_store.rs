//! Server configuration storage; never selected by user-scoped privacy operations.
use super::{AccountStore, Error, ExpectedRevision, Result};

/// Fixed server-only names. No request can supply an arbitrary service namespace.
#[derive(Clone, Copy)]
pub(crate) enum ServiceSecretName {
    ComposioProjectKey,
    ComposioWebhookSecret,
}

impl ServiceSecretName {
    pub(crate) fn service(self) -> &'static str {
        "composio"
    }

    pub(crate) fn key(self) -> &'static str {
        match self {
            Self::ComposioProjectKey => "PROJECT_API_KEY",
            Self::ComposioWebhookSecret => "WEBHOOK_SECRET",
        }
    }
}

pub(crate) struct StoredServiceSecret {
    pub revision: String,
    pub key_id: Option<String>,
    pub ciphertext: Option<Vec<u8>>,
}

fn validate_tenant(tenant: &str) -> Result<()> {
    adx_agent_core::identifier(tenant, "service secret tenant").map_err(|_| Error::Invalid)?;
    if tenant.len() > 128 {
        return Err(Error::Invalid);
    }
    Ok(())
}

impl AccountStore {
    /// Private owner port for the configuration center, never a user-privacy read.
    pub(crate) async fn service_secret_get(
        &self,
        tenant: &str,
        name: ServiceSecretName,
    ) -> Result<Option<StoredServiceSecret>> {
        validate_tenant(tenant)?;
        let row = self.pool.get().await.map_err(|_| Error::Unavailable)?.query_opt(
            "SELECT revision,key_id,encrypted_value FROM adx_accounts.service_secrets WHERE tenant_id=$1 AND service=$2 AND secret_key=$3",
            &[&tenant, &name.service(), &name.key()],
        ).await.map_err(|_| Error::Unavailable)?;
        Ok(row.map(|row| StoredServiceSecret {
            revision: row.get(0),
            key_id: row.get(1),
            ciphertext: row.get(2),
        }))
    }

    /// Operator-side CAS of already encrypted configuration. None is a tombstone.
    /// Returns Conflict for a stale/missing revision and Unavailable for DB failure.
    pub(crate) async fn service_secret_put(
        &self,
        tenant: &str,
        name: ServiceSecretName,
        value: Option<(&str, &[u8])>,
        expected: ExpectedRevision<'_>,
    ) -> Result<String> {
        validate_tenant(tenant)?;
        if let Some((id, bytes)) = value {
            if id.is_empty()
                || id.len() > 128
                || !id
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"_-.".contains(&byte))
                || !(28..=131100).contains(&bytes.len())
            {
                return Err(Error::Invalid);
            }
        }
        let revision = uuid::Uuid::new_v4().to_string();
        let key_id = value.map(|value| value.0);
        let ciphertext = value.map(|value| value.1);
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let row = match expected {
            ExpectedRevision::New => client.query_opt(
                "INSERT INTO adx_accounts.service_secrets (tenant_id,service,secret_key,revision,key_id,encrypted_value) VALUES ($1,$2,$3,$4,$5,$6) ON CONFLICT DO NOTHING RETURNING revision",
                &[&tenant, &name.service(), &name.key(), &revision, &key_id, &ciphertext],
            ).await,
            ExpectedRevision::Existing(previous) => {
                if !uuid::Uuid::parse_str(previous).is_ok_and(|parsed| parsed.to_string() == previous) { return Err(Error::Invalid); }
                client.query_opt(
                    "UPDATE adx_accounts.service_secrets SET revision=$4,key_id=$5,encrypted_value=$6,updated_at=now() WHERE tenant_id=$1 AND service=$2 AND secret_key=$3 AND revision=$7 RETURNING revision",
                    &[&tenant, &name.service(), &name.key(), &revision, &key_id, &ciphertext, &previous],
                ).await
            }
        }.map_err(|_| Error::Unavailable)?;
        row.map(|row| row.get(0)).ok_or(Error::Conflict)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ingress::accounts::{AccountStore, Error, ExpectedRevision};
    use deadpool_postgres::{Manager, Pool};

    #[tokio::test]
    #[ignore = "requires the explicit task-owned PostgreSQL database"]
    async fn server_secret_cas_is_durable_and_tenant_scoped() {
        let dsn = std::env::var("ADX_INGESTION_TEST_DATABASE_URL").unwrap();
        let config: tokio_postgres::Config = dsn.parse().unwrap();
        assert!(config
            .get_dbname()
            .unwrap_or("")
            .starts_with("dm_ingestion_test_"));
        let pool = Pool::builder(Manager::new(config, tokio_postgres::NoTls))
            .max_size(4)
            .build()
            .unwrap();
        let client = pool.get().await.unwrap();
        client
            .batch_execute(include_str!("schema.sql"))
            .await
            .unwrap();
        client
            .execute(
                "DELETE FROM adx_accounts.service_secrets WHERE tenant_id='composio-service-test'",
                &[],
            )
            .await
            .unwrap();
        drop(client);
        let store = AccountStore { pool: pool.clone() };
        let name = ServiceSecretName::ComposioProjectKey;
        assert!(store
            .service_secret_get("composio-service-test", name)
            .await
            .unwrap()
            .is_none());
        let ciphertext = vec![17; 36];
        let first = store
            .service_secret_put(
                "composio-service-test",
                name,
                Some(("gateway-v1", &ciphertext)),
                ExpectedRevision::New,
            )
            .await
            .unwrap();
        assert!(matches!(
            store
                .service_secret_put("composio-service-test", name, None, ExpectedRevision::New)
                .await,
            Err(Error::Conflict)
        ));
        assert!(matches!(
            store
                .service_secret_put(
                    "composio-service-test",
                    name,
                    None,
                    ExpectedRevision::Existing("00000000-0000-0000-0000-000000000000")
                )
                .await,
            Err(Error::Conflict)
        ));
        assert!(store
            .service_secret_get("another-tenant", name)
            .await
            .unwrap()
            .is_none());
        assert!(store
            .service_secret_get(
                "composio-service-test",
                ServiceSecretName::ComposioWebhookSecret
            )
            .await
            .unwrap()
            .is_none());
        let reopened = AccountStore { pool: pool.clone() };
        let value = reopened
            .service_secret_get("composio-service-test", name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(value.revision, first);
        assert_eq!(value.ciphertext.as_ref(), Some(&ciphertext));
        let deleted = reopened
            .service_secret_put(
                "composio-service-test",
                name,
                None,
                ExpectedRevision::Existing(&first),
            )
            .await
            .unwrap();
        let tombstone = store
            .service_secret_get("composio-service-test", name)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(tombstone.revision, deleted);
        assert!(tombstone.ciphertext.is_none() && tombstone.key_id.is_none());
        assert!(matches!(
            store
                .service_secret_put(
                    "composio-service-test",
                    name,
                    Some(("gateway-v1", &ciphertext)),
                    ExpectedRevision::Existing(&first)
                )
                .await,
            Err(Error::Conflict)
        ));
        // Tombstones still require the current revision when recreating a key.
        assert!(store
            .service_secret_put(
                "composio-service-test",
                name,
                Some(("gateway-v1", &ciphertext)),
                ExpectedRevision::Existing(&deleted)
            )
            .await
            .is_ok());
        pool.close();
        assert!(matches!(
            store
                .service_secret_get("composio-service-test", name)
                .await,
            Err(Error::Unavailable)
        ));
    }
}
