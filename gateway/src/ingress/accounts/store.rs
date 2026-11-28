use super::*;
use deadpool_postgres::{Manager, Pool, Runtime};
use tokio_postgres::{config::SslMode, Row};

#[derive(Clone)]
pub struct AccountStore {
    pub(super) pool: Pool,
}
pub(crate) struct CredentialRecord {
    pub encrypted_key: Vec<u8>,
    pub version: String,
    pub ready: bool,
}
impl AccountStore {
    /// Read the owned base set using one read-only repeatable-read snapshot.
    pub async fn admin_list(&self, grant: &admin::AdminGrant, actor: &str, query: &admin::AdminQuery) -> Result<serde_json::Value> {
        use tokio_postgres::types::ToSql;
        if query.requires_model_lookup() { return Err(Error::Invalid); }
        let plan = admin::QueryPlan::new(grant, actor, query)?;
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.build_transaction().read_only(true).isolation_level(tokio_postgres::IsolationLevel::RepeatableRead).start().await.map_err(|_| Error::Unavailable)?;
        let params: Vec<&(dyn ToSql + Sync)> = plan.parameters.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let total: i64 = tx.query_one(&format!("SELECT count(*) FROM adx_accounts.users u WHERE {}", plan.predicate), &params).await.map_err(|_| Error::Unavailable)?.get(0);
        let mut values = plan.parameters.clone();
        let mut predicate = plan.predicate.clone();
        if let Some(after) = &plan.after {
            values.push(after.clone());
            predicate.push_str(&format!(" AND u.user_id COLLATE \"C\">${}", values.len()));
        }
        let params: Vec<&(dyn ToSql + Sync)> = values.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let rows = tx.query(&format!("SELECT u.user_id,u.provider,u.display_name,u.developer_scope,u.client_id,u.status,to_char(u.created_at AT TIME ZONE 'UTC','YYYY-MM-DD\"T\"HH24:MI:SS.US\"Z\"'),c.ready FROM adx_accounts.users u LEFT JOIN adx_accounts.model_credentials c ON c.user_id=u.user_id WHERE {predicate} ORDER BY u.user_id COLLATE \"C\" LIMIT {}", plan.page_size + 1), &params).await.map_err(|_| Error::Unavailable)?;
        let more = rows.len() > plan.page_size;
        let users = rows.iter().take(plan.page_size).map(|row| admin_user_row(row, grant)).collect::<Result<Vec<_>>>()?;
        let next = if more { users.last().and_then(|user| user["user_id"].as_str()).map(|id| plan.page_token(id)).transpose()? } else { None };
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(serde_json::json!({"users":users,"total":total,"next_page_token":next,"degraded":["litellm"]}))
    }

    pub async fn admin_user(&self, grant: &admin::AdminGrant, user: &str) -> Result<serde_json::Value> {
        let query = admin::AdminQuery { filters: BTreeMap::from([("user_id".into(),user.into())]), page_size: 1, page_token: None };
        let page = self.admin_list(grant, "detail", &query).await?;
        page["users"].as_array().and_then(|users| users.first()).cloned().ok_or(Error::Forbidden)
    }

    pub(crate) async fn admin_credential(&self, grant: &admin::AdminGrant, user: &str) -> Result<Option<CredentialRecord>> {
        use tokio_postgres::types::ToSql;
        let query = admin::AdminQuery { filters: BTreeMap::from([("user_id".into(),user.into())]), page_size: 1, page_token: None };
        let plan = admin::QueryPlan::new(grant, "credential", &query)?;
        let params: Vec<&(dyn ToSql + Sync)> = plan.parameters.iter().map(|p| p as &(dyn ToSql + Sync)).collect();
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.build_transaction().read_only(true).start().await.map_err(|_| Error::Unavailable)?;
        let row = tx.query_opt(&format!("SELECT c.credential_version,c.encrypted_key,c.ready FROM adx_accounts.users u LEFT JOIN adx_accounts.model_credentials c ON c.user_id=u.user_id WHERE {}",plan.predicate), &params).await.map_err(|_| Error::Unavailable)?.ok_or(Error::Forbidden)?;
        let version: Option<String> = row.try_get(0).map_err(|_| Error::Unavailable)?;
        let record = version.map(|version| -> Result<_> { Ok(CredentialRecord { version, encrypted_key: row.try_get(1).map_err(|_| Error::Unavailable)?, ready: row.try_get(2).map_err(|_| Error::Unavailable)? }) }).transpose()?;
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok(record)
    }
    pub async fn connect(config: &AccountConfig) -> Result<Self> {
        let url = std::env::var(&config.database_url_env).map_err(|_| Error::Configuration)?;
        Self::from_url(
            &url,
            config.database_allow_plaintext,
            config.database_ca_file.as_deref(),
        )
        .await
    }
    pub async fn from_url(url: &str, allow_plaintext: bool, ca_file: Option<&str>) -> Result<Self> {
        let mut pg: tokio_postgres::Config = url.parse().map_err(|_| Error::Configuration)?;
        pg.connect_timeout(IO_TIMEOUT);
        pg.options("-c statement_timeout=10000 -c lock_timeout=5000");
        let manager = if allow_plaintext {
            pg.ssl_mode(SslMode::Disable);
            Manager::new(pg, tokio_postgres::NoTls)
        } else {
            pg.ssl_mode(SslMode::Require);
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            if let Some(path) = ca_file {
                let pem = std::fs::read(path).map_err(|_| Error::Configuration)?;
                for cert in rustls_pemfile::certs(&mut pem.as_slice()) {
                    roots
                        .add(cert.map_err(|_| Error::Configuration)?)
                        .map_err(|_| Error::Configuration)?;
                }
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::Configuration)?
            .with_root_certificates(roots)
            .with_no_client_auth();
            Manager::new(pg, tokio_postgres_rustls::MakeRustlsConnect::new(tls))
        };
        let pool = Pool::builder(manager)
            .max_size(16)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(IO_TIMEOUT))
            .create_timeout(Some(IO_TIMEOUT))
            .recycle_timeout(Some(IO_TIMEOUT))
            .build()
            .map_err(|_| Error::Configuration)?;
        let store = Self { pool };
        let client = store.pool.get().await.map_err(|_| Error::Unavailable)?;
        let rows = client
            .query("SELECT version FROM adx_accounts.schema_version", &[])
            .await
            .map_err(|_| Error::Configuration)?;
        if rows.len() != 1 || rows[0].get::<_, i32>(0) != 1 {
            return Err(Error::Configuration);
        }
        drop(client);
        Ok(store)
    }
    pub(crate) async fn login(
        &self,
        config: &AccountConfig,
        scoped: ScopedIdentity<'_>,
        agreement: Option<&str>,
        token_digest: &str,
    ) -> Result<(String, i64)> {
        let identity = scoped.identity;
        let union_id = &identity.subject;
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        let existing = tx.query_opt("SELECT user_id,status FROM adx_accounts.users WHERE provider=$1 AND developer_scope=$2 AND union_id=$3 FOR UPDATE", &[&scoped.provider,&scoped.scope,&union_id]).await.map_err(|_| Error::Unavailable)?;
        let user = match existing {
            Some(row) => row,
            None => {
                if agreement != Some(config.agreement_version.as_str()) {
                    return Err(Error::AgreementRequired);
                }
                let id = uuid::Uuid::new_v4().to_string();
                tx.query_one("INSERT INTO adx_accounts.users(user_id,provider,developer_scope,client_id,union_id,agreement_version) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(provider,developer_scope,union_id) DO UPDATE SET union_id=EXCLUDED.union_id RETURNING user_id,status", &[&id,&scoped.provider,&scoped.scope,&scoped.client_id,&union_id,&config.agreement_version]).await.map_err(|_|Error::Unavailable)?
            }
        };
        if user.get::<_, String>(1) != "active" {
            return Err(Error::AccountDisabled);
        }
        let user_id: String = user.get(0);
        // Missing profile permission must not erase a nickname learned earlier.
        tx.execute(
            "UPDATE adx_accounts.users SET display_name=COALESCE($2,display_name) WHERE user_id=$1",
            &[&user_id, &identity.display_name],
        )
        .await
        .map_err(|_| Error::Unavailable)?;
        let ttl = config.session_ttl_seconds as i64;
        let row=tx.query_one("INSERT INTO adx_accounts.sessions(token_digest,user_id,expires_at) VALUES($1,$2,EXTRACT(EPOCH FROM clock_timestamp())::bigint+$3) RETURNING expires_at", &[&token_digest,&user_id,&ttl]).await.map_err(|_|Error::Unavailable)?;
        let expiry = row.get(0);
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok((user_id, expiry))
    }
    /// Returns Unauthorized for missing/expired/revoked tokens, AccountDisabled for
    /// inactive users, and Unavailable for database failures.
    pub async fn authenticate(&self, token_digest: &str) -> Result<Principal> {
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let row=client.query_opt("SELECT s.user_id,s.expires_at,u.status FROM adx_accounts.sessions s JOIN adx_accounts.users u ON u.user_id=s.user_id WHERE s.token_digest=$1 AND s.revoked_at IS NULL AND s.expires_at>EXTRACT(EPOCH FROM clock_timestamp())", &[&token_digest]).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Unauthorized)?;
        if row.get::<_, String>(2) != "active" {
            return Err(Error::AccountDisabled);
        }
        Ok(Principal {
            user_id: row.get(0),
            expires_at: row.get(1),
            token_digest: token_digest.into(),
        })
    }
    /// Idempotently revoke a session; returns Unavailable on database failure.
    pub async fn logout(&self, token_digest: &str) -> Result<()> {
        self.pool.get().await.map_err(|_|Error::Unavailable)?.execute("UPDATE adx_accounts.sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE token_digest=$1", &[&token_digest]).await.map_err(|_|Error::Unavailable)?;
        Ok(())
    }
    pub(crate) async fn model_key_alias(&self, principal: &Principal) -> Result<String> {
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let row = client.query_opt(
            "SELECT provider,display_name FROM adx_accounts.users WHERE user_id=$1 AND status='active'",
            &[&principal.user_id],
        ).await.map_err(|_| Error::Unavailable)?.ok_or(Error::Forbidden)?;
        let provider: String = row.get(0);
        let name: Option<String> = row.get(1);
        Ok(super::model_key_alias(
            &provider,
            &principal.user_id,
            name.as_deref(),
        ))
    }

    pub(crate) async fn credential(
        &self,
        principal: &Principal,
        encrypted: Vec<u8>,
    ) -> Result<CredentialRecord> {
        let version = uuid::Uuid::new_v4().to_string();
        let row=self.pool.get().await.map_err(|_|Error::Unavailable)?.query_opt("INSERT INTO adx_accounts.model_credentials(user_id,credential_version,encrypted_key) SELECT user_id,$2,$3 FROM adx_accounts.users WHERE user_id=$1 AND status='active' ON CONFLICT(user_id) DO UPDATE SET user_id=EXCLUDED.user_id RETURNING encrypted_key,credential_version,ready", &[&principal.user_id,&version,&encrypted]).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Forbidden)?;
        Ok(Self::credential_row(row))
    }
    fn credential_row(row: Row) -> CredentialRecord {
        CredentialRecord {
            encrypted_key: row.get(0),
            version: row.get(1),
            ready: row.get(2),
        }
    }
    pub(crate) async fn mark_ready(&self, principal: &Principal, version: &str) -> Result<()> {
        let n=self.pool.get().await.map_err(|_|Error::Unavailable)?.execute("UPDATE adx_accounts.model_credentials SET ready=true WHERE user_id=$1 AND credential_version=$2", &[&principal.user_id,&version]).await.map_err(|_|Error::Unavailable)?;
        if n != 1 {
            return Err(Error::Unavailable);
        }
        Ok(())
    }
}

fn admin_user_row(row: &Row, grant: &admin::AdminGrant) -> Result<serde_json::Value> {
    let get = |index| row.try_get::<_,String>(index).map_err(|_| Error::Unavailable);
    let provider = get(1)?;
    let label = match provider.as_str() { "huawei" => "Huawei", "agc" => "AGC", _ => return Err(Error::Configuration) };
    let exclusive = match &grant.identity_scope { admin::IdentityScope::Exclusive { exclusive_tenant } => Some(exclusive_tenant), _ => None };
    let ready = row.try_get::<_,Option<bool>>(7).map_err(|_| Error::Unavailable)?;
    let key_state = match ready { Some(true) => "ready", Some(false) => "pending", None => "absent" };
    Ok(serde_json::json!({"user_id":get(0)?,"provider":provider,"provider_label":label,
        "display_name":row.try_get::<_,Option<String>>(2).map_err(|_| Error::Unavailable)?,
        "identity_scope":{"developer_scope":get(3)?,"client_id":get(4)?,"exclusive_tenant":exclusive},
        "status":get(5)?,"created_at":get(6)?,"has_key":ready.is_some(),"key_state":key_state,"has_binding":null,"binding_state":"uncertain"}))
}
