use super::*;
use deadpool_postgres::{Manager, Pool, Runtime};
use tokio_postgres::{config::SslMode, Row};

#[derive(Clone)]
pub struct AccountStore {
    pool: Pool,
}
pub(crate) struct CredentialRecord {
    pub encrypted_key: Vec<u8>,
    pub version: String,
    pub ready: bool,
}
impl AccountStore {
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
    pub async fn login(
        &self,
        config: &AccountConfig,
        client_id: &str,
        union_id: &str,
        agreement: Option<&str>,
        token_digest: &str,
    ) -> Result<(String, i64)> {
        let mut client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let tx = client.transaction().await.map_err(|_| Error::Unavailable)?;
        let existing = tx.query_opt("SELECT user_id,status FROM adx_accounts.users WHERE tenant=$1 AND provider='huawei' AND developer_scope=$2 AND union_id=$3 FOR UPDATE", &[&config.tenant,&config.developer_scope,&union_id]).await.map_err(|_| Error::Unavailable)?;
        let user = match existing {
            Some(row) => row,
            None => {
                if agreement != Some(config.agreement_version.as_str()) {
                    return Err(Error::AgreementRequired);
                }
                let id = uuid::Uuid::new_v4().to_string();
                tx.query_one("INSERT INTO adx_accounts.users(tenant,user_id,developer_scope,client_id,union_id,agreement_version) VALUES($1,$2,$3,$4,$5,$6) ON CONFLICT(tenant,provider,developer_scope,union_id) DO UPDATE SET union_id=EXCLUDED.union_id RETURNING user_id,status", &[&config.tenant,&id,&config.developer_scope,&client_id,&union_id,&config.agreement_version]).await.map_err(|_|Error::Unavailable)?
            }
        };
        if user.get::<_, String>(1) != "active" {
            return Err(Error::Forbidden);
        }
        let user_id: String = user.get(0);
        let ttl = config.session_ttl_seconds as i64;
        let row=tx.query_one("INSERT INTO adx_accounts.sessions(token_digest,tenant,user_id,expires_at) VALUES($1,$2,$3,EXTRACT(EPOCH FROM clock_timestamp())::bigint+$4) RETURNING expires_at", &[&token_digest,&config.tenant,&user_id,&ttl]).await.map_err(|_|Error::Unavailable)?;
        let expiry = row.get(0);
        tx.commit().await.map_err(|_| Error::Unavailable)?;
        Ok((user_id, expiry))
    }
    pub async fn authenticate(&self, tenant: &str, token_digest: &str) -> Result<Principal> {
        let client = self.pool.get().await.map_err(|_| Error::Unavailable)?;
        let row=client.query_opt("SELECT s.user_id,s.expires_at,u.status FROM adx_accounts.sessions s JOIN adx_accounts.users u ON u.tenant=s.tenant AND u.user_id=s.user_id WHERE s.tenant=$1 AND s.token_digest=$2 AND s.revoked_at IS NULL AND s.expires_at>EXTRACT(EPOCH FROM clock_timestamp())", &[&tenant,&token_digest]).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Unauthorized)?;
        if row.get::<_, String>(2) != "active" {
            return Err(Error::Forbidden);
        }
        Ok(Principal {
            tenant: tenant.into(),
            user_id: row.get(0),
            expires_at: row.get(1),
            token_digest: token_digest.into(),
        })
    }
    pub async fn logout(&self, tenant: &str, token_digest: &str) -> Result<()> {
        self.pool.get().await.map_err(|_|Error::Unavailable)?.execute("UPDATE adx_accounts.sessions SET revoked_at=COALESCE(revoked_at,now()) WHERE tenant=$1 AND token_digest=$2", &[&tenant,&token_digest]).await.map_err(|_|Error::Unavailable)?;
        Ok(())
    }
    pub(crate) async fn credential(
        &self,
        principal: &Principal,
        encrypted: Vec<u8>,
    ) -> Result<CredentialRecord> {
        let version = uuid::Uuid::new_v4().to_string();
        let row=self.pool.get().await.map_err(|_|Error::Unavailable)?.query_opt("INSERT INTO adx_accounts.model_credentials(tenant,user_id,credential_version,encrypted_key) SELECT tenant,user_id,$3,$4 FROM adx_accounts.users WHERE tenant=$1 AND user_id=$2 AND status='active' ON CONFLICT(tenant,user_id) DO UPDATE SET user_id=EXCLUDED.user_id RETURNING encrypted_key,credential_version,ready", &[&principal.tenant,&principal.user_id,&version,&encrypted]).await.map_err(|_|Error::Unavailable)?.ok_or(Error::Forbidden)?;
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
        let n=self.pool.get().await.map_err(|_|Error::Unavailable)?.execute("UPDATE adx_accounts.model_credentials SET ready=true WHERE tenant=$1 AND user_id=$2 AND credential_version=$3", &[&principal.tenant,&principal.user_id,&version]).await.map_err(|_|Error::Unavailable)?;
        if n != 1 {
            return Err(Error::Unavailable);
        }
        Ok(())
    }
}
