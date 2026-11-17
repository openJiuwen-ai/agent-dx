//! Private bounded PostgreSQL transport shared by ingestion and request accounting.
//! Each caller owns a dedicated pool and its own schema; no account pool is exposed.
use adx_agent_api::Error;
use deadpool_postgres::{Manager, Pool, Runtime};
use serde::Deserialize;
use std::{path::Path, sync::Arc, time::Duration};
use tokio_postgres::config::SslMode;
use zeroize::Zeroizing;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatabaseConfig {
    pub database_url_env: String,
    pub database_max_connections: usize,
    #[serde(default)]
    pub database_allow_plaintext: bool,
    #[serde(default)]
    pub database_ca_file: Option<String>,
}

impl DatabaseConfig {
    pub(crate) fn valid(&self) -> bool {
        !self.database_url_env.is_empty()
            && self.database_url_env.len() <= 128
            && self
                .database_url_env
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            && !self.database_url_env.as_bytes()[0].is_ascii_digit()
            && (1..=32).contains(&self.database_max_connections)
            && self
                .database_ca_file
                .as_ref()
                .is_none_or(|path| Path::new(path).is_absolute() && !self.database_allow_plaintext)
    }

    pub(crate) async fn open(&self) -> Result<Pool, Error> {
        let invalid = || Error::Invalid("invalid connector database configuration".into());
        if !self.valid() {
            return Err(invalid());
        }
        let url = Zeroizing::new(std::env::var(&self.database_url_env).map_err(|_| invalid())?);
        let mut config: tokio_postgres::Config = url.parse().map_err(|_| invalid())?;
        let timeout = Duration::from_secs(5);
        config.connect_timeout(timeout);
        config.options("-c statement_timeout=10000 -c lock_timeout=5000");
        let manager = if self.database_allow_plaintext {
            config.ssl_mode(SslMode::Disable);
            Manager::new(config, tokio_postgres::NoTls)
        } else {
            config.ssl_mode(SslMode::Require);
            let mut roots = rustls::RootCertStore::empty();
            roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
            if let Some(path) = &self.database_ca_file {
                let bytes = std::fs::read(path).map_err(|_| invalid())?;
                for certificate in rustls_pemfile::certs(&mut bytes.as_slice()) {
                    roots
                        .add(certificate.map_err(|_| invalid())?)
                        .map_err(|_| invalid())?;
                }
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| invalid())?
            .with_root_certificates(roots)
            .with_no_client_auth();
            Manager::new(config, tokio_postgres_rustls::MakeRustlsConnect::new(tls))
        };
        Pool::builder(manager)
            .max_size(self.database_max_connections)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(timeout))
            .create_timeout(Some(timeout))
            .recycle_timeout(Some(timeout))
            .build()
            .map_err(|_| invalid())
    }
}
