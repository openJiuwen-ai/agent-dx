//! Trusted process configuration and assembly for the existing Gateway lifecycle.
use super::{Error, Inbox, InboxLimits};
use deadpool_postgres::{Manager, Pool, Runtime};
use serde::Deserialize;
use std::{path::Path, sync::Arc, time::Duration};
use tokio_postgres::config::SslMode;
use zeroize::Zeroizing;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct IngestionConfig {
    pub database_url_env: String,
    pub database_max_connections: usize,
    #[serde(default)]
    pub database_allow_plaintext: bool,
    #[serde(default)]
    pub database_ca_file: Option<String>,
    pub workers: usize,
    pub lease_seconds: u64,
    pub delivery_timeout_seconds: u64,
    pub max_pending_events: i64,
    pub max_pending_bytes: i64,
    pub per_subscription_events: i64,
    pub debounce_milliseconds: u64,
    pub dedupe_retention_seconds: u64,
}

impl IngestionConfig {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        if self.database_url_env.is_empty()
            || self.database_url_env.len() > 128
            || !self
                .database_url_env
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || byte == b'_')
            || self.database_url_env.as_bytes()[0].is_ascii_digit()
            || self
                .database_ca_file
                .as_ref()
                .is_some_and(|path| !Path::new(path).is_absolute() || self.database_allow_plaintext)
            || !(1..=32).contains(&self.database_max_connections)
            || !(1..=16).contains(&self.workers)
            || !(3..=300).contains(&self.lease_seconds)
            || !(1..=120).contains(&self.delivery_timeout_seconds)
            || self.max_pending_events <= 0
            || self.max_pending_bytes < 1_048_576
            || self.per_subscription_events <= 0
            || self.per_subscription_events > self.max_pending_events
            || self.debounce_milliseconds > 30_000
            || !(86_400..=2_592_000).contains(&self.dedupe_retention_seconds)
        {
            return Err(Error::InvalidLimit);
        }
        Ok(())
    }

    /// Dedicated bounded pool; never borrows the account/privacy owner's pool.
    /// TLS is mandatory unless a trusted operator explicitly selects plaintext.
    /// Requires the separately applied ingestion schema; startup performs no DDL.
    pub(crate) async fn open(&self) -> Result<Inbox, Error> {
        self.validate()?;
        let url =
            Zeroizing::new(std::env::var(&self.database_url_env).map_err(|_| Error::InvalidLimit)?);
        let mut config: tokio_postgres::Config = url.parse().map_err(|_| Error::InvalidLimit)?;
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
                let bytes = std::fs::read(path).map_err(|_| Error::InvalidLimit)?;
                for certificate in rustls_pemfile::certs(&mut bytes.as_slice()) {
                    roots
                        .add(certificate.map_err(|_| Error::InvalidLimit)?)
                        .map_err(|_| Error::InvalidLimit)?;
                }
            }
            let tls = rustls::ClientConfig::builder_with_provider(Arc::new(
                rustls::crypto::ring::default_provider(),
            ))
            .with_safe_default_protocol_versions()
            .map_err(|_| Error::InvalidLimit)?
            .with_root_certificates(roots)
            .with_no_client_auth();
            Manager::new(config, tokio_postgres_rustls::MakeRustlsConnect::new(tls))
        };
        let pool = Pool::builder(manager)
            .max_size(self.database_max_connections)
            .runtime(Runtime::Tokio1)
            .wait_timeout(Some(timeout))
            .create_timeout(Some(timeout))
            .recycle_timeout(Some(timeout))
            .build()
            .map_err(|_| Error::InvalidLimit)?;
        Inbox::from_pool(
            pool,
            InboxLimits {
                max_pending_events: self.max_pending_events,
                max_pending_bytes: self.max_pending_bytes,
                per_subscription_events: self.per_subscription_events,
                debounce: Duration::from_millis(self.debounce_milliseconds),
                dedupe_retention: Duration::from_secs(self.dedupe_retention_seconds),
            },
        )
        .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn configuration() -> serde_json::Value {
        serde_json::json!({
            "database_url_env":"ADX_INGESTION_DATABASE_URL", "database_max_connections":8,
            "workers":2, "lease_seconds":30, "delivery_timeout_seconds":60,
            "max_pending_events":10000, "max_pending_bytes":67108864,
            "per_subscription_events":1000, "debounce_milliseconds":1000,
            "dedupe_retention_seconds":604800,
        })
    }

    #[test]
    fn runtime_configuration_has_explicit_limits_and_tls_by_default() {
        let config: IngestionConfig = serde_json::from_value(configuration()).unwrap();
        assert!(config.validate().is_ok());
        assert!(!config.database_allow_plaintext);
        for (name, value) in [
            ("database_max_connections", 0),
            ("database_max_connections", 33),
            ("workers", 0),
            ("workers", 17),
            ("lease_seconds", 2),
            ("lease_seconds", 301),
            ("delivery_timeout_seconds", 0),
            ("delivery_timeout_seconds", 121),
            ("max_pending_events", 0),
            ("max_pending_bytes", 1048575),
            ("per_subscription_events", 10001),
            ("debounce_milliseconds", 30001),
            ("dedupe_retention_seconds", 86399),
            ("dedupe_retention_seconds", 2592001),
        ] {
            let mut invalid = configuration();
            invalid[name] = value.into();
            assert!(
                serde_json::from_value::<IngestionConfig>(invalid)
                    .unwrap()
                    .validate()
                    .is_err(),
                "{name}={value}"
            );
        }
        let mut invalid = configuration();
        invalid["database_url_env"] = "".into();
        assert!(serde_json::from_value::<IngestionConfig>(invalid)
            .unwrap()
            .validate()
            .is_err());
        let mut unknown = configuration();
        unknown["project_api_key"] = "must-not-be-a-startup-field".into();
        assert!(serde_json::from_value::<IngestionConfig>(unknown).is_err());
    }

    #[test]
    fn ingestion_cannot_be_enabled_without_the_actual_privacy_runtime() {
        let value = serde_json::json!({
            "tenant":"tenant", "jiuwen":{"agent_type":"jiuwen","template":"jiuwen","version":"1"},
            "ingestion":configuration(),
        });
        let config: super::super::super::entrypoint::DigitalMateGatewayConfig =
            serde_json::from_value(value).unwrap();
        assert!(config.validate().is_err());
    }
}
