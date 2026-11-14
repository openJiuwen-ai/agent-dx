//! Trusted process configuration and assembly for the existing Gateway lifecycle.
use super::super::database::DatabaseConfig;
use super::{Error, Inbox, InboxLimits};
use serde::Deserialize;
use std::time::Duration;

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
    fn database(&self) -> DatabaseConfig {
        DatabaseConfig {
            database_url_env: self.database_url_env.clone(),
            database_max_connections: self.database_max_connections,
            database_allow_plaintext: self.database_allow_plaintext,
            database_ca_file: self.database_ca_file.clone(),
        }
    }

    pub(crate) fn validate(&self) -> Result<(), Error> {
        if !self.database().valid()
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
        let pool = self
            .database()
            .open()
            .await
            .map_err(|_| Error::Unavailable)?;
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
