//! Meta store, log, checkpoint and history settings with their defaults, and
//! the Meta limits validated for every role.

use super::{Cli, Config, FileConfig, MetaStoreBackend, invalid};
use afs_error::Result;

/// Meta settings; the defaults come from the Meta store itself.
pub(super) struct MetaSettings {
    pub(super) meta_store: MetaStoreBackend,
    pub(super) meta_redis_url: Option<String>,
    pub(super) meta_etcd_endpoints: Vec<String>,
    pub(super) meta_etcd_max_request_bytes: usize,
    pub(super) meta_store_prefix: String,
    pub(super) meta_record_max_bytes: usize,
    pub(super) meta_batch_max_bytes: usize,
    pub(super) meta_wal_max_bytes: u64,
    pub(super) meta_checkpoint_wal_bytes: u64,
    pub(super) meta_checkpoint_wal_records: u64,
    pub(super) meta_checkpoint_interval_ms: u64,
    pub(super) meta_wal_segment_bytes: u64,
    pub(super) meta_request_retention_ms: u64,
    pub(super) meta_request_outcomes_max: usize,
    pub(super) meta_request_bytes_max: u64,
    pub(super) meta_watch_events_max: usize,
    pub(super) meta_watch_bytes_max: u64,
    pub(super) meta_watch_retention_ms: u64,
    pub(super) allow_volatile_meta: bool,
    pub(super) ownerfs_mkdir_load_balance: bool,
}

pub(super) fn resolve(cli: &Cli, file: &FileConfig) -> MetaSettings {
    let log_defaults = crate::meta::store::local_log::LocalLogOptions::default();
    let maintenance = crate::meta::store::journal::MaintenancePolicy::default();
    let history = crate::meta::store::HistoryLimits::default();
    let meta_store = cli
        .meta_store
        .or(file.meta_store)
        .unwrap_or(MetaStoreBackend::LocalFile);
    MetaSettings {
        meta_store,
        meta_redis_url: cli.meta_redis_url.clone().or(file.meta_redis_url.clone()),
        meta_etcd_endpoints: cli
            .meta_etcd_endpoints
            .clone()
            .or(file.meta_etcd_endpoints.clone())
            .unwrap_or_default(),
        meta_etcd_max_request_bytes: cli
            .meta_etcd_max_request_bytes
            .or(file.meta_etcd_max_request_bytes)
            .unwrap_or(ETCD_DEFAULT_MAX_REQUEST_BYTES),
        meta_store_prefix: cli
            .meta_store_prefix
            .clone()
            .or(file.meta_store_prefix.clone())
            .unwrap_or_else(|| "afs/meta".into()),
        meta_record_max_bytes: cli
            .meta_record_max_bytes
            .or(file.meta_record_max_bytes)
            .unwrap_or(log_defaults.record_max_bytes),
        meta_batch_max_bytes: cli
            .meta_batch_max_bytes
            .or(file.meta_batch_max_bytes)
            .unwrap_or(log_defaults.batch_max_bytes),
        meta_wal_max_bytes: cli
            .meta_wal_max_bytes
            .or(file.meta_wal_max_bytes)
            .unwrap_or(if meta_store == MetaStoreBackend::Etcd {
                crate::meta::store::ETCD_WAL_MAX_BYTES
            } else {
                log_defaults.wal_max_bytes
            }),
        meta_checkpoint_wal_bytes: cli
            .meta_checkpoint_wal_bytes
            .or(file.meta_checkpoint_wal_bytes)
            .unwrap_or(maintenance.wal_bytes),
        meta_checkpoint_wal_records: cli
            .meta_checkpoint_wal_records
            .or(file.meta_checkpoint_wal_records)
            .unwrap_or(maintenance.wal_records),
        meta_checkpoint_interval_ms: cli
            .meta_checkpoint_interval_ms
            .or(file.meta_checkpoint_interval_ms)
            .unwrap_or(60_000),
        meta_wal_segment_bytes: cli
            .meta_wal_segment_bytes
            .or(file.meta_wal_segment_bytes)
            .unwrap_or(log_defaults.segment_bytes),
        meta_request_retention_ms: cli
            .meta_request_retention_ms
            .or(file.meta_request_retention_ms)
            .unwrap_or(86_400_000),
        meta_request_outcomes_max: cli
            .meta_request_outcomes_max
            .or(file.meta_request_outcomes_max)
            .unwrap_or(history.request_count),
        meta_request_bytes_max: cli
            .meta_request_bytes_max
            .or(file.meta_request_bytes_max)
            .unwrap_or(history.request_bytes),
        meta_watch_events_max: cli
            .meta_watch_events_max
            .or(file.meta_watch_events_max)
            .unwrap_or(history.event_count),
        meta_watch_bytes_max: cli
            .meta_watch_bytes_max
            .or(file.meta_watch_bytes_max)
            .unwrap_or(history.event_bytes),
        meta_watch_retention_ms: cli
            .meta_watch_retention_ms
            .or(file.meta_watch_retention_ms)
            .unwrap_or(3_600_000),
        allow_volatile_meta: cli
            .allow_volatile_meta
            .or(file.allow_volatile_meta)
            .unwrap_or(false),
        ownerfs_mkdir_load_balance: cli
            .ownerfs_mkdir_load_balance
            .or(file.ownerfs_mkdir_load_balance)
            .unwrap_or(false),
    }
}

/// etcd's own default `--max-request-bytes`.
pub(super) const ETCD_DEFAULT_MAX_REQUEST_BYTES: usize = 1536 * 1024;

/// Where a `redis`/`etcd` Meta log lives, validated against `meta_store`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum MetaStoreTarget {
    Local,
    Redis {
        url: String,
        prefix: String,
    },
    Etcd {
        endpoints: Vec<String>,
        prefix: String,
        max_request_bytes: usize,
    },
}

/// `redis://user:secret@host` → `redis://***@host`; anything else unchanged.
pub(super) fn redact_url(url: &str) -> String {
    match url.split_once("://") {
        Some((scheme, rest)) => match rest.split_once('@') {
            Some((_, host)) => format!("{scheme}://***@{host}"),
            None => url.to_owned(),
        },
        None => url.to_owned(),
    }
}

pub(super) fn serialize_redacted_url<S: serde::Serializer>(
    url: &Option<String>,
    serializer: S,
) -> std::result::Result<S::Ok, S::Error> {
    match url {
        Some(url) => serializer.serialize_some(&redact_url(url)),
        None => serializer.serialize_none(),
    }
}

impl Config {
    /// The configured shared-service log, or `Local` for local-file/memory.
    /// Endpoints are required by their own backend and rejected otherwise;
    /// the prefix is 1..=128 printable ASCII bytes without spaces, `*?[]\`
    /// or a trailing `/`.
    pub(crate) fn meta_store_target(&self) -> Result<MetaStoreTarget> {
        let prefix = &self.meta_store_prefix;
        if prefix.is_empty()
            || prefix.len() > 128
            || prefix.ends_with('/')
            || !prefix
                .bytes()
                .all(|c| c.is_ascii_graphic() && !b"*?[]\\".contains(&c))
        {
            return Err(invalid(
                "meta_store_prefix must be 1..=128 printable ASCII bytes without spaces, *?[]\\ or a trailing /",
            ));
        }
        let redis = self.meta_redis_url.as_ref();
        let etcd = !self.meta_etcd_endpoints.is_empty();
        match self.meta_store {
            MetaStoreBackend::Redis => {
                if !cfg!(feature = "meta-redis") {
                    return Err(invalid(
                        "meta_store = \"redis\" needs afs-meta built with the meta-redis feature",
                    ));
                }
                if etcd {
                    return Err(invalid(
                        "meta_etcd_endpoints is only valid with meta_store = \"etcd\"",
                    ));
                }
                let url = redis
                    .filter(|url| !url.is_empty())
                    .ok_or_else(|| invalid("meta_store = \"redis\" requires meta_redis_url"))?;
                Ok(MetaStoreTarget::Redis {
                    url: url.clone(),
                    prefix: prefix.clone(),
                })
            }
            MetaStoreBackend::Etcd => {
                if !cfg!(feature = "meta-etcd") {
                    return Err(invalid(
                        "meta_store = \"etcd\" needs afs-meta built with the meta-etcd feature",
                    ));
                }
                if redis.is_some() {
                    return Err(invalid(
                        "meta_redis_url is only valid with meta_store = \"redis\"",
                    ));
                }
                if !etcd || self.meta_etcd_endpoints.iter().any(String::is_empty) {
                    return Err(invalid(
                        "meta_store = \"etcd\" requires meta_etcd_endpoints",
                    ));
                }
                if !(256 * 1024..=1024 * 1024 * 1024).contains(&self.meta_etcd_max_request_bytes) {
                    return Err(invalid("meta_etcd_max_request_bytes must be 256KiB..=1GiB"));
                }
                Ok(MetaStoreTarget::Etcd {
                    endpoints: self.meta_etcd_endpoints.clone(),
                    prefix: prefix.clone(),
                    max_request_bytes: self.meta_etcd_max_request_bytes,
                })
            }
            MetaStoreBackend::LocalFile | MetaStoreBackend::InMemory => {
                if redis.is_some() || etcd {
                    return Err(invalid(
                        "meta_redis_url / meta_etcd_endpoints require meta_store = \"redis\" / \"etcd\"",
                    ));
                }
                Ok(MetaStoreTarget::Local)
            }
        }
    }

    /// Validated for every backend so a configuration accepted for one
    /// backend stays valid for the other; the memory log uses the append and
    /// WAL byte bounds only.
    pub(crate) fn meta_log_limits(&self) -> Result<crate::meta::store::local_log::LocalLogOptions> {
        if self.meta_wal_segment_bytes > self.meta_wal_max_bytes {
            return Err(invalid(
                "meta_wal_segment_bytes must not exceed meta_wal_max_bytes",
            ));
        }
        crate::meta::store::local_log::LocalLogOptions {
            record_max_bytes: self.meta_record_max_bytes,
            batch_max_bytes: self.meta_batch_max_bytes,
            wal_max_bytes: self.meta_wal_max_bytes,
            segment_bytes: self.meta_wal_segment_bytes,
            ..Default::default()
        }
        .validate()
        .map_err(|error| invalid(error.to_string()))
    }

    pub(crate) fn meta_maintenance_policy(
        &self,
    ) -> Result<crate::meta::store::journal::MaintenancePolicy> {
        use std::time::Duration;
        let defaults = crate::meta::store::journal::MaintenancePolicy::default();
        crate::meta::store::journal::MaintenancePolicy {
            wal_bytes: self.meta_checkpoint_wal_bytes,
            wal_records: self.meta_checkpoint_wal_records,
            interval: Duration::from_millis(self.meta_checkpoint_interval_ms),
            // Short test/operator intervals must also shorten the polling period.
            poll: defaults
                .poll
                .min(Duration::from_millis(self.meta_checkpoint_interval_ms)),
            ..defaults
        }
        .validate()
        .map_err(|error| invalid(error.to_string()))
    }

    pub(crate) fn meta_history_limits(&self) -> Result<crate::meta::store::HistoryLimits> {
        use std::time::Duration;
        crate::meta::store::HistoryLimits {
            request_count: self.meta_request_outcomes_max,
            request_bytes: self.meta_request_bytes_max,
            request_retention: Duration::from_millis(self.meta_request_retention_ms),
            event_count: self.meta_watch_events_max,
            event_bytes: self.meta_watch_bytes_max,
            event_retention: Duration::from_millis(self.meta_watch_retention_ms),
            ..Default::default()
        }
        .validate()
        .map_err(|error| invalid(error.to_string()))
    }
}
