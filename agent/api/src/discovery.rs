//! Background membership refresh. Request routing reads only the last successful local snapshot.
use crate::{Error, Result};
use adx_agent_core::discovery::ActivatorEndpoint;
use adx_agent_store::discovery::RedisRegistry;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    sync::{Arc, RwLock},
    time::Duration,
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct DiscoveryConfig {
    pub redis_url: String,
    pub namespace: String,
    #[serde(default = "refresh_seconds")]
    pub refresh_seconds: u64,
}
fn refresh_seconds() -> u64 {
    5
}

pub(crate) fn validate_members(
    mut members: Vec<ActivatorEndpoint>,
    allow_plaintext: bool,
) -> Result<Vec<ActivatorEndpoint>> {
    let mut ids = BTreeSet::new();
    let mut urls = BTreeSet::new();
    for member in &mut members {
        member.validate(allow_plaintext).map_err(Error::Invalid)?;
        member.url = adx_agent_core::transport::service_origin(&member.url, allow_plaintext)
            .map_err(Error::Invalid)?
            .as_str()
            .trim_end_matches('/')
            .to_owned();
        if !ids.insert(member.id.clone()) || !urls.insert(member.url.clone()) {
            return Err(Error::Invalid(
                "duplicate Activator instance id or address".into(),
            ));
        }
    }
    members.sort_by(|a, b| a.id.cmp(&b.id));
    Ok(members)
}

pub(crate) fn start(
    config: DiscoveryConfig,
    members: Arc<RwLock<Vec<ActivatorEndpoint>>>,
    allow_plaintext: bool,
) -> Result<tokio::task::JoinHandle<()>> {
    if !(1..=300).contains(&config.refresh_seconds) {
        return Err(Error::Invalid(
            "discovery refresh_seconds must be 1..300".into(),
        ));
    }
    let registry =
        RedisRegistry::new(&config.redis_url, &config.namespace, Duration::from_secs(3))?;
    let refresh_ms = config.refresh_seconds * 1000;
    let runtime = tokio::runtime::Handle::try_current()
        .map_err(|_| Error::Invalid("discovery requires a Tokio runtime".into()))?;
    Ok(runtime.spawn(async move {
        loop {
            let snapshot = match registry.members().await {
                Ok(snapshot) => validate_members(snapshot, allow_plaintext),
                Err(error) => Err(error.into()),
            };
            match snapshot {
                Ok(snapshot) => {
                    let Ok(mut current) = members.write() else {
                        return;
                    };
                    *current = snapshot;
                }
                Err(error) => {
                    tracing::warn!(%error, "Activator discovery failed; retaining last membership")
                }
            }
            // ±20% jitter prevents all Gateway replicas refreshing in lockstep.
            let entropy = uuid::Uuid::new_v4().as_u128();
            let jitter = u64::try_from(entropy % u128::from(refresh_ms * 2 / 5 + 1)).unwrap_or(0);
            tokio::time::sleep(Duration::from_millis(refresh_ms * 4 / 5 + jitter)).await;
        }
    }))
}
