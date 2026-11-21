//! Background membership refresh. Request routing reads only the last successful local snapshot.
use crate::{Error, Result};
use adx_agent_core::discovery::ActivatorEndpoint;
use adx_agent_store::discovery::RedisRegistry;
use serde::Deserialize;
use std::{
    collections::BTreeSet,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
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
    synced: Arc<AtomicBool>,
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
        // Membership-change notifications let Ingress re-read promptly on
        // register/unregister instead of waiting for the periodic refresh.
        // The periodic interval below remains as a fallback for lost
        // notifications and older Activators that do not publish.
        let mut events = registry.events();
        let mut events_open = true;
        loop {
            let snapshot = match registry.members().await {
                Ok(snapshot) => {
                    // Latch the first successful read as the initialization
                    // signal (admission readiness); an empty-but-successful
                    // read counts — "no members" is a runtime state, not an
                    // uninitialized one, and must not un-ready the Gateway.
                    // A later invalid record is likewise a runtime condition:
                    // the last membership is retained and requests fail
                    // retryably, but readiness stays latched.
                    synced.store(true, Ordering::Release);
                    validate_members(
                        // Selection filter: only ready-and-not-draining members
                        // are addressable. Records without condition fields
                        // (older Activators) deserialize as selectable, so mixed
                        // versions interoperate during a rolling upgrade.
                        snapshot
                            .into_iter()
                            .filter(|member| member.condition.selectable())
                            .map(|member| member.endpoint)
                            .collect(),
                        allow_plaintext,
                    )
                }
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
            let period = Duration::from_millis(refresh_ms * 4 / 5 + jitter);
            if events_open {
                tokio::select! {
                    _ = tokio::time::sleep(period) => {}
                    recv = events.recv() => {
                        // Some(()) -> fall through and refresh immediately.
                        // None -> notification channel closed; fall back to the
                        // periodic interval only (avoid busy-looping on a
                        // closed receiver).
                        if recv.is_none() {
                            events_open = false;
                            tokio::time::sleep(period).await;
                        }
                    }
                }
            } else {
                tokio::time::sleep(period).await;
            }
        }
    }))
}
