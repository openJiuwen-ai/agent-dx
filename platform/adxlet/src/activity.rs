//! Node-local observations. The supervisor/registration path supplies the active
//! proxy session; snapshots cannot register an arbitrary replacement session.
use adx_core::{Error, Result as CoreResult};
use adx_protocol::relay::{
    node_activity_service_server::NodeActivityService, ActivityAcknowledgement, ActivitySnapshot,
};
use std::{collections::BTreeMap, sync::Mutex, time::Duration};
use tokio::time::Instant;
use tonic::{Request, Response, Status};

pub struct ActivityReceiver {
    session: String,
    max_age: Duration,
    snapshot: Mutex<Option<Snapshot>>,
}
struct Snapshot {
    sequence: u64,
    received: Instant,
    counts: BTreeMap<String, u64>,
}
impl ActivityReceiver {
    pub fn new(session: String, max_age: Duration) -> CoreResult<Self> {
        if session.trim().is_empty() || max_age.is_zero() {
            return Err(Error::Invalid(
                "registered proxy session and positive maximum age required".into(),
            ));
        }
        Ok(Self {
            session,
            max_age,
            snapshot: Mutex::default(),
        })
    }
    /// None means unknown/stale, never evidence of idleness.
    pub fn active_streams(&self, environment: &str) -> Option<u64> {
        self.snapshot
            .lock()
            .expect("shared state lock poisoned")
            .as_ref()
            .filter(|snapshot| snapshot.received.elapsed() < self.max_age)
            .map(|snapshot| snapshot.counts.get(environment).copied().unwrap_or(0))
    }
    pub fn apply(&self, request: ActivitySnapshot) -> CoreResult<ActivityAcknowledgement> {
        if request.proxy_session_id != self.session {
            return Err(Error::Conflict);
        }
        if request.sequence == 0 {
            return Err(Error::Invalid("snapshot sequence must be positive".into()));
        }
        let mut counts = BTreeMap::new();
        for environment in request.environments {
            if environment.environment_id.trim().is_empty()
                || counts
                    .insert(environment.environment_id, environment.active_streams)
                    .is_some()
            {
                return Err(Error::Invalid(
                    "snapshot environment identities must be nonempty and unique".into(),
                ));
            }
        }
        let mut current = self.snapshot.lock().expect("shared state lock poisoned");
        if let Some(old) = current.as_ref() {
            if request.sequence < old.sequence {
                return Ok(ActivityAcknowledgement {
                    accepted_sequence: old.sequence,
                });
            }
            if request.sequence == old.sequence {
                if old.counts != counts {
                    return Err(Error::Conflict);
                }
                // Duplicates do not renew observation freshness.
                return Ok(ActivityAcknowledgement {
                    accepted_sequence: old.sequence,
                });
            }
        }
        *current = Some(Snapshot {
            sequence: request.sequence,
            received: Instant::now(),
            counts,
        });
        Ok(ActivityAcknowledgement {
            accepted_sequence: request.sequence,
        })
    }
}
#[tonic::async_trait]
impl NodeActivityService for ActivityReceiver {
    async fn report_snapshot(
        &self,
        request: Request<ActivitySnapshot>,
    ) -> Result<Response<ActivityAcknowledgement>, Status> {
        Ok(Response::new(self.apply(request.into_inner()).map_err(
            |error| match error {
                Error::Invalid(message) => Status::invalid_argument(message),
                Error::Conflict => {
                    Status::failed_precondition("stale session or conflicting snapshot")
                }
                error => Status::internal(error.to_string()),
            },
        )?))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use adx_protocol::relay::EnvironmentActivity;
    fn snapshot(sequence: u64, count: Option<u64>) -> ActivitySnapshot {
        ActivitySnapshot {
            proxy_session_id: "session".into(),
            sequence,
            environments: count
                .map(|active_streams| {
                    vec![EnvironmentActivity {
                        environment_id: "i".into(),
                        active_streams,
                    }]
                })
                .unwrap_or_default(),
        }
    }
    #[tokio::test(start_paused = true)]
    async fn full_snapshot_replaces_counts_but_missing_or_stale_observation_is_unknown() {
        let receiver = ActivityReceiver::new("session".into(), Duration::from_secs(5)).unwrap();
        assert_eq!(receiver.active_streams("i"), None);
        receiver.apply(snapshot(1, Some(2))).unwrap();
        assert_eq!(receiver.active_streams("i"), Some(2));
        receiver.apply(snapshot(2, None)).unwrap();
        assert_eq!(receiver.active_streams("i"), Some(0));
        tokio::time::advance(Duration::from_secs(5)).await;
        receiver.apply(snapshot(2, None)).unwrap();
        assert_eq!(receiver.active_streams("i"), None);
    }
    #[tokio::test]
    async fn stale_and_conflicting_reports_cannot_replace_current_state() {
        let receiver = ActivityReceiver::new("session".into(), Duration::from_secs(5)).unwrap();
        receiver.apply(snapshot(2, Some(3))).unwrap();
        assert_eq!(
            receiver.apply(snapshot(1, None)).unwrap().accepted_sequence,
            2
        );
        assert!(receiver.apply(snapshot(2, None)).is_err());
        let mut other = snapshot(3, None);
        other.proxy_session_id = "old-process".into();
        assert!(receiver.apply(other).is_err());
        assert_eq!(receiver.active_streams("i"), Some(3));
    }
}
