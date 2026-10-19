use super::*;
use adx_core::environment_control::EnvironmentControlRecord;
use std::time::{SystemTime, UNIX_EPOCH};

pub(crate) fn now_ms() -> Result<u64> {
    u64::try_from(
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|_| Error::Unavailable("clock before epoch".into()))?
            .as_millis(),
    )
    .map_err(|_| Error::Unavailable("clock out of range".into()))
}
pub(crate) fn seed(spec: &EnvironmentSpec, generation: u64) -> Option<EnvironmentControlRecord> {
    spec.compatibility
        .clone()
        .map(|c| EnvironmentControlRecord::new(spec.id.clone(), c, generation))
}
pub(super) fn transition(stored: &mut StoredEnvironment, result: &EnvironmentRecord) -> Result<()> {
    let previous = stored.result.as_ref().map(|r| r.state);
    if stored.control.is_none() {
        stored.control = seed(&stored.spec, result.assignment.generation);
    }
    let Some(c) = &mut stored.control else {
        return Ok(());
    };
    match result.state {
        EnvironmentState::Running if c.expires_at_unix_ms == 0 => {
            c.begin_run(now_ms()?, result.assignment.generation)?
        }
        EnvironmentState::Running => c.rebind(result.assignment.generation)?,
        EnvironmentState::Paused => {
            // Snapshot uses its own pause/resume internally; preserve its lease.
            let explicit = result
                .last_operation
                .as_ref()
                .is_some_and(|op| op.kind == adx_core::LifecycleKind::Pause);
            if explicit && previous != Some(EnvironmentState::Paused) {
                c.pause()?;
            }
        }
        _ => (),
    }
    Ok(())
}
impl Session {
    pub async fn environment_control(&self, id: &str) -> Result<EnvironmentControlRecord> {
        self.get(id).await?.control.ok_or(Error::NotFound)
    }
    async fn edit_environment_control<F>(&self, id: &str, mut edit: F) -> Result<StoredEnvironment>
    where
        F: FnMut(&mut StoredEnvironment) -> Result<()>,
    {
        let field = format!("environment:{id}");
        for _ in 0..ATTEMPTS {
            let [raw_header, raw_env] = self.store.fields([HEADER.into(), field.clone()]).await?;
            let mut header = self.header(&raw_header)?;
            let mut stored: StoredEnvironment = decode(raw_env.as_deref().ok_or(Error::NotFound)?)?;
            edit(&mut stored)?;
            stored.validate()?;
            header.advance()?;
            if self
                .store
                .cas(
                    raw_header.as_deref().ok_or(Error::Conflict)?,
                    &header,
                    Some((&field, encode(&stored)?)),
                )
                .await?
            {
                return Ok(stored);
            }
        }
        Err(Error::Unavailable(
            "sandbox control contention; retry request".into(),
        ))
    }
    pub async fn set_sandbox_timeout(
        &self,
        id: &str,
        seconds: u64,
        extend_only: bool,
    ) -> Result<StoredEnvironment> {
        let now = now_ms()?;
        self.edit_environment_control(id, |stored| {
            let paused = stored
                .result
                .as_ref()
                .is_some_and(|r| r.state == EnvironmentState::Paused);
            let c = stored.control.as_mut().ok_or(Error::NotFound)?;
            if c.expiry_claimed {
                return Err(Error::Conflict);
            }
            if paused && c.expires_at_unix_ms == 0 {
                if seconds == 0 || seconds > 86400 {
                    return Err(Error::Invalid(
                        "paused connect timeout must be 1..86400 seconds".into(),
                    ));
                }
                c.compatibility.timeout_seconds = seconds;
                c.revision = c.revision.checked_add(1).ok_or(Error::Conflict)?;
                Ok(())
            } else {
                c.set_timeout(now, seconds, extend_only)
            }
        })
        .await
    }
    pub async fn claim_sandbox_expiry(&self, id: &str, now: u64) -> Result<StoredEnvironment> {
        self.edit_environment_control(id, |stored| {
            let c = stored.control.as_mut().ok_or(Error::NotFound)?;
            if !c.expiry_claimed && !c.claim_expiry(now)? {
                return Err(Error::Conflict);
            }
            Ok(())
        })
        .await
    }
}
