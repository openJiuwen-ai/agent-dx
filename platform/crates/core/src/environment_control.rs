//! Compatibility configuration and a durable absolute runtime lease. Lifecycle
//! state remains owned by EnvironmentRecord; this record never schedules a VM.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct EnvironmentCompatibility {
    pub template_id: String,
    pub envd_version: String,
    pub envd_access_token: String,
    pub traffic_access_token: String,
    pub allow_public_traffic: bool,
}
impl fmt::Debug for EnvironmentCompatibility {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("EnvironmentCompatibility")
            .field("template_id", &self.template_id)
            .field("envd_version", &self.envd_version)
            .field("credentials", &"[redacted]")
            .finish()
    }
}
impl EnvironmentCompatibility {
    pub fn validate(&self) -> Result<()> {
        if self.template_id.is_empty()
            || self.envd_version.is_empty()
            || self.envd_access_token.len() < 32
            || self.traffic_access_token.len() < 32
        {
            return Err(Error::Invalid(
                "invalid sandbox compatibility configuration".into(),
            ));
        }
        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RunExpiryAction {
    Delete,
    Pause,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RunPolicy {
    pub timeout_seconds: u64,
    pub action: RunExpiryAction,
}

impl RunPolicy {
    pub fn validate(&self) -> Result<()> {
        if self.timeout_seconds == 0 {
            return Err(Error::Invalid("run timeout must be positive".into()));
        }
        let _ = self
            .timeout_seconds
            .checked_mul(1000)
            .ok_or_else(|| Error::Invalid("run timeout milliseconds overflow".into()))?;
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EnvironmentControlRecord {
    pub environment_id: String,
    pub expires_at_unix_ms: u64,
    pub started_at_unix_ms: u64,
    pub revision: u64,
    pub generation: u64,
    pub expiry_claimed: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout_override_seconds: Option<u64>,
}
impl EnvironmentControlRecord {
    pub fn new(environment_id: String, generation: u64) -> Self {
        Self {
            environment_id,
            generation,
            expires_at_unix_ms: 0,
            started_at_unix_ms: 0,
            revision: 1,
            expiry_claimed: false,
            timeout_override_seconds: None,
        }
    }
    fn advance(&mut self) -> Result<()> {
        self.revision = self.revision.checked_add(1).ok_or(Error::Conflict)?;
        Ok(())
    }
    pub fn begin_run(
        &mut self,
        now: u64,
        generation: u64,
        default_timeout_seconds: u64,
    ) -> Result<()> {
        if self.expiry_claimed {
            return Err(Error::Conflict);
        }
        self.expires_at_unix_ms = deadline(
            now,
            self.timeout_override_seconds
                .unwrap_or(default_timeout_seconds),
        )?;
        self.started_at_unix_ms = now;
        self.generation = generation;
        self.advance()
    }
    pub fn set_timeout(&mut self, now: u64, seconds: u64, extend_only: bool) -> Result<()> {
        if self.expiry_claimed || self.expires_at_unix_ms == 0 || now >= self.expires_at_unix_ms {
            return Err(Error::Conflict);
        }
        let next = deadline(now, seconds)?;
        self.expires_at_unix_ms = if extend_only {
            self.expires_at_unix_ms.max(next)
        } else {
            next
        };
        self.advance()
    }
    pub fn set_timeout_override(&mut self, seconds: u64) -> Result<()> {
        if self.expiry_claimed || seconds == 0 {
            return Err(Error::Conflict);
        }
        let _ = deadline(0, seconds)?;
        self.timeout_override_seconds = Some(seconds);
        self.advance()
    }
    pub fn claim_expiry(&mut self, now: u64) -> Result<bool> {
        if self.expiry_claimed || self.expires_at_unix_ms == 0 || now < self.expires_at_unix_ms {
            return Ok(false);
        }
        self.expiry_claimed = true;
        self.advance()?;
        Ok(true)
    }
    pub fn pause(&mut self) -> Result<()> {
        self.expires_at_unix_ms = 0;
        self.expiry_claimed = false;
        self.advance()
    }
    pub fn rebind(&mut self, generation: u64) -> Result<()> {
        if self.generation != generation {
            self.generation = generation;
            self.advance()?;
        }
        Ok(())
    }
}
fn deadline(now: u64, seconds: u64) -> Result<u64> {
    now.checked_add(seconds.checked_mul(1000).ok_or(Error::Conflict)?)
        .ok_or(Error::Conflict)
}

#[cfg(test)]
mod tests {
    use super::*;
    fn record() -> EnvironmentControlRecord {
        EnvironmentControlRecord::new("s".into(), 1)
    }
    #[test]
    fn timeout_is_absolute_connect_only_extends_and_expiry_cannot_revive() {
        let mut c = record();
        c.begin_run(1000, 1, 60).unwrap();
        c.set_timeout(2000, 10, true).unwrap();
        assert_eq!(c.expires_at_unix_ms, 61000);
        c.set_timeout(3000, 2, false).unwrap();
        assert_eq!(c.expires_at_unix_ms, 5000);
        assert!(c.set_timeout(5000, 60, false).is_err());
        assert!(c.claim_expiry(5000).unwrap());
        assert!(!c.claim_expiry(5000).unwrap());
        assert!(c.begin_run(6000, 2, 60).is_err());
    }
    #[test]
    fn zero_timeout_expires_now_and_rebind_does_not_extend() {
        let mut c = record();
        c.begin_run(1000, 1, 60).unwrap();
        c.rebind(2).unwrap();
        assert_eq!(c.expires_at_unix_ms, 61000);
        c.set_timeout(2000, 0, false).unwrap();
        assert!(c.claim_expiry(2000).unwrap());
    }
    #[test]
    fn explicit_pause_creates_new_run() {
        let mut c = record();
        c.begin_run(1000, 1, 60).unwrap();
        c.pause().unwrap();
        c.begin_run(90000, 2, 60).unwrap();
        assert_eq!(c.expires_at_unix_ms, 150000);
    }

    #[test]
    fn override_survives_pause_and_applies_to_the_next_run() {
        let mut c = record();
        c.begin_run(1_000, 1, 60).unwrap();
        c.pause().unwrap();
        c.set_timeout_override(120).unwrap();
        c.begin_run(10_000, 2, 60).unwrap();
        assert_eq!(c.timeout_override_seconds, Some(120));
        assert_eq!(c.expires_at_unix_ms, 130_000);
        c.pause().unwrap();
        c.begin_run(20_000, 3, 60).unwrap();
        assert_eq!(c.timeout_override_seconds, Some(120));
        assert_eq!(c.expires_at_unix_ms, 140_000);
    }

    #[test]
    fn running_renewal_does_not_create_or_replace_an_override() {
        let mut c = record();
        c.begin_run(1_000, 1, 60).unwrap();
        c.set_timeout(2_000, 120, false).unwrap();
        assert_eq!(c.timeout_override_seconds, None);
        c.pause().unwrap();
        c.set_timeout_override(90).unwrap();
        c.begin_run(10_000, 2, 60).unwrap();
        c.set_timeout(11_000, 30, false).unwrap();
        assert_eq!(c.timeout_override_seconds, Some(90));
    }

    #[test]
    fn run_policy_rejects_zero_and_millisecond_overflow() {
        assert!(RunPolicy {
            timeout_seconds: 0,
            action: RunExpiryAction::Delete,
        }
        .validate()
        .is_err());
        assert!(RunPolicy {
            timeout_seconds: u64::MAX,
            action: RunExpiryAction::Pause,
        }
        .validate()
        .is_err());
    }
}
