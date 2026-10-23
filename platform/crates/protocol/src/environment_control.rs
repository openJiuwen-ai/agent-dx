use crate::control as pb;
use adx_core::environment_control::{
    EnvironmentCompatibility, EnvironmentControlRecord, RunExpiryAction, RunPolicy,
};
impl From<pb::EnvironmentCompatibility> for EnvironmentCompatibility {
    fn from(v: pb::EnvironmentCompatibility) -> Self {
        Self {
            template_id: v.template_id,
            envd_version: v.envd_version,
            envd_access_token: v.envd_access_token,
            traffic_access_token: v.traffic_access_token,
            allow_public_traffic: v.allow_public_traffic,
        }
    }
}
impl From<EnvironmentCompatibility> for pb::EnvironmentCompatibility {
    fn from(v: EnvironmentCompatibility) -> Self {
        Self {
            template_id: v.template_id,
            envd_version: v.envd_version,
            envd_access_token: v.envd_access_token,
            traffic_access_token: v.traffic_access_token,
            allow_public_traffic: v.allow_public_traffic,
        }
    }
}
impl From<EnvironmentControlRecord> for pb::EnvironmentControlRecord {
    fn from(v: EnvironmentControlRecord) -> Self {
        Self {
            environment_id: v.environment_id,
            expires_at_unix_ms: v.expires_at_unix_ms,
            started_at_unix_ms: v.started_at_unix_ms,
            revision: v.revision,
            generation: v.generation,
            expiry_claimed: v.expiry_claimed,
            timeout_override_seconds: v.timeout_override_seconds,
        }
    }
}

impl TryFrom<pb::RunPolicy> for RunPolicy {
    type Error = adx_core::Error;

    fn try_from(value: pb::RunPolicy) -> Result<Self, Self::Error> {
        let action = match pb::RunExpiryAction::try_from(value.action)
            .map_err(|_| adx_core::Error::Invalid("invalid run expiry action".into()))?
        {
            pb::RunExpiryAction::Delete => RunExpiryAction::Delete,
            pb::RunExpiryAction::Pause => RunExpiryAction::Pause,
            _ => {
                return Err(adx_core::Error::Invalid(
                    "run expiry action is required".into(),
                ))
            }
        };
        let policy = Self {
            timeout_seconds: value.timeout_seconds,
            action,
        };
        policy.validate()?;
        Ok(policy)
    }
}

impl From<RunPolicy> for pb::RunPolicy {
    fn from(value: RunPolicy) -> Self {
        Self {
            timeout_seconds: value.timeout_seconds,
            action: match value.action {
                RunExpiryAction::Delete => pb::RunExpiryAction::Delete as i32,
                RunExpiryAction::Pause => pb::RunExpiryAction::Pause as i32,
            },
        }
    }
}
