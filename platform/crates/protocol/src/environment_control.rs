use crate::control as pb;
use adx_core::environment_control::{EnvironmentCompatibility, EnvironmentControlRecord};
impl From<pb::EnvironmentCompatibility> for EnvironmentCompatibility {
    fn from(v: pb::EnvironmentCompatibility) -> Self {
        Self {
            template_id: v.template_id,
            envd_version: v.envd_version,
            envd_access_token: v.envd_access_token,
            traffic_access_token: v.traffic_access_token,
            allow_public_traffic: v.allow_public_traffic,
            timeout_seconds: v.timeout_seconds,
            timeout_action: v.timeout_action,
            pause_retention_seconds: v.pause_retention_seconds,
            metadata: v.metadata.into_iter().collect(),
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
            timeout_seconds: v.timeout_seconds,
            timeout_action: v.timeout_action,
            pause_retention_seconds: v.pause_retention_seconds,
            metadata: v.metadata.into_iter().collect(),
        }
    }
}
impl From<EnvironmentControlRecord> for pb::EnvironmentControlRecord {
    fn from(v: EnvironmentControlRecord) -> Self {
        Self {
            environment_id: v.environment_id,
            compatibility: Some(v.compatibility.into()),
            expires_at_unix_ms: v.expires_at_unix_ms,
            started_at_unix_ms: v.started_at_unix_ms,
            revision: v.revision,
            generation: v.generation,
            expiry_claimed: v.expiry_claimed,
        }
    }
}
