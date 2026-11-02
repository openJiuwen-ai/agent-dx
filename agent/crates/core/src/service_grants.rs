//! Narrow service access references an existing AgentBinding; it is not a user/workspace directory.
use crate::Scope;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

pub const CREDENTIAL_AUDIENCE: &str = "credential-crypto";
pub const ORBIT_CONFIG_PURPOSE: &str = "orbit-config";
pub const MAX_GRANT_SECONDS: u64 = 31_536_000;

pub fn credential_digest(credential: &str) -> Result<String, String> {
    if !(32..=128).contains(&credential.len())
        || !credential
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err("invalid service credential".into());
    }
    Ok(format!("{:x}", Sha256::digest(credential.as_bytes())))
}

#[derive(Clone, Debug, PartialEq, Eq, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceAccess {
    pub tenant: String,
    pub workspace: String,
    pub purpose: String,
}

pub fn valid_service_identifier(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 128
        && value
            .bytes()
            .all(|c| c.is_ascii_alphanumeric() || c == b'-' || c == b'_')
}

impl ServiceAccess {
    pub fn validate(&self) -> Result<(), String> {
        if [&self.tenant, &self.workspace, &self.purpose]
            .into_iter()
            .all(|s| valid_service_identifier(s))
        {
            Ok(())
        } else {
            Err("invalid service access context".into())
        }
    }
}

#[derive(Clone, Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceGrant {
    pub scope: Scope,
    pub generation: String,
    pub audience: String,
    pub purpose: String,
    pub expires_at_unix_seconds: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IssueServiceGrant {
    pub scope: Scope,
    pub expected_generation: String,
    pub purpose: String,
    pub ttl_seconds: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct IssuedServiceGrant {
    pub credential: String,
    pub credential_digest: String,
    pub context: ServiceAccess,
    pub expires_at_unix_seconds: u64,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct VerifyServiceGrant {
    pub credential: String,
    pub expected: ServiceAccess,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct ServiceGrantDecision {
    pub allowed: bool,
}

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct RevokeServiceGrant {
    pub credential_digest: String,
}
