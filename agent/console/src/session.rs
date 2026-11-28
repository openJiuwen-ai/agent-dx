//! Signed, single-process sessions; tokens are never retained in session records.
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{collections::BTreeMap, sync::Mutex, time::{SystemTime, UNIX_EPOCH}};
use subtle::ConstantTimeEq;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Role { PlatformAdmin, TenantAdmin }
#[derive(Clone, Serialize)]
pub struct SessionRecord {
    pub session_id: String,
    pub subject_id: String,
    pub role: Role,
    pub tenant_id: Option<String>,
    pub created_at: u64,
    pub expires_at: u64,
}
#[derive(Deserialize)]
#[serde(tag = "login_type", rename_all = "snake_case", deny_unknown_fields)]
pub enum LoginInput {
    PlatformAdmin { admin_id: String, token: String },
    TenantAdmin { tenant_id: String, token: String },
}
#[derive(Debug)]
pub struct InvalidCredentials;

pub struct SessionRegistry {
    secret: Vec<u8>,
    ttl: u64,
    admins: BTreeMap<String, String>,
    tenants: BTreeMap<String, String>,
    sessions: Mutex<BTreeMap<String, SessionRecord>>,
}
fn now() -> u64 { SystemTime::now().duration_since(UNIX_EPOCH).map_or(0, |v| v.as_secs()) }
impl SessionRegistry {
    pub fn new(secret: Vec<u8>, ttl: u64, admins: BTreeMap<String, String>, tenants: BTreeMap<String, String>) -> Result<Self, String> {
        if secret.len() < 32 || ttl == 0 || admins.values().chain(tenants.values()).any(|v| hex::decode(v).is_err() || v.len() != 64) {
            return Err("invalid session secret, TTL or token digest".into());
        }
        Ok(Self { secret, ttl, admins, tenants, sessions: Mutex::new(BTreeMap::new()) })
    }
    pub fn login(&self, input: &LoginInput) -> Result<(SessionRecord, String), InvalidCredentials> {
        let (id, token, role, tenant_id, expected) = match input {
            LoginInput::PlatformAdmin { admin_id, token } => (admin_id, token, Role::PlatformAdmin, None, self.admins.get(admin_id)),
            LoginInput::TenantAdmin { tenant_id, token } => (tenant_id, token, Role::TenantAdmin, Some(tenant_id.clone()), self.tenants.get(tenant_id)),
        };
        let supplied = Sha256::digest(token.as_bytes());
        let digest = expected.and_then(|v| hex::decode(v).ok()).unwrap_or_else(|| vec![0; 32]);
        if !bool::from(supplied.as_slice().ct_eq(&digest)) || expected.is_none() { return Err(InvalidCredentials); }
        let created_at = now();
        let record = SessionRecord { session_id: uuid::Uuid::new_v4().to_string(), subject_id: id.clone(), role, tenant_id,
            created_at, expires_at: created_at.saturating_add(self.ttl) };
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret).map_err(|_| InvalidCredentials)?;
        mac.update(record.session_id.as_bytes());
        let cookie = format!("{}.{}", record.session_id, URL_SAFE_NO_PAD.encode(mac.finalize().into_bytes()));
        let mut sessions = self.sessions.lock().map_err(|_| InvalidCredentials)?;
        sessions.retain(|_, value| value.expires_at > created_at);
        sessions.insert(record.session_id.clone(), record.clone());
        Ok((record, cookie))
    }
    pub fn verify(&self, cookie: &str) -> Option<SessionRecord> {
        self.verify_at(cookie,now())
    }
    pub(crate) fn verify_at(&self, cookie: &str, timestamp:u64) -> Option<SessionRecord> {
        let (id, signature) = cookie.split_once('.')?;
        let signature = URL_SAFE_NO_PAD.decode(signature).ok()?;
        let mut mac = Hmac::<Sha256>::new_from_slice(&self.secret).ok()?;
        mac.update(id.as_bytes());
        mac.verify_slice(&signature).ok()?;
        let mut sessions = self.sessions.lock().ok()?;
        if sessions.get(id)?.expires_at <= timestamp { sessions.remove(id); return None; }
        sessions.get(id).cloned()
    }
    pub fn logout(&self, id: &str) {
        if let Ok(mut sessions) = self.sessions.lock() { sessions.remove(id); }
    }
}
