//! Pure validation/authentication helpers; no HTTP client or server dependency.
use crate::limits::SERVICE_TOKEN_MIN_BYTES;
use sha2::{Digest, Sha256};
use std::sync::atomic::{AtomicBool, Ordering};

pub fn validate_service_token(token: &str) -> Result<(), String> {
    if token.len() < SERVICE_TOKEN_MIN_BYTES || token.contains(['\r', '\n']) {
        return Err(format!(
            "service token must contain at least {SERVICE_TOKEN_MIN_BYTES} bytes and no newlines"
        ));
    }
    Ok(())
}

#[derive(Clone)]
pub struct ServiceAuth([u8; 32]);
impl ServiceAuth {
    pub fn new(token: &str) -> Result<Self, String> {
        validate_service_token(token)?;
        Ok(Self(Sha256::digest(token.as_bytes()).into()))
    }
    pub fn accepts(&self, authorization: Option<&str>) -> bool {
        let Some(token) = authorization.and_then(|v| v.strip_prefix("Bearer ")) else {
            return false;
        };
        let digest: [u8; 32] = Sha256::digest(token.as_bytes()).into();
        digest
            .iter()
            .zip(self.0)
            .fold(0u8, |difference, (a, b)| difference | (*a ^ b))
            == 0
    }
}

pub fn service_origin(address: &str, allow_http: bool) -> Result<url::Url, String> {
    let url = url::Url::parse(address).map_err(|_| "invalid service URL".to_owned())?;
    if !(url.scheme() == "https" || allow_http && url.scheme() == "http")
        || url.host_str().is_none()
        || !url.username().is_empty()
        || url.password().is_some()
        || url.path() != "/"
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return Err("service URL must be a permitted HTTP(S) origin without credentials".into());
    }
    Ok(url)
}

/// Absolute cross-process deadline. Clocks on service hosts must be synchronized.
/// A local cap may shorten the caller's budget, never extend it.
pub fn capped_deadline(deadline_unix_ms: Option<u64>, cap: std::time::Duration) -> u64 {
    let local = crate::unix_time_millis()
        .saturating_add(u64::try_from(cap.as_millis()).unwrap_or(u64::MAX));
    deadline_unix_ms.map_or(local, |deadline| deadline.min(local))
}

/// Remaining time before an absolute deadline; zero means admission must stop.
pub fn remaining_time(deadline_unix_ms: u64) -> std::time::Duration {
    std::time::Duration::from_millis(deadline_unix_ms.saturating_sub(crate::unix_time_millis()))
}

/// Records entry into a potentially mutating service call. Reading/parsing a request
/// is not a write. This is conservative, not evidence that a write actually committed.
#[derive(Default)]
pub struct RequestProgress(AtomicBool);
impl RequestProgress {
    pub fn start_write(&self) {
        self.0.store(true, Ordering::Relaxed);
    }
    pub fn may_have_written(&self) -> bool {
        self.0.load(Ordering::Relaxed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn service_auth_and_origins_keep_security_boundaries() {
        let token = "service-test-token-at-least-32-bytes";
        let auth = ServiceAuth::new(token).unwrap();
        assert!(auth.accepts(Some(&format!("Bearer {token}"))));
        for header in [None, Some("Bearer wrong"), Some(token), Some("Basic wrong")] {
            assert!(!auth.accepts(header));
        }
        assert!(ServiceAuth::new("short").is_err());
        assert!(ServiceAuth::new(&format!("{token}\n")).is_err());
        assert!(service_origin("https://localhost:443", false).is_ok());
        assert!(service_origin("http://localhost:8080", true).is_ok());
        assert!(service_origin("http://localhost:8080", false).is_err());
        for url in [
            "https://user@host",
            "https://host/path",
            "https://host/?x=1",
            "https://host/#x",
            "file:///tmp/x",
        ] {
            assert!(service_origin(url, true).is_err(), "{url}");
        }
    }
}
