//! Signed download claims and verified registration checks. No file I/O or user authentication.
use super::download_config::{absolute_directory, DownloadConfig};
use base64::{engine::general_purpose::URL_SAFE, Engine};
use ring::hmac;
use serde_json::{Map, Value};
use std::fmt;

pub const MAX_TOKEN_BYTES: usize = 16 * 1024;
pub const MAX_REGISTRATION_BYTES: usize = 64 * 1024;
pub const MAX_SECRET_BYTES: usize = 64 * 1024;

#[derive(Debug, thiserror::Error)]
pub enum TokenError {
    #[error("invalid download signing secret")]
    InvalidSecret,
    #[error("invalid download token signature")]
    InvalidSignature,
    #[error("invalid download token: {0}")]
    InvalidToken(&'static str),
    #[error("download authorization expired")]
    Expired,
    #[error("download session mismatch")]
    SessionMismatch,
    #[error("download asset registration is missing, inactive or inconsistent")]
    RegistrationMismatch,
    #[error("download file is unavailable or inconsistent")]
    FileMismatch,
    #[error("download verification input exceeds its size limit")]
    TooLarge,
}
type Result<T> = std::result::Result<T, TokenError>;

pub struct DownloadVerifier {
    key: hmac::Key,
}
impl fmt::Debug for DownloadVerifier {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("DownloadVerifier { key: [REDACTED] }")
    }
}
impl DownloadVerifier {
    /// Use trusted signing material without trimming an explicitly configured value.
    /// Returns InvalidSecret for short keys, TooLarge for oversized keys.
    pub fn new(secret: &str) -> Result<Self> {
        if secret.len() > MAX_SECRET_BYTES {
            return Err(TokenError::TooLarge);
        }
        if secret.chars().count() < 32 {
            return Err(TokenError::InvalidSecret);
        }
        Ok(Self {
            key: hmac::Key::new(hmac::HMAC_SHA256, secret.as_bytes()),
        })
    }

    /// Explicit template env takes precedence. Otherwise use bytes read from
    /// config.secret_file() in the already-authorized Sandbox. Like AgentServer,
    /// strip file whitespace, but never create a new key when missing/invalid.
    /// Returns InvalidSecret for absent/non-UTF8/short file contents or TooLarge.
    pub fn from_config(config: &DownloadConfig, file_contents: Option<&[u8]>) -> Result<Self> {
        if let Some(secret) = config.configured_secret() {
            return Self::new(secret);
        }
        let bytes = file_contents.ok_or(TokenError::InvalidSecret)?;
        if bytes.len() > MAX_SECRET_BYTES {
            return Err(TokenError::TooLarge);
        }
        let secret = std::str::from_utf8(bytes).map_err(|_| TokenError::InvalidSecret)?;
        Self::new(secret.trim())
    }

    /// Authenticate the ORIGINAL Base64URL text, then parse claims. `now` is Unix
    /// time in seconds; expected_session is optional trusted request context.
    /// No path or routing hint is exposed before signature verification. Returned
    /// claims still require check_file(), and never establish a user's identity.
    /// Returns signature, schema, size, expiry or session errors without input values.
    pub fn verify(
        &self,
        token: &str,
        now: f64,
        expected_session: Option<&str>,
    ) -> Result<SignedDownload> {
        if token.len() > MAX_TOKEN_BYTES {
            return Err(TokenError::TooLarge);
        }
        let (encoded, signature) = token.split_once('.').ok_or(TokenError::InvalidSignature)?;
        // Python compares against lowercase hexdigest; do not silently accept uppercase.
        if signature.len() != 64
            || !signature
                .bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(TokenError::InvalidSignature);
        }
        let signature = hex::decode(signature).map_err(|_| TokenError::InvalidSignature)?;
        hmac::verify(&self.key, encoded.as_bytes(), &signature)
            .map_err(|_| TokenError::InvalidSignature)?;
        let bytes = URL_SAFE
            .decode(encoded)
            .map_err(|_| TokenError::InvalidToken("encoding"))?;
        let value: Value =
            serde_json::from_slice(&bytes).map_err(|_| TokenError::InvalidToken("JSON"))?;
        let p = value
            .as_object()
            .ok_or(TokenError::InvalidToken("object required"))?;
        if p.contains_key("purpose") {
            return Err(TokenError::InvalidToken("unsupported purpose"));
        }
        let verified = match p.get("kind") {
            None | Some(Value::Null) => false,
            Some(Value::String(kind)) if kind == "verified_asset_v1" => true,
            _ => return Err(TokenError::InvalidToken("unsupported kind")),
        };
        let path = string(p, "path")?;
        let path = absolute_directory(Some(path), "path")
            .map_err(|_| TokenError::InvalidToken("absolute file path required"))?;
        let session = string(p, "sid")?.to_owned();
        if expected_session
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .is_some_and(|s| s != session.trim())
        {
            return Err(TokenError::SessionMismatch);
        }
        let expires = match p.get("exp") {
            None => {
                let keys_ok = p.len() == 2 || p.len() == 3 && p.contains_key("download_http_base");
                if !keys_ok
                    || !p
                        .values()
                        .all(|v| v.as_str().is_some_and(|s| !s.is_empty()))
                {
                    return Err(TokenError::InvalidToken("expiry required"));
                }
                None
            }
            Some(value) => Some(timestamp(value).ok_or(TokenError::InvalidToken("expiry"))?),
        };
        let (asset, name) = if verified {
            let id = normalized_hex(string(p, "asset_id")?, 32)
                .ok_or(TokenError::InvalidToken("asset ID"))?;
            let raw_digest = string(p, "digest")?.trim().to_ascii_lowercase();
            let digest = normalized_hex(
                raw_digest.strip_prefix("sha256:").unwrap_or(&raw_digest),
                64,
            )
            .ok_or(TokenError::InvalidToken("digest"))?;
            let size = p
                .get("size")
                .and_then(Value::as_u64)
                .ok_or(TokenError::InvalidToken("size"))?;
            let expires_at = expires.ok_or(TokenError::InvalidToken("expiry required"))?;
            let name = basename(p.get("name").and_then(Value::as_str).unwrap_or("download"));
            (
                Some(Asset {
                    id,
                    size,
                    digest: format!("sha256:{digest}"),
                    expires_at,
                }),
                name,
            )
        } else {
            (None, basename(&path))
        };
        let claims = SignedDownload {
            path,
            session,
            name,
            expires,
            asset,
        };
        claims.check_expiry(now)?;
        Ok(claims)
    }
}

struct Asset {
    id: String,
    size: u64,
    digest: String,
    expires_at: f64,
}
/// Verified signature, not yet checked against registration and file metadata.
/// Do not serialize or return this internal authorization state to the frontend.
pub struct SignedDownload {
    path: String,
    session: String,
    name: String,
    expires: Option<f64>,
    asset: Option<Asset>,
}
pub use crate::ingress::sandbox_files::FileMetadata;
pub struct CheckedDownload {
    path: String,
    name: String,
    size: u64,
    expires: Option<f64>,
    verified: bool,
}
impl CheckedDownload {
    pub fn verified(&self) -> bool {
        self.verified
    }
    /// Preserve ordinary integer-second and verified exact expiry during streaming.
    /// Returns Expired or InvalidToken for an invalid clock value.
    pub fn check_expiry(&self, now: f64) -> Result<()> {
        check_expiry(self.expires, self.verified, now)
    }

    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn name(&self) -> &str {
        &self.name
    }
    pub fn size(&self) -> u64 {
        self.size
    }
}
impl SignedDownload {
    pub fn path(&self) -> &str {
        &self.path
    }
    pub fn session_id(&self) -> &str {
        &self.session
    }

    /// Locate a sidecar only within the template's trusted asset root. Ordinary
    /// downloads need no sidecar. Returns RegistrationMismatch for out-of-root or
    /// hidden asset paths. This is lexical; no host filesystem resolution occurs.
    pub fn registration_path(&self, asset_root: &str) -> Result<Option<String>> {
        let Some(asset) = &self.asset else {
            return Ok(None);
        };
        let root = absolute_directory(Some(asset_root), "asset root")
            .map_err(|_| TokenError::RegistrationMismatch)?;
        let (parent, name) = self
            .path
            .rsplit_once('/')
            .ok_or(TokenError::RegistrationMismatch)?;
        let parent = if parent.is_empty() { "/" } else { parent };
        if parent != root || name.is_empty() || name.starts_with('.') {
            return Err(TokenError::RegistrationMismatch);
        }
        Ok(Some(format!(
            "{}/{}.json",
            root.trim_end_matches('/'),
            asset.id
        )))
    }

    /// Check metadata obtained for path() in the same authorized Sandbox. Verified
    /// claims also require sidecar bytes read at registration_path(asset_root).
    /// Returns expiry, root/registration, size or file-type errors. Digest is compared
    /// to the durable registration, not recalculated over downloaded content. These
    /// checks are not an atomic open/read guarantee and must precede every new read
    /// admission; the HTTP driver owns stream-time expiry/revocation handling.
    pub fn check_file(
        &self,
        asset_root: &str,
        registration: Option<&[u8]>,
        file: FileMetadata,
        now: f64,
    ) -> Result<CheckedDownload> {
        self.check_expiry(now)?;
        if !file.regular_file {
            return Err(TokenError::FileMismatch);
        }
        if let Some(asset) = &self.asset {
            self.registration_path(asset_root)?;
            let bytes = registration.ok_or(TokenError::RegistrationMismatch)?;
            if bytes.len() > MAX_REGISTRATION_BYTES {
                return Err(TokenError::TooLarge);
            }
            let p: Value =
                serde_json::from_slice(bytes).map_err(|_| TokenError::RegistrationMismatch)?;
            if !matches!(p["state"].as_str(), Some("staged" | "committed"))
                || p["asset_id"].as_str() != Some(&asset.id)
                || p["sealed_path"].as_str() != Some(&self.path)
                || timestamp(&p["expires_at"]) != Some(asset.expires_at)
                || p["size_bytes"].as_u64() != Some(asset.size)
                || p["content_digest"].as_str() != Some(&asset.digest)
            {
                return Err(TokenError::RegistrationMismatch);
            }
            if file.size != asset.size {
                return Err(TokenError::FileMismatch);
            }
        }
        Ok(CheckedDownload {
            path: self.path.clone(),
            name: self.name.clone(),
            size: file.size,
            expires: self.expires,
            verified: self.asset.is_some(),
        })
    }

    fn check_expiry(&self, now: f64) -> Result<()> {
        check_expiry(self.expires, self.asset.is_some(), now)
    }
}
fn string<'a>(p: &'a Map<String, Value>, name: &str) -> Result<&'a str> {
    p.get(name)
        .and_then(Value::as_str)
        .ok_or(TokenError::InvalidToken("required string claim"))
}
fn timestamp(value: &Value) -> Option<f64> {
    value.as_f64().filter(|n| n.is_finite() && *n >= 0.0)
}
fn normalized_hex(value: &str, size: usize) -> Option<String> {
    let normalized = value.trim().to_ascii_lowercase();
    (normalized.len() == size && normalized.bytes().all(|b| b.is_ascii_hexdigit()))
        .then_some(normalized)
}
fn basename(path: &str) -> String {
    path.trim_end_matches('/')
        .rsplit('/')
        .next()
        .filter(|s| !s.is_empty())
        .unwrap_or("download")
        .to_owned()
}

fn check_expiry(expires: Option<f64>, verified: bool, now: f64) -> Result<()> {
    if !now.is_finite() || now < 0.0 {
        return Err(TokenError::InvalidToken("invalid current time"));
    }
    if expires.is_some_and(|exp| {
        if verified {
            now > exp
        } else {
            now.trunc() > exp.trunc()
        }
    }) {
        return Err(TokenError::Expired);
    }
    Ok(())
}
