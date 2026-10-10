//! Jiuwen download claims checked against the common Sandbox file capability.
use super::{
    download_config::DownloadConfig,
    download_token::{
        CheckedDownload, DownloadVerifier, TokenError, MAX_REGISTRATION_BYTES, MAX_SECRET_BYTES,
    },
};
pub use crate::ingress::sandbox_files::ReadError;
use crate::ingress::{sandbox_files::Files, server::Ingress};
use adx_agent_api::request::RequestContext;
use bytes::Bytes;
use std::sync::Arc;
use tokio::time::Instant;
const MAX_READ_BYTES: usize = 64 * 1024;

/// Authorization failures preserve the distinction between file transport and claims.
#[derive(Debug, thiserror::Error)]
pub enum AdmissionError {
    #[error(transparent)]
    Read(#[from] ReadError),
    #[error(transparent)]
    Token(#[from] TokenError),
}

pub struct Reader {
    files: Files,
    deadline: Instant,
}
impl Reader {
    pub fn new(
        ingress: &Arc<Ingress>,
        context: &RequestContext,
        tenant: &str,
        sandbox_id: &str,
    ) -> Result<Self, ReadError> {
        Ok(Self {
            files: Files::new(ingress, context.deadline(), tenant, sandbox_id)?,
            deadline: context.deadline(),
        })
    }
    /// Admit a download using the same template version as this Sandbox's binding.
    /// Reads the existing key only when config lacks an explicit secret, then the
    /// signed asset's registration (verified only) and file metadata over Execd.
    /// Returns read errors or signature/claim/registration errors. The result is a
    /// point-in-time check, not a reusable credential: the HTTP stream must still
    /// enforce expiry and revocation, and every new download must repeat admission.
    pub async fn authorize(
        &self,
        config: &DownloadConfig,
        token: &str,
        expected_session: Option<&str>,
    ) -> Result<CheckedDownload, AdmissionError> {
        if Instant::now() >= self.deadline {
            return Err(ReadError::Timeout.into());
        }
        let secret = match config.secret_file() {
            Some(path) => Some(self.files.read_file(path, MAX_SECRET_BYTES).await?),
            None => None,
        };
        let verifier = DownloadVerifier::from_config(config, secret.as_deref())?;
        let signed = verifier.verify(token, unix_seconds()?, expected_session)?;
        let registration = match signed.registration_path(config.asset_root())? {
            Some(path) => Some(self.files.read_file(&path, MAX_REGISTRATION_BYTES).await?),
            None => None,
        };
        let metadata = self.files.metadata(signed.path()).await?;
        if Instant::now() >= self.deadline {
            return Err(ReadError::Timeout.into());
        }
        Ok(signed.check_file(
            config.asset_root(),
            registration.as_deref(),
            metadata,
            unix_seconds()?,
        )?)
    }

    /// Fetch one bounded attachment chunk after fresh admission. Require Execd to
    /// return exactly the selected interval and total size, not a silently ignored
    /// Range. Expiry is checked before and after I/O; registry checks belong to the
    /// caller's per-chunk admission. Returns range/read/protocol or expiry errors.
    pub(super) async fn read_range(
        &self,
        file: &CheckedDownload,
        start: u64,
        length: u64,
    ) -> Result<Bytes, AdmissionError> {
        let limit = usize::try_from(length).map_err(|_| ReadError::InvalidRange)?;
        if limit == 0
            || limit > MAX_READ_BYTES
            || start
                .checked_add(length)
                .is_none_or(|end| end > file.size())
        {
            return Err(ReadError::InvalidRange.into());
        }
        file.check_expiry(unix_seconds()?)?;
        let query = url::form_urlencoded::Serializer::new(String::new())
            .extend_pairs([("path", file.path()), ("type", "file")])
            .finish();
        let bytes = self
            .files
            .read(
                "GET",
                &format!("/download?{query}"),
                Bytes::new(),
                limit,
                Some((start, start + length - 1, file.size())),
            )
            .await?;
        file.check_expiry(unix_seconds()?)?;
        if bytes.len() != limit {
            return Err(ReadError::Protocol.into());
        }
        Ok(bytes)
    }
}
fn unix_seconds() -> Result<f64, ReadError> {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|elapsed| elapsed.as_secs_f64())
        .map_err(|_| ReadError::Clock)
}
