//! Explicit shared-volume delivery; never assumes access to a remote Worker's filesystem.
use crate::{Activator, Error, Result};
use adx_agent_core::{service_grants::*, AgentBinding, BindingPhase, Scope};
use serde::{Deserialize, Serialize};
use std::{fs, path::PathBuf};
#[cfg(unix)]
use std::{
    path::Path,
    time::{SystemTime, UNIX_EPOCH},
};

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CredentialDelivery {
    pub template: String,
    pub root: PathBuf,
    pub ca_file: PathBuf,
    pub uid: u32,
    pub gid: u32,
    pub ttl_seconds: u64,
    pub renew_before_seconds: u64,
}
#[derive(Deserialize, Serialize)]
struct Delivered {
    scope: Scope,
    generation: String,
    expires_at_unix_seconds: u64,
    credential_digest: String,
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pending_revocations: Vec<String>,
}
fn unavailable(_: impl std::fmt::Display) -> Error {
    Error::Unavailable("workspace credential delivery unavailable".into())
}
impl CredentialDelivery {
    /// Polling cadence for a validated delivery configuration.
    pub fn renewal_interval(&self) -> std::time::Duration {
        (std::time::Duration::from_secs(self.renew_before_seconds) / 2)
            .min(std::time::Duration::from_secs(300))
    }

    pub fn validate(&self) -> Result<()> {
        adx_agent_core::identifier(&self.template, "credential delivery template")
            .map_err(Error::Invalid)?;
        if !cfg!(unix)
            || !self.root.is_absolute()
            || !self.ca_file.is_absolute()
            || self.ttl_seconds == 0
            || self.ttl_seconds > MAX_GRANT_SECONDS
            || self.renew_before_seconds == 0
            || self.renew_before_seconds >= self.ttl_seconds
        {
            return Err(Error::Invalid(
                "invalid shared credential delivery configuration".into(),
            ));
        }
        Ok(())
    }
}

#[cfg(unix)]
fn directory(path: &Path, mode: u32, owner: Option<(u32, u32)>) -> Result<()> {
    use std::os::unix::fs::PermissionsExt;
    if path.exists()
        && fs::symlink_metadata(path)
            .map_err(unavailable)?
            .file_type()
            .is_symlink()
    {
        return Err(unavailable("symlink"));
    }
    fs::create_dir_all(path).map_err(unavailable)?;
    fs::set_permissions(path, fs::Permissions::from_mode(mode)).map_err(unavailable)?;
    if let Some((uid, gid)) = owner {
        std::os::unix::fs::chown(path, Some(uid), Some(gid)).map_err(unavailable)?;
    }
    Ok(())
}
#[cfg(unix)]
fn atomic(path: &Path, bytes: &[u8], mode: u32, owner: Option<(u32, u32)>) -> Result<()> {
    use std::{io::Write, os::unix::fs::PermissionsExt};
    let parent = path.parent().ok_or_else(|| unavailable("parent"))?;
    let mut temp = tempfile::NamedTempFile::new_in(parent).map_err(unavailable)?;
    temp.write_all(bytes).map_err(unavailable)?;
    temp.as_file()
        .set_permissions(fs::Permissions::from_mode(mode))
        .map_err(unavailable)?;
    if let Some((uid, gid)) = owner {
        std::os::unix::fs::chown(temp.path(), Some(uid), Some(gid)).map_err(unavailable)?;
    }
    temp.as_file().sync_all().map_err(unavailable)?;
    temp.persist(path).map_err(unavailable)?;
    fs::File::open(parent)
        .and_then(|f| f.sync_all())
        .map_err(unavailable)?;
    Ok(())
}
#[cfg(unix)]
fn save_delivered(path: &Path, delivered: &Delivered) -> Result<()> {
    atomic(
        path,
        &serde_json::to_vec(delivered).map_err(unavailable)?,
        0o600,
        None,
    )
}
impl Activator {
    pub fn with_credential_delivery(mut self, config: Option<CredentialDelivery>) -> Result<Self> {
        if let Some(value) = &config {
            value.validate()?;
        }
        self.credential_delivery = config;
        Ok(self)
    }
    pub(crate) async fn deliver_for_execution(
        &self,
        binding: &AgentBinding,
        execution: &adx_agent_core::environment::ExecutionSpec,
    ) -> Result<()> {
        let Some(config) = self.credential_delivery.as_ref() else {
            // Manual provisioning remains available when automatic delivery is not enabled.
            return Ok(());
        };
        if binding.scope.template != config.template
            || execution
                .env
                .get("DIGITALMATE_SECRET_STORE")
                .map(String::as_str)
                != Some("remote")
        {
            return Ok(());
        }
        if execution.env.get("DIGITALMATE_CRYPTO_TENANT") != Some(&binding.scope.tenant)
            || execution.env.get("DIGITALMATE_CRYPTO_WORKSPACE")
                != binding.parameters.get("workspace")
        {
            return Err(Error::Invalid(
                "remote DigitalMate credential context mismatch".into(),
            ));
        }
        self.deliver_credential_identity(binding, config).await
    }
    /// Binding ownership and generation are checked against Redis before any identity is reused.
    #[cfg(unix)]
    pub async fn deliver_credential_identity(
        &self,
        binding: &AgentBinding,
        config: &CredentialDelivery,
    ) -> Result<()> {
        use std::os::unix::fs::OpenOptionsExt;
        config.validate()?;
        let workspace = binding
            .parameters
            .get("workspace")
            .ok_or_else(|| Error::Invalid("workspace missing".into()))?;
        if !valid_service_identifier(workspace) {
            return Err(Error::Invalid("invalid credential workspace".into()));
        }
        let current = self.binding(&binding.scope).await?;
        if current.generation != binding.generation || current.phase != BindingPhase::Active {
            return Err(Error::Conflict(
                "credential Binding changed or deleting".into(),
            ));
        }
        directory(&config.root, 0o711, None)?;
        let metadata = config.root.join("metadata");
        directory(&metadata, 0o700, None)?;
        let locks = config.root.join("locks");
        directory(&locks, 0o700, None)?;
        let lock = fs::OpenOptions::new()
            .create(true)
            .truncate(false)
            .read(true)
            .write(true)
            .mode(0o600)
            .open(locks.join(workspace))
            .map_err(unavailable)?;
        // Nonblocking advisory locking coordinates replicas over the explicitly shared delivery volume.
        lock.try_lock().map_err(unavailable)?;
        let users = config.root.join("users");
        directory(&users, 0o711, None)?;
        let user = users.join(workspace);
        directory(&user, 0o700, Some((config.uid, config.gid)))?;
        let reference = metadata.join(format!("{workspace}.json"));
        let mut old: Option<Delivered> = match fs::read(&reference) {
            Ok(bytes) => Some(serde_json::from_slice(&bytes).map_err(unavailable)?),
            Err(error) if error.kind() == std::io::ErrorKind::NotFound => None,
            Err(error) => return Err(unavailable(error)),
        };
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(unavailable)?
            .as_secs();
        let expected = ServiceAccess {
            tenant: binding.scope.tenant.clone(),
            workspace: workspace.clone(),
            purpose: ORBIT_CONFIG_PURPOSE.into(),
        };
        if let Some(saved) = &mut old {
            if saved.scope == binding.scope
                && saved.generation == binding.generation
                && saved.expires_at_unix_seconds > now.saturating_add(config.renew_before_seconds)
            {
                if let Ok(credential) = fs::read_to_string(user.join("identity")) {
                    if credential_digest(&credential)
                        .is_ok_and(|digest| digest == saved.credential_digest)
                        && self
                            .verify_service_grant(&VerifyServiceGrant {
                                credential,
                                expected: expected.clone(),
                            })
                            .await?
                            .allowed
                    {
                        self.finish_pending_revocations(&reference, saved).await?;
                        return Ok(());
                    }
                }
            }
        }
        let ca = fs::read(&config.ca_file).map_err(unavailable)?;
        let issued = self
            .issue_service_grant(&IssueServiceGrant {
                scope: binding.scope.clone(),
                expected_generation: binding.generation.clone(),
                purpose: ORBIT_CONFIG_PURPOSE.into(),
                ttl_seconds: config.ttl_seconds,
            })
            .await?;
        let mut pending_revocations = old.map_or_else(Vec::new, |mut saved| {
            saved.pending_revocations.push(saved.credential_digest);
            saved.pending_revocations
        });
        pending_revocations.sort_unstable();
        pending_revocations.dedup();
        let mut delivered = Delivered {
            scope: binding.scope.clone(),
            generation: binding.generation.clone(),
            expires_at_unix_seconds: issued.expires_at_unix_seconds,
            credential_digest: issued.credential_digest,
            pending_revocations,
        };
        atomic(
            &user.join("ca.pem"),
            &ca,
            0o400,
            Some((config.uid, config.gid)),
        )?;
        // Journal both the replacement and all outstanding old digests before publishing
        // its bearer. A restart can then revoke even an unpublished replacement safely.
        save_delivered(&reference, &delivered)?;
        atomic(
            &user.join("identity"),
            issued.credential.as_bytes(),
            0o400,
            Some((config.uid, config.gid)),
        )?;
        self.finish_pending_revocations(&reference, &mut delivered)
            .await
    }
    #[cfg(unix)]
    async fn finish_pending_revocations(
        &self,
        reference: &Path,
        delivered: &mut Delivered,
    ) -> Result<()> {
        if delivered.pending_revocations.is_empty() {
            return Ok(());
        }
        if delivered
            .pending_revocations
            .contains(&delivered.credential_digest)
        {
            return Err(unavailable(
                "current credential cannot be pending revocation",
            ));
        }
        // The caller has published or verified the current bearer. Keep the complete
        // queue durable until every idempotent revocation has a confirmed result.
        for digest in &delivered.pending_revocations {
            self.revoke_service_grant(&RevokeServiceGrant {
                credential_digest: digest.clone(),
            })
            .await?;
        }
        delivered.pending_revocations.clear();
        save_delivered(reference, delivered)
    }
    #[cfg(not(unix))]
    pub async fn deliver_credential_identity(
        &self,
        _: &AgentBinding,
        _: &CredentialDelivery,
    ) -> Result<()> {
        Err(Error::Invalid(
            "shared credential delivery requires Unix".into(),
        ))
    }
    /// Refresh only previously delivered identities; does not create or recover Environments.
    pub async fn renew_delivered_credentials(&self, config: &CredentialDelivery) -> Result<()> {
        let metadata = config.root.join("metadata");
        if !metadata.exists() {
            return Ok(());
        }
        for entry in fs::read_dir(metadata).map_err(unavailable)? {
            let path = entry.map_err(unavailable)?.path();
            if path.extension().and_then(|x| x.to_str()) != Some("json") {
                continue;
            }
            let saved: Delivered = serde_json::from_slice(&fs::read(path).map_err(unavailable)?)
                .map_err(unavailable)?;
            match self.binding(&saved.scope).await {
                Ok(binding) if binding.phase == BindingPhase::Active => {
                    self.deliver_credential_identity(&binding, config).await?
                }
                Ok(_) | Err(Error::NotFound) => {}
                Err(error) => return Err(error),
            }
        }
        Ok(())
    }
}
