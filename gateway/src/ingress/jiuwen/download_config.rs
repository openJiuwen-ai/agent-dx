//! Download settings from the same immutable template that launches AgentServer.
use adx_agent_api::{managed::ManagedService, request::RequestContext, Error};
use adx_agent_core::{Scope, TemplateVersion};
use std::fmt;

const WORKSPACE: &str = "JIUWENSWARM_WORKSPACE";
const ASSET_ROOT: &str = "JIUWENSWARM_DOWNLOAD_ASSET_ROOT";
const SECRET: &str = "JIUWENSWARM_FILE_DOWNLOAD_SECRET";

enum SecretSource {
    Configured(String),
    SandboxFile(String),
}

/// Internal settings, not a public response or a second deployment configuration.
/// Both explicit secrets and Sandbox-local file paths come only from template env.
/// The caller must bind file reads to the authenticated user's resolved Sandbox.
pub struct DownloadConfig {
    workspace: String,
    asset_root: String,
    secret: SecretSource,
}
impl fmt::Debug for DownloadConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("DownloadConfig")
            .field("workspace", &self.workspace)
            .field("asset_root", &self.asset_root)
            .field("secret_file", &self.secret_file())
            .field(
                "configured_secret",
                &self.configured_secret().map(|_| "[REDACTED]"),
            )
            .finish()
    }
}
impl DownloadConfig {
    /// Fetch the same tenant/template/version used for binding activation through
    /// ManagedService's existing immutable-template cache. This does not resolve or
    /// create a binding or Sandbox. Scope tenant must come from verified identity.
    /// Returns scope/configuration, missing-template, control or deadline errors.
    pub async fn load(
        managed: &ManagedService,
        context: &RequestContext,
        scope: &Scope,
    ) -> Result<Self, Error> {
        scope.validate().map_err(Error::Invalid)?;
        let template = managed
            .template(context, &scope.tenant, &scope.template, &scope.version)
            .await?;
        Self::from_template(&template)
    }

    /// Validate shared settings without consulting Ingress env, HOME, UID or disk.
    /// Requires explicit absolute workspace and asset directories. A nonempty
    /// explicit secret must satisfy AgentServer's 32-character minimum; empty or
    /// absent means read the Sandbox's existing workspace secret file later.
    /// Returns Invalid for malformed templates, missing/relative/traversing paths
    /// or short explicit secrets. Error text never includes secret values.
    pub fn from_template(template: &TemplateVersion) -> Result<Self, Error> {
        template.validate().map_err(Error::Invalid)?;
        let workspace =
            absolute_directory(template.env.get(WORKSPACE).map(String::as_str), WORKSPACE)?;
        // AgentServer trims the asset-root setting, but not workspace or env secrets.
        let asset_root =
            absolute_directory(template.env.get(ASSET_ROOT).map(|s| s.trim()), ASSET_ROOT)?;
        let secret = match template.env.get(SECRET).filter(|s| !s.is_empty()) {
            Some(value) => {
                if value.chars().count() < 32 {
                    return Err(Error::Invalid(format!(
                        "{SECRET} requires at least 32 characters"
                    )));
                }
                SecretSource::Configured(value.clone())
            }
            None => SecretSource::SandboxFile(format!(
                "{}/config/.file_download_secret",
                workspace.trim_end_matches('/')
            )),
        };
        Ok(Self {
            workspace,
            asset_root,
            secret,
        })
    }

    pub fn workspace(&self) -> &str {
        &self.workspace
    }
    pub fn asset_root(&self) -> &str {
        &self.asset_root
    }
    /// Privileged signing material for the internal verifier; never send or log it.
    pub fn configured_secret(&self) -> Option<&str> {
        match &self.secret {
            SecretSource::Configured(value) => Some(value),
            SecretSource::SandboxFile(_) => None,
        }
    }
    /// Read only from the current authorized Sandbox. Missing files are errors;
    /// Ingress must not invoke AgentServer's secret-generation fallback.
    pub fn secret_file(&self) -> Option<&str> {
        match &self.secret {
            SecretSource::SandboxFile(path) => Some(path),
            SecretSource::Configured(_) => None,
        }
    }
}

pub(super) fn absolute_directory(value: Option<&str>, name: &str) -> Result<String, Error> {
    let Some(value) = value else {
        return Err(Error::Invalid(format!("template env requires {name}")));
    };
    if !value.starts_with('/') || value.contains('\0') || value.split('/').any(|p| p == "..") {
        return Err(Error::Invalid(format!(
            "{name} requires an absolute Sandbox directory without parent traversal"
        )));
    }
    // Lexical POSIX normalization only. Never resolve a Sandbox path on the host.
    let parts: Vec<_> = value
        .split('/')
        .filter(|p| !p.is_empty() && *p != ".")
        .collect();
    Ok(format!("/{}", parts.join("/")))
}
