//! Template bind mounts with inline host paths or stable Volume references.
use crate::ValidationResult;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, BTreeSet};

pub type Parameters = BTreeMap<String, String>;
const MAX_PATH: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum MountType {
    Bind,
}

/// Ownership and permissions applied only when a Local source directory is created.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct CreateOptions {
    pub uid: u32,
    pub gid: u32,
    /// Four octal digits, from "0000" through "0777".
    pub mode: String,
}

impl CreateOptions {
    pub(crate) fn validate(&self) -> ValidationResult {
        if self.uid == u32::MAX || self.gid == u32::MAX {
            return Err("create_options uid/gid must be below 4294967295".into());
        }
        if self.mode.len() != 4
            || !self.mode.starts_with('0')
            || !self.mode.bytes().all(|value| matches!(value, b'0'..=b'7'))
        {
            return Err("create_options mode must be four octal digits from 0000 to 0777".into());
        }
        Ok(())
    }
}

/// Agent templates currently expose Local bind mounts; Platform also supports other sources.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Mount {
    #[serde(rename = "type")]
    pub kind: MountType,
    #[serde(default, skip_serializing_if = "String::is_empty")]
    pub host_path: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub volume_uid: Option<String>,
    pub target: String,
    #[serde(default)]
    pub options: Vec<String>,
    /// Create a missing Local source directory on the execution node.
    #[serde(default, skip_serializing_if = "std::ops::Not::not")]
    pub create_if_missing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_options: Option<CreateOptions>,
}

pub(crate) fn parameter_name(name: &str) -> bool {
    !name.is_empty()
        && name.len() <= 64
        && name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_')
}

pub(crate) fn relative_path(path: &str) -> ValidationResult {
    if path.is_empty()
        || path.len() > MAX_PATH
        || path
            .chars()
            .any(|c| c.is_control() || matches!(c, '\\' | '$' | '{' | '}'))
        || path.split('/').any(|part| matches!(part, "" | "." | ".."))
    {
        return Err(
            "mount paths require nonempty components without traversal or expansion".into(),
        );
    }
    Ok(())
}

fn absolute_path(path: &str) -> ValidationResult {
    if path.len() > MAX_PATH {
        return Err("mount path too long".into());
    }
    relative_path(
        path.strip_prefix('/')
            .ok_or("mount path must be absolute")?,
    )
}

/// Parameters are bounded strings. Each template field validates its rendered value.
pub fn validate_parameters(parameters: &Parameters) -> ValidationResult {
    if parameters.len() > 64 {
        return Err("at most 64 binding parameters are allowed".into());
    }
    for (key, value) in parameters {
        if !parameter_name(key) {
            return Err("invalid binding parameter name".into());
        }
        if value.len() > 8192 || value.contains('\0') {
            return Err("invalid parameter value".into());
        }
    }
    Ok(())
}

impl Mount {
    fn render_path(&self, parameters: Option<&Parameters>) -> Result<String, String> {
        let mut remaining = self.host_path.as_str();
        let mut output = String::new();
        if let Some(first) = remaining.find("${") {
            let prefix = &remaining[..first];
            let root_end = prefix
                .rfind('/')
                .ok_or("mount requires a fixed absolute root")?;
            absolute_path(&prefix[..root_end])?;
        }
        while let Some(start) = remaining.find("${") {
            let literal = &remaining[..start];
            if literal.contains(['$', '{', '}']) {
                return Err("invalid mount placeholder".into());
            }
            output.push_str(literal);
            let rest = &remaining[start + 2..];
            let end = rest.find('}').ok_or("unterminated mount placeholder")?;
            let name = &rest[..end];
            if !parameter_name(name) {
                return Err("invalid mount parameter name".into());
            }
            output.push_str(match parameters {
                Some(parameters) => {
                    let value = parameters
                        .get(name)
                        .ok_or_else(|| format!("missing mount parameter: {name}"))?;
                    relative_path(value)?;
                    value
                }
                None => "parameter",
            });
            remaining = &rest[end + 1..];
        }
        if remaining.contains(['$', '{', '}']) {
            return Err("invalid mount placeholder".into());
        }
        output.push_str(remaining);
        absolute_path(&output)?;
        Ok(output)
    }

    fn validate_options(&self) -> ValidationResult {
        if let Some(options) = &self.create_options {
            if !self.create_if_missing {
                return Err("create_options requires create_if_missing".into());
            }
            options.validate()?;
        }
        absolute_path(&self.target)?;
        let mut seen = BTreeSet::new();
        for option in &self.options {
            if !matches!(option.as_str(), "ro" | "rw" | "nosuid" | "nodev" | "noexec")
                || !seen.insert(option.as_str())
            {
                return Err("unsupported or duplicate Agent mount option".into());
            }
        }
        if seen.contains("ro") && seen.contains("rw") {
            return Err("mount cannot be both read-only and read-write".into());
        }
        Ok(())
    }

    /// Validate syntax at template publication without resolving parameters.
    pub fn validate_template(&self) -> ValidationResult {
        if let Some(id) = &self.volume_id {
            self.validate_options()?;
            if !self.host_path.is_empty() || self.create_if_missing || self.create_options.is_some()
            {
                return Err(
                    "Volume reference cannot include inline source or initialization".into(),
                );
            }
            crate::volumes::volume_id(&crate::parameters::render(id, None)?)?;
            if let Some(uid) = &self.volume_uid {
                crate::volumes::identifier(uid)?;
            }
            return Ok(());
        }
        if self.volume_uid.is_some() {
            return Err("volume_uid requires volume_id".into());
        }
        if self.host_path.len() > MAX_PATH {
            return Err("mount path too long".into());
        }
        self.validate_options()?;
        self.render_path(None).map(|_| ())
    }

    /// Execution specs must contain only concrete paths, never unresolved placeholders.
    pub fn validate(&self) -> ValidationResult {
        self.validate_template()?;
        match &self.volume_id {
            Some(id) => crate::volumes::volume_id(id),
            None => absolute_path(&self.host_path),
        }
    }

    /// Missing parameters or invalid paths fail before any Sandbox side effect.
    pub fn render(&self, parameters: &Parameters) -> Result<Self, String> {
        self.validate_template()?;
        validate_parameters(parameters)?;
        self.render_validated(parameters)
    }

    pub(crate) fn render_validated(&self, parameters: &Parameters) -> Result<Self, String> {
        let mut result = self.clone();
        if let Some(id) = &self.volume_id {
            result.volume_id = Some(crate::parameters::render(id, Some(parameters))?);
        } else {
            result.host_path = self.render_path(Some(parameters))?;
        }
        result.validate()?;
        Ok(result)
    }
}

pub fn validate_mounts(mounts: &[Mount]) -> ValidationResult {
    if mounts.len() > 32 {
        return Err("at most 32 Agent mounts are allowed".into());
    }
    let mut targets = BTreeSet::new();
    for mount in mounts {
        mount.validate_template()?;
        if !targets.insert(&mount.target) {
            return Err("duplicate mount target".into());
        }
    }
    Ok(())
}
