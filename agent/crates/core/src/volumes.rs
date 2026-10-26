//! Stable directory definitions rendered before Environment creation.
use crate::{
    mounts::{CreateOptions, Parameters},
    parameters, ValidationResult,
};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AccessMode {
    #[default]
    Shared,
    Exclusive,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case", deny_unknown_fields)]
pub enum VolumeSource {
    Directory {
        storage_domain: String,
        path: String,
    },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeSpec {
    pub id: String,
    pub source: VolumeSource,
    #[serde(default)]
    pub access_mode: AccessMode,
    #[serde(default)]
    pub create_if_missing: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_options: Option<CreateOptions>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct VolumeRef {
    pub id: String,
    pub uid: String,
}

pub(crate) fn volume_id(value: &str) -> ValidationResult {
    if value.is_empty()
        || value.len() > 256
        || matches!(value, "." | "..")
        || !value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'.' | b'_' | b'-'))
    {
        return Err("invalid Volume ID: expected 1–256 URL-safe ASCII characters".into());
    }
    Ok(())
}

pub(crate) fn identifier(value: &str) -> ValidationResult {
    if value.trim().is_empty()
        || value.len() > 256
        || value
            .chars()
            .any(|c| c.is_control() || matches!(c, '$' | '{' | '}'))
    {
        return Err("invalid Volume identifier".into());
    }
    Ok(())
}
impl VolumeSpec {
    pub fn render(&self, values: Option<&Parameters>) -> Result<Self, String> {
        let VolumeSource::Directory {
            storage_domain,
            path,
        } = &self.source;
        let result = Self {
            id: parameters::render(&self.id, values)?,
            source: VolumeSource::Directory {
                storage_domain: parameters::render(storage_domain, values)?,
                path: parameters::render(path, values)?,
            },
            ..self.clone()
        };
        result.validate()?;
        Ok(result)
    }
    pub fn validate(&self) -> ValidationResult {
        volume_id(&self.id)?;
        let VolumeSource::Directory {
            storage_domain,
            path,
        } = &self.source;
        identifier(storage_domain)?;
        crate::mounts::relative_path(path)?;
        if let Some(options) = &self.create_options {
            if !self.create_if_missing {
                return Err("Volume create_options requires create_if_missing".into());
            }
            options.validate()?;
        }
        Ok(())
    }
}
