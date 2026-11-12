//! Deployment-owned local root filesystems selected by public template name.
use crate::{execution::WorkloadProcess, Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct LocalRootfsTemplate {
    pub path: String,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
    #[serde(default)]
    pub process: Option<WorkloadProcess>,
}

impl LocalRootfsTemplate {
    pub fn validate(&self) -> Result<()> {
        let path = Path::new(&self.path);
        if !path.is_absolute()
            || self.path == "/"
            || self.path.contains('\0')
            || path
                .components()
                .any(|part| matches!(part, Component::ParentDir | Component::CurDir))
            || self.env.iter().any(|(key, value)| {
                key.is_empty() || key.contains(['=', '\0']) || value.contains('\0')
            })
        {
            return Err(Error::Invalid(
                "local rootfs template requires an absolute artifact path and valid environment"
                    .into(),
            ));
        }
        if let Some(process) = &self.process {
            process.validate()?;
        }
        Ok(())
    }
}

/// Stable node capability required by requests selecting a registered template.
pub fn capability_label(name: &str) -> Result<String> {
    if name.is_empty()
        || name.len() > 128
        || !name
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-'))
    {
        return Err(Error::Invalid("local rootfs template name must use 1..128 ASCII letters, digits, underscores or hyphens".into()));
    }
    Ok(format!("ADX_LOCAL_ROOTFS_{name}"))
}
