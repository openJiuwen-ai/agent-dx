//! Deployment-owned runtime root and bootstrap, persisted with each Instance.
use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    path::{Component, Path},
};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct RuntimeEnvironment {
    pub rootfs: Rootfs,
    pub bootstrap: Bootstrap,
    #[serde(default)]
    pub env: BTreeMap<String, String>,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rootfs {
    pub runtime: String,
    pub r#type: String,
    pub path: String,
    #[serde(default)]
    pub readonly: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub r#type: String,
    pub root: String,
    pub target: String,
    pub entrypoint: Vec<String>,
}
fn absolute(value: &str) -> bool {
    Path::new(value).is_absolute()
        && value != "/"
        && !value.contains('\0')
        && !Path::new(value)
            .components()
            .any(|c| matches!(c, Component::ParentDir | Component::CurDir))
}
impl RuntimeEnvironment {
    pub fn validate(&self) -> Result<()> {
        if self.rootfs.r#type != "local"
            || self.bootstrap.r#type != "erofs"
            || self.rootfs.runtime.trim().is_empty()
            || !absolute(&self.rootfs.path)
            || !absolute(&self.bootstrap.root)
            || !absolute(&self.bootstrap.target)
            || self
                .bootstrap
                .entrypoint
                .first()
                .is_none_or(|v| !absolute(v) || !Path::new(v).starts_with(&self.bootstrap.target))
            || self.bootstrap.entrypoint.iter().any(|v| v.contains('\0'))
            || self
                .env
                .iter()
                .any(|(k, v)| k.is_empty() || k.contains(['=', '\0']) || v.contains('\0'))
        {
            return Err(Error::Invalid("runtime environment requires local rootfs, EROFS bootstrap, absolute paths and an entrypoint beneath its mount target".into()));
        }
        Ok(())
    }
}
