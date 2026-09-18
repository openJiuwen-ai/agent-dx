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
    #[serde(default)]
    pub path: String,
    #[serde(default)]
    pub image: String,
    #[serde(default)]
    pub readonly: bool,
}
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Bootstrap {
    pub r#type: String,
    #[serde(default)]
    pub root: String,
    #[serde(default)]
    pub image: String,
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
        let rootfs_source = match self.rootfs.r#type.as_str() {
            "local" => absolute(&self.rootfs.path) && self.rootfs.image.is_empty(),
            "image" => self.rootfs.path.is_empty() && valid_image(&self.rootfs.image),
            _ => false,
        };
        let bootstrap_source = match self.bootstrap.r#type.as_str() {
            "erofs" => absolute(&self.bootstrap.root) && self.bootstrap.image.is_empty(),
            "image" => self.bootstrap.root.is_empty() && valid_image(&self.bootstrap.image),
            _ => false,
        };
        let matching_sources = match (self.rootfs.r#type.as_str(), self.bootstrap.r#type.as_str()) {
            ("local", "erofs") => self.rootfs.path == self.bootstrap.root,
            ("image", "image") => self.rootfs.image == self.bootstrap.image,
            _ => false,
        };
        if !rootfs_source
            || !bootstrap_source
            || !matching_sources
            || self.rootfs.runtime.trim().is_empty()
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
            return Err(Error::Invalid("runtime environment requires one matching local/EROFS or OCI image source, an absolute target and an entrypoint beneath it".into()));
        }
        Ok(())
    }
}

fn valid_image(value: &str) -> bool {
    let value = value.trim();
    let Some((repository, digest)) = value.rsplit_once("@sha256:") else {
        return false;
    };
    !repository.is_empty()
        && !repository.chars().any(char::is_whitespace)
        && digest.len() == 64
        && digest
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
}
