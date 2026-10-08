use super::unavailable;
use adx_core::{Error, Result};

/// Dedicated node-local endpoint: execution-scoped credentials are independent
/// of the deployment's optional component mTLS certificates.
#[derive(serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct StreamConfig {
    pub listen: std::net::SocketAddr,
    pub advertised_address: String,
    pub key_file: std::path::PathBuf,
}
impl StreamConfig {
    /// Persist the node key once; a node-process restart must accept existing
    /// executions' reconnect credentials. Never inject this key into a runtime.
    pub fn load_key(&self) -> Result<Vec<u8>> {
        use std::{fs::OpenOptions, io::Write, os::unix::fs::OpenOptionsExt};
        if !self.key_file.is_absolute() || !self.advertised_address.starts_with("http://") {
            return Err(Error::Invalid(
                "runtime control requires an absolute key_file and http advertised_address".into(),
            ));
        }
        let endpoint = tonic::transport::Endpoint::from_shared(self.advertised_address.clone())
            .map_err(|_| Error::Invalid("invalid runtime control address".into()))?;
        if endpoint
            .uri()
            .host()
            .and_then(|host| host.parse::<std::net::Ipv4Addr>().ok())
            .is_none()
        {
            return Err(Error::Invalid("runtime control advertised_address must use a node IPv4 address for sandboxd network isolation".into()));
        }
        if endpoint.uri().query().is_some()
            || endpoint
                .uri()
                .authority()
                .is_some_and(|a| a.as_str().contains('@'))
        {
            return Err(Error::Invalid(
                "runtime control address cannot contain credentials or query".into(),
            ));
        }
        if let Some(parent) = self.key_file.parent() {
            std::fs::create_dir_all(parent)
                .map_err(|_| unavailable("cannot create runtime control key directory"))?;
        }
        match OpenOptions::new()
            .write(true)
            .create_new(true)
            .mode(0o600)
            .open(&self.key_file)
        {
            Ok(mut file) => {
                let value = format!(
                    "{}{}",
                    uuid::Uuid::new_v4().simple(),
                    uuid::Uuid::new_v4().simple()
                );
                file.write_all(value.as_bytes())
                    .and_then(|_| file.sync_all())
                    .map_err(|_| unavailable("cannot persist runtime control key"))?;
                if let Some(parent) = self.key_file.parent() {
                    std::fs::File::open(parent)
                        .and_then(|f| f.sync_all())
                        .map_err(|_| unavailable("cannot persist runtime control key directory"))?;
                }
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => (),
            Err(_) => return Err(unavailable("cannot create runtime control key")),
        }
        let metadata = std::fs::symlink_metadata(&self.key_file)
            .map_err(|_| unavailable("cannot inspect runtime control key"))?;
        use std::os::unix::fs::PermissionsExt;
        if !metadata.file_type().is_file() || metadata.permissions().mode() & 0o077 != 0 {
            return Err(Error::Invalid(
                "runtime control key must be a private regular file (0600 or 0400)".into(),
            ));
        }
        let secret = std::fs::read(&self.key_file)
            .map_err(|_| unavailable("cannot read runtime control key"))?;
        if !(32..=4096).contains(&secret.len()) || std::str::from_utf8(&secret).is_err() {
            return Err(Error::Invalid(
                "runtime control key requires 32..4096 UTF-8 bytes".into(),
            ));
        }
        Ok(secret)
    }
}
