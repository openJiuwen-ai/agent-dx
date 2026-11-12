//! Files uploaded before a source instance exists. No snapshot catalog or runtime ownership.
use adx_core::{snapshot_preparation::PreparationFile, Error, Result};
use futures_util::TryStreamExt;
use object_store::{
    aws::{AmazonS3, AmazonS3Builder},
    buffered::BufWriter,
    path::Path as Key,
    signer::Signer,
    ObjectStore, ObjectStoreExt,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::{path::Path, sync::Arc, time::Duration};
use tokio::io::AsyncWriteExt;

const MAX_FILE_BYTES: u64 = 512 * 1024 * 1024;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct FileStorageConfig {
    pub bucket: String,
    pub region: String,
    pub endpoint: String,
    pub prefix: String,
    #[serde(default)]
    pub allow_http: bool,
}

fn unavailable(_: impl std::fmt::Display) -> Error {
    // Storage errors can contain presigned URLs and credentials; do not expose them.
    Error::Unavailable("preparation file storage unavailable".into())
}
fn invalid(message: &str) -> Error {
    Error::Invalid(format!("preparation file: {message}"))
}

pub fn valid_files_hash(value: &str) -> bool {
    value.len() == 64 && value.bytes().all(|b| b.is_ascii_hexdigit())
}
fn component(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 256
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(invalid("invalid upload identity"));
    }
    Ok(())
}

impl FileStorageConfig {
    pub fn validate(&self) -> Result<()> {
        if self.bucket.is_empty()
            || self.region.is_empty()
            || !(self.endpoint.starts_with("https://")
                || (self.allow_http && self.endpoint.starts_with("http://")))
            || self.prefix.is_empty()
            || self.prefix.split('/').any(|part| component(part).is_err())
        {
            return Err(invalid(
                "bucket, region, trusted endpoint and prefix required",
            ));
        }
        let endpoint =
            reqwest::Url::parse(&self.endpoint).map_err(|_| invalid("invalid storage endpoint"))?;
        if !endpoint.username().is_empty()
            || endpoint.password().is_some()
            || endpoint.query().is_some()
            || endpoint.fragment().is_some()
            || endpoint.host_str().is_none()
        {
            return Err(invalid(
                "storage endpoint must not contain credentials, query or fragment",
            ));
        }
        Ok(())
    }
    pub fn build(&self) -> Result<FileStore> {
        self.validate()?;
        adx_transport::install_crypto_provider();
        let store = AmazonS3Builder::from_env()
            .with_bucket_name(&self.bucket)
            .with_region(&self.region)
            .with_endpoint(&self.endpoint)
            .with_allow_http(self.allow_http)
            .build()
            .map_err(unavailable)?;
        let scope = format!(
            "{:x}",
            Sha256::digest(
                serde_json::to_vec(&(&self.bucket, &self.region, &self.endpoint, &self.prefix))
                    .map_err(unavailable)?
            )
        );
        Ok(FileStore {
            store: Arc::new(store),
            prefix: self.prefix.clone(),
            scope,
        })
    }
}

#[derive(Clone)]
pub struct FileStore {
    store: Arc<AmazonS3>,
    prefix: String,
    scope: String,
}

impl FileStore {
    fn tenant_prefix(&self, tenant: &str) -> String {
        format!("{}/{:x}", self.prefix, Sha256::digest(tenant.as_bytes()))
    }
    fn upload_key(
        &self,
        tenant: &str,
        template: &str,
        preparation: &str,
        hash: &str,
    ) -> Result<Key> {
        component(template)?;
        component(preparation)?;
        if tenant.is_empty() || !valid_files_hash(hash) {
            return Err(invalid("invalid file reference"));
        }
        Ok(Key::from(format!(
            "{}/{template}/{preparation}/uploads/{hash}.tar",
            self.tenant_prefix(tenant)
        )))
    }
    pub async fn remove(&self, tenant: &str, file: &PreparationFile) -> Result<()> {
        file.validate()?;
        if file.scope != self.scope
            || !file
                .location
                .starts_with(&format!("{}/", self.tenant_prefix(tenant)))
            || !file.location.contains("/sealed/")
        {
            return Err(invalid("file scope mismatch"));
        }
        self.store
            .delete(&Key::from(file.location.clone()))
            .await
            .map_err(unavailable)
    }
    /// Terminal status polling performs bounded cleanup; the dedicated bucket prefix also needs TTL.
    pub async fn cleanup(&self, tenant: &str, template: &str, preparation: &str) -> Result<()> {
        component(template)?;
        component(preparation)?;
        if tenant.is_empty() {
            return Err(invalid("tenant required"));
        }
        let prefix = Key::from(format!(
            "{}/{template}/{preparation}/",
            self.tenant_prefix(tenant)
        ));
        let mut objects = self.store.list(Some(&prefix));
        let mut count = 0;
        while let Some(object) = objects.try_next().await.map_err(unavailable)? {
            if count >= 1024 {
                return Err(invalid("cleanup list limit reached"));
            }
            self.store
                .delete(&object.location)
                .await
                .map_err(unavailable)?;
            count += 1;
        }
        Ok(())
    }

    pub async fn upload_url(
        &self,
        tenant: &str,
        template: &str,
        preparation: &str,
        hash: &str,
    ) -> Result<String> {
        let key = self.upload_key(tenant, template, preparation, hash)?;
        self.store
            .signed_url(reqwest::Method::PUT, &key, Duration::from_secs(3600))
            .await
            .map(|url| url.to_string())
            .map_err(unavailable)
    }

    /// Copy SDK-writable bytes into a private immutable object before accepting a plan.
    pub async fn seal(
        &self,
        tenant: &str,
        template: &str,
        preparation: &str,
        hash: &str,
    ) -> Result<PreparationFile> {
        let upload = self.upload_key(tenant, template, preparation, hash)?;
        let input = self.store.get(&upload).await.map_err(|error| {
            if matches!(error, object_store::Error::NotFound { .. }) {
                invalid("uploaded file missing")
            } else {
                unavailable(error)
            }
        })?;
        if input.meta.size == 0 || input.meta.size > MAX_FILE_BYTES {
            return Err(invalid("uploaded file exceeds limit"));
        }
        let location = format!(
            "{}/{template}/{preparation}/sealed/{}.tar",
            self.tenant_prefix(tenant),
            uuid::Uuid::new_v4()
        );
        let mut output = BufWriter::with_capacity(
            Arc::clone(&self.store) as Arc<dyn ObjectStore>,
            Key::from(location.clone()),
            8 * 1024 * 1024,
        );
        let mut stream = input.into_stream();
        let mut digest = Sha256::new();
        let mut size = 0u64;
        let copied: Result<()> = async {
            while let Some(chunk) = stream.try_next().await.map_err(unavailable)? {
                size = size
                    .checked_add(chunk.len() as u64)
                    .ok_or_else(|| invalid("uploaded file exceeds limit"))?;
                if size > MAX_FILE_BYTES {
                    return Err(invalid("uploaded file exceeds limit"));
                }
                digest.update(&chunk);
                output.write_all(&chunk).await.map_err(unavailable)?;
            }
            output.shutdown().await.map_err(unavailable)?;
            Ok(())
        }
        .await;
        if let Err(error) = copied {
            let _ = output.abort().await;
            let _ = self.store.delete(&Key::from(location)).await;
            return Err(error);
        }
        Ok(PreparationFile {
            scope: self.scope.clone(),
            location,
            size_bytes: size,
            sha256: format!("{:x}", digest.finalize()),
        })
    }

    /// Read only a sealed object in the authenticated tenant's configured store.
    pub async fn download(
        &self,
        tenant: &str,
        file: &PreparationFile,
        target: &Path,
    ) -> Result<()> {
        file.validate()?;
        if file.scope != self.scope
            || !file
                .location
                .starts_with(&format!("{}/", self.tenant_prefix(tenant)))
            || !file.location.contains("/sealed/")
        {
            return Err(invalid("file is outside authorized storage scope"));
        }
        let input = self
            .store
            .get(&Key::from(file.location.clone()))
            .await
            .map_err(unavailable)?;
        if input.meta.size != file.size_bytes {
            return Err(invalid("sealed file size changed"));
        }
        let mut output = tokio::fs::OpenOptions::new()
            .write(true)
            .create_new(true)
            .open(target)
            .await
            .map_err(unavailable)?;
        let mut stream = input.into_stream();
        let mut size = 0u64;
        let mut digest = Sha256::new();
        while let Some(chunk) = stream.try_next().await.map_err(unavailable)? {
            size = size
                .checked_add(chunk.len() as u64)
                .ok_or_else(|| invalid("sealed file exceeds limit"))?;
            if size > file.size_bytes {
                return Err(invalid("sealed file exceeds limit"));
            }
            digest.update(&chunk);
            output.write_all(&chunk).await.map_err(unavailable)?;
        }
        output.shutdown().await.map_err(unavailable)?;
        if size != file.size_bytes || format!("{:x}", digest.finalize()) != file.sha256 {
            return Err(invalid("sealed file checksum mismatch"));
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[tokio::test]
    async fn foreign_tenant_and_storage_scope_are_rejected_before_io() {
        let store = FileStorageConfig {
            bucket: "builds".into(),
            region: "test".into(),
            endpoint: "https://store.invalid".into(),
            prefix: "contexts".into(),
            allow_http: false,
        }
        .build()
        .unwrap();
        let mut file = PreparationFile {
            scope: store.scope.clone(),
            location: format!(
                "{}/template/build/sealed/context.tar",
                store.tenant_prefix("tenant-a")
            ),
            size_bytes: 1,
            sha256: "a".repeat(64),
        };
        let temp = tempfile::tempdir().unwrap();
        let target = temp.path().join("archive.tar");
        assert!(matches!(
            store.download("tenant-b", &file, &target).await,
            Err(Error::Invalid(_))
        ));
        assert!(matches!(
            store.remove("tenant-b", &file).await,
            Err(Error::Invalid(_))
        ));
        file.scope = "different-store".into();
        assert!(matches!(
            store.download("tenant-a", &file, &target).await,
            Err(Error::Invalid(_))
        ));
        assert!(!target.exists());
    }

    #[test]
    fn file_store_requires_explicit_trusted_transport_and_hash_shape() {
        let mut config = FileStorageConfig {
            bucket: "builds".into(),
            region: "test".into(),
            endpoint: "http://store:9000".into(),
            prefix: "preparation-files".into(),
            allow_http: false,
        };
        assert!(config.validate().is_err());
        config.allow_http = true;
        assert!(config.validate().is_ok());
        config.prefix = "../snapshots".into();
        assert!(config.validate().is_err());
        assert!(valid_files_hash(&"a".repeat(64)));
        assert!(!valid_files_hash("../other"));
    }
}
