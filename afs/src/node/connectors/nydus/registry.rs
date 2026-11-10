//! OCI Registry protocol and immutable source metadata. No AFS dependency.
use std::{
    collections::BTreeMap,
    sync::Arc,
    time::{Duration, Instant},
};

use afs_error::{Error, Result};
use prost::bytes::Bytes;
use reqwest::{Client, Method, Response, StatusCode, Url, header};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use super::RegistryConfig;

const MANIFEST_LIMIT: usize = 4 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct RegistrySource {
    pub source_key: String,
    pub registry: String,
    pub repository: String,
    pub blob_digest: String,
    pub size: u64,
}

#[derive(Clone, Debug, Deserialize, Serialize, Eq, PartialEq)]
#[serde(deny_unknown_fields)]
pub struct ImagePlan {
    pub source_key: String,
    pub registry: String,
    pub repository: String,
    pub manifest_digest: String,
    pub blobs: Vec<RegistrySource>,
}

struct Source {
    config: RegistryConfig,
    password: Option<String>,
    bearer: Option<String>,
}

pub struct RegistryProvider {
    client: Client,
    sources: BTreeMap<String, Source>,
    node_id: String,
    tokens: tokio::sync::Mutex<BTreeMap<String, (String, Instant)>>,
}

impl RegistryProvider {
    pub fn new(sources: &BTreeMap<String, RegistryConfig>, node_id: &str) -> Result<Arc<Self>> {
        let _ = rustls::crypto::ring::default_provider().install_default();
        let client = Client::builder()
            .redirect(reqwest::redirect::Policy::none())
            .build()
            .map_err(|_| unavailable("cannot construct Registry HTTP client"))?;
        let mut resolved = BTreeMap::new();
        for (key, config) in sources {
            validate_component(key)?;
            validate_registry(config)?;
            resolved.insert(
                key.clone(),
                Source {
                    config: config.clone(),
                    password: read_secret(config.password_file.as_deref())?,
                    bearer: read_secret(config.bearer_token_file.as_deref())?,
                },
            );
        }
        Ok(Arc::new(Self {
            client,
            sources: resolved,
            node_id: node_id.into(),
            tokens: Default::default(),
        }))
    }

    pub fn validate_plan(&self, plan: &ImagePlan, key: &str, manifest: &str) -> Result<()> {
        let source = self.source(key)?;
        if plan.source_key != key
            || plan.registry != source.config.registry
            || plan.repository != source.config.repository
            || plan.manifest_digest != digest(manifest)?
            || plan.blobs.is_empty()
            || plan.blobs.len() > 4096
        {
            return Err(invalid(
                "prepared image source identity conflicts with request",
            ));
        }
        for blob in &plan.blobs {
            self.validate_blob(blob)?;
        }
        Ok(())
    }

    fn source(&self, key: &str) -> Result<&Source> {
        self.sources
            .get(key)
            .ok_or_else(|| Error::coded(afs_error::IO_NOT_FOUND, "unknown Registry source key"))
    }

    fn validate_blob(&self, blob: &RegistrySource) -> Result<&Source> {
        let source = self.source(&blob.source_key)?;
        if source.config.registry != blob.registry
            || source.config.repository != blob.repository
            || digest(&blob.blob_digest)? != blob.blob_digest
            || blob.size > i64::MAX as u64
        {
            return Err(invalid("lazy source does not match configured Registry"));
        }
        Ok(source)
    }

    pub async fn describe(
        &self,
        key: &str,
        manifest: &str,
        deadline: Instant,
    ) -> Result<ImagePlan> {
        let source = self.source(key)?;
        let manifest = digest(manifest)?;
        let url = format!(
            "{}/v2/{}/manifests/{}",
            source.config.registry.trim_end_matches('/'),
            source.config.repository,
            manifest
        );
        let response = self.request(key, &url, None, deadline).await?;
        if response.status() != StatusCode::OK {
            return Err(origin_status(response.status()));
        }
        let bytes = bounded_body(response, MANIFEST_LIMIT, deadline).await?;
        if format!("sha256:{:x}", Sha256::digest(&bytes)) != manifest {
            return Err(invalid("Registry manifest digest mismatch"));
        }
        #[derive(Deserialize)]
        struct Descriptor {
            digest: String,
            size: u64,
        }
        #[derive(Deserialize)]
        struct Manifest {
            #[serde(rename = "schemaVersion")]
            schema: u32,
            layers: Vec<Descriptor>,
            config: Descriptor,
        }
        let parsed: Manifest = serde_json::from_slice(&bytes).map_err(|_| {
            invalid("expected a concrete image manifest, not a tag or platform index")
        })?;
        if parsed.schema != 2 || parsed.layers.is_empty() || parsed.layers.len() > 4095 {
            return Err(invalid("invalid or oversized image manifest"));
        }
        let mut blobs = BTreeMap::new();
        for item in parsed
            .layers
            .into_iter()
            .chain(std::iter::once(parsed.config))
        {
            let id = digest(&item.digest)?;
            if item.size > i64::MAX as u64 {
                return Err(invalid("blob length exceeds file limit"));
            }
            if blobs
                .insert(id.clone(), item.size)
                .is_some_and(|old| old != item.size)
            {
                return Err(invalid("conflicting sizes for the same blob"));
            }
        }
        Ok(ImagePlan {
            source_key: key.into(),
            registry: source.config.registry.clone(),
            repository: source.config.repository.clone(),
            manifest_digest: manifest,
            blobs: blobs
                .into_iter()
                .map(|(blob_digest, size)| RegistrySource {
                    source_key: key.into(),
                    registry: source.config.registry.clone(),
                    repository: source.config.repository.clone(),
                    blob_digest,
                    size,
                })
                .collect(),
        })
    }

    pub async fn fetch_range(
        &self,
        blob: &RegistrySource,
        offset: u64,
        length: usize,
        deadline: Instant,
    ) -> Result<Bytes> {
        self.validate_blob(blob)?;
        let end = offset
            .checked_add(length as u64)
            .filter(|n| *n > offset && *n <= blob.size)
            .ok_or_else(|| invalid("invalid Registry byte range"))?;
        let range = format!("bytes={offset}-{}", end - 1);
        let url = format!(
            "{}/v2/{}/blobs/{}",
            blob.registry.trim_end_matches('/'),
            blob.repository,
            blob.blob_digest
        );
        let response = self
            .request(&blob.source_key, &url, Some(&range), deadline)
            .await?;
        let expected = format!("bytes {offset}-{}/{}", end - 1, blob.size);
        if response.status() != StatusCode::PARTIAL_CONTENT {
            return Err(origin_status(response.status()));
        }
        if response
            .headers()
            .get(header::CONTENT_RANGE)
            .and_then(|v| v.to_str().ok())
            != Some(&expected)
            || response
                .headers()
                .get(header::CONTENT_ENCODING)
                .is_some_and(|v| v != "identity")
        {
            return Err(invalid("Registry did not honor the exact byte range"));
        }
        let body = bounded_body(response, length, deadline).await?;
        if body.len() != length {
            return Err(invalid("short Registry range response"));
        }
        Ok(body)
    }

    async fn request(
        &self,
        key: &str,
        uri: &str,
        range: Option<&str>,
        deadline: Instant,
    ) -> Result<Response> {
        let source = self.source(key)?;
        let original = Url::parse(uri).map_err(|_| invalid("invalid Registry request URL"))?;
        let mut url = original.clone();
        let mut challenged = false;
        for _ in 0..7 {
            let remaining = deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| unavailable("Registry deadline exceeded"))?;
            let mut request = self.client.request(Method::GET, url.clone()).timeout(remaining)
                .header(header::ACCEPT_ENCODING,"identity").header("x-afs-node", &self.node_id)
                .header(header::ACCEPT,"application/vnd.oci.image.manifest.v1+json, application/vnd.docker.distribution.manifest.v2+json");
            if let Some(range) = range {
                request = request.header(header::RANGE, range);
            }
            if url.origin() == original.origin() {
                let token = source.bearer.clone().or_else(|| {
                    self.tokens.try_lock().ok().and_then(|tokens| {
                        tokens
                            .get(key)
                            .filter(|(_, until)| *until > Instant::now())
                            .map(|(value, _)| value.clone())
                    })
                });
                if let Some(token) = token {
                    request = request.bearer_auth(token);
                } else if let Some(user) = &source.config.username {
                    request = request.basic_auth(user, source.password.as_ref());
                }
            }
            let response = request
                .send()
                .await
                .map_err(|_| unavailable("Registry request failed"))?;
            if response.status() == StatusCode::UNAUTHORIZED
                && !challenged
                && url.origin() == original.origin()
            {
                let challenge = response
                    .headers()
                    .get(header::WWW_AUTHENTICATE)
                    .and_then(|v| v.to_str().ok())
                    .unwrap_or("");
                if challenge.starts_with("Bearer ") {
                    self.obtain_token(key, challenge, deadline).await?;
                    challenged = true;
                    continue;
                }
            }
            if response.status().is_redirection() {
                let location = response
                    .headers()
                    .get(header::LOCATION)
                    .and_then(|v| v.to_str().ok())
                    .ok_or_else(|| invalid("Registry redirect has no location"))?;
                let next = url
                    .join(location)
                    .map_err(|_| invalid("invalid Registry redirect"))?;
                if !matches!(next.scheme(), "http" | "https")
                    || (next.scheme() == "http"
                        && (!source.config.allow_insecure_http || url.scheme() == "https"))
                    || !next.username().is_empty()
                    || next.password().is_some()
                {
                    return Err(invalid("unsupported Registry redirect"));
                }
                url = next;
                continue;
            }
            return Ok(response);
        }
        Err(unavailable(
            "Registry redirect/authentication limit exceeded",
        ))
    }

    async fn obtain_token(&self, key: &str, challenge: &str, deadline: Instant) -> Result<()> {
        let source = self.source(key)?;
        let params: BTreeMap<_, _> = challenge
            .trim_start_matches("Bearer ")
            .split(',')
            .filter_map(|part| {
                part.trim()
                    .split_once('=')
                    .map(|(k, v)| (k, v.trim_matches('"')))
            })
            .collect();
        let realm = params
            .get("realm")
            .ok_or_else(|| invalid("missing Registry auth realm"))?;
        let mut url = Url::parse(realm).map_err(|_| invalid("invalid Registry auth realm"))?;
        let origin =
            Url::parse(&source.config.registry).map_err(|_| invalid("invalid Registry URL"))?;
        let same_origin = url.origin() == origin.origin();
        if !matches!(url.scheme(), "http" | "https")
            || (!same_origin && url.scheme() != "https")
            || !url.username().is_empty()
            || url.password().is_some()
        {
            return Err(invalid("unsupported Registry auth realm"));
        }
        url.query_pairs_mut().append_pair(
            "scope",
            &format!("repository:{}:pull", source.config.repository),
        );
        if let Some(service) = params.get("service") {
            url.query_pairs_mut().append_pair("service", service);
        }
        let mut request = self.client.get(url).timeout(
            deadline
                .checked_duration_since(Instant::now())
                .ok_or_else(|| unavailable("Registry auth expired"))?,
        );
        // Never disclose credentials to an arbitrary realm from a remote challenge.
        if (same_origin || source.config.auth_realm.as_deref() == Some(*realm))
            && let Some(user) = &source.config.username
        {
            request = request.basic_auth(user, source.password.as_ref());
        }
        let response = request
            .send()
            .await
            .map_err(|_| unavailable("Registry token request failed"))?;
        if response.status() != StatusCode::OK {
            return Err(origin_status(response.status()));
        }
        #[derive(Deserialize)]
        struct Token {
            token: Option<String>,
            access_token: Option<String>,
            expires_in: Option<u64>,
        }
        let token: Token =
            serde_json::from_slice(&bounded_body(response, 64 * 1024, deadline).await?)
                .map_err(|_| invalid("invalid Registry token response"))?;
        let value = token
            .token
            .or(token.access_token)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| invalid("empty Registry token"))?;
        self.tokens.lock().await.insert(
            key.into(),
            (
                value,
                Instant::now()
                    + Duration::from_secs(
                        token
                            .expires_in
                            .unwrap_or(60)
                            .clamp(1, 3600)
                            .saturating_sub(1),
                    ),
            ),
        );
        Ok(())
    }
}

pub fn digest(value: &str) -> Result<String> {
    let hex = value.strip_prefix("sha256:").unwrap_or(value);
    if hex.len() != 64
        || !hex
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    {
        return Err(invalid("expected a lowercase sha256 digest"));
    }
    Ok(format!("sha256:{hex}"))
}

pub fn validate_component(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 128
        || value == "."
        || value == ".."
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"._-".contains(&b))
    {
        return Err(invalid("invalid source or directory name"));
    }
    Ok(())
}

fn validate_registry(config: &RegistryConfig) -> Result<()> {
    let url = Url::parse(&config.registry).map_err(|_| invalid("invalid Registry URL"))?;
    if url.scheme() == "http" && !config.allow_insecure_http {
        return Err(Error::coded(
            afs_error::CONFIG_INVALID,
            "Nydus Registry requires HTTPS unless allow_insecure_http is enabled",
        ));
    }
    if !matches!(url.scheme(), "http" | "https")
        || !url.username().is_empty()
        || url.password().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
        || url.path() != "/"
    {
        return Err(invalid(
            "Registry URL must be a credential-free HTTP(S) origin",
        ));
    }
    if config.repository.len() > 256 {
        return Err(invalid("repository too long"));
    }
    for segment in config.repository.split('/') {
        validate_component(segment)?;
    }
    Ok(())
}

fn read_secret(path: Option<&std::path::Path>) -> Result<Option<String>> {
    path.map(|path| {
        let value = std::fs::read_to_string(path)?;
        if value.len() > 64 * 1024 {
            return Err(invalid("Registry credential is oversized"));
        }
        Ok(value.trim().to_string())
    })
    .transpose()
}

async fn bounded_body(mut response: Response, limit: usize, deadline: Instant) -> Result<Bytes> {
    tokio::time::timeout_at(tokio::time::Instant::from_std(deadline), async move {
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| unavailable("Registry body interrupted"))?
        {
            if chunk.len() > limit.saturating_sub(bytes.len()) {
                return Err(invalid("oversized Registry response"));
            }
            bytes.extend_from_slice(&chunk);
        }
        Ok(Bytes::from(bytes))
    })
    .await
    .map_err(|_| unavailable("Registry body deadline exceeded"))?
}
fn origin_status(status: StatusCode) -> Error {
    let code = match status {
        StatusCode::NOT_FOUND => afs_error::IO_NOT_FOUND,
        StatusCode::UNAUTHORIZED | StatusCode::FORBIDDEN => afs_error::IO_PERMISSION_DENIED,
        _ => afs_error::IO_UNAVAILABLE,
    };
    Error::coded(
        code,
        format!("Registry response status {}", status.as_u16()),
    )
}
fn invalid(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_INVALID, message)
}
fn unavailable(message: &str) -> Error {
    Error::coded(afs_error::NODE_VFS_UNAVAILABLE, message)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registry_requires_https_without_explicit_opt_in() {
        let mut config: RegistryConfig = serde_json::from_value(serde_json::json!({
            "registry": "http://registry.example.com",
            "repository": "test/image"
        }))
        .unwrap();
        assert!(validate_registry(&config).is_err());
        config.allow_insecure_http = true;
        assert!(validate_registry(&config).is_ok());
        config.allow_insecure_http = false;
        config.registry = "https://registry.example.com".into();
        assert!(validate_registry(&config).is_ok());
    }

    #[test]
    fn accepts_only_fixed_safe_digest_and_source_names() {
        assert_eq!(
            digest(&"a".repeat(64)).unwrap(),
            format!("sha256:{}", "a".repeat(64))
        );
        for bad in ["latest", "sha256:../escape", "sha512:abcd", ""] {
            assert!(digest(bad).is_err());
        }
        for bad in ["..", "repo/other", "user@host", ""] {
            assert!(validate_component(bad).is_err());
        }
    }
    async fn response_fixture(
        response: &'static str,
    ) -> (
        Arc<RegistryProvider>,
        RegistrySource,
        tokio::task::JoinHandle<String>,
    ) {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let registry = format!("http://{}", listener.local_addr().unwrap());
        let server = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0; 1024];
            while !request.windows(4).any(|w| w == b"\r\n\r\n") {
                let n = stream.read(&mut buf).await.unwrap();
                assert!(n > 0);
                request.extend_from_slice(&buf[..n]);
            }
            stream.write_all(response.as_bytes()).await.unwrap();
            String::from_utf8(request).unwrap()
        });
        let config = RegistryConfig {
            registry: registry.clone(),
            allow_insecure_http: true,
            repository: "test/image".into(),
            username: None,
            password_file: None,
            bearer_token_file: None,
            auth_realm: None,
        };
        let provider =
            RegistryProvider::new(&BTreeMap::from([("test".into(), config)]), "node-a").unwrap();
        let source = RegistrySource {
            source_key: "test".into(),
            registry,
            repository: "test/image".into(),
            blob_digest: format!("sha256:{}", "a".repeat(64)),
            size: 8,
        };
        (provider, source, server)
    }

    #[tokio::test]
    async fn registry_exact_range_and_short_wrong_or_ignored_ranges() {
        for (response, valid) in [
            (
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-5/8\r\nContent-Length: 4\r\nConnection: close\r\n\r\ncdef",
                true,
            ),
            (
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 2-5/8\r\nContent-Length: 2\r\nConnection: close\r\n\r\ncd",
                false,
            ),
            (
                "HTTP/1.1 206 Partial Content\r\nContent-Range: bytes 0-3/8\r\nContent-Length: 4\r\nConnection: close\r\n\r\nabcd",
                false,
            ),
            (
                "HTTP/1.1 200 OK\r\nContent-Length: 8\r\nConnection: close\r\n\r\nabcdefgh",
                false,
            ),
            (
                "HTTP/1.1 503 Unavailable\r\nContent-Length: 0\r\nConnection: close\r\n\r\n",
                false,
            ),
        ] {
            let (provider, source, server) = response_fixture(response).await;
            let body = provider
                .fetch_range(&source, 2, 4, Instant::now() + Duration::from_secs(2))
                .await;
            assert_eq!(body.is_ok(), valid);
            if valid {
                assert_eq!(body.unwrap(), Bytes::from_static(b"cdef"));
            }
            let request = server.await.unwrap().to_lowercase();
            assert!(request.contains("range: bytes=2-5"));
            assert!(request.contains("accept-encoding: identity"));
        }
    }
}
