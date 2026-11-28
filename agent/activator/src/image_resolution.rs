//! Resolve configured OCI registry tags once, at template publication.
use adx_core::checkpoint::is_oci_digest;
use reqwest::{header, Client, RequestBuilder, Response, StatusCode, Url};
use serde::Deserialize;
use std::{collections::BTreeMap, path::Path, time::Duration};

const ACCEPT: &str = concat!(
    "application/vnd.oci.image.manifest.v1+json,",
    "application/vnd.docker.distribution.manifest.v2+json,",
    "application/vnd.oci.image.index.v1+json,",
    "application/vnd.docker.distribution.manifest.list.v2+json"
);

#[derive(Deserialize)]
struct DockerConfig {
    auths: BTreeMap<String, DockerAuth>,
}

#[derive(Deserialize)]
struct DockerAuth {
    auth: Option<String>,
}

#[derive(Deserialize)]
struct RegistryToken {
    token: Option<String>,
    access_token: Option<String>,
}

pub(super) struct ImageResolver {
    client: Client,
    auths: BTreeMap<String, DockerAuth>,
}

impl ImageResolver {
    pub(super) fn from_file(path: &Path) -> crate::Result<Self> {
        let config: DockerConfig = std::fs::read(path)
            .ok()
            .and_then(|bytes| serde_json::from_slice(&bytes).ok())
            .ok_or_else(|| {
                crate::Error::Invalid("cannot read image registry auth config".into())
            })?;
        let client = Client::builder()
            .https_only(true)
            .redirect(reqwest::redirect::Policy::none())
            .timeout(Duration::from_secs(3))
            .build()
            .map_err(|_| {
                crate::Error::Unavailable("cannot initialize image registry client".into())
            })?;
        Ok(Self {
            client,
            auths: config.auths,
        })
    }

    #[cfg(test)]
    pub(super) fn for_test_registry(registry: String, certificate: &[u8]) -> Self {
        let client = Client::builder()
            .https_only(true)
            .no_proxy()
            .add_root_certificate(reqwest::Certificate::from_pem(certificate).unwrap())
            .timeout(Duration::from_secs(3))
            .build()
            .unwrap();
        Self {
            client,
            auths: [(registry, DockerAuth { auth: None })].into(),
        }
    }

    fn auth_for(&self, registry: &str, endpoint: &Url) -> Option<&DockerAuth> {
        self.auths.get(registry).or_else(|| {
            self.auths.iter().find_map(|(key, auth)| {
                let url = if key.contains("://") {
                    Url::parse(key)
                } else {
                    Url::parse(&format!("https://{key}"))
                }
                .ok()?;
                // Docker permits server-address keys with a scheme and a legacy
                // API suffix. Other paths cannot grant registry-wide credentials.
                (url.origin() == endpoint.origin()
                    && matches!(url.path(), "/" | "/v1" | "/v1/" | "/v2" | "/v2/")
                    && url.query().is_none()
                    && url.fragment().is_none()
                    && url.username().is_empty()
                    && url.password().is_none())
                .then_some(auth)
            })
        })
    }

    pub(super) async fn resolve(&self, image: &str) -> Option<String> {
        let (registry, repository, tag) = tagged_image(image)?;
        let url: Url = format!("https://{registry}/v2/{repository}/manifests/{tag}")
            .parse()
            .ok()?;
        let auth = self.auth_for(registry, &url)?;
        match self.manifest_digest(url, repository, auth).await {
            Ok(digest) => Some(format!("{registry}/{repository}@{digest}")),
            Err(reason) => {
                tracing::warn!(registry, repository, %reason, "template image digest could not be resolved");
                None
            }
        }
    }

    async fn manifest_digest(
        &self,
        url: Url,
        repository: &str,
        auth: &DockerAuth,
    ) -> Result<String, String> {
        let request = || self.client.head(url.clone()).header(header::ACCEPT, ACCEPT);
        let authorization = auth
            .auth
            .as_ref()
            .map(|auth| {
                let mut value = header::HeaderValue::from_str(&format!("Basic {auth}"))
                    .map_err(|_| "invalid registry authorization header")?;
                value.set_sensitive(true);
                Ok::<_, &str>(value)
            })
            .transpose()?;
        let authenticated = |request: RequestBuilder| match &authorization {
            Some(value) => request.header(header::AUTHORIZATION, value),
            None => request,
        };
        let mut response = send(authenticated(request()), "manifest request").await?;
        if response.status() == StatusCode::UNAUTHORIZED {
            let (mut realm, service) = bearer_challenge(&response)
                .ok_or("missing or invalid Registry Bearer challenge")?;
            if realm.origin() != url.origin()
                || !realm.username().is_empty()
                || realm.password().is_some()
            {
                return Err("Registry Bearer realm has an unauthorized origin or userinfo".into());
            }
            realm
                .query_pairs_mut()
                .append_pair("service", &service)
                .append_pair("scope", &format!("repository:{repository}:pull"));
            let token = send(authenticated(self.client.get(realm)), "token request").await?;
            if !token.status().is_success() {
                return Err(format!(
                    "Registry token request returned HTTP {}",
                    token.status().as_u16()
                ));
            }
            let token: RegistryToken = token
                .json()
                .await
                .map_err(|_| "invalid Registry token response")?;
            let bearer = token
                .token
                .or(token.access_token)
                .ok_or("Registry token response omitted the token")?;
            response = send(
                request().bearer_auth(bearer),
                "authenticated manifest request",
            )
            .await?;
        }
        if response.status() != StatusCode::OK {
            return Err(format!(
                "Registry manifest request returned HTTP {}",
                response.status().as_u16()
            ));
        }
        let digest = response
            .headers()
            .get("docker-content-digest")
            .and_then(|value| value.to_str().ok())
            .filter(|value| is_oci_digest(value))
            .ok_or("Registry manifest response omitted a valid SHA-256 digest")?;
        Ok(digest.to_owned())
    }
}

async fn send(request: RequestBuilder, operation: &str) -> Result<Response, String> {
    // Raw reqwest errors can contain the URL or server-provided data.
    request.send().await.map_err(|error| {
        let reason = if error.is_timeout() {
            "timed out"
        } else if error.is_connect() {
            "could not connect"
        } else {
            "failed"
        };
        format!("Registry {operation} {reason}")
    })
}

fn bearer_challenge(response: &Response) -> Option<(Url, String)> {
    let challenge = response
        .headers()
        .get(header::WWW_AUTHENTICATE)?
        .to_str()
        .ok()?;
    let (scheme, parameters) = challenge.split_once(' ')?;
    if !scheme.eq_ignore_ascii_case("bearer") {
        return None;
    }
    let fields: BTreeMap<_, _> = parameters
        .split(',')
        .filter_map(|pair| {
            let (key, value) = pair.trim().split_once('=')?;
            Some((key, value.strip_prefix('"')?.strip_suffix('"')?))
        })
        .collect();
    Some((
        fields.get("realm")?.parse().ok()?,
        fields.get("service")?.to_string(),
    ))
}

fn tagged_image(image: &str) -> Option<(&str, &str, &str)> {
    let (registry, path) = image.split_once('/')?;
    if registry.is_empty()
        || !registry
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'-' | b':'))
    {
        return None;
    }
    let (repository, tag) = path.rsplit_once(':').unwrap_or((path, "latest"));
    if !repository.split('/').all(|part| {
        part.as_bytes()
            .first()
            .is_some_and(u8::is_ascii_alphanumeric)
            && part.bytes().all(|b| {
                b.is_ascii_lowercase() || b.is_ascii_digit() || matches!(b, b'.' | b'_' | b'-')
            })
    }) || tag.len() > 128
        || !tag
            .as_bytes()
            .first()
            .is_some_and(|b| b.is_ascii_alphanumeric() || *b == b'_')
        || !tag
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    {
        return None;
    }
    Some((registry, repository, tag))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn docker_auth_matches_url_keys_by_registry_origin() {
        let resolver = ImageResolver {
            client: Client::new(),
            auths: [
                ("https://REGISTRY.example:443/v1/", "match"),
                ("registry.example:5000/", "explicit-port"),
                ("https://other.example/private", "repository"),
                ("https://user@untrusted.example", "userinfo"),
            ]
            .into_iter()
            .map(|(key, auth)| {
                (
                    key.to_owned(),
                    DockerAuth {
                        auth: Some(auth.to_owned()),
                    },
                )
            })
            .collect(),
        };
        let auth = |registry: &str| {
            let url = Url::parse(&format!("https://{registry}/v2/app/manifests/one")).unwrap();
            resolver
                .auth_for(registry, &url)
                .and_then(|entry| entry.auth.as_deref())
        };
        assert_eq!(auth("registry.example"), Some("match"));
        assert_eq!(auth("registry.example:5000"), Some("explicit-port"));
        assert_eq!(auth("registry.example:5001"), None);
        assert_eq!(auth("other.example"), None);
        assert_eq!(auth("untrusted.example"), None);
    }
}
