//! Management credentials and the current Node's advertised management route.
use afs_error::{Error, Result};
use axum::{
    extract::{Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::Next,
    response::{IntoResponse, Response},
};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::{
    collections::HashMap,
    io::Read,
    path::{Path, PathBuf},
};

#[derive(Clone, Debug, Default, Deserialize, Serialize)]
#[serde(default, deny_unknown_fields)]
pub struct ManagementConfig {
    pub token_file: Option<PathBuf>,
    pub advertised_endpoint: Option<String>,
    pub deployment: Option<String>,
    pub adx_worker_id: Option<String>,
}

pub(crate) type AdminToken = [u8; 32];
const ENDPOINT: &str = "afs.management_rest_addr";
const DEPLOYMENT: &str = "afs.deployment";
const ADX_WORKER: &str = "afs.adx_worker_id";

pub(crate) fn load_token(path: &Path) -> Result<AdminToken> {
    let mut contents = String::new();
    std::fs::File::open(path)?
        .take(4097)
        .read_to_string(&mut contents)?;
    let token = contents.trim();
    if contents.len() > 4096
        || token.len() < 32
        || !token.bytes().all(|byte| byte.is_ascii_graphic())
    {
        return Err(invalid(
            "management token must contain 32–4096 visible ASCII bytes",
        ));
    }
    Ok(Sha256::digest(token).into())
}

#[cfg(test)]
pub(crate) fn test_token(token: &str) -> AdminToken {
    Sha256::digest(token).into()
}

pub(crate) fn authorized(headers: &HeaderMap, expected: AdminToken) -> bool {
    let Some(token) = headers
        .get(header::AUTHORIZATION)
        .and_then(|header| header.to_str().ok())
        .and_then(|header| header.strip_prefix("Bearer "))
    else {
        return false;
    };
    Sha256::digest(token)
        .iter()
        .zip(expected)
        .fold(0, |difference, (actual, expected)| {
            difference | (actual ^ expected)
        })
        == 0
}

pub(crate) async fn require_admin(
    State(token): State<AdminToken>,
    request: Request,
    next: Next,
) -> Response {
    if authorized(request.headers(), token) {
        next.run(request).await
    } else {
        StatusCode::FORBIDDEN.into_response()
    }
}

#[derive(Clone, Debug, Serialize)]
pub(crate) struct NodeAdvertisement {
    pub management_rest_addr: String,
    pub deployment: String,
    pub adx_worker_id: Option<String>,
    pub connectors: Vec<String>,
}

impl NodeAdvertisement {
    pub(crate) fn from_registration(
        labels: &HashMap<String, String>,
        capabilities: &[String],
    ) -> Result<Option<Self>> {
        let Some(endpoint) = labels.get(ENDPOINT) else {
            return Ok(None);
        };
        let url = reqwest::Url::parse(endpoint).map_err(|_| invalid("invalid management URL"))?;
        if !matches!(url.scheme(), "http" | "https")
            || url.host().is_none()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.path() != "/"
            || url.query().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid("management address must be an HTTP(S) origin"));
        }
        let deployment = labels
            .get(DEPLOYMENT)
            .filter(|name| valid_label(name))
            .ok_or_else(|| invalid("management deployment label is required"))?;
        let adx_worker_id = labels.get(ADX_WORKER).cloned();
        if adx_worker_id.as_ref().is_some_and(|id| !valid_label(id)) {
            return Err(invalid("invalid ADX worker id"));
        }
        let mut connectors: Vec<_> = capabilities
            .iter()
            .filter_map(|cap| cap.strip_prefix("connector:"))
            .filter(|name| valid_label(name))
            .map(str::to_owned)
            .collect();
        connectors.sort_unstable();
        connectors.dedup();
        Ok(Some(Self {
            management_rest_addr: url.origin().ascii_serialization(),
            deployment: deployment.clone(),
            adx_worker_id,
            connectors,
        }))
    }
}

impl ManagementConfig {
    pub(crate) fn registration_labels(&self) -> Result<HashMap<String, String>> {
        let mut labels = HashMap::new();
        if let Some(endpoint) = &self.advertised_endpoint {
            if self.token_file.is_none() {
                return Err(invalid(
                    "an advertised management address requires a token file",
                ));
            }
            labels.insert(ENDPOINT.to_owned(), endpoint.clone());
        }
        if let Some(deployment) = &self.deployment {
            labels.insert(DEPLOYMENT.to_owned(), deployment.clone());
        }
        if let Some(worker_id) = &self.adx_worker_id {
            labels.insert(ADX_WORKER.to_owned(), worker_id.clone());
        }
        NodeAdvertisement::from_registration(&labels, &[])?;
        Ok(labels)
    }
}

fn valid_label(label: &str) -> bool {
    !label.is_empty()
        && label.len() <= 128
        && label
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || b"._-".contains(&byte))
}

fn invalid(message: &str) -> Error {
    Error::coded(afs_error::CONFIG_INVALID, message)
}
