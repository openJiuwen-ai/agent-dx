//! E2B data-plane target parsing and authorization.
//!
//! This module deliberately operates on the published, already-authoritative
//! route. It does not discover an Environment or accept a client-supplied
//! network target.
use http::{header, HeaderMap, Request, StatusCode};
use percent_encoding::percent_decode_str;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

pub const ENVD_PORT: u16 = 49_983;
pub const EXECD_PORT: u16 = 50_090;
const ACCESS_TOKEN: &str = "x-access-token";
const TRAFFIC_ACCESS_TOKEN: &str = "e2b-traffic-access-token";

/// E2B management endpoints handled by the API Server. This is deliberately
/// path-boundary aware so an application path such as `/snapshots-old` is not
/// captured by the control-plane proxy.
pub fn is_management_path(path: &str) -> bool {
    matches!(path, "/v2/sandboxes" | "/snapshots" | "/api-keys")
        || path
            .strip_prefix("/api-keys/")
            .is_some_and(|suffix| !suffix.is_empty())
        || path
            .strip_prefix("/v2/sandboxes/")
            .is_some_and(|suffix| !suffix.is_empty())
        || path
            .strip_prefix("/sandboxes/")
            .is_some_and(|suffix| !suffix.is_empty())
        || path
            .strip_prefix("/snapshots/")
            .is_some_and(|suffix| !suffix.is_empty())
        || path
            .strip_prefix("/templates/")
            .is_some_and(|suffix| !suffix.is_empty())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct E2bTarget {
    pub sandbox_id: String,
    pub port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum E2bTargetError {
    InvalidTarget,
    ConflictingTarget,
}

#[derive(Debug, Clone, Deserialize, PartialEq, Eq, Default)]
pub struct E2bRouteConfig {
    pub envd_token_sha256: String,
    pub traffic_token_sha256: String,
    pub allow_public_traffic: bool,
    pub envd_ready: bool,
    pub expires_at_unix_ms: u64,
    pub control_revision: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum E2bAuthorizationError {
    Unavailable,
    Forbidden,
    Unauthorized,
}

impl E2bAuthorizationError {
    pub fn status(self) -> StatusCode {
        match self {
            Self::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
            Self::Forbidden => StatusCode::FORBIDDEN,
            Self::Unauthorized => StatusCode::UNAUTHORIZED,
        }
    }
}

/// Parses the E2B host form and/or E2B forwarding headers. If both select a
/// target, they must select precisely the same stable sandbox and port.
pub fn parse_target<B>(
    request: &Request<B>,
    domain: &str,
) -> Result<Option<E2bTarget>, E2bTargetError> {
    let from_host = request_host(request)
        .map(|host| parse_host(host, domain))
        .transpose()?
        .flatten();
    let from_headers = parse_headers(request.headers())?;
    match (from_host, from_headers) {
        (Some(host), Some(headers)) if host != headers => Err(E2bTargetError::ConflictingTarget),
        (Some(target), _) | (_, Some(target)) => Ok(Some(target)),
        (None, None) => Ok(None),
    }
}

fn request_host<B>(request: &Request<B>) -> Option<&str> {
    request
        .headers()
        .get(header::HOST)
        .and_then(|value| value.to_str().ok())
        .or_else(|| request.uri().authority().map(http::uri::Authority::as_str))
}

fn parse_host(host: &str, domain: &str) -> Result<Option<E2bTarget>, E2bTargetError> {
    if domain.trim().is_empty() {
        return Ok(None);
    }
    let authority = host
        .parse::<http::uri::Authority>()
        .map_err(|_| E2bTargetError::InvalidTarget)?;
    let hostname = authority.host().trim_end_matches('.').to_ascii_lowercase();
    let suffix = format!(".{}", domain.trim_end_matches('.').to_ascii_lowercase());
    let Some(label) = hostname.strip_suffix(&suffix) else {
        return Ok(None);
    };
    let Some((port, sandbox_id)) = label.split_once('-') else {
        return Ok(None);
    };
    if !port.bytes().all(|byte| byte.is_ascii_digit()) {
        return Ok(None);
    }
    target(sandbox_id, port)
        .map(Some)
        .ok_or(E2bTargetError::InvalidTarget)
}

fn parse_headers(headers: &HeaderMap) -> Result<Option<E2bTarget>, E2bTargetError> {
    let id = unique_header(headers, "e2b-sandbox-id")?;
    let port = unique_header(headers, "e2b-sandbox-port")?;
    match (id, port) {
        (None, None) => Ok(None),
        (Some(id), Some(port)) => target(id, port)
            .map(Some)
            .ok_or(E2bTargetError::InvalidTarget),
        _ => Err(E2bTargetError::InvalidTarget),
    }
}

fn unique_header<'a>(
    headers: &'a HeaderMap,
    name: &str,
) -> Result<Option<&'a str>, E2bTargetError> {
    let mut values = headers.get_all(name).iter();
    let Some(value) = values.next() else {
        return Ok(None);
    };
    if values.next().is_some() {
        return Err(E2bTargetError::InvalidTarget);
    }
    value
        .to_str()
        .map(Some)
        .map_err(|_| E2bTargetError::InvalidTarget)
}

fn target(sandbox_id: &str, port: &str) -> Option<E2bTarget> {
    let sandbox_id = sandbox_id.trim();
    let port = port.parse::<u16>().ok().filter(|port| *port != 0)?;
    if !valid_sandbox_id(sandbox_id) {
        return None;
    }
    Some(E2bTarget {
        sandbox_id: sandbox_id.to_owned(),
        port,
    })
}

fn valid_sandbox_id(value: &str) -> bool {
    value.len() <= 63
        && !value.is_empty()
        && value
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || byte == b'-')
        && !value.starts_with('-')
        && !value.ends_with('-')
}

impl E2bRouteConfig {
    pub fn authorize<B>(
        &self,
        request: &Request<B>,
        target: &E2bTarget,
    ) -> Result<(), E2bAuthorizationError> {
        if !self.envd_ready
            || self.expires_at_unix_ms == 0
            || self.expires_at_unix_ms <= now_unix_ms()
        {
            return Err(E2bAuthorizationError::Unavailable);
        }
        match target.port {
            EXECD_PORT => Err(E2bAuthorizationError::Forbidden),
            ENVD_PORT => {
                if blocked_envd_path(request.uri().path()) {
                    return Err(E2bAuthorizationError::Forbidden);
                }
                verify_header(request.headers(), ACCESS_TOKEN, &self.envd_token_sha256)
            }
            port if reserved_port(port) => Err(E2bAuthorizationError::Forbidden),
            _ if self.allow_public_traffic => Ok(()),
            _ => verify_header(
                request.headers(),
                TRAFFIC_ACCESS_TOKEN,
                &self.traffic_token_sha256,
            ),
        }
    }
}

fn verify_header(
    headers: &HeaderMap,
    name: &str,
    expected_sha256: &str,
) -> Result<(), E2bAuthorizationError> {
    let value = unique_header(headers, name).map_err(|_| E2bAuthorizationError::Unauthorized)?;
    let Some(value) = value.filter(|value| !value.is_empty()) else {
        return Err(E2bAuthorizationError::Unauthorized);
    };
    if expected_sha256.len() != 64 || !expected_sha256.bytes().all(|byte| byte.is_ascii_hexdigit())
    {
        return Err(E2bAuthorizationError::Unavailable);
    }
    let actual = hex_sha256(value.as_bytes());
    if constant_time_eq(actual.as_bytes(), expected_sha256.as_bytes()) {
        Ok(())
    } else {
        Err(E2bAuthorizationError::Unauthorized)
    }
}

fn reserved_port(port: u16) -> bool {
    // Platform control ports must never be exposed as E2B application ports.
    matches!(port, EXECD_PORT | 8765 | 8766)
}

fn blocked_envd_path(path: &str) -> bool {
    let Ok(decoded) = percent_decode_str(path).decode_utf8() else {
        return true;
    };
    let normalized = format!(
        "/{}",
        decoded
            .split('/')
            .filter(|segment| !segment.is_empty())
            .collect::<Vec<_>>()
            .join("/")
    );
    [
        "/init",
        "/freeze",
        "/unfreeze",
        "/collapse",
        "/fsfreeze",
        "/fsthaw",
    ]
    .iter()
    .any(|blocked| normalized == *blocked || normalized.starts_with(&format!("{blocked}/")))
}

fn hex_sha256(value: &[u8]) -> String {
    let digest = Sha256::digest(value);
    digest.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    if left.len() != right.len() {
        return false;
    }
    left.iter()
        .zip(right)
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        })
        == 0
}

fn now_unix_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(host: Option<&str>, path: &str) -> Request<()> {
        let mut request = Request::builder().uri(path).body(()).unwrap();
        if let Some(host) = host {
            request
                .headers_mut()
                .insert(header::HOST, host.parse().unwrap());
        }
        request
    }

    fn config(public: bool) -> E2bRouteConfig {
        E2bRouteConfig {
            envd_token_sha256: hex_sha256(b"envd"),
            traffic_token_sha256: hex_sha256(b"traffic"),
            allow_public_traffic: public,
            envd_ready: true,
            expires_at_unix_ms: u64::MAX,
            control_revision: 1,
        }
    }

    #[test]
    fn parses_e2b_host_and_header_and_rejects_conflicts() {
        let mut request = request(
            Some("49983-sandbox-a.example.test"),
            "/process.Process/Start",
        );
        request
            .headers_mut()
            .insert("e2b-sandbox-id", "sandbox-a".parse().unwrap());
        request
            .headers_mut()
            .insert("e2b-sandbox-port", "49983".parse().unwrap());
        assert_eq!(
            parse_target(&request, "example.test").unwrap(),
            Some(E2bTarget {
                sandbox_id: "sandbox-a".into(),
                port: ENVD_PORT
            })
        );
        request
            .headers_mut()
            .insert("e2b-sandbox-port", "8080".parse().unwrap());
        assert_eq!(
            parse_target(&request, "example.test"),
            Err(E2bTargetError::ConflictingTarget)
        );
    }

    #[test]
    fn header_target_requires_both_fields() {
        let mut request = request(None, "/");
        request
            .headers_mut()
            .insert("e2b-sandbox-id", "sandbox-a".parse().unwrap());
        assert_eq!(
            parse_target(&request, "example.test"),
            Err(E2bTargetError::InvalidTarget)
        );
    }

    #[test]
    fn management_paths_are_boundary_aware_and_data_target_still_parses() {
        for path in [
            "/api-keys",
            "/api-keys/key-id",
            "/v2/sandboxes",
            "/v2/sandboxes/sandbox-a/connect",
            "/sandboxes/sandbox-a",
            "/snapshots",
            "/snapshots/snapshot-a",
            "/templates/snapshot-a",
        ] {
            assert!(is_management_path(path), "{path}");
        }
        for path in [
            "/v2/sandbox",
            "/snapshots-old",
            "/api-keys-old",
            "/templates",
            "/files",
        ] {
            assert!(!is_management_path(path), "{path}");
        }
        let mut data_request = request(None, "/v2/sandboxes");
        data_request
            .headers_mut()
            .insert("e2b-sandbox-id", "sandbox-a".parse().unwrap());
        data_request
            .headers_mut()
            .insert("e2b-sandbox-port", "49983".parse().unwrap());
        assert_eq!(
            parse_target(&data_request, "example.test").unwrap(),
            Some(E2bTarget {
                sandbox_id: "sandbox-a".into(),
                port: ENVD_PORT,
            })
        );
    }

    #[test]
    fn envd_requires_access_token_and_blocks_encoded_internal_paths() {
        let mut envd_request = request(Some("49983-sandbox-a.example.test"), "/files");
        let target = parse_target(&envd_request, "example.test")
            .unwrap()
            .unwrap();
        assert_eq!(
            config(true).authorize(&envd_request, &target),
            Err(E2bAuthorizationError::Unauthorized)
        );
        envd_request
            .headers_mut()
            .insert(ACCESS_TOKEN, "envd".parse().unwrap());
        assert_eq!(config(true).authorize(&envd_request, &target), Ok(()));
        let mut blocked = request(Some("49983-sandbox-a.example.test"), "/%69nit");
        blocked
            .headers_mut()
            .insert(ACCESS_TOKEN, "envd".parse().unwrap());
        assert_eq!(
            config(true).authorize(&blocked, &target),
            Err(E2bAuthorizationError::Forbidden)
        );
    }

    #[test]
    fn private_application_requires_traffic_token_and_reserved_ports_are_denied() {
        let target = E2bTarget {
            sandbox_id: "sandbox-a".into(),
            port: 8080,
        };
        let mut request = request(None, "/");
        assert_eq!(
            config(false).authorize(&request, &target),
            Err(E2bAuthorizationError::Unauthorized)
        );
        request
            .headers_mut()
            .insert(TRAFFIC_ACCESS_TOKEN, "traffic".parse().unwrap());
        assert_eq!(config(false).authorize(&request, &target), Ok(()));
        let reserved = E2bTarget {
            port: EXECD_PORT,
            ..target
        };
        assert_eq!(
            config(true).authorize(&request, &reserved),
            Err(E2bAuthorizationError::Forbidden)
        );
    }

    #[test]
    fn envd_readiness_and_token_domains_are_fail_closed() {
        let envd_target = E2bTarget {
            sandbox_id: "sandbox-a".into(),
            port: ENVD_PORT,
        };
        let application_target = E2bTarget {
            sandbox_id: "sandbox-a".into(),
            port: 8080,
        };
        let mut envd_request = request(None, "/files");
        envd_request
            .headers_mut()
            .insert(TRAFFIC_ACCESS_TOKEN, "traffic".parse().unwrap());
        assert_eq!(
            config(false).authorize(&envd_request, &envd_target),
            Err(E2bAuthorizationError::Unauthorized)
        );
        let mut application_request = request(None, "/");
        application_request
            .headers_mut()
            .insert(ACCESS_TOKEN, "envd".parse().unwrap());
        assert_eq!(
            config(false).authorize(&application_request, &application_target),
            Err(E2bAuthorizationError::Unauthorized)
        );
        let mut unavailable = config(true);
        unavailable.envd_ready = false;
        assert_eq!(
            unavailable.authorize(&envd_request, &envd_target),
            Err(E2bAuthorizationError::Unavailable)
        );
    }
}
