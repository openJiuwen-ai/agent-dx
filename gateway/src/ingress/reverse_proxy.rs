use bytes::Bytes;
use http::{header, HeaderMap, HeaderValue, Request, Response, StatusCode, Uri};
use http_body_util::{combinators::UnsyncBoxBody, BodyExt, Full};
use hyper::body::Incoming;
use hyper_util::client::legacy::{connect::HttpConnector, Client};
use hyper_util::rt::{TokioExecutor, TokioIo, TokioTimer};
use serde::Deserialize;
use std::collections::HashSet;
use std::error::Error;
use std::net::SocketAddr;
use std::time::Duration;

type BoxError = Box<dyn Error + Send + Sync>;
type ProxyBody = UnsyncBoxBody<Bytes, BoxError>;

/// Retained idle connections are bounded separately from active requests.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ReverseProxyConfig {
    pub max_idle_connections: usize,
    pub idle_timeout: Duration,
    pub connect_timeout: Duration,
}

impl Default for ReverseProxyConfig {
    fn default() -> Self {
        Self {
            max_idle_connections: 512,
            idle_timeout: Duration::from_secs(30),
            connect_timeout: Duration::from_secs(5),
        }
    }
}

/// A deployment-owned route; request headers never choose an upstream address.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProxyRoute {
    pub name: String,
    pub path_prefix: String,
    pub upstream: String,
    #[serde(default)]
    pub strip_prefix: bool,
    #[serde(default)]
    pub host: Option<String>,
}

impl ProxyRoute {
    pub fn matches<B>(&self, request: &Request<B>) -> bool {
        let path = request.uri().path();
        let path_matches = self.path_prefix == "/"
            || path
                .strip_prefix(&self.path_prefix)
                .is_some_and(|suffix| suffix.is_empty() || suffix.starts_with('/'));
        let host_matches = self.host.as_ref().is_none_or(|host| {
            request
                .headers()
                .get(header::HOST)
                .and_then(|value| value.to_str().ok())
                .and_then(|value| value.parse::<http::uri::Authority>().ok())
                .is_some_and(|authority| authority.host().eq_ignore_ascii_case(host))
        });
        path_matches && host_matches
    }

    fn path_and_query(&self, uri: &Uri) -> String {
        let path = if self.strip_prefix && self.path_prefix != "/" {
            uri.path()
                .strip_prefix(&self.path_prefix)
                .unwrap_or(uri.path())
        } else {
            uri.path()
        };
        let path = if path.is_empty() { "/" } else { path };
        match uri.query() {
            Some(query) => format!("{path}?{query}"),
            None => path.to_owned(),
        }
    }
}

pub fn parse_proxy_routes(input: &str) -> Result<Vec<ProxyRoute>, String> {
    let mut routes: Vec<ProxyRoute> = serde_json::from_str(input)
        .map_err(|error| format!("invalid proxy routes JSON: {error}"))?;
    let mut names = HashSet::new();
    let mut matches = HashSet::new();
    for route in &mut routes {
        if route.name.trim().is_empty() || !names.insert(route.name.clone()) {
            return Err("proxy route names must be non-empty and unique".into());
        }
        if !route.path_prefix.starts_with('/')
            || route.path_prefix.contains(['?', '#', '%'])
            || route.path_prefix.contains("//")
            || route
                .path_prefix
                .parse::<http::uri::PathAndQuery>()
                .is_err()
            || route
                .path_prefix
                .split('/')
                .any(|segment| matches!(segment, "." | ".."))
        {
            return Err(format!(
                "invalid path_prefix for proxy route {}",
                route.name
            ));
        }
        if route.path_prefix != "/" {
            route.path_prefix = route.path_prefix.trim_end_matches('/').to_owned();
        }
        if let Some(host) = &mut route.host {
            let authority: http::uri::Authority = host
                .parse()
                .map_err(|_| format!("invalid host for proxy route {}", route.name))?;
            if authority.port().is_some() || host.contains('@') {
                return Err("proxy route host must not contain a port or credentials".into());
            }
            *host = host.to_ascii_lowercase();
        }
        if !matches.insert((route.host.clone(), route.path_prefix.clone())) {
            return Err("duplicate proxy route host/path_prefix".into());
        }
        let upstream: Uri = route
            .upstream
            .parse()
            .map_err(|_| format!("invalid upstream for proxy route {}", route.name))?;
        if upstream.scheme_str() != Some("http")
            || upstream.authority().is_none()
            || upstream
                .authority()
                .is_some_and(|authority| authority.as_str().contains('@'))
            || upstream
                .path_and_query()
                .is_some_and(|path| path.as_str() != "/")
        {
            return Err("proxy upstream must be an HTTP origin such as http://grafana:3000".into());
        }
    }
    Ok(routes)
}

#[derive(Clone)]
pub struct ReverseProxy {
    client: Client<HttpConnector, Incoming>,
}

impl ReverseProxy {
    pub fn new(config: ReverseProxyConfig) -> Self {
        let mut connector = HttpConnector::new();
        connector.set_nodelay(true);
        connector.set_connect_timeout(Some(config.connect_timeout));
        Self {
            client: Client::builder(TokioExecutor::new())
                .pool_timer(TokioTimer::new())
                .pool_idle_timeout(config.idle_timeout)
                .pool_max_idle_per_host(config.max_idle_connections)
                .retry_canceled_requests(false)
                .build(connector),
        }
    }

    pub async fn proxy(
        &self,
        mut request: Request<Incoming>,
        route: &ProxyRoute,
        peer: SocketAddr,
    ) -> Response<ProxyBody> {
        let upgrade = requested_upgrade(request.headers());
        let downstream_upgrade = upgrade.as_ref().map(|_| hyper::upgrade::on(&mut request));
        let original_host = request.headers().get(header::HOST).cloned();
        remove_hop_headers(request.headers_mut());
        request.headers_mut().remove("forwarded");
        request.headers_mut().remove("x-forwarded-prefix");
        request
            .headers_mut()
            .insert("x-forwarded-proto", HeaderValue::from_static("https"));
        let peer_ip =
            HeaderValue::from_str(&peer.ip().to_string()).expect("IP is a valid header value");
        request
            .headers_mut()
            .insert("x-forwarded-for", peer_ip.clone());
        request.headers_mut().insert("x-real-ip", peer_ip);
        request.headers_mut().remove("x-forwarded-host");
        if let Some(host) = original_host {
            request.headers_mut().insert("x-forwarded-host", host);
        }
        if route.strip_prefix && route.path_prefix != "/" {
            if let Ok(prefix) = HeaderValue::from_str(&route.path_prefix) {
                request.headers_mut().insert("x-forwarded-prefix", prefix);
            }
        }
        if let Some(value) = &upgrade {
            request
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
            request.headers_mut().insert(header::UPGRADE, value.clone());
        }
        let upstream: Uri = match route.upstream.parse() {
            Ok(uri) => uri,
            Err(error) => return bad_gateway(error),
        };
        let uri = Uri::builder()
            .scheme("http")
            .authority(
                upstream
                    .authority()
                    .map_or("", |authority| authority.as_str()),
            )
            .path_and_query(route.path_and_query(request.uri()))
            .build();
        *request.uri_mut() = match uri {
            Ok(uri) => uri,
            Err(error) => return bad_gateway(error),
        };
        let mut response = match self.client.request(request).await {
            Ok(response) => response,
            Err(error) => return bad_gateway(error),
        };
        let response_upgrade = requested_upgrade(response.headers());
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            let Some(downstream_upgrade) = downstream_upgrade else {
                return bad_gateway("unsolicited upstream protocol upgrade");
            };
            if response_upgrade != upgrade {
                return bad_gateway("upstream protocol upgrade does not match request");
            }
            let upstream_upgrade = hyper::upgrade::on(&mut response);
            tokio::spawn(async move {
                if let (Ok(downstream), Ok(upstream)) =
                    tokio::join!(downstream_upgrade, upstream_upgrade)
                {
                    let (mut down_read, mut down_write) =
                        tokio::io::split(TokioIo::new(downstream));
                    let (mut up_read, mut up_write) = tokio::io::split(TokioIo::new(upstream));
                    // WebSocket EOF/error ends the upgraded session in both directions.
                    tokio::select! {
                        _ = tokio::io::copy(&mut down_read, &mut up_write) => {},
                        _ = tokio::io::copy(&mut up_read, &mut down_write) => {},
                    }
                }
            });
        }
        remove_hop_headers(response.headers_mut());
        if response.status() == StatusCode::SWITCHING_PROTOCOLS {
            response
                .headers_mut()
                .insert(header::CONNECTION, HeaderValue::from_static("upgrade"));
            if let Some(upgrade) = response_upgrade {
                response.headers_mut().insert(header::UPGRADE, upgrade);
            }
        }
        response.map(|body| {
            body.map_err(|error| -> BoxError { Box::new(error) })
                .boxed_unsync()
        })
    }
}

fn requested_upgrade(headers: &HeaderMap) -> Option<HeaderValue> {
    let requested = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .any(|token| token.trim().eq_ignore_ascii_case("upgrade"));
    requested
        .then(|| headers.get(header::UPGRADE).cloned())
        .flatten()
}

fn remove_hop_headers(headers: &mut HeaderMap) {
    let nominated: Vec<_> = headers
        .get_all(header::CONNECTION)
        .iter()
        .filter_map(|value| value.to_str().ok())
        .flat_map(|value| value.split(','))
        .filter_map(|name| name.trim().parse::<header::HeaderName>().ok())
        .collect();
    for name in nominated {
        headers.remove(name);
    }
    for name in [
        "connection",
        "keep-alive",
        "proxy-connection",
        "proxy-authenticate",
        "proxy-authorization",
        "te",
        "trailer",
        "transfer-encoding",
        "upgrade",
    ] {
        headers.remove(name);
    }
}

fn bad_gateway(error: impl std::fmt::Display) -> Response<ProxyBody> {
    Response::builder()
        .status(StatusCode::BAD_GATEWAY)
        .body(
            Full::new(Bytes::from(error.to_string()))
                .map_err(|never| -> BoxError { match never {} })
                .boxed_unsync(),
        )
        .unwrap()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn routes_match_segment_boundaries_and_optional_host() {
        let routes = parse_proxy_routes(
            r#"[{"name":"dashboard","path_prefix":"/grafana/","upstream":"http://grafana:3000","host":"Public.Example","strip_prefix":true}]"#,
        ).unwrap();
        let route = &routes[0];
        assert_eq!(route.path_prefix, "/grafana");
        for (path, host, expected) in [
            ("/grafana", "public.example", true),
            ("/grafana/api/live?x=1", "PUBLIC.EXAMPLE:8443", true),
            ("/grafana2", "public.example", false),
            ("/grafana/api/live", "another.example", false),
        ] {
            let request = Request::builder()
                .uri(path)
                .header(header::HOST, host)
                .body(())
                .unwrap();
            assert_eq!(route.matches(&request), expected, "{path} {host}");
        }
        assert_eq!(
            route.path_and_query(&"/grafana?q=a%2Fb".parse().unwrap()),
            "/?q=a%2Fb"
        );
        assert_eq!(
            route.path_and_query(&"/grafana/api/a%2Fb?x=1&x=2".parse().unwrap()),
            "/api/a%2Fb?x=1&x=2"
        );
        let mut preserved = route.clone();
        preserved.strip_prefix = false;
        assert_eq!(
            preserved.path_and_query(&"/grafana/api/live".parse().unwrap()),
            "/grafana/api/live"
        );
    }

    #[test]
    fn routes_reject_ambiguous_or_invalid_configuration() {
        for input in [
            r#"[{"name":"a","path_prefix":"/app","upstream":"https://app:3000"}]"#,
            r#"[{"name":"a","path_prefix":"/app","upstream":"http://app:3000/base"}]"#,
            r#"[{"name":"a","path_prefix":"/app","upstream":"http://user:password@app:3000"}]"#,
            r#"[{"name":"a","path_prefix":"/app","upstream":"http://app","host":"example.com:443"}]"#,
            r#"[{"name":"a","path_prefix":"/app?x=1","upstream":"http://app"}]"#,
            r#"[{"name":"a","path_prefix":"/app space","upstream":"http://app"}]"#,
            r#"[{"name":"a","path_prefix":"/app/../other","upstream":"http://app"}]"#,
            r#"[{"name":"a","path_prefix":"/app","upstream":"http://app","strip_prefx":true}]"#,
            r#"[{"name":"a","path_prefix":"/app","upstream":"http://a"},{"name":"b","path_prefix":"/app/","upstream":"http://b"}]"#,
            r#"[{"name":"a","path_prefix":"/app","upstream":"http://a"},{"name":"a","path_prefix":"/other","upstream":"http://b"}]"#,
        ] {
            assert!(parse_proxy_routes(input).is_err(), "accepted {input}");
        }
        assert!(parse_proxy_routes("[]").unwrap().is_empty());
    }

    #[test]
    fn response_hop_headers_are_removed_without_collapsing_cookies() {
        let mut headers = HeaderMap::new();
        headers.append(
            header::CONNECTION,
            HeaderValue::from_static("keep-alive, x-private"),
        );
        headers.append(header::CONNECTION, HeaderValue::from_static("x-second"));
        headers.insert("x-private", HeaderValue::from_static("secret"));
        headers.insert("x-second", HeaderValue::from_static("secret"));
        headers.insert(
            header::TRANSFER_ENCODING,
            HeaderValue::from_static("chunked"),
        );
        headers.append(
            header::SET_COOKIE,
            HeaderValue::from_static("a=1; Path=/app/"),
        );
        headers.append(
            header::SET_COOKIE,
            HeaderValue::from_static("b=2; Path=/app/"),
        );
        remove_hop_headers(&mut headers);
        assert_eq!(headers.len(), 2);
        assert_eq!(headers.get_all(header::SET_COOKIE).iter().count(), 2);
    }
}
